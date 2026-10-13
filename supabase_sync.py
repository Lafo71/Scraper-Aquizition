"""Synchronisation des annonces Acquizition vers Supabase (Postgres)."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import pandas as pd
import psycopg2
from psycopg2.extras import Json, execute_values

log = logging.getLogger("acquizition")

TABLE = "acquizition_annonces"
RUNS_TABLE = "acquizition_runs"

# Colonne -> type Postgres, dans l'ordre du CSV.
COLUMN_TYPES = {
    "id": "text primary key",
    "url": "text not null",
    "premium": "boolean",
    "en_vedette": "boolean",
    "verifiee": "boolean",
    "date_publication": "timestamptz",
    "titre": "text",
    "prix": "numeric",
    "prix_affiche": "text",
    "revenu": "numeric",
    "baiia": "numeric",
    "benefice_net": "numeric",
    "employes": "integer",
    "fondation": "integer",
    "emplacement_principal": "text",
    "emplacement_secondaire": "text",
    "emplacement_tertiaire": "text",
    "secteur_principal": "text",
    "secteur_secondaire": "text",
    "secteur_tertiaire": "text",
    "description_entreprise": "text",
    "avantages_competitifs": "text",
    "clientele": "text",
    "concurrence": "text",
    "brevets": "text",
    "permis": "text",
    "site_web": "text",
    "periode_resultats": "text",
    "marge_baiia_pct": "integer",
    "marge_nette_pct": "integer",
    "immobilier_et_actifs_cles": "text",
    "location_equipement_immobilier": "text",
    "raison_vente_code": "text",
    "raison_vente": "text",
    "options_financement": "text",
    "soutien_apres_vente": "text",
}
DATA_COLUMNS = list(COLUMN_TYPES)

SCHEMA_SQL = f"""
create table if not exists {TABLE} (
    {", ".join(f"{col} {typ}" for col, typ in COLUMN_TYPES.items())},
    raw jsonb,
    actif boolean not null default true,
    premiere_vue timestamptz not null default now(),
    derniere_vue timestamptz not null default now()
);
create index if not exists {TABLE}_actif_idx on {TABLE} (actif);

create table if not exists {RUNS_TABLE} (
    id bigserial primary key,
    debut timestamptz not null,
    fin timestamptz not null default now(),
    statut text not null,
    total_api integer,
    nb_annonces integer,
    nb_echecs integer,
    nb_nouvelles integer,
    nb_retirees integer,
    validations jsonb,
    erreur text
);
"""

# Coordonnées personnelles du vendeur : présentes dans l'API, exclues de la base.
PRIVATE_SELLING_FIELDS = ("first_name_contact", "last_name_contact", "email_contact", "telephone_contact")


def _connect(database_url: str):
    return psycopg2.connect(database_url, sslmode="require", connect_timeout=20)


def _db_value(col: str, value):
    if value is None or value == "":
        return None
    if col == "prix_affiche":
        return str(value)
    return value


def _public_raw(record: dict) -> dict:
    selling = {k: v for k, v in (record.get("selling_details") or {}).items()
               if k not in PRIVATE_SELLING_FIELDS}
    return {**record, "selling_details": selling}


def ensure_schema(cur) -> None:
    cur.execute(SCHEMA_SQL)


def sync(database_url: str, df: pd.DataFrame, records: list[dict], validations: list[dict],
         total: int, nb_failed: int, started_at: datetime, complete: bool) -> None:
    raw_by_id = {r["_id"]: _public_raw(r) for r in records}
    now = datetime.now(timezone.utc)
    rows = [
        tuple(_db_value(col, row[col]) for col in DATA_COLUMNS) + (Json(raw_by_id.get(row["id"])), now)
        for row in df.to_dict("records")
    ]
    insert_cols = DATA_COLUMNS + ["raw", "derniere_vue"]
    updates = ", ".join(f"{col} = excluded.{col}" for col in insert_cols if col != "id")

    with _connect(database_url) as conn, conn.cursor() as cur:
        ensure_schema(cur)
        inserted = execute_values(
            cur,
            f"insert into {TABLE} ({', '.join(insert_cols)}) values %s "
            f"on conflict (id) do update set {updates}, actif = true "
            f"returning (xmax = 0)",
            rows,
            page_size=200,
            fetch=True,
        ) if rows else []
        nb_new = sum(1 for (is_new,) in inserted if is_new)

        # Désactivation seulement si le run est complet, sinon on retirerait des annonces encore en ligne.
        nb_removed = 0
        if complete:
            cur.execute(f"update {TABLE} set actif = false where actif and derniere_vue < %s", (now,))
            nb_removed = cur.rowcount

        cur.execute(
            f"insert into {RUNS_TABLE} (debut, fin, statut, total_api, nb_annonces, nb_echecs, "
            f"nb_nouvelles, nb_retirees, validations) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (started_at, datetime.now(timezone.utc), "ok" if complete else "incomplet", total,
             len(rows), nb_failed, nb_new, nb_removed, Json(validations)),
        )
    conn.close()
    log.info("Supabase : %d annonces upsert (%d nouvelles, %d retirées)", len(rows), nb_new, nb_removed)


def record_failed_run(database_url: str, started_at: datetime, error: str) -> None:
    try:
        with _connect(database_url) as conn, conn.cursor() as cur:
            ensure_schema(cur)
            cur.execute(f"insert into {RUNS_TABLE} (debut, fin, statut, erreur) values (%s, %s, 'erreur', %s)",
                        (started_at, datetime.now(timezone.utc), error[:4000]))
        conn.close()
    except Exception:
        log.exception("Impossible d'enregistrer l'échec du run dans Supabase")
