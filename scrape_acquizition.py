"""Extrait toutes les annonces de https://www.acquizition.biz/fr/businessesForSale.

Navigateur : Bright Data Scraping Browser si BROWSER_WS est défini, sinon Chromium local.
Sorties : acquizition_annonces.csv / .xlsx, acquizition_raw.json, erreurs.log,
puis synchronisation Supabase si DATABASE_URL est défini.

Installation :
    pip install -r requirements.txt && playwright install chromium
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import sys
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from dotenv import load_dotenv
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError, sync_playwright

import supabase_sync

load_dotenv()
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

START_URL = "https://www.acquizition.biz/fr/businessesForSale"
SEARCH_URL = "https://acquizition.biz/listing/search?number_of_listing={size}&page={page}"
DETAIL_URL = "https://acquizition.biz/listing/unauth/"
REGIONS_URL = "https://www.acquizition.biz/locales/fr/regions.json"
INDUSTRIES_URL = "https://www.acquizition.biz/locales/fr/industries.json"
LISTING_URL = "https://www.acquizition.biz/fr/fl/"

PAGE_SIZE = 100
BATCH_SIZE = 50
CONCURRENCY = 4
MAX_ATTEMPTS = 3
RETRY_DELAYS = [1, 2, 4]
BATCH_PAUSE = 0.5
NAVIGATION_TIMEOUT_MS = 120_000

OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "."))
CSV_PATH = OUTPUT_DIR / "acquizition_annonces.csv"
XLSX_PATH = OUTPUT_DIR / "acquizition_annonces.xlsx"
RAW_PATH = OUTPUT_DIR / "acquizition_raw.json"
ERRORS_PATH = OUTPUT_DIR / "erreurs.log"

PERIODE_RESULTATS = "Plus récente déclaration fiscale"
PRIX_OUVERT = "Ouvert aux offres"
SELLING_REASONS = {
    "Retirement": "Retraite",
    "Pursuing another business": "Nouvelle occasion d'affaires",
    "Relocating": "Déménagement",
    "Lost interest": "Perte d'intérêt",
    "Personal reasons": "Raison personnelle",
    "Other": "Autre",
}
FINANCING = {
    True: "Je suis ouvert à discuter des options de financement",
    False: "Je n'offrirai pas de financement",
}

COLUMNS = [
    # Métadonnées
    "id", "url", "premium", "en_vedette", "verifiee", "date_publication",
    # Information générale
    "titre", "prix", "prix_affiche", "revenu", "baiia", "benefice_net",
    "employes", "fondation",
    "emplacement_principal", "emplacement_secondaire", "emplacement_tertiaire",
    "secteur_principal", "secteur_secondaire", "secteur_tertiaire",
    "description_entreprise", "avantages_competitifs", "clientele", "concurrence",
    "brevets", "permis", "site_web",
    # Portrait financier
    "periode_resultats", "marge_baiia_pct", "marge_nette_pct",
    "immobilier_et_actifs_cles", "location_equipement_immobilier",
    # Information liée à la vente
    "raison_vente_code", "raison_vente", "options_financement", "soutien_apres_vente",
]

SPOT_CHECKS = {
    "6ac8c35e49149eb0d91dcf79": {
        "prix": 1200000, "revenu": 900000, "baiia": 300000, "marge_baiia_pct": 33,
        "employes": 3, "fondation": 2022,
        "emplacement_principal": "Québec, Montérégie",
        "secteur_principal": "Construction, Bâtiments",
        "raison_vente": "Raison personnelle",
        "site_web": "https://isolationtoit.ca/",
    },
    "6aa98da349149eb0d91dcf51": {
        "revenu": 830000, "baiia": 70000, "benefice_net": 64000,
        "marge_baiia_pct": 8, "marge_nette_pct": 8,
        "raison_vente": "Nouvelle occasion d'affaires",
        "options_financement": "Je n'offrirai pas de financement",
    },
}

FETCH_JS = """
async ({url, method, body}) => {
    const r = await fetch(url, {
        method,
        headers: method === 'POST' ? {'Content-Type': 'application/json'} : {},
        body,
    });
    return {status: r.status, text: await r.text()};
}
"""

DETAILS_JS = """
async ({ids, baseUrl, concurrency, attempts, delays}) => {
    const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
    const results = new Array(ids.length);
    let next = 0;

    async function fetchOne(id) {
        let status = 0;
        let reason = '';
        for (let i = 0; i < attempts; i++) {
            try {
                const r = await fetch(baseUrl + id);
                status = r.status;
                const text = await r.text();
                if (status === 200) {
                    try {
                        const data = JSON.parse(text);
                        if (data && typeof data === 'object') return {id, status, text};
                        reason = 'JSON vide';
                    } catch (e) {
                        reason = 'JSON invalide';
                    }
                } else {
                    reason = 'HTTP ' + status;
                }
            } catch (e) {
                reason = String(e);
            }
            if (i < attempts - 1) await sleep(delays[i] * 1000);
        }
        return {id, status, failed: true, reason};
    }

    async function worker() {
        while (next < ids.length) {
            const i = next++;
            results[i] = await fetchOne(ids[i]);
        }
    }

    await Promise.all(Array.from({length: Math.min(concurrency, ids.length)}, worker));
    return results;
}
"""

log = logging.getLogger("acquizition")


# --------------------------------------------------------------------------- API


def api_request(page: Page, url: str, method: str = "GET", body: str | None = None,
                allow_empty: bool = False):
    """Appel fetch() dans la page avec retry. Renvoie le JSON, ou None si corps vide autorisé."""
    status = 0
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            res = page.evaluate(FETCH_JS, {"url": url, "method": method, "body": body})
            status, text = res["status"], res["text"].strip()
            if allow_empty and not text and status in (200, 204):
                return None
            if status == 200:
                return json.loads(text)
            reason = f"HTTP {status}"
        except json.JSONDecodeError:
            reason = "JSON invalide"
        except Exception as exc:
            reason = str(exc).splitlines()[0]
        log.warning("Tentative %d/%d échouée (%s) : %s %s", attempt, MAX_ATTEMPTS, reason, method, url)
        if attempt < MAX_ATTEMPTS:
            time.sleep(RETRY_DELAYS[attempt - 1])
    raise RuntimeError(f"Échec après {MAX_ATTEMPTS} tentatives : {method} {url} (dernier status {status})")


def load_dictionaries(page: Page) -> dict[str, dict[str, str]]:
    regions = api_request(page, REGIONS_URL)["regions"]
    industries = api_request(page, INDUSTRIES_URL)["industries"]
    log.info("Dictionnaires : %d régions, %d secteurs", len(regions), len(industries))
    return {"regions": regions, "industries": industries}


def get_all_ids(page: Page) -> tuple[list[str], int]:
    ids: list[str] = []
    seen: set[str] = set()
    total: int | None = None
    page_num = 0
    while True:
        url = SEARCH_URL.format(size=PAGE_SIZE, page=page_num)
        data = api_request(page, url, method="POST", body="{}", allow_empty=True)
        if not data or not data.get("tile_list"):
            break
        total = data.get("total", total)
        for tile in data["tile_list"]:
            if tile["_id"] not in seen:
                seen.add(tile["_id"])
                ids.append(tile["_id"])
        n_pages = math.ceil(total / PAGE_SIZE) if total else "?"
        log.info("Page de liste %d/%s : %d ids uniques (total API %s)", page_num + 1, n_pages, len(ids), total)
        if total is not None and len(ids) >= total:
            break
        page_num += 1

    if total is None:
        raise RuntimeError("L'API de recherche n'a renvoyé aucune annonce.")
    if len(ids) != total:
        raise RuntimeError(f"Pagination incomplète : {len(ids)} ids uniques collectés, l'API annonce total={total}.")
    return ids, total


def fetch_details(page: Page, ids: list[str]) -> tuple[list[dict], list[dict]]:
    records: list[dict] = []
    failed: list[dict] = []
    for start in range(0, len(ids), BATCH_SIZE):
        batch = ids[start:start + BATCH_SIZE]
        results = page.evaluate(DETAILS_JS, {
            "ids": batch, "baseUrl": DETAIL_URL, "concurrency": CONCURRENCY,
            "attempts": MAX_ATTEMPTS, "delays": RETRY_DELAYS,
        })
        for res in results:
            if res.get("failed"):
                failed.append(res)
            else:
                records.append(json.loads(res["text"]))
        log.info("Détails %d/%d (%d en échec)", start + len(batch), len(ids), len(failed))
        if start + BATCH_SIZE < len(ids):
            time.sleep(BATCH_PAUSE)
    return records, failed


# --------------------------------------------------------------------- Transform


def html_to_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        value = "\n".join(str(v) for v in value if v is not None)
    text = BeautifulSoup(str(value), "html.parser").get_text("\n")
    lines = (re.sub(r"\s+", " ", line).strip() for line in text.splitlines())
    return "\n".join(line for line in lines if line)


def translate_codes(codes, mapping: dict[str, str]) -> str:
    if not codes:
        return ""
    if not isinstance(codes, list):
        codes = [codes]
    return ", ".join(mapping.get(str(code), str(code)) for code in codes if code is not None)


def number(value):
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def percent(part, revenue):
    if part is None or revenue is None or revenue <= 0:
        return None
    return round(part / revenue * 100)


def to_iso_utc(timestamp) -> str:
    if timestamp is None:
        return ""
    if timestamp > 1e11:
        timestamp /= 1000
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat(timespec="seconds")


def transform(record: dict, dicts: dict[str, dict[str, str]]) -> dict:
    basic = record.get("basic_info") or {}
    financial = record.get("financial_details") or {}
    additional = record.get("additional_info") or {}
    selling = record.get("selling_details") or {}
    regions, industries = dicts["regions"], dicts["industries"]

    title = basic.get("listing_title")
    if isinstance(title, list):
        title = title[0] if title else ""
    prix = number(basic.get("asking_price"))
    revenu = number(financial.get("revenue"))
    baiia = number(financial.get("ebitda"))
    benefice_net = number(financial.get("net_income"))
    reason_code = selling.get("selling_reason")

    return {
        "id": record["_id"],
        "url": LISTING_URL + record["_id"],
        "premium": bool(record.get("priority_exposure")),
        "en_vedette": bool(record.get("featured")),
        "verifiee": bool(record.get("is_verified")),
        "date_publication": to_iso_utc(record.get("published_date")),
        "titre": (title or "").strip(),
        "prix": prix,
        "prix_affiche": PRIX_OUVERT if prix is None else prix,
        "revenu": revenu,
        "baiia": baiia,
        "benefice_net": benefice_net,
        "employes": number(additional.get("number_employees")),
        "fondation": number(additional.get("date_founded")),
        "emplacement_principal": translate_codes(basic.get("region_1"), regions),
        "emplacement_secondaire": translate_codes(basic.get("region_2"), regions),
        "emplacement_tertiaire": translate_codes(basic.get("region_3"), regions),
        "secteur_principal": translate_codes(basic.get("industry_1"), industries),
        "secteur_secondaire": translate_codes(basic.get("industry_2"), industries),
        "secteur_tertiaire": translate_codes(basic.get("industry_3"), industries),
        "description_entreprise": html_to_text(basic.get("about_company")),
        "avantages_competitifs": html_to_text(basic.get("competitive_advantages")),
        "clientele": html_to_text(basic.get("customer_base")),
        "concurrence": html_to_text(basic.get("competitors")),
        "brevets": html_to_text(basic.get("patent")),
        "permis": html_to_text(basic.get("license")),
        "site_web": (additional.get("website") or "").strip(),
        "periode_resultats": PERIODE_RESULTATS,
        "marge_baiia_pct": percent(baiia, revenu),
        "marge_nette_pct": percent(benefice_net, revenu),
        "immobilier_et_actifs_cles": html_to_text(financial.get("assets")),
        "location_equipement_immobilier": html_to_text(financial.get("leases")),
        "raison_vente_code": reason_code or "",
        "raison_vente": SELLING_REASONS.get(reason_code, reason_code) if reason_code else "",
        "options_financement": FINANCING.get(selling.get("financing_option"), ""),
        "soutien_apres_vente": html_to_text(selling.get("offered_support")),
    }


# -------------------------------------------------------------------- Validation


def is_filled(value) -> bool:
    return value is not None and value != ""


def validate(df: pd.DataFrame, total: int, failed: list[dict]) -> list[dict]:
    results = []

    def check(name: str, status: str, detail: str) -> None:
        results.append({"name": name, "status": status, "detail": detail})
        print(f"[{status}] {name} — {detail}")

    print("\n===== VALIDATIONS =====")
    n_rows, n_unique = len(df), df["id"].nunique()
    check("complétude", "PASS" if n_rows == total and n_unique == n_rows else "FAIL",
          f"lignes={n_rows}, ids uniques={n_unique}, total API={total}")
    check("échecs", "PASS" if not failed else "FAIL", f"{len(failed)} id(s) en échec dans {ERRORS_PATH.name}")

    rows = df.set_index("id")
    for listing_id, expected in SPOT_CHECKS.items():
        name = f"contrôle {listing_id}"
        if listing_id not in rows.index:
            check(name, "SKIP", "annonce absente (retirée du site ?)")
            continue
        row = rows.loc[listing_id]
        diffs = [f"{col}: attendu {want!r}, obtenu {row[col]!r}"
                 for col, want in expected.items() if row[col] != want]
        check(name, "FAIL" if diffs else "PASS", "; ".join(diffs) or f"{len(expected)} champs conformes")

    print("\n===== TAUX DE REMPLISSAGE =====")
    fill_rates = {col: round(df[col].map(is_filled).mean() * 100, 1) if n_rows else 0.0 for col in COLUMNS}
    for col, rate in fill_rates.items():
        print(f"{col:32s} {rate:6.1f} %")
    results.append({"name": "taux_remplissage", "status": "INFO", "detail": fill_rates})
    return results


# -------------------------------------------------------------------------- Main


def open_browser(pw):
    ws = os.getenv("BROWSER_WS")
    if ws:
        log.info("Connexion au Scraping Browser Bright Data")
        return pw.chromium.connect_over_cdp(ws, timeout=NAVIGATION_TIMEOUT_MS)
    log.info("BROWSER_WS absent : Chromium local headless")
    return pw.chromium.launch(headless=True)


def scrape() -> tuple[dict, list[str], int, list[dict], list[dict]]:
    with sync_playwright() as pw:
        browser = open_browser(pw)
        try:
            page = browser.new_page()
            page.set_default_timeout(NAVIGATION_TIMEOUT_MS)
            try:
                page.goto(START_URL, wait_until="networkidle", timeout=NAVIGATION_TIMEOUT_MS)
            except PlaywrightTimeoutError:
                log.warning("networkidle non atteint, on continue (l'origine du site est chargée)")
            dicts = load_dictionaries(page)
            ids, total = get_all_ids(page)
            records, failed = fetch_details(page, ids)
        finally:
            browser.close()
    return dicts, ids, total, records, failed


def write_outputs(df: pd.DataFrame, records: list[dict], failed: list[dict]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    RAW_PATH.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    ERRORS_PATH.write_text(
        "".join(f"{f['id']}\tstatus={f.get('status')}\t{f.get('reason', '')}\n" for f in failed),
        encoding="utf-8",
    )
    df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig", sep=",")
    xlsx_df = df.map(lambda v: ILLEGAL_CHARACTERS_RE.sub("", v) if isinstance(v, str) else v)
    with pd.ExcelWriter(XLSX_PATH, engine="openpyxl") as writer:
        xlsx_df.to_excel(writer, index=False, sheet_name="Annonces")
        writer.sheets["Annonces"].freeze_panes = "A2"
    log.info("Fichiers écrits : %s, %s, %s, %s", CSV_PATH, XLSX_PATH, RAW_PATH, ERRORS_PATH)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        stream=sys.stdout)
    started_at = datetime.now(timezone.utc)
    database_url = os.getenv("DATABASE_URL")

    try:
        dicts, ids, total, records, failed = scrape()
    except Exception as exc:
        log.exception("Échec du scraping")
        if database_url:
            supabase_sync.record_failed_run(database_url, started_at, str(exc))
        return 1

    df = pd.DataFrame([transform(r, dicts) for r in records], columns=COLUMNS, dtype=object)
    write_outputs(df, records, failed)
    results = validate(df, total, failed)
    complete = all(r["status"] == "PASS" for r in results[:2])

    if database_url:
        supabase_sync.sync(database_url, df, records, results, total, len(failed),
                           started_at, complete)
    else:
        log.info("DATABASE_URL absent : pas de synchronisation Supabase")

    return 0 if complete else 1


if __name__ == "__main__":
    sys.exit(main())
