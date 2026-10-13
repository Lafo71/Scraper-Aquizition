# Le navigateur tourne chez Bright Data (BROWSER_WS) : pas besoin d'installer Chromium dans l'image.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY scrape_acquizition.py supabase_sync.py ./

CMD ["python", "scrape_acquizition.py"]
