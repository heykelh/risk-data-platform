"""
Ingestion FRED API → Cloudflare R2 (couche Bronze)
Séries : taux 10Y, 2Y, Fed Funds, inflation, chômage
"""
import os
import json
import boto3
import requests
from datetime import date
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

FRED_KEY    = os.environ["FRED_API_KEY"]
ACCOUNT_ID  = os.environ["R2_ACCOUNT_ID"]
ACCESS_KEY  = os.environ["R2_ACCESS_KEY_ID"]
SECRET_KEY  = os.environ["R2_SECRET_ACCESS_KEY"]
BUCKET      = os.environ["R2_BUCKET_NAME"]
ENDPOINT    = os.environ["R2_ENDPOINT_URL"]

SERIES = {
    "DGS10":    "US Treasury 10Y",
    "DGS2":     "US Treasury 2Y",
    "DFF":      "Fed Funds Rate",
    "CPIAUCSL": "CPI inflation",
    "UNRATE":   "Unemployment rate",
}


def get_r2_client():
    """Client boto3 configuré pour Cloudflare R2."""
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def fetch_series(series_id: str) -> dict:
    """Appelle l'API FRED et retourne les observations."""
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_KEY,
        "file_type": "json",
        "observation_start": "2000-01-01",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def upload_to_r2(client, payload: bytes, key: str) -> None:
    """Upload un fichier dans le bucket R2."""
    client.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=payload,
        ContentType="application/json",
    )


def main():
    today = date.today().isoformat()
    client = get_r2_client()

    print(f"Ingestion FRED — {today}")
    print("-" * 40)

    for series_id, label in SERIES.items():
        try:
            data = fetch_series(series_id)
            payload = json.dumps(data, indent=2).encode("utf-8")
            key = f"bronze/fred/ingestion_date={today}/{series_id}.json"
            upload_to_r2(client, payload, key)
            nb = len(data.get("observations", []))
            print(f"  ✓ {series_id:12s} ({label}) — {nb} obs → {key}")
        except Exception as e:
            print(f"  ✗ {series_id} — ERREUR : {e}")
            raise

    print("-" * 40)
    print("Ingestion FRED terminée.")


if __name__ == "__main__":
    main()