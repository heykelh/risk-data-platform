"""
Ingestion EBA EU-wide Stress Test → MinIO (couche Bronze)
Source : https://www.eba.europa.eu/risk-analysis-and-data/eu-wide-stress-testing
On utilise le dataset 2023 (dernier disponible en CSV public)
"""
import os
import io
import requests
import pandas as pd
from datetime import date
from dotenv import load_dotenv
from common.r2_client import get_r2_client, upload_parquet

load_dotenv()

# URL publique EBA stress test 2023 — expositions par banque/pays/secteur
EBA_DATASETS = {
    "credit_risk": (
        "https://www.eba.europa.eu/sites/default/files/2023-07/"
        "f0c7c9aa-f272-430b-a141-4efe6f88f5d3.csv"
    ),
}

# Si l'URL EBA est inaccessible, on génère des données synthétiques réalistes
FALLBACK_SYNTHETIC = True


def generate_synthetic_eba(n_rows: int = 50_000) -> pd.DataFrame:
    """
    Génère un dataset synthétique EBA réaliste.
    Utilisé si l'URL publique EBA est inaccessible.
    Structure identique au vrai fichier EBA.
    """
    import numpy as np
    rng = np.random.default_rng(42)

    banks = [f"BANK_{i:03d}" for i in range(1, 51)]
    countries = ["FR", "DE", "IT", "ES", "NL", "BE", "PT", "AT", "GR", "PL"]
    sectors = ["BANK", "CORP", "RETAIL", "SOV", "REAL_ESTATE", "OTHER"]
    report_dates = pd.date_range("2020-01-01", "2023-12-31", freq="QS")

    rows = []
    for _ in range(n_rows):
        bank = rng.choice(banks)
        country = rng.choice(countries)
        sector = rng.choice(sectors)
        report_date = rng.choice(report_dates)
        exposure = round(rng.lognormal(mean=12, sigma=2), 2)
        rwa = round(exposure * rng.uniform(0.2, 0.8), 2)
        pd_val = round(rng.beta(1, 20), 4)
        lgd = round(rng.uniform(0.1, 0.6), 4)
        rows.append({
            "bank_code": bank,
            "counterparty_code": f"CPT_{rng.integers(1000, 9999)}",
            "country": country,
            "sector": sector,
            "exposure_amount": exposure,
            "rwa_amount": rwa,
            "pd": pd_val,
            "lgd": lgd,
            "report_date": pd.Timestamp(report_date).strftime("%Y-%m-%d"),
            "currency": "EUR",
            "data_source": "SYNTHETIC_EBA",
        })

    return pd.DataFrame(rows)


def fetch_eba_csv(url: str) -> pd.DataFrame:
    """Télécharge et parse le CSV EBA."""
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text), sep=";", low_memory=False)
    return df


def normalize_eba(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise les colonnes pour correspondre au schéma cible."""
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]
    df["_ingested_at"] = pd.Timestamp.utcnow().isoformat()
    df["data_source"] = "EBA_STRESS_TEST_2023"
    return df


def main():
    today = date.today().isoformat()
    client = get_r2_client()

    print(f"Ingestion EBA — {today}")
    print("-" * 40)

    for dataset_name, url in EBA_DATASETS.items():
        try:
            print(f"  → Téléchargement EBA {dataset_name}...")
            df = fetch_eba_csv(url)
            df = normalize_eba(df)
            print(f"  ✓ {len(df):,} lignes récupérées depuis EBA")
        except Exception as e:
            if FALLBACK_SYNTHETIC:
                print(f"  ⚠ URL EBA inaccessible ({e})")
                print(f"  → Génération données synthétiques réalistes...")
                df = generate_synthetic_eba(n_rows=50_000)
                print(f"  ✓ {len(df):,} lignes synthétiques générées")
            else:
                print(f"  ✗ ERREUR : {e}")
                raise

        # Sérialisation Parquet (colonne report_date pour partitionnement)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        key = f"bronze/eba/ingestion_date={today}/{dataset_name}.parquet"
        upload_parquet(client, buffer.read(), key)
        print(f"  ✓ Uploadé → {key}")
        print(f"  ✓ Taille : {buffer.tell() / 1024:.1f} KB")

    print("-" * 40)
    print("Ingestion EBA terminée.")


if __name__ == "__main__":
    main()