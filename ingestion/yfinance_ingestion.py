"""
Ingestion Yahoo Finance → MinIO (couche Bronze)
Données : prix et volatilité des actions bancaires européennes
Proxy pour la valorisation des collatéraux
"""
import os
import io
import json
import yfinance as yf
import pandas as pd
from datetime import date, timedelta
from dotenv import load_dotenv
from common.r2_client import get_r2_client, upload_parquet

load_dotenv()

# Actions bancaires européennes + indices de référence
TICKERS = {
    "BNP.PA":  "BNP Paribas",
    "GLE.PA":  "Société Générale",
    "ACA.PA":  "Crédit Agricole",
    "DBK.DE":  "Deutsche Bank",
    "HSBA.L":  "HSBC",
    "SAN.MC":  "Santander",
    "UCG.MI":  "UniCredit",
    "^STOXX50E": "Euro Stoxx 50",
    "^VIX":    "VIX Volatility",
}

START_DATE = "2018-01-01"


def fetch_prices(ticker: str) -> pd.DataFrame:
    """Télécharge l'historique de prix via yfinance."""
    t = yf.Ticker(ticker)
    df = t.history(start=START_DATE, auto_adjust=True)
    df = df.reset_index()
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    df["ticker"] = ticker
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    # Calcul volatilité 30 jours glissants
    df["returns"] = df["close"].pct_change()
    df["volatility_30d"] = df["returns"].rolling(30).std() * (252 ** 0.5)
    df["_ingested_at"] = pd.Timestamp.utcnow().isoformat()
    return df[["date", "ticker", "open", "high", "low", "close",
               "volume", "returns", "volatility_30d", "_ingested_at"]]


def main():
    today = date.today().isoformat()
    client = get_r2_client()

    print(f"Ingestion Yahoo Finance — {today}")
    print("-" * 40)

    all_frames = []
    for ticker, label in TICKERS.items():
        try:
            df = fetch_prices(ticker)
            all_frames.append(df)
            print(f"  ✓ {ticker:12s} ({label}) — {len(df):,} jours")
        except Exception as e:
            print(f"  ✗ {ticker} — ERREUR : {e}")

    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        buffer = io.BytesIO()
        combined.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        key = f"bronze/yfinance/ingestion_date={today}/prices.parquet"
        upload_parquet(client, buffer.read(), key)
        print(f"\n  ✓ {len(combined):,} lignes totales → {key}")

    print("-" * 40)
    print("Ingestion Yahoo Finance terminée.")


if __name__ == "__main__":
    main()