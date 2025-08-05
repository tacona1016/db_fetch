# jobs/update_supabase.py

import yfinance as yf
import pandas as pd
import psycopg2
import os
from datetime import datetime

TICKERS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]
PERIOD = "5d"
INTERVAL = "1h"

# Supabase 연결 정보 (GitHub Actions에서 환경변수로 전달됨)
DB_NAME = os.environ["SUPABASE_DB"]
DB_USER = os.environ["SUPABASE_USER"]
DB_PASSWORD = os.environ["SUPABASE_PASSWORD"]
DB_HOST = os.environ["SUPABASE_HOST"]
DB_PORT = os.environ.get("SUPABASE_PORT", 5432)

def fetch_data(ticker):
    df = yf.download(ticker, period=PERIOD, interval=INTERVAL)
    df.reset_index(inplace=True)
    df["ticker"] = ticker
    return df

def upload_data(df):
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()

    for _, row in df.iterrows():
        timestamp = row["Datetime"]
        if isinstance(timestamp, pd.Timestamp):
            timestamp = timestamp.to_pydatetime()

        cursor.execute("""
            INSERT INTO stock_data (
                timestamp, open, high, low, close, adj_close, volume, ticker
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp, ticker) DO NOTHING
        """, (
            timestamp,
            float(row["Open"]),
            float(row["High"]),
            float(row["Low"]),
            float(row["Close"]),
            float(row["Adj Close"]) if "Adj Close" in row else float(row["Close"]),
            int(row["Volume"]),
            str(row["ticker"])
        ))

    conn.commit()
    cursor.close()
    conn.close()

def main():
    all_data = []
    for ticker in TICKERS:
        df = fetch_data(ticker)
        all_data.append(df)

    combined_df = pd.concat(all_data, ignore_index=True)
    upload_data(combined_df)

if __name__ == "__main__":
    main()
