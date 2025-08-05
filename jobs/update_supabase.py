import pandas as pd
import psycopg2
import yfinance as yf
import os
from dotenv import load_dotenv

load_dotenv()

TICKERS = ["AAPL", "MSFT", "TSLA"]
PERIOD = "5d"
INTERVAL = "1h"

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
        try:
            # 강제 변환 (Series 타입 방지)
            timestamp = row["Datetime"]
            if hasattr(timestamp, "to_pydatetime"):
                timestamp = timestamp.to_pydatetime()

            open_ = float(row["Open"])
            high = float(row["High"])
            low = float(row["Low"])
            close = float(row["Close"])
            adj_close = float(row["Adj Close"]) if "Adj Close" in row else close
            volume = int(row["Volume"])
            ticker = str(row["ticker"])

            cursor.execute("""
                INSERT INTO stock_data (
                    timestamp, open, high, low, close, adj_close, volume, ticker
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (timestamp, ticker) DO NOTHING
            """, (
                timestamp, open_, high, low, close, adj_close, volume, ticker
            ))

        except Exception as e:
            print("❌ Error inserting row:", e)
            print(row)
            continue

    conn.commit()
    cursor.close()
    conn.close()

def main():
    all_data = pd.concat([fetch_data(t) for t in TICKERS], ignore_index=True)
    upload_data(all_data)

if __name__ == "__main__":
    main()
