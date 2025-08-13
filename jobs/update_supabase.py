import pandas as pd
import psycopg2
import yfinance as yf
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import warnings

# 모든 경고 무시
warnings.filterwarnings("ignore")
load_dotenv()

import os
from sqlalchemy import create_engine, text

host = os.environ["SUPABASE_HOST"]
port = os.environ.get("SUPABASE_PORT", 5432)
db   = os.environ["SUPABASE_DB"]
user = os.environ["SUPABASE_USER"]
pw   = os.environ["SUPABASE_PASSWORD"]

url = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}?sslmode=require"
engine = create_engine(url, connect_args={"sslmode":"require"}, pool_pre_ping=True)

tickers = ["AAPL", "MSFT", "TSLA"]
period = "10y"
interval = "1d"

df = yf.download(tickers, period=period, interval=interval, group_by="ticker", auto_adjust=True)
df = df.stack(level=0)
df = df.reset_index()
df.columns = df.columns.str.lower()

with engine.begin() as conn:
    df.to_sql("stock_data", con=conn, if_exists="replace", index=False)
    print('DB saved')