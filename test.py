import psycopg2

conn = psycopg2.connect(
    dbname="postgres",
    user="your_user",
    password="정확한_비번",
    host="aws-0-ap-northeast-2.pooler.supabase.com",
    port=6543
)
print("✅ 연결 성공")
conn.close()