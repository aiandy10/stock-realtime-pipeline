import time
import random
import datetime as dt
import psycopg2
ipv4_host = socket.getaddrinfo("db.kgutffsckhqxaouwjlhn.supabase.co", None, socket.AF_INET)[0][4][0]
# Symbols to simulate
symbols = ["RELIANCE", "TCS", "INFY", "HDFCBANK"]

# Supabase connection details
conn = psycopg2.connect(
    host="db.kgutffsckhqxaouwjlhn.supabase.co",
    port=5432,
    dbname="postgres",
    user="postgres",
    password="Pwdmui@1007",   # if your password contains '@', escape it as %40 in URIs, but here psycopg2 handles it fine
    sslmode="require"
)
cur = conn.cursor()

print("✅ Connected to Supabase Postgres")

# Continuous tick generation
while True:
    tick = {
        "symbol": random.choice(symbols),
        "ts": dt.datetime.now(dt.UTC).isoformat(),
        "price": round(random.uniform(2000, 3000), 2),
        "volume": random.randint(100, 1000)
    }

    cur.execute(
        "INSERT INTO public.ticks (symbol, ts, price, volume) VALUES (%s, %s, %s, %s)",
        (tick["symbol"], tick["ts"], tick["price"], tick["volume"])
    )
    conn.commit()

    print("Inserted:", tick)
    time.sleep(2)