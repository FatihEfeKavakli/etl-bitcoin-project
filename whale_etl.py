from datetime import datetime, timedelta, timezone
import json
import io

from airflow import DAG
from airflow.operators.python import PythonOperator

from minio import Minio
import psycopg2

RAW_BUCKET = "crypto-raw"
CLEAN_BUCKET = "crypto-clean"

MINIO_CLIENT = Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False,
)


def _connect_postgres():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="crypto",
        user="crypto_user",
        password="crypto_pass",
    )
    conn.autocommit = True
    return conn


def process_recent_files(**context):
    """RAW bucket'tan son dosyaları okuyup whale trade'leri filtreler."""

    now = datetime.now(timezone.utc)

    # Örnek: sadece bugünün dosyalarına bakalım
    prefix = f"bitcoin/trades/{now.year:04d}/{now.month:02d}/{now.day:02d}/"

    objects = list(MINIO_CLIENT.list_objects(RAW_BUCKET, prefix=prefix, recursive=True))

    if not objects:
        print("Hiç ham dosya bulunamadı.")
        return

    whale_trades = []

    for obj in objects:
        print(f"Dosya okunuyor: {obj.object_name}")
        resp = MINIO_CLIENT.get_object(RAW_BUCKET, obj.object_name)
        try:
            raw_bytes = resp.read()
        finally:
            resp.close()
            resp.release_conn()

        try:
            trades = json.loads(raw_bytes.decode("utf-8"))
        except json.JSONDecodeError:
            print(f"JSON parse hatası: {obj.object_name}")
            continue

        for trade in trades:
            # Beklenen: {"price": "...", "qty": "...", "isBuyerMaker": bool, "time": ...}
            try:
                price = float(trade["price"])
                qty = float(trade["qty"])
                value_usd = price * qty
            except (KeyError, ValueError, TypeError):
                continue

            if value_usd >= 50000.0:
                side = "SELL" if trade.get("isBuyerMaker") else "BUY"
                whale_trades.append(
                    {
                        "symbol": trade.get("symbol", "BTCUSDT"),
                        "price": price,
                        "quantity": qty,
                        "value_usd": value_usd,
                        "side": side,
                        "trade_time": datetime.fromtimestamp(trade["time"] / 1000.0, tz=timezone.utc).isoformat(),
                    }
                )

    if not whale_trades:
        print("Whale trade bulunamadı.")
        return

    # ---- MinIO'ya temiz veriyi yaz ----
    clean_key = f"bitcoin/whales/{now.year:04d}/{now.month:02d}/{now.day:02d}/{now.hour:02d}/whales_{int(now.timestamp())}.json"
    clean_bytes = json.dumps(whale_trades, ensure_ascii=False).encode("utf-8")

    MINIO_CLIENT.make_bucket(CLEAN_BUCKET) if not MINIO_CLIENT.bucket_exists(CLEAN_BUCKET) else None

    MINIO_CLIENT.put_object(
        bucket_name=CLEAN_BUCKET,
        object_name=clean_key,
        data=io.BytesIO(clean_bytes),      # ÖNEMLİ: bytes değil, BytesIO veriyoruz
        length=len(clean_bytes),
        content_type="application/json",
    )

    print(f"{len(whale_trades)} whale trade {CLEAN_BUCKET}/{clean_key} içine yazıldı.")

    # ---- PostgreSQL'e yaz ----
    conn = _connect_postgres()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS whale_trades (
            id SERIAL PRIMARY KEY,
            symbol TEXT,
            price DOUBLE PRECISION,
            quantity DOUBLE PRECISION,
            value_usd DOUBLE PRECISION,
            side TEXT,
            trade_time TIMESTAMPTZ
        );
        """
    )

    for wt in whale_trades:
        cur.execute(
            """
            INSERT INTO whale_trades (symbol, price, quantity, value_usd, side, trade_time)
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            (
                wt["symbol"],
                wt["price"],
                wt["quantity"],
                wt["value_usd"],
                wt["side"],
                wt["trade_time"],
            ),
        )

    cur.close()
    conn.close()

    print(f"{len(whale_trades)} whale trade PostgreSQL whale_trades tablosuna yazıldı.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="whale_etl",
    default_args=default_args,
    description="Binance stream verisinden whale trade ETL",
    schedule_interval="*/2 * * * *",  # 2 dakikada bir
    start_date=datetime(2025, 12, 4, tzinfo=timezone.utc),
    catchup=False,
    tags=["crypto", "whale", "minio", "postgres"],
) as dag:

    process_recent_raw_files = PythonOperator(
        task_id="process_recent_raw_files",
        python_callable=process_recent_files,
        provide_context=True,
    )

