import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
import os

import websockets
from minio import Minio

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
RAW_BUCKET = os.getenv("RAW_BUCKET", "crypto-raw")


def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def ensure_bucket(client, bucket_name: str):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


async def collect_and_flush(flush_interval_sec=10):
    client = get_minio_client()
    ensure_bucket(client, RAW_BUCKET)

    while True:
        try:
            async with websockets.connect(BINANCE_WS_URL) as ws:
                print("Binance BTCUSDT stream'e bağlandı.")
                buffer = []
                last_flush = datetime.now(timezone.utc)

                async for raw in ws:
                    msg = json.loads(raw)
                    buffer.append(msg)

                    now = datetime.now(timezone.utc)
                    delta = (now - last_flush).total_seconds()

                    if delta >= flush_interval_sec and buffer:
                        ts_str = now.strftime("%Y-%m-%dT%H-%M-%S")
                        object_path = now.strftime(
                            f"bitcoin/trades/%Y/%m/%d/%H/trades_{ts_str}.json"
                        )

                        tmp_path = Path(f"/tmp/{ts_str}.json")
                        tmp_path.parent.mkdir(parents=True, exist_ok=True)

                        with tmp_path.open("w", encoding="utf-8") as f:
                            json.dump(buffer, f)

                        client.fput_object(
                            RAW_BUCKET,
                            object_path,
                            str(tmp_path),
                            content_type="application/json",
                        )

                        print(
                            f"{len(buffer)} trade MinIO'ya yazıldı: "
                            f"{RAW_BUCKET}/{object_path}"
                        )

                        buffer = []
                        last_flush = now

        except Exception as e:
            print("WebSocket hatası, yeniden bağlanılacak:", e)
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(collect_and_flush(flush_interval_sec=10))

