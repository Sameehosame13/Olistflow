import csv
import json
import time
from datetime import datetime
from pathlib import Path
from confluent_kafka import Producer

BROKER = "localhost:9092"
SCRIPT_DIR = Path(__file__).resolve().parent
CSV_DIR = SCRIPT_DIR.parent / "dataset"

ORDERS_FILE = "orders.csv"
TIMESTAMP_COLUMN = "order_purchase_timestamp"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S" 
SCALE_FACTOR = 86400*10

KEYED_FILES = {"orders.csv", "order_items.csv", "order_payments.csv"}
KEY_COLUMN = "order_id"

producer = Producer({"bootstrap.servers": BROKER})


def get_key(csv_path: Path, row: dict) -> bytes | None:
    if csv_path.name in KEYED_FILES:
        return row[KEY_COLUMN].encode("utf-8")
    return None


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for {msg.topic()}: {err}")

MAX_RETRIES = 10

def produce_row(topic: str, row: dict, key: bytes | None):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            producer.produce(
                topic,
                key=key,
                value=json.dumps(row).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)
            return
        except BufferError:
            wait = 0.1 * attempt
            print(f"Buffer full, retrying in {wait:.1f}s (attempt {attempt}/{MAX_RETRIES})")
            producer.poll(wait)

    raise RuntimeError(f"Failed to produce to '{topic}' after {MAX_RETRIES} retries")


def produce_batch(csv_path: Path):
    topic = csv_path.stem
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            produce_row(topic, row, get_key(csv_path, row))
    producer.flush()
    print(f"Done (batch): {csv_path.name} -> topic '{topic}'")


def produce_orders_realtime(csv_path: Path):
    topic = csv_path.stem
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    rows.sort(key=lambda r: datetime.strptime(r[TIMESTAMP_COLUMN], TIMESTAMP_FORMAT))

    prev_ts = None
    for row in rows:
        current_ts = datetime.strptime(row[TIMESTAMP_COLUMN], TIMESTAMP_FORMAT)
        if prev_ts is not None:
            real_gap_seconds = (current_ts - prev_ts).total_seconds()
            time.sleep(max(real_gap_seconds, 0) / SCALE_FACTOR)
        prev_ts = current_ts

        produce_row(topic, row, get_key(csv_path, row))

    producer.flush()
    print(f"Done (real-time replay): {csv_path.name} -> topic '{topic}'")


def main():
    if not CSV_DIR.exists():
        print(f"Error: Dataset directory not found at {CSV_DIR}")
        return

    csv_files = sorted(CSV_DIR.glob("*.csv"))
    if len(csv_files) != 6:
        print(f"Warning: expected 6 CSVs in {CSV_DIR}, found {len(csv_files)}")

    for csv_file in csv_files:
        if csv_file.name == ORDERS_FILE:
            produce_orders_realtime(csv_file)
        else:
            produce_batch(csv_file)


if __name__ == "__main__":
    main()