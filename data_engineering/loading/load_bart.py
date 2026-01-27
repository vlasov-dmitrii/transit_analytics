import os
import glob
import pandas as pd
import psycopg2
from datetime import datetime

RAW_DIR = "/app/data/raw"

DB_CONFIG = {
    "host": "bart_postgres",
    "port": 5432,
    "dbname": "bart_dw",
    "user": "bart",
    "password": "bart"
}


def get_ingestion_ts_from_filename(filename: str) -> datetime:
    ts = os.path.splitext(filename)[0].split("_")[-1]
    ts_str = datetime.today().strftime("%Y%m%d") + ts
    return datetime.strptime(ts_str, "%Y%m%d%H%M%S")


def connect_db():
    return psycopg2.connect(**DB_CONFIG)


def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bart_trip_updates (
            ingestion_ts TIMESTAMPTZ,
            trip_id TEXT,
            route_id TEXT,
            stop_id TEXT,
            stop_sequence INT,
            scheduled_time TIMESTAMPTZ,
            actual_time TIMESTAMPTZ,
            delay_minutes FLOAT,
            vehicle_id TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS bart_service_alerts (
            ingestion_ts TIMESTAMPTZ,
            alert_id TEXT,
            route_id TEXT,
            description TEXT,
            severity TEXT
        );
        """)
        conn.commit()


def load_trip_updates(conn):
    files = glob.glob(f"{RAW_DIR}/bart_trip_updates_*.parquet")

    for file in files:
        ingestion_ts = get_ingestion_ts_from_filename(os.path.basename(file))
        df = pd.read_parquet(file)
        df["ingestion_ts"] = ingestion_ts

        records = df[
            [
                "ingestion_ts",
                "trip_id",
                "route_id",
                "stop_id",
                "stop_sequence",
                "scheduled_time",
                "actual_time",
                "delay_minutes",
                "vehicle_id",
            ]
        ].values.tolist()

        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO bart_trip_updates VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                records,
            )
        conn.commit()
        print(f"Loaded {len(records)} trip updates from {file}")


def load_service_alerts(conn):
    files = glob.glob(f"{RAW_DIR}/bart_service_alerts_*.parquet")

    for file in files:
        ingestion_ts = get_ingestion_ts_from_filename(os.path.basename(file))
        df = pd.read_parquet(file)
        df["ingestion_ts"] = ingestion_ts

        records = df[
            [
                "ingestion_ts",
                "alert_id",
                "route_id",
                "description",
                "severity",
            ]
        ].values.tolist()

        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO bart_service_alerts VALUES (%s,%s,%s,%s,%s)
                """,
                records,
            )
        conn.commit()
        print(f"Loaded {len(records)} service alerts from {file}")


def main():
    conn = connect_db()
    create_tables(conn)
    load_trip_updates(conn)
    load_service_alerts(conn)
    conn.close()
    print("Loading completed successfully")


if __name__ == "__main__":
    main()
