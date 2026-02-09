import os
import glob
import pandas as pd
import psycopg2
from datetime import datetime

RAW_DIR = os.getenv('RAW_DIR', '/app/data/raw')

DB_CONFIG = {
    "host": os.getenv('DB_HOST', 'bart_postgres'),
    "port": int(os.getenv('DB_PORT', 5432)),
    "dbname": os.getenv('DB_NAME', 'bart_dw'),
    "user": os.getenv('DB_USER', 'bart'),
    "password": os.getenv('DB_PASSWORD', 'bart')
}

def connect_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✓ Connected to PostgreSQL")
        return conn
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        raise

def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS bart_trip_updates CASCADE;")
        cur.execute("DROP TABLE IF EXISTS bart_service_alerts CASCADE;")

        cur.execute("""
        CREATE TABLE bart_trip_updates (
            ingestion_ts TIMESTAMPTZ NOT NULL,
            trip_id TEXT,
            route_id TEXT,
            stop_id TEXT,
            stop_sequence INTEGER DEFAULT 0,
            arrival_delay INTEGER,
            arrival_time TIMESTAMPTZ,
            departure_delay INTEGER,
            departure_time TIMESTAMPTZ,
            schedule_relationship INTEGER DEFAULT 0
        );
        """)

        cur.execute("""
        CREATE INDEX idx_trip_updates_ingestion_ts ON bart_trip_updates(ingestion_ts);
        CREATE INDEX idx_trip_updates_route_id ON bart_trip_updates(route_id);
        CREATE INDEX idx_trip_updates_stop_id ON bart_trip_updates(stop_id);
        """)

        cur.execute("""
        CREATE TABLE bart_service_alerts (
            ingestion_ts TIMESTAMPTZ NOT NULL,
            alert_id TEXT,
            cause INTEGER,
            effect INTEGER,
            header_text TEXT,
            description_text TEXT,
            affected_routes TEXT,
            affected_stops TEXT,
            active_period_start TIMESTAMPTZ,
            active_period_end TIMESTAMPTZ
        );
        """)

        cur.execute("""
        CREATE INDEX idx_service_alerts_ingestion_ts ON bart_service_alerts(ingestion_ts);
        """)

        conn.commit()
        print("✓ Tables created with indexes")

def load_trip_updates(conn):
    files = glob.glob(f"{RAW_DIR}/bart_trip_updates_*.parquet")
    if not files:
        print("⚠️  No trip update files found")
        return

    print(f"✓ Found {len(files)} trip update file(s)")
    total_loaded = 0

    for file in files:
        try:
            print(f"📂 Loading {os.path.basename(file)}")
            df = pd.read_parquet(file)
            print(f"   Records: {len(df):,}")
            
            # Handle column name variations
            if 'timestamp' in df.columns and 'ingestion_ts' not in df.columns:
                df['ingestion_ts'] = df['timestamp']
            elif 'ingestion_ts' not in df.columns:
                df['ingestion_ts'] = datetime.now()
            
            # Add missing columns
            if 'stop_sequence' not in df.columns:
                df['stop_sequence'] = 0
            if 'schedule_relationship' not in df.columns:
                df['schedule_relationship'] = 0

            required_columns = [
                "ingestion_ts", "trip_id", "route_id", "stop_id",
                "stop_sequence", "arrival_delay", "arrival_time",
                "departure_delay", "departure_time", "schedule_relationship"
            ]

            for col in required_columns:
                if col not in df.columns:
                    df[col] = None if col not in ['stop_sequence', 'schedule_relationship'] else 0

            records = df[required_columns].values.tolist()

            # Batch insert for performance
            batch_size = 5000
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO bart_trip_updates
                        (ingestion_ts, trip_id, route_id, stop_id, stop_sequence,
                         arrival_delay, arrival_time, departure_delay, departure_time,
                         schedule_relationship)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        batch
                    )
                conn.commit()

            total_loaded += len(records)
            print(f"   ✓ Inserted: {len(records):,} records")

        except Exception as e:
            print(f"   ✗ Error: {e}")
            conn.rollback()

    print(f"✓ Total trip updates loaded: {total_loaded:,}")

def load_service_alerts(conn):
    files = glob.glob(f"{RAW_DIR}/bart_service_alerts_*.parquet")
    if not files:
        return

    print(f"✓ Found {len(files)} service alert file(s)")
    total_loaded = 0

    for file in files:
        try:
            print(f"📂 Loading {os.path.basename(file)}")
            df = pd.read_parquet(file)
            
            if 'timestamp' in df.columns and 'ingestion_ts' not in df.columns:
                df['ingestion_ts'] = df['timestamp']
            elif 'ingestion_ts' not in df.columns:
                df['ingestion_ts'] = datetime.now()

            required_columns = [
                "ingestion_ts", "alert_id", "cause", "effect",
                "header_text", "description_text", "affected_routes",
                "affected_stops", "active_period_start", "active_period_end"
            ]

            for col in required_columns:
                if col not in df.columns:
                    df[col] = None

            records = df[required_columns].values.tolist()

            batch_size = 1000
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO bart_service_alerts
                        (ingestion_ts, alert_id, cause, effect, header_text,
                         description_text, affected_routes, affected_stops,
                         active_period_start, active_period_end)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        batch
                    )
                conn.commit()

            total_loaded += len(records)
            print(f"   ✓ Inserted: {len(records):,} records")

        except Exception as e:
            print(f"   ✗ Error: {e}")
            conn.rollback()

    print(f"✓ Total service alerts loaded: {total_loaded:,}")

def main():
    print("="*60)
    print("BART Data Loader - Fixed Version")
    print("="*60)
    conn = connect_db()
    create_tables(conn)
    load_trip_updates(conn)
    load_service_alerts(conn)
    conn.close()
    print("="*60)
    print("✅ Data loading complete!")
    print("Dashboard: http://localhost:8501")
    print("="*60)

if __name__ == "__main__":
    main()
