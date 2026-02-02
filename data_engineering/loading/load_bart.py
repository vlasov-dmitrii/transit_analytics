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

def connect_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Successfully connected to database")
        return conn
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        raise

def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bart_trip_updates (
            ingestion_ts TIMESTAMPTZ,
            trip_id TEXT,
            route_id TEXT,
            stop_id TEXT,
            arrival_time TIMESTAMPTZ,
            departure_time TIMESTAMPTZ,
            arrival_delay FLOAT,
            departure_delay FLOAT
        );
        """)
        
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bart_service_alerts (
            ingestion_ts TIMESTAMPTZ,
            alert_id TEXT,
            affected_routes TEXT,
            description_text TEXT,
            cause TEXT
        );
        """)
        
        conn.commit()
        print("Tables created successfully")

def load_trip_updates(conn):
    files = glob.glob(f"{RAW_DIR}/bart_trip_updates_*.parquet")
    
    if not files:
        print("No trip update files found")
        return
    
    print(f"Found {len(files)} trip update file(s)")
    
    total_loaded = 0
    for file in files:
        try:
            print(f"Processing: {os.path.basename(file)}")
            df = pd.read_parquet(file)
            
            if 'timestamp' in df.columns:
                df['ingestion_ts'] = df['timestamp']
            else:
                df['ingestion_ts'] = datetime.now()
            
            records = df[
                [
                    "ingestion_ts",
                    "trip_id",
                    "route_id",
                    "stop_id",
                    "arrival_time",
                    "departure_time",
                    "arrival_delay",
                    "departure_delay"
                ]
            ].values.tolist()
            
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO bart_trip_updates 
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    records,
                )
            conn.commit()
            
            total_loaded += len(records)
            print(f"Loaded {len(records)} trip updates")
            
        except Exception as e:
            print(f"Error loading {file}: {e}")
            conn.rollback()
            continue
    
    print(f"Total trip updates loaded: {total_loaded}")

def load_service_alerts(conn):
    files = glob.glob(f"{RAW_DIR}/bart_service_alerts_*.parquet")
    
    if not files:
        print("No service alert files found")
        return
    
    print(f"Found {len(files)} service alert file(s)")
    
    total_loaded = 0
    for file in files:
        try:
            print(f"Processing: {os.path.basename(file)}")
            df = pd.read_parquet(file)
            
            if 'timestamp' in df.columns:
                df['ingestion_ts'] = df['timestamp']
            else:
                df['ingestion_ts'] = datetime.now()
            
            records = df[
                [
                    "ingestion_ts",
                    "alert_id",
                    "affected_routes",
                    "description_text",
                    "cause"
                ]
            ].values.tolist()
            
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO bart_service_alerts 
                    VALUES (%s,%s,%s,%s,%s)
                    """,
                    records,
                )
            conn.commit()
            
            total_loaded += len(records)
            print(f"Loaded {len(records)} service alerts")
            
        except Exception as e:
            print(f"Error loading {file}: {e}")
            conn.rollback()
            continue
    
    print(f"Total service alerts loaded: {total_loaded}")

def verify_data(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM bart_trip_updates")
        trip_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM bart_service_alerts")
        alert_count = cur.fetchone()[0]
        
        print(f"Database Summary:")
        print(f"Trip updates: {trip_count:,} records")
        print(f"Service alerts: {alert_count:,} records")
        
        if trip_count > 0:
            cur.execute("""
                SELECT route_id, COUNT(*) as count 
                FROM bart_trip_updates 
                WHERE route_id IS NOT NULL
                GROUP BY route_id 
                ORDER BY count DESC 
                LIMIT 5
            """)
            print(f"Top routes by updates:")
            for route, count in cur.fetchall():
                print(f"Route {route}: {count:,} updates")

def main():
    print("BART Data Loader")
    
    try:
        conn = connect_db()
        
        create_tables(conn)
        
        load_trip_updates(conn)
        load_service_alerts(conn)
        
        verify_data(conn)
        
        conn.close()
        
        print("Loading completed successfully!")
        
    except Exception as e:
        print(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()