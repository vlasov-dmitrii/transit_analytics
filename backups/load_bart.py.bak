import os
import glob
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import TimestampType, IntegerType

RAW_DIR = os.getenv('RAW_DIR', '/app/data/raw')

DB_CONFIG = {
    "host": os.getenv('DB_HOST', 'bart_postgres'),
    "port": int(os.getenv('DB_PORT', 5432)),
    "database": os.getenv('DB_NAME', 'bart_dw'),
    "user": os.getenv('DB_USER', 'bart'),
    "password": os.getenv('DB_PASSWORD', 'bart')
}


def get_spark_session():
    """Initialize Spark session with PostgreSQL support."""
    return SparkSession.builder \
        .appName("BART Data Loader") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()


def get_jdbc_url():
    """Construct PostgreSQL JDBC URL."""
    return f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"


def create_tables(spark):
    """Create database tables with indexes."""
    jdbc_url = get_jdbc_url()
    properties = {
        "user": DB_CONFIG["user"],
        "password": DB_CONFIG["password"],
        "driver": "org.postgresql.Driver"
    }
    
    connection = spark._jvm.java.sql.DriverManager.getConnection(
        jdbc_url, properties["user"], properties["password"]
    )
    
    statement = connection.createStatement()
    
    statement.execute("DROP TABLE IF EXISTS bart_trip_updates CASCADE")
    statement.execute("DROP TABLE IF EXISTS bart_service_alerts CASCADE")
    
    statement.execute("""
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
        )
    """)
    
    statement.execute("CREATE INDEX idx_trip_updates_ingestion_ts ON bart_trip_updates(ingestion_ts)")
    statement.execute("CREATE INDEX idx_trip_updates_route_id ON bart_trip_updates(route_id)")
    statement.execute("CREATE INDEX idx_trip_updates_stop_id ON bart_trip_updates(stop_id)")
    
    statement.execute("""
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
        )
    """)
    
    statement.execute("CREATE INDEX idx_service_alerts_ingestion_ts ON bart_service_alerts(ingestion_ts)")
    
    statement.close()
    connection.close()
    
    print("Tables created with indexes")


def load_trip_updates(spark):
    """Load trip updates using PySpark."""
    files = glob.glob(f"{RAW_DIR}/bart_trip_updates_*.parquet")
    
    if not files:
        print("No trip update files found")
        return
    
    print(f"Found {len(files)} trip update file(s)")
    
    jdbc_url = get_jdbc_url()
    properties = {
        "user": DB_CONFIG["user"],
        "password": DB_CONFIG["password"],
        "driver": "org.postgresql.Driver",
        "batchsize": "10000",
        "reWriteBatchedInserts": "true"
    }
    
    total_loaded = 0
    
    for file in files:
        print(f"Loading {os.path.basename(file)}")
        
        df = spark.read.parquet(file)
        
        if "timestamp" in df.columns and "ingestion_ts" not in df.columns:
            df = df.withColumnRenamed("timestamp", "ingestion_ts")
        elif "ingestion_ts" not in df.columns:
            df = df.withColumn("ingestion_ts", lit(datetime.now()).cast(TimestampType()))
        
        required_columns = {
            "stop_sequence": 0,
            "schedule_relationship": 0
        }
        
        for col_name, default_value in required_columns.items():
            if col_name not in df.columns:
                df = df.withColumn(col_name, lit(default_value).cast(IntegerType()))
        
        for col_name in ["trip_id", "route_id", "stop_id", "arrival_delay", 
                        "arrival_time", "departure_delay", "departure_time"]:
            if col_name not in df.columns:
                df = df.withColumn(col_name, lit(None))
        
        df = df.select(
            "ingestion_ts", "trip_id", "route_id", "stop_id", "stop_sequence",
            "arrival_delay", "arrival_time", "departure_delay", 
            "departure_time", "schedule_relationship"
        )
        
        record_count = df.count()
        print(f"Records: {record_count:,}")
        
        df.write.jdbc(
            url=jdbc_url,
            table="bart_trip_updates",
            mode="append",
            properties=properties
        )
        
        total_loaded += record_count
        print(f"Inserted: {record_count:,} records")
    
    print(f"Total trip updates loaded: {total_loaded:,}")


def load_service_alerts(spark):
    """Load service alerts using PySpark."""
    files = glob.glob(f"{RAW_DIR}/bart_service_alerts_*.parquet")
    
    if not files:
        print("No service alert files found")
        return
    
    print(f"Found {len(files)} service alert file(s)")
    
    jdbc_url = get_jdbc_url()
    properties = {
        "user": DB_CONFIG["user"],
        "password": DB_CONFIG["password"],
        "driver": "org.postgresql.Driver",
        "batchsize": "5000",
        "reWriteBatchedInserts": "true"
    }
    
    total_loaded = 0
    
    for file in files:
        print(f"Loading {os.path.basename(file)}")
        
        df = spark.read.parquet(file)
        
        if "timestamp" in df.columns and "ingestion_ts" not in df.columns:
            df = df.withColumnRenamed("timestamp", "ingestion_ts")
        elif "ingestion_ts" not in df.columns:
            df = df.withColumn("ingestion_ts", lit(datetime.now()).cast(TimestampType()))
        
        required_columns = [
            "alert_id", "cause", "effect", "header_text", "description_text",
            "affected_routes", "affected_stops", "active_period_start", "active_period_end"
        ]
        
        for col_name in required_columns:
            if col_name not in df.columns:
                df = df.withColumn(col_name, lit(None))
        
        df = df.select("ingestion_ts", *required_columns)
        
        record_count = df.count()
        print(f"Records: {record_count:,}")
        
        df.write.jdbc(
            url=jdbc_url,
            table="bart_service_alerts",
            mode="append",
            properties=properties
        )
        
        total_loaded += record_count
        print(f"Inserted: {record_count:,} records")
    
    print(f"Total service alerts loaded: {total_loaded:,}")


def main():
    print("=" * 60)
    print("BART Data Loader - PySpark Version")
    print("=" * 60)
    
    spark = get_spark_session()
    
    try:
        create_tables(spark)
        load_trip_updates(spark)
        load_service_alerts(spark)
        
        print("=" * 60)
        print("Data loading complete")
        print("Dashboard: http://localhost:8501")
        print("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
