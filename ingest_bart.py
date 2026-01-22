"""
BART Data Ingestion Script
Fetches real-time BART transit data and saves to parquet files

Usage:
    python data_engineering/scripts/ingest_bart_data.py

Output:
    - bart_trip_updates_YYYYMMDD_HHMMSS.parquet
    - bart_service_alerts_YYYYMMDD_HHMMSS.parquet
"""

import os
import sys
import logging
import requests
import pandas as pd
from datetime import datetime
from google.transit import gtfs_realtime_pb2
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

TRIP_URL = "http://api.bart.gov/gtfsrt/tripupdate.aspx"
ALERTS_URL = "http://api.bart.gov/gtfsrt/alerts.aspx"


class BARTDataIngestion:
    
    def __init__(self, output_dir='./data/raw'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory: {self.output_dir}")
    
    def fetch_and_parse_trip_updates(self, url):
        logger.info("Fetching BART trip updates")
        
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        
        updates = []
        feed_timestamp = datetime.fromtimestamp(feed.header.timestamp)
        
        for entity in feed.entity:
            if entity.HasField('trip_update'):
                trip = entity.trip_update
                
                trip_id = trip.trip.trip_id if trip.HasField('trip') else None
                route_id = trip.trip.route_id if trip.HasField('trip') else None
                
                for stop_update in trip.stop_time_update:
                    update_record = {
                        'timestamp': feed_timestamp,
                        'trip_id': trip_id,
                        'route_id': route_id,
                        'stop_id': stop_update.stop_id if stop_update.HasField('stop_id') else None,
                        'stop_sequence': stop_update.stop_sequence if stop_update.HasField('stop_sequence') else None,
                    }
                    
                    if stop_update.HasField('arrival'):
                        update_record['arrival_delay'] = stop_update.arrival.delay if stop_update.arrival.HasField('delay') else None
                        update_record['arrival_time'] = datetime.fromtimestamp(stop_update.arrival.time) if stop_update.arrival.HasField('time') else None
                    else:
                        update_record['arrival_delay'] = None
                        update_record['arrival_time'] = None
                    
                    if stop_update.HasField('departure'):
                        update_record['departure_delay'] = stop_update.departure.delay if stop_update.departure.HasField('delay') else None
                        update_record['departure_time'] = datetime.fromtimestamp(stop_update.departure.time) if stop_update.departure.HasField('time') else None
                    else:
                        update_record['departure_delay'] = None
                        update_record['departure_time'] = None
                    
                    updates.append(update_record)
        
        df = pd.DataFrame(updates)
        logger.info(f"Parsed {len(df)} trip updates from {len(feed.entity)} trips")
        
        return df
    
    def fetch_and_parse_service_alerts(self, url):
        logger.info("Fetching BART service alerts")
        
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        
        alerts = []
        feed_timestamp = datetime.fromtimestamp(feed.header.timestamp)
        
        for entity in feed.entity:
            if entity.HasField('alert'):
                alert = entity.alert
                
                affected_routes = []
                affected_stops = []
                
                for informed_entity in alert.informed_entity:
                    if informed_entity.HasField('route_id'):
                        affected_routes.append(informed_entity.route_id)
                    if informed_entity.HasField('stop_id'):
                        affected_stops.append(informed_entity.stop_id)
                
                header = alert.header_text.translation[0].text if alert.header_text.translation else None
                description = alert.description_text.translation[0].text if alert.description_text.translation else None
                
                active_start = None
                active_end = None
                if alert.active_period:
                    if alert.active_period[0].HasField('start'):
                        active_start = datetime.fromtimestamp(alert.active_period[0].start)
                    if alert.active_period[0].HasField('end'):
                        active_end = datetime.fromtimestamp(alert.active_period[0].end)
                
                alert_record = {
                    'timestamp': feed_timestamp,
                    'alert_id': entity.id,
                    'cause': alert.cause if alert.HasField('cause') else None,
                    'effect': alert.effect if alert.HasField('effect') else None,
                    'header_text': header,
                    'description_text': description,
                    'affected_routes': ','.join(set(affected_routes)) if affected_routes else None,
                    'affected_stops': ','.join(set(affected_stops)) if affected_stops else None,
                    'active_period_start': active_start,
                    'active_period_end': active_end,
                }
                
                alerts.append(alert_record)
        
        df = pd.DataFrame(alerts)
        logger.info(f"Parsed {len(df)} service alerts")
        
        return df
    
    def save_to_parquet(self, df, data_type):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"bart_{data_type}_{timestamp}.parquet"
        filepath = self.output_dir / filename
        
        df.to_parquet(filepath, index=False)
        logger.info(f"Saved {len(df)} records to {filepath}")
        
        return filepath
    
    def analyze_data(self, trips_df, alerts_df):
        logger.info(f"Trip Updates: {len(trips_df)} total, {trips_df['trip_id'].nunique()} unique trips, {trips_df['route_id'].nunique()} routes active")
        logger.info(f"Service Alerts: {len(alerts_df)} total")
        
        if len(trips_df) > 0 and trips_df['arrival_delay'].notna().any():
            # only looking at delays over 1 minute
            delayed = trips_df[trips_df['arrival_delay'].notna() & (trips_df['arrival_delay'] > 60)]
            
            if len(delayed) > 0:
                avg_delay_min = delayed['arrival_delay'].mean() / 60
                logger.info(f"Delays: {len(delayed)} stops with delays > 1 min, average {avg_delay_min:.1f} minutes")
                
                route_delays = delayed.groupby('route_id')['arrival_delay'].agg(['count', 'mean'])
                route_delays = route_delays.sort_values('mean', ascending=False)
                
                if len(route_delays) > 0:
                    top_delayed = route_delays.head(3)
                    for route, row in top_delayed.iterrows():
                        logger.info(f"Route {route}: {int(row['count'])} delays, avg {row['mean']/60:.1f} min")
            else:
                logger.info("No significant delays detected (all within 1 minute)")
    
    def run(self):
        logger.info(f"Starting BART data ingestion at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            trips_df = self.fetch_and_parse_trip_updates(TRIP_URL)
            alerts_df = self.fetch_and_parse_service_alerts(ALERTS_URL)
            
            trip_file = self.save_to_parquet(trips_df, 'trip_updates')
            alert_file = self.save_to_parquet(alerts_df, 'service_alerts')
            
            self.analyze_data(trips_df, alerts_df)
            
            logger.info("Ingestion completed successfully")
            
            return 0
            
        except requests.exceptions.Timeout:
            logger.error("Request timeout")
            return 1
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error: {e}")
            return 1
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return 1


def main():
    output_dir = os.getenv('RAW_DIR', './data/raw')
    
    ingestion = BARTDataIngestion(output_dir=output_dir)
    return ingestion.run()


if __name__ == "__main__":
    sys.exit(main())