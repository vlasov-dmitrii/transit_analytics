import os
import glob
import pandas as pd
from datetime import datetime

def find_latest_bart_files(data_dir='./data/raw'):
    patterns = {
        'trips': 'bart_trip_updates_*.parquet',
        'alerts': 'bart_service_alerts_*.parquet'
    }
    
    files = {}
    for key, pattern in patterns.items():
        matching_files = glob.glob(os.path.join(data_dir, pattern))
        if matching_files:
            files[key] = max(matching_files, key=os.path.getmtime)
        else:
            files[key] = None
    
    return files

def explore_trip_data(filepath):
    df = pd.read_parquet(filepath)
    
    print(f"\nTrip Updates Dataset:")
    print(f"  Total updates: {len(df):,}")
    print(f"  Unique trips: {df['trip_id'].nunique():,}")
    print(f"  Unique routes: {df['route_id'].nunique()}")
    print(f"  Data timestamp: {df['timestamp'].iloc[0]}")
    
    print(f"\nActive Routes:")
    route_counts = df.groupby('route_id')['trip_id'].nunique().sort_values(ascending=False)
    for route, count in route_counts.items():
        print(f"  Route {route}: {count} active trips")
    
    delays = df[df['arrival_delay'].notna()]
    
    if len(delays) > 0:
        print(f"\nDelay Analysis:")
        print(f"  Updates with delay information: {len(delays):,}")
        
        avg_delay = delays['arrival_delay'].mean() / 60
        max_delay = delays['arrival_delay'].max() / 60
        min_delay = delays['arrival_delay'].min() / 60
        
        print(f"  Average delay: {avg_delay:.1f} minutes")
        print(f"  Maximum delay: {max_delay:.1f} minutes")
        print(f"  Minimum delay: {min_delay:.1f} minutes")
        
        on_time = len(delays[delays['arrival_delay'].abs() <= 60])
        minor_delay = len(delays[(delays['arrival_delay'] > 60) & (delays['arrival_delay'] <= 300)])
        major_delay = len(delays[delays['arrival_delay'] > 300])
        early = len(delays[delays['arrival_delay'] < -60])
        
        total = len(delays)
        print(f"\nPerformance Distribution:")
        print(f"  On-time (±1 min): {on_time:,} ({on_time/total*100:.1f}%)")
        print(f"  Minor delay (1-5 min): {minor_delay:,} ({minor_delay/total*100:.1f}%)")
        print(f"  Major delay (>5 min): {major_delay:,} ({major_delay/total*100:.1f}%)")
        print(f"  Running early: {early:,} ({early/total*100:.1f}%)")
        
        route_performance = delays.groupby('route_id').agg({
            'arrival_delay': ['mean', 'count']
        }).round(2)
        route_performance.columns = ['avg_delay_sec', 'num_stops']
        route_performance['avg_delay_min'] = route_performance['avg_delay_sec'] / 60
        route_performance = route_performance.sort_values('avg_delay_min', ascending=False)
        
        print(f"\nRoutes by Average Delay:")
        for route, row in route_performance.head(5).iterrows():
            print(f"  Route {route}: {row['avg_delay_min']:.1f} min average ({int(row['num_stops'])} stops)")
    
    return df

def explore_alert_data(filepath):
    df = pd.read_parquet(filepath)
    
    print(f"\nService Alerts:")
    print(f"  Total alerts: {len(df)}")
    
    if len(df) > 0:
        for i, row in df.iterrows():
            print(f"\n  Alert #{i+1}:")
            if row['header_text']:
                print(f"    {row['header_text'][:80]}")
            if row['affected_routes']:
                print(f"    Routes: {row['affected_routes']}")
            if row['active_period_start']:
                print(f"    Since: {row['active_period_start']}")
        
        route_alerts = {}
        for routes_str in df['affected_routes'].dropna():
            for route in routes_str.split(','):
                route = route.strip()
                route_alerts[route] = route_alerts.get(route, 0) + 1
        
        if route_alerts:
            print(f"\nAlerts by Route:")
            for route, count in sorted(route_alerts.items(), key=lambda x: x[1], reverse=True):
                print(f"  Route {route}: {count} alert(s)")
    else:
        print("  No active service alerts")
    
    return df

def create_summary_report(trips_df, alerts_df):
    print(f"\nSystem Summary:")
    print(f"  Active trips: {trips_df['trip_id'].nunique():,}")
    print(f"  Routes in service: {trips_df['route_id'].nunique()}")
    print(f"  Stops tracked: {len(trips_df):,}")
    print(f"  Service alerts: {len(alerts_df)}")
    
    delays = trips_df[trips_df['arrival_delay'].notna()]
    if len(delays) > 0:
        on_time = len(delays[delays['arrival_delay'].abs() <= 60])
        on_time_pct = (on_time / len(delays)) * 100
        print(f"  On-time performance: {on_time_pct:.1f}%")
    
    print(f"\nData Quality:")
    print(f"  Trip updates with delay data: {trips_df['arrival_delay'].notna().sum():,}")
    print(f"  Trip updates with timestamp data: {trips_df['arrival_time'].notna().sum():,}")

def main():
    print(f"\nBART Data Exploration - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    files = find_latest_bart_files()
    
    if not any(files.values()):
        print("No BART data files found.")
        return
    
    trips_df = None
    alerts_df = None
    
    if files['trips']:
        trips_df = explore_trip_data(files['trips'])
    else:
        print("\nTrip update data not available.")
    
    if files['alerts']:
        alerts_df = explore_alert_data(files['alerts'])
    else:
        print("\nService alert data not available.")
    
    if trips_df is not None and alerts_df is not None:
        create_summary_report(trips_df, alerts_df)

if __name__ == "__main__":
    main()