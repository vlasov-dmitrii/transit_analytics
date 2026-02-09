import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

st.set_page_config(
    page_title="BART Transit Analytics",
    page_icon="🚇",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host="bart_postgres",
        port=5432,
        dbname="bart_dw",
        user="bart",
        password="bart"
    )

@st.cache_data(ttl=60)
def load_data(query):
    conn = get_connection()
    df = pd.read_sql(query, conn)
    return df

def safe_float(value, default=0.0):
    if value is None or pd.isna(value):
        return default
    return float(value)

def main():
    st.title("BART Transit Analytics Dashboard")
    st.markdown("Real-time performance monitoring and delay analysis")
    
    with st.sidebar:
        st.header("Settings")
        
        if st.button("Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        
        st.subheader("Date Range")
        days_back = st.slider("Days of data to show", 1, 30, 7)
        
        st.markdown("---")
        st.markdown("### About This Data")
        st.info(f"Showing last {days_back} days of BART trip data")
    
    st.header("System Overview")
    
    summary_query = f"""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT trip_id) as unique_trips,
        COUNT(DISTINCT route_id) as active_routes,
        COUNT(DISTINCT stop_id) as stops_tracked,
        ROUND(COALESCE((AVG(arrival_delay) FILTER (WHERE arrival_delay IS NOT NULL))/60, 0)::numeric, 2) as avg_delay_minutes,
        ROUND(COALESCE((MAX(arrival_delay) FILTER (WHERE arrival_delay IS NOT NULL))/60, 0)::numeric, 1) as max_delay_minutes,
        MIN(ingestion_ts) as earliest_data,
        MAX(ingestion_ts) as latest_data
    FROM bart_trip_updates
    WHERE ingestion_ts >= NOW() - INTERVAL '{days_back} days'
    """
    
    summary = load_data(summary_query).iloc[0]
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Trip Updates",
            f"{int(summary['total_records']):,}",
            help="Total number of trip update records collected"
        )
    
    with col2:
        st.metric(
            "Unique Trips",
            f"{int(summary['unique_trips']):,}",
            help="Number of distinct trips tracked"
        )
    
    with col3:
        avg_delay = safe_float(summary['avg_delay_minutes'])
        st.metric(
            "Average Delay",
            f"{avg_delay:.2f} min",
            help="System-wide average delay in minutes"
        )
    
    with col4:
        max_delay = safe_float(summary['max_delay_minutes'])
        st.metric(
            "Max Delay",
            f"{max_delay:.1f} min",
            help="Worst delay observed in the period"
        )
    
    if summary['earliest_data'] is not None and summary['latest_data'] is not None:
        earliest = summary['earliest_data'].strftime('%Y-%m-%d %H:%M')
        latest = summary['latest_data'].strftime('%Y-%m-%d %H:%M')
        st.info(f"Data from {earliest} to {latest} | {summary['active_routes']} routes | {summary['stops_tracked']} stops")
    else:
        st.warning("No data found in the selected date range. Try increasing the number of days.")
        return
    
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.subheader("On-Time Performance by Route")
        
        route_perf_query = f"""
        SELECT 
            route_id,
            COUNT(*) as total_trips,
            COUNT(CASE WHEN COALESCE(arrival_delay, 0) <= 60 THEN 1 END) as on_time,
            ROUND((100.0 * COUNT(CASE WHEN COALESCE(arrival_delay, 0) <= 60 THEN 1 END) / COUNT(*))::numeric, 1) as on_time_pct,
            ROUND(COALESCE((AVG(arrival_delay) FILTER (WHERE arrival_delay IS NOT NULL))/60, 0)::numeric, 2) as avg_delay_minutes
        FROM bart_trip_updates
        WHERE route_id IS NOT NULL 
          AND ingestion_ts >= NOW() - INTERVAL '{days_back} days'
        GROUP BY route_id
        HAVING COUNT(*) >= 10
        ORDER BY on_time_pct DESC
        """
        
        route_perf = load_data(route_perf_query)
        
        if not route_perf.empty:
            fig = px.bar(
                route_perf,
                x='route_id',
                y='on_time_pct',
                text='on_time_pct',
                title='On-Time Performance by Route (within 1 minute)',
                labels={'route_id': 'Route', 'on_time_pct': 'On-Time %'},
                color='on_time_pct',
                color_continuous_scale='RdYlGn',
                range_color=[80, 100]
            )
            fig.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
            fig.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No data available for this period")
    
    with col_right:
        st.subheader("Average Delay by Route")
        
        if not route_perf.empty:
            route_delay = route_perf.sort_values('avg_delay_minutes', ascending=False).head(10)
            
            fig = px.bar(
                route_delay,
                x='route_id',
                y='avg_delay_minutes',
                text='avg_delay_minutes',
                title='Routes with Highest Average Delays',
                labels={'route_id': 'Route', 'avg_delay_minutes': 'Avg Delay (min)'},
                color='avg_delay_minutes',
                color_continuous_scale='Reds'
            )
            fig.update_traces(texttemplate='%{text:.2f}', textposition='outside')
            fig.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Trip Volume by Hour of Day")
    
    hourly_query = f"""
    SELECT 
        EXTRACT(HOUR FROM arrival_time) as hour,
        COUNT(*) as trip_count,
        ROUND(COALESCE((AVG(arrival_delay) FILTER (WHERE arrival_delay IS NOT NULL))/60, 0)::numeric, 2) as avg_delay
    FROM bart_trip_updates
    WHERE arrival_time IS NOT NULL
      AND ingestion_ts >= NOW() - INTERVAL '{days_back} days'
    GROUP BY hour
    ORDER BY hour
    """
    
    hourly = load_data(hourly_query)
    
    if not hourly.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.line(
                hourly,
                x='hour',
                y='trip_count',
                title='Trip Updates by Hour',
                labels={'hour': 'Hour of Day', 'trip_count': 'Number of Updates'},
                markers=True
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.line(
                hourly,
                x='hour',
                y='avg_delay',
                title='Average Delay by Hour',
                labels={'hour': 'Hour of Day', 'avg_delay': 'Avg Delay (min)'},
                markers=True,
                color_discrete_sequence=['red']
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Delay Distribution")
    
    delay_dist_query = f"""
    SELECT 
        CASE 
            WHEN COALESCE(arrival_delay, 0) <= 0 THEN 'Early/On-Time'
            WHEN arrival_delay > 0 AND arrival_delay <= 60 THEN '0-1 min'
            WHEN arrival_delay > 60 AND arrival_delay <= 180 THEN '1-3 min'
            WHEN arrival_delay > 180 AND arrival_delay <= 300 THEN '3-5 min'
            WHEN arrival_delay > 300 AND arrival_delay <= 600 THEN '5-10 min'
            ELSE '10+ min'
        END as delay_category,
        COUNT(*) as count
    FROM bart_trip_updates
    WHERE ingestion_ts >= NOW() - INTERVAL '{days_back} days'
    GROUP BY delay_category
    """
    
    delay_dist = load_data(delay_dist_query)
    
    if not delay_dist.empty:
        category_order = ['Early/On-Time', '0-1 min', '1-3 min', '3-5 min', '5-10 min', '10+ min']
        
        all_categories = pd.DataFrame({'delay_category': category_order})
        delay_dist = all_categories.merge(delay_dist, on='delay_category', how='left')
        delay_dist['count'] = delay_dist['count'].fillna(0).astype(int)
        
        delay_dist = delay_dist[delay_dist['count'] > 0]
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.pie(
                delay_dist,
                values='count',
                names='delay_category',
                title='Distribution of Delays',
                category_orders={'delay_category': category_order}
            )
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(
                delay_dist,
                x='delay_category',
                y='count',
                title='Delay Frequency',
                labels={'delay_category': 'Delay Category', 'count': 'Count'},
                category_orders={'delay_category': category_order}
            )
            fig.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Route Deep Dive")
    
    routes_query = """
    SELECT DISTINCT route_id 
    FROM bart_trip_updates 
    WHERE route_id IS NOT NULL 
    ORDER BY route_id
    """
    routes = load_data(routes_query)['route_id'].tolist()
    
    if routes:
        selected_route = st.selectbox("Select a route to analyze", routes)
        
        if selected_route:
            route_detail_query = f"""
            SELECT 
                stop_id,
                COUNT(*) as visits,
                ROUND(COALESCE((AVG(arrival_delay) FILTER (WHERE arrival_delay IS NOT NULL))/60, 0)::numeric, 2) as avg_delay,
                ROUND(COALESCE((MAX(arrival_delay) FILTER (WHERE arrival_delay IS NOT NULL))/60, 0)::numeric, 1) as max_delay,
                COUNT(CASE WHEN COALESCE(arrival_delay, 0) > 60 THEN 1 END) as delayed_arrivals
            FROM bart_trip_updates
            WHERE route_id = '{selected_route}'
              AND stop_id IS NOT NULL
              AND ingestion_ts >= NOW() - INTERVAL '{days_back} days'
            GROUP BY stop_id
            HAVING COUNT(*) >= 5
            ORDER BY avg_delay DESC
            LIMIT 15
            """
            
            route_detail = load_data(route_detail_query)
            
            if not route_detail.empty:
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown(f"**Route {selected_route} - Stops with Most Delays**")
                    fig = px.bar(
                        route_detail.head(10),
                        x='stop_id',
                        y='avg_delay',
                        title=f'Average Delay by Stop (Route {selected_route})',
                        labels={'stop_id': 'Stop ID', 'avg_delay': 'Avg Delay (min)'},
                        color='avg_delay',
                        color_continuous_scale='Reds'
                    )
                    fig.update_layout(height=350)
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    st.markdown(f"**Route {selected_route} - Stop Statistics**")
                    st.dataframe(
                        route_detail,
                        column_config={
                            "stop_id": "Stop ID",
                            "visits": st.column_config.NumberColumn("Visits", format="%d"),
                            "avg_delay": st.column_config.NumberColumn("Avg Delay (min)", format="%.2f"),
                            "max_delay": st.column_config.NumberColumn("Max Delay (min)", format="%.1f"),
                            "delayed_arrivals": st.column_config.NumberColumn("Delayed Arrivals", format="%d")
                        },
                        hide_index=True,
                        height=350
                    )
    
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        <p>BART Transit Analytics Platform | Built with Streamlit, PostgreSQL, and Docker</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()