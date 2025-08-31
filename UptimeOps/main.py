from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, Column, String, DateTime, Boolean, Integer, text, func
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timedelta
import pandas as pd
import os
import secrets
import logging
from typing import Dict, List
import asyncio
from concurrent.futures import ThreadPoolExecutor
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Store Monitoring API",
    description="""
    Restaurant Store Monitoring API for uptime and downtime analysis.
    
    ## Features
    - Historical data processing from CSV files
    - Real-time report generation
    - Optimized for large datasets
    - Chunked data processing to avoid timeouts
    
    ## Usage
    1. Initialize database: POST /initialize_database/
    2. Trigger report: POST /trigger_report/
    3. Check status: GET /get_report/{report_id}/
    4. Download report: GET /download_report/{report_id}/
    """,
    version="2.0.0",
)

# Database configuration using your Supabase credentials
DATABASE_URL = 'postgresql://postgres.qypppoidgqujdtxgdols:LkGJh6VCk5SGWi45@aws-1-ap-south-1.pooler.supabase.com:6543/postgres?sslmode=require'

# Create engine with optimizations for large datasets
engine = create_engine(
    DATABASE_URL,
    pool_size=20,
    max_overflow=30,
    pool_pre_ping=True,
    pool_recycle=3600,
    echo=False,
    connect_args={
        "options": "-c statement_timeout=600000 -c idle_in_transaction_session_timeout=300000"
    }
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Updated model names as requested
class StoreStatus(Base):
    __tablename__ = 'store_status'
    store_id = Column(String, primary_key=True)
    timestamp_utc = Column(DateTime, primary_key=True)
    status = Column(Boolean, nullable=False)

class MenuHours(Base):  # Renamed from StoreBusinessHours
    __tablename__ = 'menu_hours'
    store_id = Column(String, primary_key=True)
    day_of_week = Column(Integer, primary_key=True)
    start_time_local = Column(String)
    end_time_local = Column(String)

class Timezones(Base):  # Renamed from StoreTimezone (plural as requested)
    __tablename__ = 'timezones'
    store_id = Column(String, primary_key=True)
    timezone_str = Column(String, default='America/Chicago')

class ReportStatus(Base):
    __tablename__ = 'report_status'
    report_id = Column(String, primary_key=True)
    status = Column(Boolean, default=False)

def get_db():
    """Dependency to get database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def generate_unique_report_id() -> str:
    """Generate a unique report_id."""
    return secrets.token_urlsafe(8)

def fetch_store_data_optimized() -> tuple:
    """Fetch data using optimized queries with chunking to avoid timeouts."""
    logger.info("Starting optimized data fetch...")
    
    try:
        with engine.connect() as conn:
            # Get total counts first
            status_count = conn.execute(text("SELECT COUNT(*) FROM store_status")).fetchone()[0]
            hours_count = conn.execute(text("SELECT COUNT(*) FROM menu_hours")).fetchone()[0]
            tz_count = conn.execute(text("SELECT COUNT(*) FROM timezones")).fetchone()[0]
            
            logger.info(f"Data counts - Status: {status_count:,}, Hours: {hours_count:,}, Timezones: {tz_count:,}")
            
            # Fetch menu hours and timezones (usually smaller datasets)
            df_menu_hours = pd.read_sql_query("SELECT * FROM menu_hours", conn)
            df_timezones = pd.read_sql_query("SELECT * FROM timezones", conn)
            
            # For store_status, use chunked reading to avoid timeout
            chunk_size = 50000  # Process 50k records at a time
            df_status_chunks = []
            
            logger.info(f"Fetching store_status data in chunks of {chunk_size:,}")
            
            # Get data in time-ordered chunks (most recent first)
            offset = 0
            chunk_num = 1
            
            while True:
                chunk_query = text(f"""
                    SELECT store_id, timestamp_utc, status 
                    FROM store_status 
                    ORDER BY timestamp_utc DESC 
                    LIMIT {chunk_size} OFFSET {offset}
                """)
                
                chunk_df = pd.read_sql_query(chunk_query, conn)
                
                if chunk_df.empty:
                    break
                
                logger.info(f"Fetched chunk {chunk_num} with {len(chunk_df):,} rows")
                df_status_chunks.append(chunk_df)
                
                offset += chunk_size
                chunk_num += 1
                
                # Limit to reasonable amount of recent data for performance
                if len(df_status_chunks) * chunk_size >= 500000:  # Limit to 500k recent records
                    logger.info("Reached data limit for performance optimization")
                    break
            
            # Combine all chunks
            if df_status_chunks:
                df_store_status = pd.concat(df_status_chunks, ignore_index=True)
                logger.info(f"Combined {len(df_status_chunks)} chunks into {len(df_store_status):,} total rows")
            else:
                df_store_status = pd.DataFrame(columns=['store_id', 'timestamp_utc', 'status'])
            
            return df_store_status, df_menu_hours, df_timezones
            
    except Exception as e:
        logger.error(f"Error in data fetch: {e}")
        raise

def get_current_max_timestamp() -> datetime:
    """Get the maximum timestamp from store_status data as current time."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT MAX(timestamp_utc) FROM store_status")).fetchone()
            if result and result[0]:
                return result[0]
            else:
                return datetime.now()
    except Exception as e:
        logger.warning(f"Could not get max timestamp: {e}")
        return datetime.now()

def calculate_store_uptime_downtime(store_id: str, df_polls: pd.DataFrame, df_menu_hours: pd.DataFrame, 
                                  current_timestamp: datetime) -> Dict:
    """
    Calculate uptime and downtime for a store with proper business hours consideration.
    
    Logic:
    1. Filter polls for the store
    2. Define time windows (last hour, day, week)
    3. For each window, calculate total business hours
    4. Extrapolate uptime/downtime based on available polls within business hours
    """
    
    store_polls = df_polls[df_polls['store_id'] == store_id].copy()
    
    if store_polls.empty:
        # No data available - assume store was down for all business hours
        return {
            'store_id': store_id,
            'uptime_last_hour': 0, 'uptime_last_day': 0, 'uptime_last_week': 0,
            'downtime_last_hour': 60, 'downtime_last_day': 24, 'downtime_last_week': 168
        }
    
    # Ensure timestamp is datetime
    store_polls['timestamp_utc'] = pd.to_datetime(store_polls['timestamp_utc'])
    store_polls = store_polls.sort_values('timestamp_utc')
    
    # Get store menu hours (business hours)
    store_menu_hours = df_menu_hours[df_menu_hours['store_id'] == store_id]
    
    # If no menu hours, assume 24/7 operation
    if store_menu_hours.empty:
        total_hours_per_day = 24
        is_24_7 = True
    else:
        # Calculate average business hours per day
        daily_hours = []
        for _, row in store_menu_hours.iterrows():
            try:
                start_time = datetime.strptime(row['start_time_local'], '%H:%M:%S').time()
                end_time = datetime.strptime(row['end_time_local'], '%H:%M:%S').time()
                
                # Handle day spanning (e.g., 22:00 to 02:00)
                if end_time <= start_time:
                    hours = (24 - start_time.hour - start_time.minute/60) + (end_time.hour + end_time.minute/60)
                else:
                    hours = (end_time.hour + end_time.minute/60) - (start_time.hour + start_time.minute/60)
                
                daily_hours.append(max(0, hours))
            except:
                daily_hours.append(12)  # Default to 12 hours if parsing fails
        
        total_hours_per_day = np.mean(daily_hours) if daily_hours else 12
        is_24_7 = False
    
    # Define time windows
    one_hour_ago = current_timestamp - timedelta(hours=1)
    one_day_ago = current_timestamp - timedelta(days=1)  
    one_week_ago = current_timestamp - timedelta(weeks=1)
    
    def calculate_period_stats(start_time: datetime, end_time: datetime, period_name: str):
        """Calculate stats for a specific time period."""
        
        period_polls = store_polls[
            (store_polls['timestamp_utc'] >= start_time) & 
            (store_polls['timestamp_utc'] <= end_time)
        ]
        
        if period_polls.empty:
            # No polls in this period - use last known status before the period
            before_period = store_polls[store_polls['timestamp_utc'] < start_time]
            if not before_period.empty:
                last_known_status = before_period.iloc[-1]['status']
                uptime_ratio = 1.0 if last_known_status else 0.0
            else:
                uptime_ratio = 0.0  # Assume down if no historical data
        else:
            # Calculate uptime ratio from available polls
            active_polls = len(period_polls[period_polls['status'] == True])
            total_polls = len(period_polls)
            uptime_ratio = active_polls / total_polls if total_polls > 0 else 0.0
        
        return uptime_ratio
    
    # Calculate for each period
    uptime_ratio_hour = calculate_period_stats(one_hour_ago, current_timestamp, "hour")
    uptime_ratio_day = calculate_period_stats(one_day_ago, current_timestamp, "day")
    uptime_ratio_week = calculate_period_stats(one_week_ago, current_timestamp, "week")
    
    # Convert to actual hours/minutes considering business hours
    if is_24_7:
        # 24/7 operation
        uptime_hour_mins = uptime_ratio_hour * 60
        downtime_hour_mins = 60 - uptime_hour_mins
        
        uptime_day_hours = uptime_ratio_day * 24
        downtime_day_hours = 24 - uptime_day_hours
        
        uptime_week_hours = uptime_ratio_week * 168  # 7 * 24
        downtime_week_hours = 168 - uptime_week_hours
    else:
        # Business hours operation
        uptime_hour_mins = uptime_ratio_hour * min(60, total_hours_per_day * 60)
        downtime_hour_mins = min(60, total_hours_per_day * 60) - uptime_hour_mins
        
        uptime_day_hours = uptime_ratio_day * total_hours_per_day
        downtime_day_hours = total_hours_per_day - uptime_day_hours
        
        uptime_week_hours = uptime_ratio_week * (total_hours_per_day * 7)
        downtime_week_hours = (total_hours_per_day * 7) - uptime_week_hours
    
    return {
        'store_id': store_id,
        'uptime_last_hour': round(max(0, uptime_hour_mins), 2),
        'uptime_last_day': round(max(0, uptime_day_hours), 2),
        'uptime_last_week': round(max(0, uptime_week_hours), 2),
        'downtime_last_hour': round(max(0, downtime_hour_mins), 2),
        'downtime_last_day': round(max(0, downtime_day_hours), 2),
        'downtime_last_week': round(max(0, downtime_week_hours), 2)
    }

async def generate_store_monitoring_report(report_id: str):
    """Generate the store monitoring report asynchronously."""
    logger.info(f"Starting report generation for: {report_id}")
    
    try:
        # Fetch data
        df_store_status, df_menu_hours, df_timezones = fetch_store_data_optimized()
        current_timestamp = get_current_max_timestamp()
        
        logger.info(f"Using current timestamp: {current_timestamp}")
        logger.info(f"Processing {len(df_store_status):,} status records for report")
        
        # Get unique store IDs
        if not df_store_status.empty:
            unique_stores = df_store_status['store_id'].unique()
        else:
            # Fallback to stores in menu_hours or timezones
            unique_stores = set()
            if not df_menu_hours.empty:
                unique_stores.update(df_menu_hours['store_id'].unique())
            if not df_timezones.empty:
                unique_stores.update(df_timezones['store_id'].unique())
            unique_stores = list(unique_stores)
        
        logger.info(f"Processing {len(unique_stores)} unique stores")
        
        # Process stores in batches to avoid memory issues
        batch_size = 100
        all_results = []
        
        for i in range(0, len(unique_stores), batch_size):
            batch_stores = unique_stores[i:i + batch_size]
            batch_results = []
            
            for store_id in batch_stores:
                result = calculate_store_uptime_downtime(
                    store_id, df_store_status, df_menu_hours, current_timestamp
                )
                batch_results.append(result)
            
            all_results.extend(batch_results)
            logger.info(f"Processed batch {i//batch_size + 1}/{(len(unique_stores)-1)//batch_size + 1}")
        
        # Create report DataFrame
        if all_results:
            report_df = pd.DataFrame(all_results)
            
            # Ensure columns are in the correct order
            column_order = [
                'store_id', 'uptime_last_hour', 'uptime_last_day', 'uptime_last_week',
                'downtime_last_hour', 'downtime_last_day', 'downtime_last_week'
            ]
            report_df = report_df[column_order]
            
            logger.info(f"Report generated with {len(report_df)} stores")
        else:
            # Create empty report with correct structure
            report_df = pd.DataFrame(columns=[
                'store_id', 'uptime_last_hour', 'uptime_last_day', 'uptime_last_week',
                'downtime_last_hour', 'downtime_last_day', 'downtime_last_week'
            ])
            logger.warning("No store data available - created empty report")
        
        # Save report to CSV
        os.makedirs('report_data', exist_ok=True)
        report_file_path = f"report_data/report_{report_id}.csv"
        report_df.to_csv(report_file_path, index=False)
        
        logger.info(f"Report saved to: {report_file_path}")
        
        # Update report status in database
        with SessionLocal() as db:
            existing_report = db.query(ReportStatus).filter_by(report_id=report_id).first()
            if existing_report:
                existing_report.status = True
            else:
                new_report = ReportStatus(report_id=report_id, status=True)
                db.add(new_report)
            db.commit()
        
        logger.info(f"Report {report_id} completed successfully!")
        
    except Exception as e:
        logger.error(f"Error generating report {report_id}: {e}")
        
        # Mark report as failed
        try:
            with SessionLocal() as db:
                existing_report = db.query(ReportStatus).filter_by(report_id=report_id).first()
                if existing_report:
                    existing_report.status = False
                    db.commit()
        except Exception as db_error:
            logger.error(f"Failed to update report status: {db_error}")

# API Routes


@app.post("/trigger_report/", tags=["Reports"])
async def trigger_report(background_tasks: BackgroundTasks):
    """Trigger report generation with optimized processing."""
    try:
        report_id = generate_unique_report_id()
        
        # Create initial report status
        with SessionLocal() as db:
            report_status = ReportStatus(report_id=report_id, status=False)
            db.add(report_status)
            db.commit()
        
        # Add background task for report generation
        background_tasks.add_task(generate_store_monitoring_report, report_id)
        
        logger.info(f"Report triggered with ID: {report_id}")
        return {
            "report_id": report_id,
            "status": "triggered",
            "message": "Report generation started. Use /get_report/{report_id}/ to check status."
        }
        
    except Exception as e:
        logger.error(f"Error triggering report: {e}")
        raise HTTPException(status_code=500, detail="Failed to trigger report generation")

@app.get("/get_report/{report_id}/", tags=["Reports"])
async def get_report(report_id: str):
    """Get report status and details."""
    try:
        # Check if report exists in database
        with SessionLocal() as db:
            report_status = db.query(ReportStatus).filter_by(report_id=report_id).first()
        
        if not report_status:
            raise HTTPException(status_code=404, detail="Report not found")
        
        report_file_path = f"report_data/report_{report_id}.csv"
        
        if os.path.exists(report_file_path):
            file_size = os.path.getsize(report_file_path)
            
            # Get row count from CSV
            try:
                df = pd.read_csv(report_file_path)
                row_count = len(df)
            except:
                row_count = "unknown"
            
            return {
                "status": "Complete",
                "report_id": report_id,
                "csv_file": report_file_path,
                "file_size_bytes": file_size,
                "store_count": row_count,
                "download_url": f"/download_report/{report_id}/",
                "generated_at": datetime.now().isoformat()
            }
        else:
            return {
                "status": "Running",
                "report_id": report_id,
                "message": "Report is still being generated. Please check again in a few moments."
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting report {report_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get report status")

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting Store Monitoring API...")
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)