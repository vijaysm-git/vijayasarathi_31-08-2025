import pandas as pd
from sqlalchemy import create_engine, Column, String, DateTime, Boolean, Integer, text
from sqlalchemy.ext.declarative import declarative_base
from dateutil.parser import parse
from dateutil import tz
import logging
import os
from typing import Iterator


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Database connection using your Supabase credentials
DATABASE_URL = 'postgresql://postgres.qypppoidgqujdtxgdols:LkGJh6VCk5SGWi45@aws-1-ap-south-1.pooler.supabase.com:6543/postgres?sslmode=require'

Base = declarative_base()

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

class Timezones(Base):  # Renamed from StoreTimezone
    __tablename__ = 'timezones'
    store_id = Column(String, primary_key=True)
    timezone_str = Column(String, default='America/Chicago')

class ReportStatus(Base):
    __tablename__ = 'report_status'
    report_id = Column(String, primary_key=True)
    status = Column(Boolean, default=False)

def create_tables_with_optimization(engine):
    """Create all tables with optimizations for large datasets."""
    try:
        # Drop existing tables if they exist
        Base.metadata.drop_all(engine)
        
        # Create all tables
        Base.metadata.create_all(engine)
        
        # Add indexes for better query performance
        with engine.connect() as conn:
            # Indexes for store_status table (most queried)
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_store_status_store_id 
                ON store_status(store_id);
            """))
            
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_store_status_timestamp 
                ON store_status(timestamp_utc DESC);
            """))
            
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_store_status_composite 
                ON store_status(store_id, timestamp_utc DESC);
            """))
            
            # Indexes for menu_hours
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_menu_hours_store_id 
                ON menu_hours(store_id);
            """))
            
            # Disable RLS for better performance
            tables = ['store_status', 'menu_hours', 'timezones', 'report_status']
            for table in tables:
                try:
                    conn.execute(text(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY;"))
                    logger.info(f" RLS disabled for {table}")
                except Exception as e:
                    logger.warning(f" Could not disable RLS for {table}: {e}")
            
            conn.commit()
        
        logger.info("Tables created successfully with optimizations")
        
    except Exception as e:
        logger.error(f" Error creating tables: {e}")
        raise

def convert_to_datetime(timestamp_str):
    """Convert timestamp string to datetime object."""
    try:
        # Parse the timestamp and ensure it's in UTC
        timestamp_dt = parse(timestamp_str)
        if timestamp_dt.tzinfo is None:
            # Assume UTC if no timezone info
            timestamp_dt = timestamp_dt.replace(tzinfo=tz.tzutc())
        else:
            # Convert to UTC
            timestamp_dt = timestamp_dt.astimezone(tz.tzutc())
        
        # Return naive datetime in UTC (PostgreSQL will treat as UTC)
        return timestamp_dt.replace(tzinfo=None)
    except Exception as e:
        logger.warning(f" Error converting timestamp '{timestamp_str}': {e}")
        return None

def process_csv_in_chunks(csv_path: str, chunk_size: int = 10000) -> Iterator[pd.DataFrame]:
    """Process CSV file in chunks to handle large files efficiently."""
    logger.info(f" Processing {csv_path} in chunks of {chunk_size}")
    
    try:
        # First, peek at the file to understand its structure
        sample_df = pd.read_csv(csv_path, nrows=5)
        logger.info(f" Columns in {csv_path}: {list(sample_df.columns)}")
        logger.info(f" Sample data:\n{sample_df.head()}")
        
        # Read the file in chunks
        chunk_reader = pd.read_csv(csv_path, chunksize=chunk_size)
        
        for i, chunk in enumerate(chunk_reader):
            logger.info(f" Processing chunk {i+1} with {len(chunk)} rows")
            yield chunk
            
    except Exception as e:
        logger.error(f" Error reading CSV {csv_path}: {e}")
        raise

def preprocess_store_status_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """Preprocess a chunk of store status data."""
    # Handle different column name variations
    column_mapping = {}
    for col in chunk.columns:
        col_lower = col.lower().strip()
        if 'store' in col_lower and 'id' in col_lower:
            column_mapping[col] = 'store_id'
        elif 'timestamp' in col_lower and 'utc' in col_lower:
            column_mapping[col] = 'timestamp_utc'
        elif col_lower == 'status':
            column_mapping[col] = 'status'
    
    chunk = chunk.rename(columns=column_mapping)
    
    # Convert status to boolean
    if 'status' in chunk.columns:
        status_mapping = {
            'active': True, 'inactive': False,
            'Active': True, 'Inactive': False,
            'ACTIVE': True, 'INACTIVE': False,
            1: True, 0: False,
            '1': True, '0': False,
            True: True, False: False
        }
        
        original_len = len(chunk)
        chunk['status'] = chunk['status'].map(status_mapping)
        chunk = chunk.dropna(subset=['status'])
        logger.info(f"Status mapping: kept {len(chunk)}/{original_len} rows")
    
    # Convert timestamps
    if 'timestamp_utc' in chunk.columns:
        original_len = len(chunk)
        chunk['timestamp_utc'] = chunk['timestamp_utc'].apply(convert_to_datetime)
        chunk = chunk.dropna(subset=['timestamp_utc'])
        logger.info(f"Timestamp conversion: kept {len(chunk)}/{original_len} rows")
    
    # Ensure we have required columns
    required_columns = ['store_id', 'timestamp_utc', 'status']
    chunk = chunk[required_columns]
    
    return chunk

def preprocess_menu_hours_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """Preprocess a chunk of menu hours data."""
    # Handle different column name variations
    column_mapping = {}
    for col in chunk.columns:
        col_lower = col.lower().strip()
        if 'store' in col_lower and 'id' in col_lower:
            column_mapping[col] = 'store_id'
        elif 'day' in col_lower and ('week' in col_lower or 'of' in col_lower):
            column_mapping[col] = 'day_of_week'
        elif 'start' in col_lower and 'time' in col_lower:
            column_mapping[col] = 'start_time_local'
        elif 'end' in col_lower and 'time' in col_lower:
            column_mapping[col] = 'end_time_local'
    
    chunk = chunk.rename(columns=column_mapping)
    
    # Convert day_of_week to integer
    if 'day_of_week' in chunk.columns:
        original_len = len(chunk)
        chunk['day_of_week'] = pd.to_numeric(chunk['day_of_week'], errors='coerce')
        chunk = chunk.dropna(subset=['day_of_week'])
        chunk['day_of_week'] = chunk['day_of_week'].astype(int)
        logger.info(f"Day conversion: kept {len(chunk)}/{original_len} rows")
    
    return chunk

def preprocess_timezones_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """Preprocess a chunk of timezone data."""
    # Handle different column name variations
    column_mapping = {}
    for col in chunk.columns:
        col_lower = col.lower().strip()
        if 'store' in col_lower and 'id' in col_lower:
            column_mapping[col] = 'store_id'
        elif 'timezone' in col_lower or 'tz' in col_lower:
            column_mapping[col] = 'timezone_str'
    
    chunk = chunk.rename(columns=column_mapping)
    
    # Set default timezone if missing
    if 'timezone_str' not in chunk.columns and 'store_id' in chunk.columns:
        chunk['timezone_str'] = 'America/Chicago'
        logger.info("Added default timezone 'America/Chicago'")
    
    return chunk

def save_chunk_to_db(chunk: pd.DataFrame, table_name: str, engine, is_first_chunk: bool = False):
    """Save a chunk of data to the database."""
    try:
        if chunk.empty:
            logger.warning(f" Empty chunk for {table_name}")
            return
        
        # For first chunk, replace the table; for subsequent chunks, append
        if_exists = 'replace' if is_first_chunk else 'append'
        
        chunk.to_sql(
            table_name,
            engine,
            if_exists=if_exists,
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logger.info(f"Saved {len(chunk)} rows to {table_name} ({'replaced' if is_first_chunk else 'appended'})")
        
    except Exception as e:
        logger.error(f" Error saving chunk to {table_name}: {e}")
        raise

def load_csv_data_optimized():
    """Load all CSV data using chunked processing."""
    try:
        # Create database engine with connection pooling
        engine = create_engine(
            DATABASE_URL,
            pool_size=20,
            max_overflow=30,
            pool_pre_ping=True,
            pool_recycle=3600,
            connect_args={
                "options": "-c statement_timeout=600000"  # 10 minutes timeout
            }
        )
        
        # Create tables with optimizations
        create_tables_with_optimization(engine)
        
        # CSV file configurations
        csv_configs = [
            {
                'path': 'data/store_status.csv',
                'table': 'store_status',
                'preprocess_func': preprocess_store_status_chunk
            },
            {
                'path': 'data/menu_hours.csv',
                'table': 'menu_hours',  # Changed from store_business_hours
                'preprocess_func': preprocess_menu_hours_chunk
            },
            {
                'path': 'data/timezones.csv',
                'table': 'timezones',  # Changed from store_timezone
                'preprocess_func': preprocess_timezones_chunk
            }
        ]
        
        # Process each CSV file
        for config in csv_configs:
            csv_path = config['path']
            table_name = config['table']
            preprocess_func = config['preprocess_func']
            
            if not os.path.exists(csv_path):
                logger.warning(f" CSV file not found: {csv_path}")
                continue
            
            logger.info(f"Starting to load {csv_path} into {table_name}")
            
            is_first_chunk = True
            total_rows = 0
            
            try:
                for chunk in process_csv_in_chunks(csv_path, chunk_size=5000):  # Smaller chunks for memory efficiency
                    # Preprocess the chunk
                    processed_chunk = preprocess_func(chunk)
                    
                    if not processed_chunk.empty:
                        # Save to database
                        save_chunk_to_db(processed_chunk, table_name, engine, is_first_chunk)
                        total_rows += len(processed_chunk)
                        is_first_chunk = False
                    
                logger.info(f" Successfully loaded {total_rows} total rows into {table_name}")
                
            except Exception as e:
                logger.error(f"Error processing {csv_path}: {e}")
                continue
        
        # Verify data was loaded
        logger.info("Verifying data load...")
        with engine.connect() as conn:
            for config in csv_configs:
                table_name = config['table']
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    count = result.fetchone()[0]
                    logger.info(f" {table_name}: {count:,} rows")
                except Exception as e:
                    logger.error(f" Error counting {table_name}: {e}")
        
        logger.info("Database setup completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f" Failed to setup database: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

def main():
    """Main function to load and process all CSV data."""
    try:
        success = load_csv_data_optimized()
        if success:
            logger.info(" All data loaded successfully!")
        else:
            logger.error(" Data loading failed!")
            
    except Exception as e:
        logger.error(f" Main function failed: {e}")

if __name__ == "__main__":
    main()