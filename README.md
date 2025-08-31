# Store Monitoring API

A FastAPI-based restaurant store monitoring system that analyzes uptime and downtime patterns from historical data. This API processes large CSV datasets and generates comprehensive reports on store performance metrics.

## 📋 Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Database Setup](#database-setup)
- [Running the Application](#running-the-application)
- [API Endpoints](#api-endpoints)
- [Testing with Postman](#testing-with-postman)
- [Data Structure](#data-structure)
- [Output Format](#output-format)
- [Performance Optimizations](#performance-optimizations)
- [Troubleshooting](#troubleshooting)

## ✨ Features

- **Large Dataset Processing**: Handles CSV files with millions of records using chunked processing
- **Real-time Report Generation**: Asynchronous background report processing
- **Optimized Database Operations**: Uses connection pooling and indexing for performance
- **Flexible Data Handling**: Automatically maps various CSV column formats
- **Business Hours Awareness**: Calculates uptime/downtime based on actual operating hours
- **RESTful API**: Clean API design with comprehensive documentation

## 🔧 Prerequisites

### Python Version
- **Python 3.8 or higher** (recommended: Python 3.9+)

### Database
- **PostgreSQL** database (Supabase or any PostgreSQL instance)
- Database connection with read/write permissions

## 📦 Installation

### 1. Clone the Repository
```bash
git clone <your-repository-url>
cd store-monitoring-api
```

### 2. Create Virtual Environment
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

### 3. Install Required Packages
```bash
pip install fastapi
pip install uvicorn
pip install sqlalchemy
pip install psycopg2-binary
pip install pandas
pip install python-dateutil
pip install numpy
pip install python-multipart
```

**Or install all at once:**
```bash
pip install fastapi uvicorn sqlalchemy psycopg2-binary pandas python-dateutil numpy python-multipart
```

### 4. Create Requirements File (Optional)
Create a `requirements.txt` file:
```txt
fastapi==0.104.1
uvicorn==0.24.0
sqlalchemy==2.0.23
psycopg2-binary==2.9.9
pandas==2.1.4
python-dateutil==2.8.2
numpy==1.24.4
python-multipart==0.0.6
```

Install from requirements:
```bash
pip install -r requirements.txt
```

## 🗄️ Database Setup

### 1. Update Database Credentials
Edit the `DATABASE_URL` in both `database.py` and `main.py`:
```python
DATABASE_URL = 'postgresql://username:password@host:port/database?sslmode=require'
```

### 2. Prepare CSV Data
Create a `data/` directory and place your CSV files:
```
data/
├── store_status.csv
├── menu_hours.csv
└── timezones.csv
```

### 3. Initialize Database
```bash
python database.py
```

This will:
- Create all required tables
- Load CSV data in optimized chunks
- Create database indexes for performance
- Verify data integrity

## 🚀 Running the Application

### Development Mode
```bash
python main.py
```

### Production Mode
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

The API will be available at:
- **Local**: http://localhost:8000
- **Documentation**: http://localhost:8000/docs
- **Alternative Docs**: http://localhost:8000/redoc

## 🔌 API Endpoints

### 1. Trigger Report Generation
**POST** `/trigger_report/`

Starts background report generation process.

**Response:**
```json
{
  "report_id": "abc123def",
  "status": "triggered",
  "message": "Report generation started. Use /get_report/{report_id}/ to check status."
}
```

### 2. Check Report Status
**GET** `/get_report/{report_id}/`

Check if report is complete and get details.

**Response (Running):**
```json
{
  "status": "Running",
  "report_id": "abc123def",
  "message": "Report is still being generated. Please check again in a few moments."
}
```

**Response (Complete):**
```json
{
  "status": "Complete",
  "report_id": "abc123def",
  "csv_file": "report_data/report_abc123def.csv",
  "file_size_bytes": 2048576,
  "store_count": 1000,
  "download_url": "/download_report/abc123def/",
  "generated_at": "2024-01-15T10:30:00"
}
```

### 3. Download Report (Implementation needed)
**GET** `/download_report/{report_id}/`

Download the generated CSV report file.

## 🧪 Testing with Postman

### Collection Setup
1. **Create New Collection**: "Store Monitoring API"
2. **Set Base URL**: `http://localhost:8000`
3. **Add Environment Variables**:
   - `base_url`: `http://localhost:8000`
   - `report_id`: (will be set dynamically)

### Test Sequence

#### 1. Trigger Report
- **Method**: POST
- **URL**: `{{base_url}}/trigger_report/`
- **Headers**: `Content-Type: application/json`
- **Tests** (Postman Tests tab):
```javascript
pm.test("Status code is 200", function () {
    pm.response.to.have.status(200);
});

pm.test("Response has report_id", function () {
    var jsonData = pm.response.json();
    pm.expect(jsonData).to.have.property('report_id');
    pm.environment.set("report_id", jsonData.report_id);
});
```

#### 2. To check status and download the report
- **Method**: GET
- **URL**: `{{base_url}}/get_report/{{report_id}}/`
- **Tests**:
```javascript
pm.test("Status code is 200", function () {
    pm.response.to.have.status(200);
});

pm.test("Report status is valid", function () {
    var jsonData = pm.response.json();
    pm.expect(jsonData.status).to.be.oneOf(["Running", "Complete"]);
});
```



## 📊 Data Structure

### Input CSV Files

#### store_status.csv
```csv
store_id,timestamp_utc,status
1001,2023-01-25 12:09:39.388884 UTC,active
1002,2023-01-25 12:09:39.388884 UTC,inactive
```

#### menu_hours.csv
```csv
store_id,day_of_week,start_time_local,end_time_local
1001,0,00:00:00,23:59:59
1002,1,06:00:00,18:00:00
```

#### timezones.csv
```csv
store_id,timezone_str
1001,America/Chicago
1002,America/New_York
```

### Database Tables

The application creates optimized PostgreSQL tables:
- `store_status`: Historical status data with composite indexes
- `menu_hours`: Business hours per store and day
- `timezones`: Store timezone information
- `report_status`: Report generation tracking

## 📈 Output Format

### Generated CSV Report
```csv
store_id,uptime_last_hour,uptime_last_day,uptime_last_week,downtime_last_hour,downtime_last_day,downtime_last_week
1001,45.5,18.2,120.8,14.5,5.8,47.2
1002,60.0,22.5,155.3,0.0,1.5,12.7
```

**Column Descriptions:**
- `store_id`: Unique store identifier
- `uptime_last_hour`: Minutes of uptime in the last hour
- `uptime_last_day`: Hours of uptime in the last day
- `uptime_last_week`: Hours of uptime in the last week
- `downtime_last_hour`: Minutes of downtime in the last hour
- `downtime_last_day`: Hours of downtime in the last day
- `downtime_last_week`: Hours of downtime in the last week

## ⚡ Performance Optimizations

### Database Optimizations
- **Connection Pooling**: 20 connections with 30 overflow
- **Composite Indexes**: Multi-column indexes for faster queries
- **Chunked Processing**: 50K record chunks to prevent timeouts
- **Statement Timeouts**: 10-minute timeout for large operations

### Memory Management
- **Batch Processing**: Stores processed in batches of 100
- **Chunk Size Control**: Configurable chunk sizes for different datasets
- **Garbage Collection**: Explicit cleanup of large DataFrames

### API Optimizations
- **Asynchronous Processing**: Background report generation
- **Status Tracking**: Database-backed status monitoring
- **File Streaming**: Efficient file download handling

## 🛠️ Troubleshooting

### Common Issues

#### Database Connection Failed
```bash
# Check database URL format
DATABASE_URL = 'postgresql://user:password@host:port/database?sslmode=require'

# Test connection
python -c "from sqlalchemy import create_engine; engine = create_engine('your_db_url'); print('Connected!')"
```

#### Large Dataset Timeouts
- Reduce `chunk_size` in `process_csv_in_chunks()` function
- Increase `statement_timeout` in database connection
- Process smaller date ranges

#### Memory Issues
- Reduce `batch_size` in report generation
- Increase system RAM or use swap file
- Process data in smaller time windows

#### CSV Format Issues
- Check CSV column names match expected format
- Verify timestamp format consistency
- Ensure no missing required columns

### Logging
Check logs for detailed error information:
```bash
# The application logs all operations with timestamps
# Logs include data processing progress and error details
```

## 🔄 Development Workflow

### 1. Data Preparation
```bash
# Place CSV files in data/ directory
mkdir data
# Copy your CSV files to data/
```

### 2. Database Initialization
```bash
python database.py
```

### 3. Start API Server
```bash
python main.py
```

### 4. Test with Postman
- Import the provided Postman collection
- Run the test sequence
- Verify report generation

## 📝 Future Enhancements

See the [Notion documentation](https://www.notion.so/Take-home-interview-Store-Monitoring-25a51038046c80958973cc5ecd8e612c) for planned improvements and feature roadmap.

## 🔗 Sample Outputs

Example reports are available at:
- [Sample Report 1](https://docs.google.com/spreadsheets/d/1KGzZc6JZZ8PTXXdKaw1ziUE0gmCOb1CR71Ugs-FKgvI/edit?usp=sharing)
- [Sample Report 2](https://docs.google.com/spreadsheets/d/1bP-11xybxbNht3ZOkvqbXCZcn3dYKBUpDfgMCgUEcvc/edit?usp=sharing)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

**Note**: Make sure to update the database connection URL before running the application. The current URL in the code should be replaced with your actual database credentials.
