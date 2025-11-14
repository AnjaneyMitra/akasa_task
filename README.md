# FlightOps Pulse Pipeline

A data engineering pipeline for processing airline flight operations data. This pipeline ingests CSV flight data, cleanses and validates it, calculates key operational metrics, and persists results to MySQL.

## Overview

The pipeline performs the following operations:
1. **Data Ingestion** - Reads flight data from CSV
2. **Data Cleansing** - Deduplicates and validates records
3. **Metrics Calculation** - Computes 7 key operational metrics
4. **MySQL Persistence** - Stores curated data in MySQL database
5. **Metrics Export** - Outputs metrics to JSON file

## Prerequisites

### Software Requirements
- **Python 3.7+** (Python 3.9+ recommended)
- **MySQL 5.7+** or **MySQL 8.0+**
- MySQL server must be running

### Python Dependencies
- pandas
- mysql-connector-python
- python-dotenv

## Installation

### 1. Clone or Download Project
```bash
cd /path/to/flightops-pipeline
```

### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure MySQL Database

**Option A: Using the provided SQL script**
```bash
mysql -u root -p < schema.sql
```

**Option B: Manual setup**
```sql
CREATE DATABASE flightops_db;
USE flightops_db;

CREATE TABLE flights (
    flight_id VARCHAR(10) PRIMARY KEY,
    aircraft_id VARCHAR(10) NOT NULL,
    origin VARCHAR(5) NOT NULL,
    destination VARCHAR(5) NOT NULL,
    scheduled_departure DATETIME NOT NULL,
    actual_departure DATETIME,
    scheduled_arrival DATETIME NOT NULL,
    actual_arrival DATETIME,
    status VARCHAR(10) NOT NULL,
    delay_minutes INT,
    fare_usd DECIMAL(10,2) NOT NULL,
    flight_date DATE NOT NULL,
    INDEX idx_flight_date (flight_date),
    INDEX idx_status (status),
    INDEX idx_aircraft (aircraft_id)
);
```

### 4. Configure Environment Variables

Create a `.env` file in the project root:
```env
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_mysql_password
DB_NAME=flightops_db
```

**Important:** Replace `your_mysql_password` with your actual MySQL root password.

## Usage

### Running the Pipeline

```bash
python pipeline.py
```

### Expected Output

The pipeline will:
1. Read `flights.csv` from the current directory
2. Display progress for each step
3. Create `output/metrics.json` with calculated metrics
4. Persist cleansed data to MySQL `flightops_db.flights` table

### Output Files

- **output/metrics.json** - Contains all calculated metrics in JSON format

### Sample Metrics Output

```json
{
  "total_flights": 1150,
  "completed_flights": 980,
  "cancellation_rate": 14.78,
  "average_delay_minutes": 15.23,
  "median_duration_minutes": 135.5,
  "top_routes": [
    {"route": "MUM→DEL", "count": 45},
    {"route": "DEL→BLR", "count": 42},
    {"route": "BLR→MUM", "count": 38}
  ],
  "aircraft_utilization": {
    "A320": 250,
    "B737": 220,
    "A321": 180,
    "B787": 175,
    "A350": 165,
    "B777": 160
  },
  "top_aircraft_by_revenue": [
    {"aircraft": "B787", "revenue": 35420.50},
    {"aircraft": "A350", "revenue": 32150.75}
  ]
}
```

## Metrics Explained

### 1. Total Flights
Count of unique flights after deduplication.

### 2. Completed Flights
Count of flights with status='completed'.

### 3. Cancellation Rate
Percentage of cancelled flights: `(cancelled / total) × 100`

### 4. Average Delay Minutes
Mean delay for completed flights, calculated from actual vs scheduled departure times.
- **Note:** Delay is recalculated from timestamps, not using the CSV's `delay_minutes` column.

### 5. Median Duration Minutes
Median flight duration for completed flights, calculated from actual arrival - actual departure.

### 6. Top 3 Routes
Top 3 most frequent routes in format "ORIGIN→DESTINATION".

### 7. Aircraft Utilization
Count of flights per aircraft type (all aircraft types).

### 8. Top 2 Aircraft by Revenue
Top 2 aircraft types by total revenue (sum of fare_usd).

## Data Processing Rules

### Deduplication
- Duplicates identified by `flight_id`
- **First occurrence is kept**
- Applied before validation

### Validation Rules
Records are removed if they violate:
1. `fare_usd < 0` (negative fares)
2. `scheduled_departure > scheduled_arrival` (invalid schedule)
3. Missing values in critical fields

### Timestamp Handling
- Date/time format: ISO 8601 (`2025-10-06T04:06:00`)
- Cancelled flights have empty `actual_departure` and `actual_arrival`
- `flight_date` is extracted from `scheduled_departure`

### Calculation Rules
- **Delay calculation:** Only for completed flights, recalculated from timestamps
- **Duration calculation:** Only for completed flights (actual_arrival - actual_departure)
- **Revenue calculation:** Sum of `fare_usd` (cancelled flights have $0 fare)

## Idempotency

The pipeline is **safe to re-run** multiple times:
- Uses `INSERT IGNORE` to skip duplicate `flight_id` entries
- PRIMARY KEY constraint on `flight_id` prevents duplicates
- Re-running with the same CSV will not create duplicate records

**Test:**
```bash
python pipeline.py  # First run
python pipeline.py  # Second run - no duplicates created
```

## Project Structure

```
flightops-pipeline/
├── flights.csv              # Input CSV file
├── pipeline.py              # Main pipeline script
├── schema.sql              # MySQL database schema
├── requirements.txt         # Python dependencies
├── .env                    # Database credentials (create this)
├── README.md               # This file
└── output/
    └── metrics.json        # Generated metrics
```

## Troubleshooting

### MySQL Connection Error
```
Error: Access denied for user 'root'@'localhost'
```
**Solution:** Check your `.env` file has correct MySQL credentials.

### Database Does Not Exist
```
Error: Unknown database 'flightops_db'
```
**Solution:** Run the `schema.sql` script to create the database.

### Module Not Found
```
ModuleNotFoundError: No module named 'pandas'
```
**Solution:** Install dependencies with `pip install -r requirements.txt`

### Empty Metrics
If metrics show zero values, check:
1. CSV file is in the correct location (`flights.csv`)
2. CSV has the expected column structure
3. Data quality (check for excessive duplicates or invalid records)

## Assumptions

1. **CSV Location:** `flights.csv` is in the same directory as `pipeline.py`
2. **MySQL Connection:** MySQL server is running on localhost
3. **Timestamp Format:** All timestamps in ISO 8601 format (YYYY-MM-DDTHH:MM:SS)
4. **Timezone:** Timestamps are in local time (no timezone conversion)
5. **Duplicate Handling:** First occurrence of duplicate `flight_id` is kept
6. **Cancelled Flights:** Have empty strings for `actual_departure` and `actual_arrival`
7. **Aircraft Identifier:** The `aircraft_id` column is used for aircraft type (e.g., A320, B737)

## Technical Notes

### Performance
- Batch insertion for MySQL (all records in single transaction)
- Pandas used for efficient data manipulation
- Indexed columns in MySQL for faster queries

### Data Quality
- Records must pass all validation rules
- Timestamps must be valid and parseable
- Critical fields cannot be null

### Future Enhancements
- Command-line arguments for file paths
- Logging to file
- Data quality reports
- Incremental processing
- Email notifications

## Development Time

**Total development time:** ~45 minutes
- Project setup: 5 min
- Database schema: 5 min
- Data ingestion: 5 min
- Data cleansing: 10 min
- Metrics calculation: 15 min
- MySQL persistence: 5 min
- Documentation: 5 min

## License

Educational project - no license restrictions.

## Contact

For issues or questions, refer to the pipeline output logs for debugging information.
