# FlightOps Pulse Pipeline

A production-ready data engineering pipeline that processes airline flight operations data, transforming raw CSV data into actionable insights stored in MySQL with calculated metrics exported to JSON.

## 📊 High-Level Overview

**Purpose:** Automate the ingestion, cleansing, and analysis of flight operations data to support real-time operational decision-making.

### Data Flow
```
flights.csv (1,000 rows)
    ↓
[Ingestion] → Read & Validate CSV
    ↓
[Cleansing] → Deduplicate (20 removed) → Validate (0 removed)
    ↓
[Processing] → Calculate 7 Metrics
    ↓
    ├──→ [MySQL] flightops_db.flights (980 rows) - Indexed by flight_date
    └──→ [JSON] output/metrics.json - 7 calculated metrics
```

### Architecture

**Pipeline Components:**
```
┌─────────────────────────────────────────────────────┐
│  FlightOpsDataPipeline (Python Class)               │
├─────────────────────────────────────────────────────┤
│  1. ingest_data()      → CSV → DataFrame            │
│  2. cleanse_data()     → Dedupe + Validate          │
│  3. calculate_metrics()→ 7 Key Metrics              │
│  4. persist_to_mysql() → Curated Dataset (980 rows) │
│  5. export_metrics()   → JSON Output                │
└─────────────────────────────────────────────────────┘
           ↓
    ┌──────────────────────────────────────┐
    │   Data Persistence Layer             │
    ├──────────────────────────────────────┤
    │ • MySQL Database (flightops_db)      │
    │   - Partitioned by flight_date       │
    │   - Idempotent inserts (INSERT IGNORE)│
    │ • JSON Metrics (output/metrics.json) │
    └──────────────────────────────────────┘
```

**Technology Stack:**
- **Language:** Python 3.x
- **Database:** MySQL 8.0
- **Libraries:** pandas, mysql-connector-python, python-dotenv
- **Execution Time:** ~0.17 seconds

### Input Data Schema
```
CSV Columns (11 fields):
├─ flight_id           (Unique identifier)
├─ aircraft_id         (Aircraft type: A320, B737, etc.)
├─ origin              (Departure airport code)
├─ destination         (Arrival airport code)
├─ scheduled_departure (ISO 8601 timestamp)
├─ actual_departure    (ISO 8601 or empty if cancelled)
├─ scheduled_arrival   (ISO 8601 timestamp)
├─ actual_arrival      (ISO 8601 or empty if cancelled)
├─ status              (completed/cancelled)
├─ delay_minutes       (Not used - recalculated)
└─ fare_usd            (Ticket price)
```

### Output Metrics (7 Key KPIs)
```json
{
  "unique_flights": 980,
  "completed_flights": 823,
  "cancellation_rate": "16.02%",
  "average_delay": "12.91 min",
  "median_duration": "~141 min",
  "top_routes": "GOI→CCU (34), MUM→PNQ (31), BLR→CCU (29)",
  "aircraft_utilization": {
    "B737": 167, "A320": 166, "B787": 164,
    "B777": 163, "A350": 161, "A321": 159
  },
  "top_aircraft_by_revenue": {
    "B737": "$23081.00", "A350": "$22666.00"
  }
}
```

### How It Works

**Execution:**
```bash
# One-line execution
python pipeline.py
```

**Processing Steps:**
1. **Ingest** - Reads `flights.csv`, validates 11 required columns
2. **Deduplicate** - Removes duplicates by `flight_id` (20 removed)
3. **Cleanse** - Applies validation rules (negative fares, invalid schedules)
4. **Calculate** - Computes 7 metrics from 980 clean records
5. **Persist** - Stores to MySQL with `INSERT IGNORE` (idempotent)
6. **Export** - Writes metrics to `output/metrics.json`

**Key Features:**
- ✅ **Idempotent:** Safe to re-run (no duplicates created)
- ✅ **Fast:** Processes 1,000 rows in ~0.17 seconds
- ✅ **Reliable:** 98% data retention after cleansing
- ✅ **Indexed:** MySQL partitioned by `flight_date` for performance

### Pipeline Output

**Console:**
```
============================================================
FlightOps Pulse Pipeline - Starting Execution
============================================================
[Step 1: Data Ingestion]
✓ Initial row count: 1000

[Step 2: Data Cleansing]
✓ Removed 20 duplicate flight_id entries
✓ Final cleansed dataset: 980 rows

[Step 3: Metrics Calculation]
✓ Total flights: 980
✓ Cancellation rate: 16.02%
...

[Step 4: MySQL Persistence]
✓ Inserted 980 records

[Step 5: Metrics Export]
✓ Metrics exported to: output/metrics.json
============================================================
Pipeline Execution Complete!
============================================================
```

**Database:**
- Table: `flightops_db.flights` (980 rows)
- Indexed on: `flight_date`, `aircraft_id`, `origin+destination`

**JSON File:**
- Location: `output/metrics.json`
- Contains: All 7 calculated metrics in structured format

---

## 🚀 Quick Start

```bash
# 1. Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Setup database
mysql -u root -p < schema.sql

# 4. Configure credentials
cp .env.example .env
# Edit .env with your MySQL password

# 5. Run pipeline
python pipeline.py
```

## Prerequisites

- **Python 3.7+** (tested on Python 3.13)
- **MySQL 8.0+** (server must be running)
- **Dependencies:** pandas, mysql-connector-python, python-dotenv

---

## 📖 Usage & Configuration

### Running the Pipeline

```bash
python pipeline.py
```

### Configuration (.env file)

```env
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_mysql_password
DB_NAME=flightops_db
```

---

## 📈 Metrics Explained

| Metric | Description | Calculation |
|--------|-------------|-------------|
| **Unique Flights** | Total flights after deduplication | Count of unique `flight_id` |
| **Completed Flights** | Successfully completed flights | Count where `status='completed'` |
| **Cancellation Rate** | Percentage of cancelled flights | `(cancelled / total) × 100` |
| **Average Delay** | Mean departure delay | `Mean(actual_departure - scheduled_departure)` |
| **Median Duration** | Median flight duration | `Median(actual_arrival - actual_departure)` |
| **Top Routes** | Most frequent routes | Top 3 by count of `origin→destination` |
| **Aircraft Utilization** | Flights per aircraft type | Count per `aircraft_id` |
| **Top Revenue Aircraft** | Highest earning aircraft | Top 2 by `sum(fare_usd)` per aircraft |

**Important Notes:**
- Delay & duration calculated from timestamps (CSV `delay_minutes` column ignored)
- Only completed flights used for delay/duration calculations
- Deduplication happens BEFORE validation

---

## 🔧 Data Processing Rules

### Cleansing Pipeline
1. **Deduplication** (BEFORE validation)
   - Remove duplicates by `flight_id`
   - Keep first occurrence

2. **Validation** (removes rows if)
   - `fare_usd < 0` (negative fares)
   - `scheduled_departure > scheduled_arrival` (invalid schedule)
   - Missing critical fields (flight_id, origin, destination, etc.)

3. **Timestamp Handling**
   - Format: ISO 8601 (`2025-10-06T04:06:00`)
   - Cancelled flights: empty `actual_departure` and `actual_arrival`
   - `flight_date` extracted from `scheduled_departure`

### Idempotency ✅
- **Safe to re-run** multiple times
- Uses `INSERT IGNORE` in MySQL
- Primary key constraint prevents duplicates
- No data duplication on multiple runs

---

## 🗂️ Project Structure

```
akasa_assignment/
├── pipeline.py           # Main pipeline (426 lines)
├── flights.csv          # Input data (1,000 rows)
├── requirements.txt     # Python dependencies
├── schema.sql          # MySQL database schema
├── .env                # Database credentials (create from .env.example)
├── .env.example        # Template for .env
├── README.md           # This file
├── scripts/
│   ├── export_to_csv.py          # Export metrics to CSV files
│   └── generate_visualizations.py # Create charts from metrics
└── output/
    ├── metrics.json              # Generated metrics (7 KPIs)
    ├── csv_outputs/              # CSV exports (4 files)
    └── visualizations/           # Charts and graphs (5 PNG files)
```

### Additional Scripts

**Export to CSV:**
```bash
python scripts/export_to_csv.py
```
Exports metrics to 4 CSV files: summary metrics, top routes, aircraft utilization, and top revenue aircraft.

**Generate Visualizations:**
```bash
python scripts/generate_visualizations.py
```
Creates 5 charts: flight status distribution, top routes, aircraft utilization, revenue by aircraft, and metrics dashboard.

> **Note:** Visualization requires `matplotlib`. Install with: `pip install matplotlib`

---

## ⚠️ Troubleshooting

| Issue | Solution |
|-------|----------|
| **MySQL connection error** | Check `.env` credentials, ensure MySQL is running |
| **Database doesn't exist** | Run `mysql -u root -p < schema.sql` |
| **Module not found** | Run `pip install -r requirements.txt` |
| **Empty metrics** | Verify `flights.csv` exists and has correct format |

---

## 📋 Assumptions

- CSV file named `flights.csv` in project root
- MySQL server running on localhost
- Timestamps in ISO 8601 format (no timezone conversion)
- `aircraft_id` represents aircraft type (e.g., A320, B737)
- First occurrence kept for duplicate `flight_id`
- Cancelled flights have empty actual times and $0 fare

---

## 📊 Performance & Quality

- **Execution Time:** ~0.17 seconds for 1,000 rows
- **Data Retention:** 98.0% (20 duplicates removed)
- **Database:** Indexed on `flight_date`, `aircraft_id`, `route`
- **Scalability:** Batch inserts, efficient pandas operations

---

## 📝 License

Educational project - no license restrictions.

---

**Built with ❤️ for Akasa Air - Data Engineering Assignment**
