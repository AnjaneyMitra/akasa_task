# Instructions for Claude Sonnet 4.5: FlightOps Pulse Pipeline Development

## Context
You are helping build a minimal data engineering pipeline for airline flight operations data. The solution uses **Python 3.x** and **MySQL** to process CSV data, compute metrics, and persist results.

**Time constraint:** 60 minutes total
**Environment:** VSCode on local machine

---

## Step-by-Step Development Process

### Step 1: Project Setup (5 minutes)
1. Create project directory structure:
   ```
   flightops-pipeline/
   ├── data/
   │   └── flights.csv          # Input CSV file
   ├── output/
   │   └── metrics.json         # Generated metrics
   ├── pipeline.py              # Main pipeline script
   ├── requirements.txt         # Python dependencies
   └── README.md               # Documentation
   ```

2. Create `requirements.txt` with:
   ```
   pandas
   mysql-connector-python
   python-dotenv
   ```

3. Create `.env` file for MySQL credentials:
   ```
   DB_HOST=localhost
   DB_USER=root
   DB_PASSWORD=your_password
   DB_NAME=flightops_db
   ```

### Step 2: MySQL Database Setup (5 minutes)
1. Create SQL schema for the curated dataset table
2. Design table with columns matching CSV structure
3. Add partitioning by flight date (or indexed date column)
4. Include primary key constraint on `flight_id` for idempotency

Provide the SQL DDL statements to create:
- Database
- Table with appropriate data types
- Indexes for performance

### Step 3: Data Ingestion Module (10 minutes)
Create function to:
1. Read CSV file using pandas
2. Display initial row count and sample data
3. Check for required columns
4. Handle missing values appropriately

**Expected columns:** flight_id, aircraft_id, origin, destination, scheduled_departure, actual_departure, scheduled_arrival, actual_arrival, status, delay_minutes, fare_usd

**Note:** 
- There is NO separate `flight_date` column - extract date from `scheduled_departure`
- `delay_minutes` column exists but should NOT be used for calculations (calculate from timestamps)
- `aircraft_id` exists but use this as the aircraft type identifier
- Cancelled flights have empty `actual_departure` and `actual_arrival` values

### Step 4: Data Cleansing Module (10 minutes)
Implement validation logic:

1. **Deduplication:**
   - Remove duplicate `flight_id` entries
   - Keep first occurrence
   - Log how many duplicates removed

2. **Validation rules:**
   - `fare_usd >= 0` (remove negatives)
   - `scheduled_departure <= scheduled_arrival` (remove invalid)
   - Remove rows with null critical fields

3. Report cleansing statistics

### Step 5: Metrics Calculation Module (15 minutes)
Implement functions to calculate:

1. **Total flights** - Count after deduplication
2. **Cancellation rate** - (cancelled / total) * 100
3. **Average delay** - Mean of (actual_departure - scheduled_departure) for completed flights
4. **Median duration** - Median of (actual_arrival - actual_departure) for completed flights
5. **Top 3 routes** - Group by (origin, destination), count, sort, take top 3
6. **Aircraft utilization** - Count flights per aircraft_type
7. **Top 2 aircraft by revenue** - Sum fare_usd per aircraft_type, take top 2

Each metric should handle edge cases (empty data, all cancelled, etc.)

### Step 6: MySQL Persistence Module (8 minutes)
Implement idempotent data loading:

1. Connect to MySQL using credentials from `.env`
2. Use `INSERT IGNORE` or `REPLACE INTO` for idempotency
3. Batch insert cleansed data
4. Extract and store flight_date from `scheduled_departure` timestamp
5. Add INDEX on flight_date for efficient querying
6. Handle connection errors gracefully
7. Close connections properly

### Step 7: Metrics Export (3 minutes)
1. Structure metrics as JSON object
2. Write to `output/metrics.json`
3. Format for readability (indented JSON)

### Step 8: Main Pipeline Orchestration (2 minutes)
Create main execution flow:
```python
if __name__ == "__main__":
    # 1. Load and validate config
    # 2. Ingest CSV
    # 3. Cleanse data
    # 4. Calculate metrics
    # 5. Persist to MySQL
    # 6. Export metrics to JSON
    # 7. Print summary
```

### Step 9: Documentation (2 minutes)
Create README.md with:
1. Prerequisites (Python 3.x, MySQL)
2. Installation steps
3. Configuration (`.env` setup)
4. How to run: `python pipeline.py`
5. Expected output location
6. Assumptions made

---

## Development Guidelines

### Code Quality
- Use clear variable names
- Add docstrings to functions
- Include inline comments for complex logic
- Handle exceptions with try-except blocks
- Log progress at each major step

### Testing Approach
- Print intermediate results during development
- Verify row counts at each stage
- Check sample records after transformations
- Validate metrics against expected outputs

### Incremental Development
1. Build one module at a time
2. Test each module independently
3. Integration test after Step 8
4. Refine if time permits

---

## Deliverable Checklist

Before completion, ensure:
- [ ] All 7 metrics calculated correctly
- [ ] Pipeline runs without errors
- [ ] Curated data in MySQL
- [ ] metrics.json generated
- [ ] Pipeline is idempotent (can re-run safely)
- [ ] README.md complete
- [ ] Code is commented
- [ ] Expected outputs match sample data

---

## Sample Interaction Flow

**User provides:** CSV file with flight data

**Claude should:**
1. Analyze CSV structure
2. Generate complete `pipeline.py` with all modules
3. Provide SQL setup script
4. Create requirements.txt and README.md
5. Explain how to run and verify results

**Output locations:**
- Cleansed data → MySQL `flightops_db.flights` table
- Metrics → `output/metrics.json`
- Logs → Console output