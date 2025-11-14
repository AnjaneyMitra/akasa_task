# Important Constraints & Development Notes

## ⏱️ Time Constraints
- **Total time available:** 60 minutes
- **Code development:** ~45 minutes
- **Testing & documentation:** ~15 minutes
- **Priority:** Correctness > Optimization > Polish

---

## 🎯 Critical Requirements

### 1. Idempotency (CRITICAL)
⚠️ **The pipeline MUST be safe to re-run multiple times without duplicating data**

**Implementation approaches:**
- Use `INSERT IGNORE` in MySQL (skips duplicates)
- Use `REPLACE INTO` (updates existing records)
- Use `ON DUPLICATE KEY UPDATE` with primary key constraint
- Clear and reload strategy with truncate (less preferred)

**Test:** Run pipeline twice with same CSV - database should have same record count

### 2. Deduplication Logic
- Deduplicate by `flight_id` column
- **Keep first occurrence** of duplicates
- Apply deduplication **before** validation
- Log how many duplicates were removed

### 3. Validation Rules (STRICT)
Must remove records that violate:
1. `fare_usd < 0` → INVALID (remove)
2. `scheduled_departure > scheduled_arrival` → INVALID (remove)
3. Missing critical fields → INVALID (remove)

**Order matters:** Deduplicate first, then validate

---

## 📊 Metrics Calculation Rules

### Delay Calculation
- **Only for completed flights** (status == 'completed')
- **CRITICAL:** Ignore the `delay_minutes` column in CSV
- Recalculate: Delay = `actual_departure - scheduled_departure`
- Convert to **minutes** (timestamps are in ISO format)
- Average of all completed flights
- Handle empty `actual_departure` (cancelled flights) by filtering them out first

### Duration Calculation
- **Only for completed flights** (status == 'completed')
- Duration = `actual_arrival - actual_departure`
- Result in **minutes**
- **Median** (not average)
- Handle empty timestamps for cancelled flights by filtering first

### Cancellation Rate
- Formula: `(cancelled_flights / total_flights) * 100`
- Express as percentage
- Total flights = after deduplication

### Top Routes
- Format: `"ORIGIN→DESTINATION"`
- Count flights per route
- Return **top 3** by count
- Sort descending by count

### Aircraft Utilization
- Count flights per `aircraft_id` (not aircraft_type)
- Include all aircraft types present in the data
- Simple count, not percentage
- Example: A320, B737, B787, A321, A350, B777

### Top Aircraft by Revenue
- Sum `fare_usd` per `aircraft_id` (not aircraft_type)
- Return **top 2** by revenue
- Sort descending by total revenue
- Note: fare_usd is 0 for cancelled flights

---

## 🗄️ MySQL Considerations

### Connection Management
- Use connection pooling or explicit close
- Handle connection failures gracefully
- Don't hardcode credentials (use `.env`)

### Table Design
- `flight_id` should be PRIMARY KEY (ensures uniqueness)
- Use appropriate data types:
  - VARCHAR(10) for flight_id
  - VARCHAR(10) for aircraft_id
  - VARCHAR(5) for origin, destination (airport codes)
  - DATETIME for timestamp fields
  - DECIMAL(10,2) for fare_usd
  - VARCHAR(10) for status
  - INT for delay_minutes (optional, store pre-calculated)
  - DATE for flight_date (extract from scheduled_departure)
- Add INDEX on `flight_date` for partitioning/querying
- Handle NULL values for actual_departure/actual_arrival (cancelled flights)

### Data Loading Strategy
```sql
-- Preferred approach for idempotency:
INSERT IGNORE INTO flights (flight_id, ...)
VALUES (?, ...)

-- Or with primary key:
CREATE TABLE flights (
    flight_id VARCHAR(50) PRIMARY KEY,
    ...
)
```

---

## 📁 Expected Data Format

### CSV Columns (actual structure)
- `flight_id` - Unique identifier (has duplicates that need deduplication)
- `aircraft_id` - Aircraft type (e.g., B737, A320, B787)
- `origin` - Airport code (e.g., MUM, DEL, BLR)
- `destination` - Airport code
- `scheduled_departure` - ISO format timestamp (e.g., 2025-10-06T04:06:00)
- `actual_departure` - ISO format timestamp (empty for cancelled)
- `scheduled_arrival` - ISO format timestamp
- `actual_arrival` - ISO format timestamp (empty for cancelled)
- `status` - "completed" or "cancelled"
- `delay_minutes` - Pre-calculated delay (DO NOT USE - recalculate from timestamps)
- `fare_usd` - Fare amount

**CRITICAL NOTES:**
- NO separate `flight_date` column - must extract from `scheduled_departure`
- Cancelled flights have empty strings for `actual_departure` and `actual_arrival`
- Use `aircraft_id` as the aircraft type for metrics
- The `delay_minutes` column exists but ignore it - calculate from actual timestamps

### Date/Time Handling
- Parse datetime strings correctly (ISO 8601 format: `2025-10-06T04:06:00`)
- Use pandas `pd.to_datetime()` for conversion
- Extract flight_date from `scheduled_departure` as DATE only
- Handle timezone (timestamps appear to be naive/local time)
- Calculate time differences in minutes
- Empty strings for timestamps indicate cancelled flights

---

## ⚠️ Common Pitfalls to Avoid

### 1. Metric Calculation Errors
- ❌ Don't use the `delay_minutes` column from CSV (recalculate from timestamps)
- ❌ Don't include cancelled flights in delay/duration calculations
- ❌ Don't use average for median duration
- ❌ Don't forget to multiply by 100 for percentage
- ❌ Don't use `aircraft_type` - the column is `aircraft_id`

### 2. Deduplication Mistakes
- ❌ Don't deduplicate after loading to MySQL
- ❌ Don't keep last occurrence (must keep first)
- ❌ Don't deduplicate by other columns
- ⚠️ There ARE actual duplicates in the data (check row counts)

### 3. Idempotency Failures
- ❌ Don't use plain `INSERT` without handling duplicates
- ❌ Don't append without checking existence
- ❌ Don't forget PRIMARY KEY constraint

### 4. Data Type Issues
- ❌ Don't treat times as strings for calculations
- ❌ Don't use integer for fare (use decimal/float)
- ❌ Don't ignore null/NaN/empty string values in calculations
- ❌ Don't assume `flight_date` column exists (must extract from timestamp)

### 5. Empty String Handling
- ❌ Empty strings ('') for actual_departure/actual_arrival indicate cancelled flights
- ❌ Must convert empty strings to NaT/None before time calculations
- ❌ Don't try to calculate delay/duration on cancelled flights

---

## ✅ Validation Against Sample Data

**After analyzing the actual CSV, here are key observations:**

1. **Total rows in CSV:** 1,231 rows
2. **Duplicates present:** Yes - flight_id appears multiple times (e.g., F690, F872, F451, F924, F102, etc.)
3. **Cancelled flights:** Many flights with status='cancelled' and empty actual timestamps
4. **Aircraft types present:** A320, A321, A350, B737, B777, B787
5. **Date range:** 2025-10-05 to 2025-10-07 (3 days)
6. **Empty strings:** Used for actual_departure/arrival in cancelled flights

**Expected outputs after processing:**
- Unique flights: ~1,100-1,150 (after deduplication)
- Cancellation rate: ~10-15%
- Top routes will be among: MUM, DEL, BLR, CCU, GOI, HYD, PNQ
- Aircraft types: 6 different types (A320, A321, A350, B737, B777, B787)

**If your results differ significantly, check:**
1. Deduplication by flight_id (keep first occurrence)
2. Filtering completed vs cancelled correctly
3. Empty string handling for timestamps
4. Time calculation units (minutes)
5. Using aircraft_id not aircraft_type
6. Ignoring the delay_minutes column

---

## 🔧 Dependencies & Environment

### Python Version
- Minimum: Python 3.7
- Recommended: Python 3.9+

### Required Libraries
- `pandas` - Data manipulation
- `mysql-connector-python` - MySQL driver
- `python-dotenv` - Environment variables

### MySQL Version
- MySQL 5.7+ or MySQL 8.0+
- Ensure server is running before pipeline execution

---

## 📝 Output Format

### metrics.json Structure
```json
{
  "total_flights": 10,
  "completed_flights": 8,
  "cancellation_rate": 20.0,
  "average_delay_minutes": 11.25,
  "median_duration_minutes": 120.0,
  "top_routes": [
    {"route": "MUM→DEL", "count": 3},
    {"route": "DEL→BLR", "count": 2},
    {"route": "MUM→BLR", "count": 1}
  ],
  "aircraft_utilization": {
    "A320": 4,
    "B737": 3,
    "B787": 2
  },
  "top_aircraft_by_revenue": [
    {"aircraft": "A320", "revenue": 525.0},
    {"aircraft": "B737", "revenue": 420.0}
  ]
}
```

---

## 🚀 Minimal Viable Product (MVP) Focus

**Must have:**
- Data ingestion from CSV
- Deduplication and validation
- All 7 metrics calculated
- MySQL persistence with idempotency
- JSON export

**Nice to have (if time permits):**
- Logging to file
- Error recovery
- Progress bars
- Unit tests
- Command-line arguments

**Skip for MVP:**
- Web interface
- Data visualization
- Advanced partitioning strategies
- Performance optimization
- Extensive error handling for edge cases

---

## 🎓 Key Assumptions to Document

Document these in README.md:
1. CSV file location and naming convention
2. MySQL connection details (localhost assumed)
3. Duplicate handling strategy (keep first)
4. Timezone handling for timestamps
5. Invalid data exclusion criteria
6. Missing value treatment