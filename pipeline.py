#!/usr/bin/env python3
"""
FlightOps Pulse Pipeline
Data Engineering Pipeline for Airline Flight Operations Data

Author: Claude (Sonnet 4.5)
Date: 2025-11-14
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error
import json
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class FlightOpsDataPipeline:
    """Main pipeline class for processing flight operations data"""
    
    def __init__(self, csv_path='flights.csv', output_dir='output'):
        """
        Initialize pipeline with file paths and configuration
        
        Args:
            csv_path: Path to input CSV file
            output_dir: Directory for output files
        """
        self.csv_path = csv_path
        self.output_dir = output_dir
        self.df_raw = None
        self.df_clean = None
        self.metrics = {}
        
        # MySQL configuration from environment
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': os.getenv('DB_NAME', 'flightops_db')
        }
        
        print("=" * 60)
        print("FlightOps Pulse Pipeline - Starting Execution")
        print("=" * 60)
    
    def ingest_data(self):
        """
        Step 1: Read CSV file and perform initial validation
        
        Returns:
            DataFrame: Raw flight data
        """
        print("\n[Step 1: Data Ingestion]")
        print(f"Reading CSV from: {self.csv_path}")
        
        try:
            # Read CSV file
            self.df_raw = pd.read_csv(self.csv_path)
            
            print(f"✓ Initial row count: {len(self.df_raw)}")
            print(f"✓ Columns found: {list(self.df_raw.columns)}")
            
            # Display sample data
            print("\nSample records:")
            print(self.df_raw.head(3).to_string())
            
            # Check for required columns
            required_cols = ['flight_id', 'aircraft_id', 'origin', 'destination',
                           'scheduled_departure', 'actual_departure', 
                           'scheduled_arrival', 'actual_arrival',
                           'status', 'delay_minutes', 'fare_usd']
            
            missing_cols = set(required_cols) - set(self.df_raw.columns)
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            print(f"✓ All required columns present")
            
            return self.df_raw
            
        except Exception as e:
            print(f"✗ Error during data ingestion: {e}")
            raise
    
    def cleanse_data(self):
        """
        Step 2: Cleanse data through deduplication and validation
        
        Returns:
            DataFrame: Cleaned flight data
        """
        print("\n[Step 2: Data Cleansing]")
        
        df = self.df_raw.copy()
        initial_count = len(df)
        
        # Step 2.1: Deduplication (BEFORE validation)
        print("\nDeduplication:")
        duplicates_count = df.duplicated(subset=['flight_id'], keep='first').sum()
        df = df.drop_duplicates(subset=['flight_id'], keep='first')
        print(f"✓ Removed {duplicates_count} duplicate flight_id entries")
        print(f"✓ Rows after deduplication: {len(df)}")
        
        # Step 2.2: Convert datetime columns
        print("\nDatetime conversion:")
        datetime_cols = ['scheduled_departure', 'actual_departure', 
                        'scheduled_arrival', 'actual_arrival']
        
        for col in datetime_cols:
            # Replace empty strings with NaN before conversion
            df[col] = df[col].replace('', pd.NaT)
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        print(f"✓ Converted datetime columns")
        
        # Step 2.3: Validation rules
        print("\nValidation:")
        rows_before = len(df)
        
        # Rule 1: fare_usd must be >= 0
        negative_fares = df['fare_usd'] < 0
        df = df[~negative_fares]
        print(f"✓ Removed {negative_fares.sum()} rows with negative fare_usd")
        
        # Rule 2: scheduled_departure must be <= scheduled_arrival
        invalid_schedule = df['scheduled_departure'] > df['scheduled_arrival']
        df = df[~invalid_schedule]
        print(f"✓ Removed {invalid_schedule.sum()} rows with invalid schedule")
        
        # Rule 3: Remove rows with missing critical fields
        critical_fields = ['flight_id', 'aircraft_id', 'origin', 'destination',
                          'scheduled_departure', 'scheduled_arrival', 'status', 'fare_usd']
        missing_critical = df[critical_fields].isnull().any(axis=1)
        df = df[~missing_critical]
        print(f"✓ Removed {missing_critical.sum()} rows with missing critical fields")
        
        rows_after = len(df)
        total_removed = rows_before - rows_after
        print(f"✓ Total rows removed by validation: {total_removed}")
        
        # Extract flight_date from scheduled_departure
        df['flight_date'] = df['scheduled_departure'].dt.date
        
        self.df_clean = df
        
        print(f"\n✓ Final cleansed dataset: {len(self.df_clean)} rows")
        print(f"✓ Data quality: {len(self.df_clean)/initial_count*100:.1f}% retained")
        
        return self.df_clean
    
    def calculate_metrics(self):
        """
        Step 3: Calculate all required metrics
        
        Returns:
            dict: Dictionary containing all metrics
        """
        print("\n[Step 3: Metrics Calculation]")
        
        df = self.df_clean.copy()
        metrics = {}
        
        # Metric 1: Total flights (after deduplication)
        metrics['total_flights'] = len(df)
        print(f"✓ Total flights: {metrics['total_flights']}")
        
        # Separate completed and cancelled flights
        df_completed = df[df['status'] == 'completed'].copy()
        df_cancelled = df[df['status'] == 'cancelled'].copy()
        
        metrics['completed_flights'] = len(df_completed)
        print(f"✓ Completed flights: {metrics['completed_flights']}")
        
        # Metric 2: Cancellation rate
        metrics['cancellation_rate'] = round(
            (len(df_cancelled) / metrics['total_flights']) * 100, 2
        ) if metrics['total_flights'] > 0 else 0.0
        print(f"✓ Cancellation rate: {metrics['cancellation_rate']}%")
        
        # Metric 3: Average delay (only for completed flights)
        # Calculate delay from timestamps (ignore delay_minutes column)
        if len(df_completed) > 0:
            df_completed['calculated_delay'] = (
                df_completed['actual_departure'] - df_completed['scheduled_departure']
            ).dt.total_seconds() / 60  # Convert to minutes
            
            metrics['average_delay_minutes'] = round(
                df_completed['calculated_delay'].mean(), 2
            )
        else:
            metrics['average_delay_minutes'] = 0.0
        
        print(f"✓ Average delay: {metrics['average_delay_minutes']} minutes")
        
        # Metric 4: Median duration (only for completed flights)
        if len(df_completed) > 0:
            df_completed['calculated_duration'] = (
                df_completed['actual_arrival'] - df_completed['actual_departure']
            ).dt.total_seconds() / 60  # Convert to minutes
            
            metrics['median_duration_minutes'] = round(
                df_completed['calculated_duration'].median(), 2
            )
        else:
            metrics['median_duration_minutes'] = 0.0
        
        print(f"✓ Median duration: {metrics['median_duration_minutes']} minutes")
        
        # Metric 5: Top 3 routes
        df['route'] = df['origin'] + '→' + df['destination']
        route_counts = df['route'].value_counts().head(3)
        
        metrics['top_routes'] = [
            {"route": route, "count": int(count)}
            for route, count in route_counts.items()
        ]
        
        print(f"✓ Top 3 routes:")
        for i, route in enumerate(metrics['top_routes'], 1):
            print(f"  {i}. {route['route']}: {route['count']} flights")
        
        # Metric 6: Aircraft utilization (count per aircraft_id)
        aircraft_counts = df['aircraft_id'].value_counts().to_dict()
        metrics['aircraft_utilization'] = {
            aircraft: int(count) for aircraft, count in aircraft_counts.items()
        }
        
        print(f"✓ Aircraft utilization:")
        for aircraft, count in sorted(metrics['aircraft_utilization'].items()):
            print(f"  {aircraft}: {count} flights")
        
        # Metric 7: Top 2 aircraft by revenue
        aircraft_revenue = df.groupby('aircraft_id')['fare_usd'].sum().sort_values(ascending=False).head(2)
        
        metrics['top_aircraft_by_revenue'] = [
            {"aircraft": aircraft, "revenue": round(float(revenue), 2)}
            for aircraft, revenue in aircraft_revenue.items()
        ]
        
        print(f"✓ Top 2 aircraft by revenue:")
        for i, aircraft in enumerate(metrics['top_aircraft_by_revenue'], 1):
            print(f"  {i}. {aircraft['aircraft']}: ${aircraft['revenue']:.2f}")
        
        self.metrics = metrics
        return metrics
    
    def persist_to_mysql(self):
        """
        Step 4: Persist cleansed data to MySQL database
        Uses INSERT IGNORE for idempotency
        """
        print("\n[Step 4: MySQL Persistence]")
        
        connection = None
        cursor = None
        
        try:
            # Connect to MySQL
            print(f"Connecting to MySQL at {self.db_config['host']}...")
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()
            
            print(f"✓ Connected to database: {self.db_config['database']}")
            
            # Prepare data for insertion
            df = self.df_clean.copy()
            
            # Convert datetime columns to strings for MySQL
            df['scheduled_departure_str'] = df['scheduled_departure'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df['scheduled_arrival_str'] = df['scheduled_arrival'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df['flight_date_str'] = df['flight_date'].astype(str)
            
            # Handle nullable datetime columns
            df['actual_departure_str'] = df['actual_departure'].apply(
                lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else None
            )
            df['actual_arrival_str'] = df['actual_arrival'].apply(
                lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else None
            )
            
            # INSERT IGNORE for idempotency (skips duplicates)
            insert_query = """
            INSERT IGNORE INTO flights (
                flight_id, aircraft_id, origin, destination,
                scheduled_departure, actual_departure,
                scheduled_arrival, actual_arrival,
                status, delay_minutes, fare_usd, flight_date
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            # Prepare data tuples
            data_tuples = []
            for _, row in df.iterrows():
                data_tuples.append((
                    row['flight_id'],
                    row['aircraft_id'],
                    row['origin'],
                    row['destination'],
                    row['scheduled_departure_str'],
                    row['actual_departure_str'],
                    row['scheduled_arrival_str'],
                    row['actual_arrival_str'],
                    row['status'],
                    None,  # delay_minutes (we calculate it, not store the CSV value)
                    float(row['fare_usd']),
                    row['flight_date_str']
                ))
            
            # Batch insert
            print(f"Inserting {len(data_tuples)} records...")
            cursor.executemany(insert_query, data_tuples)
            connection.commit()
            
            print(f"✓ Inserted {cursor.rowcount} new records (duplicates skipped)")
            
            # Verify insertion
            cursor.execute("SELECT COUNT(*) FROM flights")
            total_in_db = cursor.fetchone()[0]
            print(f"✓ Total records in database: {total_in_db}")
            
        except Error as e:
            print(f"✗ MySQL Error: {e}")
            if connection:
                connection.rollback()
            raise
            
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
                print("✓ MySQL connection closed")
    
    def export_metrics(self):
        """
        Step 5: Export metrics to JSON file
        """
        print("\n[Step 5: Metrics Export]")
        
        output_path = os.path.join(self.output_dir, 'metrics.json')
        
        try:
            # Ensure output directory exists
            os.makedirs(self.output_dir, exist_ok=True)
            
            # Write metrics to JSON file
            with open(output_path, 'w') as f:
                json.dump(self.metrics, f, indent=2)
            
            print(f"✓ Metrics exported to: {output_path}")
            print(f"\nMetrics Summary:")
            print(json.dumps(self.metrics, indent=2))
            
        except Exception as e:
            print(f"✗ Error exporting metrics: {e}")
            raise
    
    def run(self):
        """
        Main execution flow - orchestrates all pipeline steps
        """
        try:
            start_time = datetime.now()
            
            # Step 1: Ingest data
            self.ingest_data()
            
            # Step 2: Cleanse data
            self.cleanse_data()
            
            # Step 3: Calculate metrics
            self.calculate_metrics()
            
            # Step 4: Persist to MySQL
            self.persist_to_mysql()
            
            # Step 5: Export metrics
            self.export_metrics()
            
            # Summary
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print("\n" + "=" * 60)
            print("Pipeline Execution Complete!")
            print("=" * 60)
            print(f"✓ Total execution time: {duration:.2f} seconds")
            print(f"✓ Records processed: {len(self.df_clean)}")
            print(f"✓ Metrics calculated: {len(self.metrics)}")
            print(f"✓ Data persisted to MySQL: {self.db_config['database']}.flights")
            print(f"✓ Metrics exported to: {os.path.join(self.output_dir, 'metrics.json')}")
            print("=" * 60)
            
            return True
            
        except Exception as e:
            print(f"\n✗ Pipeline failed: {e}")
            import traceback
            traceback.print_exc()
            return False


if __name__ == "__main__":
    import sys
    
    # Accept optional command-line arguments
    csv_path = sys.argv[1] if len(sys.argv) > 1 else 'flights.csv'
    output_dir = sys.argv[2] if len(sys.argv) > 2 else 'output'
    
    # Initialize and run pipeline
    pipeline = FlightOpsDataPipeline(
        csv_path=csv_path,
        output_dir=output_dir
    )
    
    success = pipeline.run()
    
    # Exit with appropriate code
    exit(0 if success else 1)
