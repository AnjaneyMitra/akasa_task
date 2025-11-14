#!/usr/bin/env python3
"""
CSV Export Script
Converts metrics.json to multiple CSV files for analysis

Author: Claude (Sonnet 4.5)
Date: 2025-11-14
"""

import json
import csv
import os
from pathlib import Path


class MetricsCSVExporter:
    """Export metrics from JSON to CSV files"""
    
    def __init__(self, json_path='output/metrics.json', csv_dir='output/csv_outputs'):
        """
        Initialize CSV exporter
        
        Args:
            json_path: Path to metrics.json file
            csv_dir: Directory to save CSV files
        """
        self.json_path = json_path
        self.csv_dir = csv_dir
        self.metrics = None
        
        # Ensure output directory exists
        os.makedirs(self.csv_dir, exist_ok=True)
        
        print("=" * 60)
        print("Metrics CSV Export - Starting")
        print("=" * 60)
    
    def load_metrics(self):
        """Load metrics from JSON file"""
        print(f"\n[Loading Metrics]")
        print(f"Reading from: {self.json_path}")
        
        try:
            with open(self.json_path, 'r') as f:
                self.metrics = json.load(f)
            
            print(f"✓ Metrics loaded successfully")
            return self.metrics
            
        except FileNotFoundError:
            print(f"✗ Error: {self.json_path} not found")
            print("  Please run pipeline.py first to generate metrics")
            raise
        except json.JSONDecodeError as e:
            print(f"✗ Error: Invalid JSON format - {e}")
            raise
    
    def export_summary_metrics(self):
        """Export summary metrics to CSV"""
        print(f"\n[Exporting Summary Metrics]")
        
        output_file = os.path.join(self.csv_dir, 'summary_metrics.csv')
        
        # Create summary data
        summary_data = [
            {'Metric': 'Unique Flights', 'Value': self.metrics['unique_flights']},
            {'Metric': 'Completed Flights', 'Value': self.metrics['completed_flights']},
            {'Metric': 'Cancellation Rate', 'Value': self.metrics['cancellation_rate']},
            {'Metric': 'Average Delay', 'Value': self.metrics['average_delay']},
            {'Metric': 'Median Duration', 'Value': self.metrics['median_duration']},
        ]
        
        # Write to CSV
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['Metric', 'Value'])
            writer.writeheader()
            writer.writerows(summary_data)
        
        print(f"✓ Summary metrics exported to: {output_file}")
        return output_file
    
    def export_top_routes(self):
        """Export top routes to CSV"""
        print(f"\n[Exporting Top Routes]")
        
        output_file = os.path.join(self.csv_dir, 'top_routes.csv')
        
        # Parse top routes string
        # Format: "GOI→CCU (34), MUM→PNQ (31), BLR→CCU (29)"
        routes_str = self.metrics['top_routes']
        routes_list = routes_str.split(', ')
        
        routes_data = []
        for i, route_str in enumerate(routes_list, 1):
            # Parse "GOI→CCU (34)" format
            route, count = route_str.rsplit(' (', 1)
            count = count.rstrip(')')
            
            routes_data.append({
                'Rank': i,
                'Route': route,
                'Flight Count': int(count)
            })
        
        # Write to CSV
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['Rank', 'Route', 'Flight Count'])
            writer.writeheader()
            writer.writerows(routes_data)
        
        print(f"✓ Top routes exported to: {output_file}")
        print(f"  Routes exported: {len(routes_data)}")
        return output_file
    
    def export_aircraft_utilization(self):
        """Export aircraft utilization to CSV"""
        print(f"\n[Exporting Aircraft Utilization]")
        
        output_file = os.path.join(self.csv_dir, 'aircraft_utilization.csv')
        
        # Parse aircraft utilization string
        # Format: "B737 (167), A320 (166), B787 (164), ..."
        aircraft_str = self.metrics['aircraft_utilization']
        aircraft_list = aircraft_str.split(', ')
        
        aircraft_data = []
        for aircraft_str_item in aircraft_list:
            # Parse "B737 (167)" format
            aircraft, count = aircraft_str_item.rsplit(' (', 1)
            count = int(count.rstrip(')'))
            
            aircraft_data.append({
                'Aircraft Type': aircraft,
                'Flight Count': count,
                'Percentage': round((count / self.metrics['unique_flights']) * 100, 2)
            })
        
        # Sort by count descending
        aircraft_data.sort(key=lambda x: x['Flight Count'], reverse=True)
        
        # Write to CSV
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['Aircraft Type', 'Flight Count', 'Percentage'])
            writer.writeheader()
            writer.writerows(aircraft_data)
        
        print(f"✓ Aircraft utilization exported to: {output_file}")
        print(f"  Aircraft types: {len(aircraft_data)}")
        return output_file
    
    def export_top_revenue_aircraft(self):
        """Export top revenue aircraft to CSV"""
        print(f"\n[Exporting Top Revenue Aircraft]")
        
        output_file = os.path.join(self.csv_dir, 'top_revenue_aircraft.csv')
        
        # Parse top revenue string
        # Format: "B737 = $23081, A350 = $22666"
        revenue_str = self.metrics['top_aircraft_by_revenue']
        revenue_list = revenue_str.split(', ')
        
        revenue_data = []
        for i, rev_str in enumerate(revenue_list, 1):
            # Parse "B737 = $23081" format
            aircraft, revenue = rev_str.split(' = ')
            revenue = revenue.replace('$', '').replace(',', '')
            
            revenue_data.append({
                'Rank': i,
                'Aircraft Type': aircraft,
                'Total Revenue': f'${float(revenue):,.2f}'
            })
        
        # Write to CSV
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['Rank', 'Aircraft Type', 'Total Revenue'])
            writer.writeheader()
            writer.writerows(revenue_data)
        
        print(f"✓ Top revenue aircraft exported to: {output_file}")
        print(f"  Aircraft exported: {len(revenue_data)}")
        return output_file
    
    def export_all(self):
        """Export all metrics to CSV files"""
        try:
            # Load metrics
            self.load_metrics()
            
            # Export all CSV files
            files_created = []
            files_created.append(self.export_summary_metrics())
            files_created.append(self.export_top_routes())
            files_created.append(self.export_aircraft_utilization())
            files_created.append(self.export_top_revenue_aircraft())
            
            # Summary
            print("\n" + "=" * 60)
            print("CSV Export Complete!")
            print("=" * 60)
            print(f"✓ Files created: {len(files_created)}")
            for file in files_created:
                print(f"  - {file}")
            print("=" * 60)
            
            return True
            
        except Exception as e:
            print(f"\n✗ Export failed: {e}")
            import traceback
            traceback.print_exc()
            return False


if __name__ == "__main__":
    exporter = MetricsCSVExporter()
    success = exporter.export_all()
    exit(0 if success else 1)
