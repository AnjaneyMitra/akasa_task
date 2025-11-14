#!/usr/bin/env python3
"""
Visualization Script
Creates charts and graphs from metrics.json

Author: Claude (Sonnet 4.5)
Date: 2025-11-14
"""

import json
import os
from pathlib import Path

try:
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False


class MetricsVisualizer:
    """Generate visualizations from metrics data"""
    
    def __init__(self, json_path='output/metrics.json', viz_dir='output/visualizations'):
        """
        Initialize visualizer
        
        Args:
            json_path: Path to metrics.json file
            viz_dir: Directory to save visualizations
        """
        self.json_path = json_path
        self.viz_dir = viz_dir
        self.metrics = None
        
        # Ensure output directory exists
        os.makedirs(self.viz_dir, exist_ok=True)
        
        print("=" * 60)
        print("Metrics Visualization - Starting")
        print("=" * 60)
    
    def check_dependencies(self):
        """Check if required libraries are installed"""
        if not MATPLOTLIB_AVAILABLE:
            print("\n⚠️  WARNING: matplotlib not installed")
            print("Install with: pip install matplotlib")
            return False
        return True
    
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
    
    def plot_flight_status_distribution(self):
        """Create pie chart for completed vs cancelled flights"""
        print(f"\n[Creating Flight Status Distribution Chart]")
        
        completed = self.metrics['completed_flights']
        total = self.metrics['unique_flights']
        cancelled = total - completed
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Data
        sizes = [completed, cancelled]
        labels = [f'Completed\n({completed} flights)', f'Cancelled\n({cancelled} flights)']
        colors = ['#4CAF50', '#F44336']
        explode = (0.05, 0.05)
        
        # Create pie chart
        wedges, texts, autotexts = ax.pie(
            sizes, 
            labels=labels, 
            colors=colors,
            autopct='%1.1f%%',
            startangle=90,
            explode=explode,
            textprops={'fontsize': 11, 'weight': 'bold'}
        )
        
        # Equal aspect ratio ensures circular pie
        ax.axis('equal')
        
        plt.title('Flight Status Distribution', fontsize=14, weight='bold', pad=20)
        
        output_file = os.path.join(self.viz_dir, '01_flight_status_distribution.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"✓ Chart saved: {output_file}")
        return output_file
    
    def plot_top_routes(self):
        """Create horizontal bar chart for top routes"""
        print(f"\n[Creating Top Routes Chart]")
        
        # Parse top routes string
        routes_str = self.metrics['top_routes']
        routes_list = routes_str.split(', ')
        
        routes = []
        counts = []
        for route_str in routes_list:
            route, count = route_str.rsplit(' (', 1)
            count = count.rstrip(')')
            routes.append(route)
            counts.append(int(count))
        
        # Create horizontal bar chart
        fig, ax = plt.subplots(figsize=(10, 6))
        
        bars = ax.barh(routes, counts, color='#2196F3', edgecolor='black', linewidth=1.2)
        
        # Add value labels on bars
        for i, (bar, count) in enumerate(zip(bars, counts)):
            ax.text(count + 0.5, i, str(count), va='center', fontsize=10, weight='bold')
        
        ax.set_xlabel('Number of Flights', fontsize=11, weight='bold')
        ax.set_ylabel('Route', fontsize=11, weight='bold')
        ax.set_title('Top 3 Most Frequent Routes', fontsize=14, weight='bold', pad=20)
        ax.grid(axis='x', alpha=0.3, linestyle='--')
        
        plt.tight_layout()
        
        output_file = os.path.join(self.viz_dir, '02_top_routes.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"✓ Chart saved: {output_file}")
        return output_file
    
    def plot_aircraft_utilization(self):
        """Create bar chart for aircraft utilization"""
        print(f"\n[Creating Aircraft Utilization Chart]")
        
        # Parse aircraft utilization string
        # Format: "B737 (167), A320 (166), B787 (164), ..."
        aircraft_str = self.metrics['aircraft_utilization']
        aircraft_list = aircraft_str.split(', ')
        
        aircraft_data = []
        for aircraft_str_item in aircraft_list:
            # Parse "B737 (167)" format
            aircraft, count = aircraft_str_item.rsplit(' (', 1)
            count = int(count.rstrip(')'))
            aircraft_data.append((aircraft, count))
        
        # Sort by count (descending)
        aircraft_data.sort(key=lambda x: x[1], reverse=True)
        aircraft_types = [item[0] for item in aircraft_data]
        flight_counts = [item[1] for item in aircraft_data]
        
        # Create bar chart
        fig, ax = plt.subplots(figsize=(10, 6))
        
        bars = ax.bar(aircraft_types, flight_counts, color='#FF9800', edgecolor='black', linewidth=1.2)
        
        # Add value labels on bars
        for bar, count in zip(bars, flight_counts):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + 1,
                   str(count), ha='center', va='bottom', fontsize=10, weight='bold')
        
        ax.set_xlabel('Aircraft Type', fontsize=11, weight='bold')
        ax.set_ylabel('Number of Flights', fontsize=11, weight='bold')
        ax.set_title('Aircraft Utilization', fontsize=14, weight='bold', pad=20)
        ax.grid(axis='y', alpha=0.3, linestyle='--')
        
        plt.tight_layout()
        
        output_file = os.path.join(self.viz_dir, '03_aircraft_utilization.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"✓ Chart saved: {output_file}")
        return output_file
    
    def plot_top_revenue_aircraft(self):
        """Create bar chart for top revenue aircraft"""
        print(f"\n[Creating Top Revenue Aircraft Chart]")
        
        # Parse top revenue string
        # Format: "B737 = $23081, A350 = $22666"
        revenue_str = self.metrics['top_aircraft_by_revenue']
        revenue_list = revenue_str.split(', ')
        
        aircraft_types = []
        revenues = []
        for rev_str in revenue_list:
            # Parse "B737 = $23081" format
            aircraft, revenue = rev_str.split(' = ')
            revenue_val = float(revenue.replace('$', '').replace(',', ''))
            aircraft_types.append(aircraft)
            revenues.append(revenue_val)
        
        # Create bar chart
        fig, ax = plt.subplots(figsize=(10, 6))
        
        bars = ax.bar(aircraft_types, revenues, color='#4CAF50', edgecolor='black', linewidth=1.2)
        
        # Add value labels on bars
        for bar, rev in zip(bars, revenues):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + 200,
                   f'${rev:,.0f}', ha='center', va='bottom', fontsize=11, weight='bold')
        
        ax.set_xlabel('Aircraft Type', fontsize=11, weight='bold')
        ax.set_ylabel('Total Revenue (USD)', fontsize=11, weight='bold')
        ax.set_title('Top 2 Aircraft by Revenue', fontsize=14, weight='bold', pad=20)
        ax.grid(axis='y', alpha=0.3, linestyle='--')
        
        # Format y-axis with dollar signs
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        plt.tight_layout()
        
        output_file = os.path.join(self.viz_dir, '04_top_revenue_aircraft.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"✓ Chart saved: {output_file}")
        return output_file
    
    def plot_key_metrics_summary(self):
        """Create summary dashboard with key metrics"""
        print(f"\n[Creating Key Metrics Summary Dashboard]")
        
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle('Flight Operations Metrics Dashboard', fontsize=16, weight='bold')
        
        # Metric 1: Total Flights
        ax1.text(0.5, 0.7, str(self.metrics['unique_flights']), 
                ha='center', va='center', fontsize=48, weight='bold', color='#2196F3')
        ax1.text(0.5, 0.3, 'Unique Flights', 
                ha='center', va='center', fontsize=16, weight='bold')
        ax1.axis('off')
        ax1.set_facecolor('#E3F2FD')
        
        # Metric 2: Cancellation Rate
        cancel_rate = self.metrics['cancellation_rate']
        ax2.text(0.5, 0.7, cancel_rate, 
                ha='center', va='center', fontsize=48, weight='bold', color='#F44336')
        ax2.text(0.5, 0.3, 'Cancellation Rate', 
                ha='center', va='center', fontsize=16, weight='bold')
        ax2.axis('off')
        ax2.set_facecolor('#FFEBEE')
        
        # Metric 3: Average Delay
        ax3.text(0.5, 0.7, self.metrics['average_delay'], 
                ha='center', va='center', fontsize=48, weight='bold', color='#FF9800')
        ax3.text(0.5, 0.3, 'Average Delay', 
                ha='center', va='center', fontsize=16, weight='bold')
        ax3.axis('off')
        ax3.set_facecolor('#FFF3E0')
        
        # Metric 4: Median Duration
        ax4.text(0.5, 0.7, self.metrics['median_duration'], 
                ha='center', va='center', fontsize=48, weight='bold', color='#4CAF50')
        ax4.text(0.5, 0.3, 'Median Duration', 
                ha='center', va='center', fontsize=16, weight='bold')
        ax4.axis('off')
        ax4.set_facecolor('#E8F5E9')
        
        plt.tight_layout()
        
        output_file = os.path.join(self.viz_dir, '05_key_metrics_dashboard.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"✓ Dashboard saved: {output_file}")
        return output_file
    
    def generate_all(self):
        """Generate all visualizations"""
        try:
            # Check dependencies
            if not self.check_dependencies():
                print("\n✗ Visualization failed: matplotlib not installed")
                return False
            
            # Load metrics
            self.load_metrics()
            
            # Generate all visualizations
            files_created = []
            files_created.append(self.plot_flight_status_distribution())
            files_created.append(self.plot_top_routes())
            files_created.append(self.plot_aircraft_utilization())
            files_created.append(self.plot_top_revenue_aircraft())
            files_created.append(self.plot_key_metrics_summary())
            
            # Summary
            print("\n" + "=" * 60)
            print("Visualization Complete!")
            print("=" * 60)
            print(f"✓ Visualizations created: {len(files_created)}")
            for file in files_created:
                print(f"  - {file}")
            print("=" * 60)
            
            return True
            
        except Exception as e:
            print(f"\n✗ Visualization failed: {e}")
            import traceback
            traceback.print_exc()
            return False


if __name__ == "__main__":
    visualizer = MetricsVisualizer()
    success = visualizer.generate_all()
    exit(0 if success else 1)
