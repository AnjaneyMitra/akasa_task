-- FlightOps Database Setup Script
-- Run this script before executing the pipeline

-- Create database
CREATE DATABASE IF NOT EXISTS flightops_db;
USE flightops_db;

-- Drop table if exists (for clean re-runs)
DROP TABLE IF EXISTS flights;

-- Create flights table with appropriate data types
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
