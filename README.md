# NYC Taxi Data Platform - Data Engineering Technical Assessment

## Project Overview
This project implements a **scalable data platform** using **NYC Taxi & Limousine Commission (TLC) Trip Record Data** to support **batch processing, dimensional modeling, and analytics queries**. It ingests **3 months** of **Yellow and Green Taxi trip records**, applies **transformations and aggregations**, and **stores** the processed data in a queryable format.

This pipeline is implemented using **Apache Spark (PySpark)** for efficient batch processing.

---

## 1. Data Architecture Design

### Architecture Diagram
<img width="993" alt="Screenshot 2025-02-26 at 8 01 27 PM" src="https://github.com/user-attachments/assets/b585ed7e-1564-4651-811d-b49ab1bc73b9" />


### Data Pipeline Workflow
1. **Data Ingestion**:
   - Reads **Yellow and Green Taxi trip data** from **NYC TLC** in **Parquet** format.
   - Reads **Lookup Tables** (Taxi Zones, Rate Codes, Payment Types) in **CSV** format.

2. **Data Processing (ETL using PySpark)**:
   - **Data Cleaning**: Removes duplicates, filters invalid trips.
   - **Transformations**:
     - **Trip duration & speed calculation**
     - **Outlier detection in fare amounts**
     - **Joining with lookup tables**
   - **Aggregation**:
     - **Hourly, daily, and monthly revenue analysis**
     - **Trip patterns**
     - **Driver performance metrics**
   - **Partitioning & Storage**:
     - Data is partitioned by **year, month, day** for efficient querying.

3. **Data Storage & Querying**:
   - Processed data is stored in **Parquet format** in `data/processed/`.
   - Fact and Dimension tables support **OLAP-style queries**.

---

## 2. Data Design & Justifications

### How Late-Arriving Data is Handled
- Late-arriving trip data is **incrementally processed** in each pipeline run.
- Dimension tables use **Slowly Changing Dimensions (SCD Type 2)** for historical tracking.

### Storage Partitioning Strategy
- Data is **partitioned by year, month, day** to:
  - Improve **query performance**.
  - Reduce **scanning overhead**.
- **Fact table**: Partitioned by `year`, `month`, `day` for **time-based analysis**.
- **Dimension tables**: Use **SCD Type 2** with `effective_date`, `end_date`.

### Performance Optimization
- **Columnar storage (Parquet format)**: Enables **faster reads**.
- **Partition pruning**: Reduces unnecessary data scanning.
- **Bucketing** on `PULocationID`, `DOLocationID`: Optimizes **geo-based queries**.
- **Broadcast joins** for small lookup tables.

### Data Retention Policies
- **Raw data**: Retained for **1 year**.
- **Processed data**: Retained for **5 years**, then archived.

---

## 3. Data Modeling
The **dimensional model** follows a **star schema** with **one fact table** and **multiple dimension tables**.

### Fact Table: `fact_trips`
Stores trip-level records with the following attributes:
- `trip_id`
- `tpep_pickup_datetime`, `tpep_dropoff_datetime`
- `PULocationID`, `DOLocationID`
- `trip_distance`
- `fare_amount`, `total_amount`
- `trip_duration`, `trip_speed`
- `payment_type`, `tip_amount`, `tip_percentage`

### Dimension Tables
1. **`dim_location`** (Taxi Zone Information)  
   - `location_id`, `borough`, `zone_name`, `effective_date`, `end_date`
   - Uses **SCD Type 2** for tracking historical changes.

2. **`dim_driver_performance`**  
   - `driver_id`, `total_trips`, `total_revenue`, `avg_trip_duration`, `avg_fare_per_trip`, `avg_speed`, `avg_tip_percentage`
   - Aggregated **driver performance metrics**.

---

## 4. Implementation

### Batch Processing Pipeline (`etl_pipeline.py`)
1. **Extract**:
   - Reads raw taxi data (`data/raw/`) and lookup tables (`data/raw/taxi_zone_lookup.csv`).
2. **Transform**:
   - Cleans data, calculates **trip duration, speed**.
   - Joins with **dim_location** for geo analysis.
   - Aggregates **daily/hourly revenue**.
3. **Load**:
   - Saves partitioned Parquet files in `data/processed/fact_trips/`.

### Slowly Changing Dimensions (`handle_scd.py`)
- Ensures **historical tracking** of taxi zones.
- Updates `dim_location` table using **SCD Type 2**.

### Analytics Queries (`analytics.py`)
Implements **2024 trip analysis**:
- **Top 10 pickup locations by revenue**.
- **Trip patterns (Weekday vs Weekend)**.
- **Top 10 longest trips (>10 miles).**

### Driver Performance Metrics (`driver_metrics.py`)
- Computes **driver revenue, speed, tip percentage**.

---

## 5. Running the Project
### Prerequisites
- Python 3.x
- PySpark (`pip install pyspark`)

### Execution Steps
```bash
# Activate virtual environment
source venv/bin/activate  # Mac/Linux
venv\Scripts\activate      # Windows

# Run ETL Pipeline
python3 scripts/etl_pipeline.py

# Run SCD Update for Taxi Zones
python3 scripts/handle_scd.py

# Run Analytics Queries
python3 scripts/analytics.py

# Compute Driver Performance Metrics
python3 scripts/driver_metrics.py
```

---
