# DA24B001 — DA3408 Assignment 3
## Spark vs. Ray: The Data Engineering Duel

This repository contains my submission for **DA3408 Assignment 3**, where I compared **Apache Spark** and **Ray Data** on a large NYC Yellow Taxi dataset.

The main goal was to implement a similar data-cleaning and transformation pipeline in both frameworks and compare how they performed.

### What the pipeline does

The pipeline includes:

1. Reading the NYC Taxi Parquet files
2. Selecting the required columns
3. Handling missing values
4. Removing duplicate records
5. Processing timestamps
6. Filtering invalid trips
7. Joining the trip data with the taxi zone lookup
8. Calculating average speed using a Python UDF
9. Writing the processed data to Parquet

### Repository Structure

```text
da24b001_da3408_a3/
│
├── spark_clean.py
├── ray_clean.py
│
├── report/
│   └── da24b001_a3_aiops.pdf
│
├── screenshots/
│   ├── spark_master.png
│   └── ray_dashboard.png
│
└── results/
    └── benchmark_results.json
