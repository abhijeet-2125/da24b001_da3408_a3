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

## Comparison Metrics

The following metrics were recorded during the Spark and Ray executions:

| Metric | Spark | Ray |
|---|---:|---:|
| Total execution time | 218.8738 s | 769.6395 s |
| Python UDF time | 0.4463 s | 3.2980 s |
| Peak process CPU | 323.0% | 101.1% |
| Peak process memory | 16.6% | 11.8% |

Based on the completed benchmark runs, Spark was approximately **3.52× faster** than Ray.

The Python UDF also completed faster in Spark. Ray's UDF execution time was approximately **7.39×** the Spark UDF time.

The peak CPU and memory values above refer to **process-level utilization observed during the runs**, not the total CPU or RAM capacity of the VM.

### Final Comparison

| Aspect | Winner |
|---|---|
| Total execution time | **Spark** |
| Python UDF execution | **Spark** |
| Peak process CPU | **Spark** |
| Peak process memory | **Ray** |
| Overall for this ETL workload | **Spark** |

###AI-first vs. BI-first


AI-first:
I would lean toward Ray when the project is primarily Python/AI-oriented and needs flexible task execution, model training, inference, or integration with Python-based ML workloads. Ray's Python-first design makes it attractive when distributed computation is closely coupled with AI workflows.


BI-first:
I would choose Spark for a BI-first project involving large-scale structured data, SQL/DataFrame transformations, ETL, joins, aggregations, and analytical reporting. My experiment also supports this choice: Spark completed the structured ETL pipeline in 218.87 seconds, compared with 769.64 seconds for Ray.


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


