import json
import time
from pyspark.sql import SparkSession,functions as F,types as T
from pyspark import StorageLevel

DATA_GLOB = "/home/abhijeet/da3408_a3/data/yellow_tripdata_*.parquet"
ZONE_PATH = "/home/abhijeet/da3408_a3/data/taxi_zone_lookup.csv"
OUTPUT_PATH = "/home/abhijeet/da3408_a3/output/spark_cleaned"
LOG_PATH = "/home/abhijeet/da3408_a3/logs/spark_metrics.json"
UDF_SAMPLE_FILE = "/home/abhijeet/da3408_a3/data/yellow_tripdata_2025-01.parquet"

def calculate_average_speed(distance, pickup_time, dropoff_time):
    if distance is None or pickup_time is None or dropoff_time is None:
        return None
    duration_seconds = (dropoff_time - pickup_time).total_seconds()
    if duration_seconds <= 0:
        return None
    return float(distance)/(duration_seconds/3600.0)

average_speed_udf = F.udf(calculate_average_speed,T.DoubleType())
#sparksession
spark = (
    SparkSession.builder
    .appName("DA3408_A3_Spark_Cleaning")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
total_start = time.perf_counter()

#ingestion
print("\n[1/5] Reading taxi Parquet files...")
raw_df = spark.read.parquet(DATA_GLOB)
print("Input schema:")
raw_df.printSchema()

#cleaning
print("\n[2/5] Cleaning data...")
required_columns = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID"]
trips = raw_df.select(*required_columns)
trips = trips.dropna(subset=[
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "PULocationID",
        "DOLocationID"])

trips = trips.dropDuplicates() #removing duplicates

trips = (trips.withColumn("pickup_ts",F.to_timestamp("tpep_pickup_datetime"))
    .withColumn("dropoff_ts",F.to_timestamp("tpep_dropoff_datetime"))
    .withColumn("pickup_date",F.to_date("pickup_ts")))


trips = trips.filter((F.col("trip_distance") >= 0) & (F.col("dropoff_ts") > F.col("pickup_ts")))


#heavy join
print("\n[3/5] Performing heavy Location-ID joins...")
zones = (
    spark.read
    .option("header", "true")
    .csv(ZONE_PATH)
    .select(
        F.col("LocationID").cast("long").alias("LocationID"),
        F.col("Borough").alias("Borough"),
        F.col("Zone").alias("Zone"),
        F.col("service_zone").alias("service_zone")).dropDuplicates(["LocationID"]))

# Pickup lookup table
pickup_zones = zones.select(
    F.col("LocationID").alias("PULocationID"),
    F.col("Borough").alias("pickup_borough"),
    F.col("Zone").alias("pickup_zone"),
    F.col("service_zone").alias("pickup_service_zone"))

# Dropoff lookup table
dropoff_zones = zones.select(
    F.col("LocationID").alias("DOLocationID"),
    F.col("Borough").alias("dropoff_borough"),
    F.col("Zone").alias("dropoff_zone"),
    F.col("service_zone").alias("dropoff_service_zone"))

# Repartition before joins so the join becomes a distributed shuffle
trips = trips.repartition(8, "PULocationID")

joined_df = trips.join(pickup_zones,on="PULocationID",how="left")
joined_df = joined_df.repartition(8, "DOLocationID")
joined_df = joined_df.join(dropoff_zones,on="DOLocationID",how="left")


print("\n[4/5] Applying Python UDF...")
result_df = joined_df.withColumn("average_speed_mph",average_speed_udf(F.col("trip_distance"),F.col("pickup_ts"),F.col("dropoff_ts")))
print("\n[5/5] Writing Parquet output...")
(result_df.write.mode("overwrite").parquet(OUTPUT_PATH))

total_time = time.perf_counter() - total_start


#benchmarking
print("\nRunning Python UDF microbenchmark...")
sample_df = (spark.read.parquet(UDF_SAMPLE_FILE).select("trip_distance","tpep_pickup_datetime","tpep_dropoff_datetime").dropna().limit(100000)
    .withColumn("pickup_ts",F.to_timestamp("tpep_pickup_datetime"))
    .withColumn("dropoff_ts",F.to_timestamp("tpep_dropoff_datetime"))
    .select("trip_distance","pickup_ts","dropoff_ts",)
    .persist(StorageLevel.MEMORY_AND_DISK))

sample_count = sample_df.count()
udf_start = time.perf_counter()
udf_result = (sample_df.withColumn("average_speed_mph",average_speed_udf(F.col("trip_distance"),F.col("pickup_ts"),F.col("dropoff_ts")))
    .select(F.avg("average_speed_mph").alias("average_speed"))
    .collect())
udf_time = time.perf_counter() - udf_start
sample_df.unpersist()

metrics = {
    "framework": "Apache Spark",
    "spark_version": spark.version,
    "total_execution_time_seconds": round(total_time,4),
    "udf_benchmark_rows": sample_count,
    "python_udf_time_seconds": round(udf_time,4),
    "output_path": OUTPUT_PATH,
    "input_pattern": DATA_GLOB}

with open(LOG_PATH, "w") as f:
    json.dump(metrics, f, indent=4)

print("Spark Pipeline completed and the summary:")
print(f"Spark version       : {spark.version}")
print(f"Total execution time: {total_time:.4f} seconds")
print(f"UDF benchmark rows  : {sample_count}")
print(f"Python UDF time     : {udf_time:.4f} seconds")
print(f"Output              : {OUTPUT_PATH}")
print(f"Metrics log         : {LOG_PATH}")
spark.stop()