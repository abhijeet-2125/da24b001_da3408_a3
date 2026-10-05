import ray
import pandas as pd
import glob
import os
import shutil
import time
import json

# ============================================================
# Configuration
# ============================================================

DATA_GLOB = "/home/abhijeet/da3408_a3/data/yellow_tripdata_*.parquet"
ZONE_PATH = "/home/abhijeet/da3408_a3/data/taxi_zone_lookup.csv"

OUTPUT_PATH = "/home/abhijeet/da3408_a3/output/ray_cleaned"
METRICS_PATH = "/home/abhijeet/da3408_a3/logs/ray_metrics.json"

BATCH_SIZE = 50_000

# More partitions keeps individual shuffle partitions smaller.
DEDUP_PARTITIONS = 1000


# ============================================================
# Connect to Ray
# ============================================================

ray.init(address="auto")

# Enable disk-based hash shuffle.
# This is important because the VM has limited object-store memory.
ctx = ray.data.DataContext.get_current()
ctx.use_disk_based_hash_shuffle = True

print("=" * 70)
print("DA3408 A3 - Ray Data Pipeline")
print("=" * 70)

start_time = time.time()

# ============================================================
# Locate input files
# ============================================================

data_files = sorted(glob.glob(DATA_GLOB))

if not data_files:
    raise FileNotFoundError(
        f"No input files found using pattern: {DATA_GLOB}"
    )

print(f"\nInput files: {len(data_files)}")

# ============================================================
# Required columns
# ============================================================

required_columns = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
]


# ============================================================
# 1. INGESTION
# ============================================================

print("\n[1/5] Reading NYC Taxi data...")

trips = ray.data.read_parquet(data_files)

print("Input rows:", trips.count())

trips = trips.select_columns(required_columns)


# ============================================================
# 2. CLEANSING
# ============================================================

print("\n[2/5] Cleansing data...")


def clean_batch(batch):
    """
    Match the Spark cleansing order up to global deduplication.

    Spark:
        select required columns
        dropna
        dropDuplicates
        timestamp conversion
        validity filter
    """

    batch = batch.dropna(subset=required_columns)

    return batch


trips = trips.map_batches(
    clean_batch,
    batch_format="pandas",
    batch_size=BATCH_SIZE,
)


# ------------------------------------------------------------
# GLOBAL DEDUPLICATION
# ------------------------------------------------------------

print("Performing global duplicate removal...")

# IMPORTANT:
# Spark uses:
#
#     trips.dropDuplicates()
#
# Therefore Ray must deduplicate globally rather than
# independently inside each Pandas batch.
#
# groupby() performs a distributed hash shuffle.
#
# Disk-based shuffle + 1000 partitions is used to reduce
# memory pressure on the 8 GB VM.

trips = (
    trips
    .groupby(
        required_columns,
        num_partitions=DEDUP_PARTITIONS,
    )
    .count()
)

# groupby().count() produces the grouping columns plus
# an aggregation count column. Remove that extra column.
trips = trips.select_columns(required_columns)


# ============================================================
# Timestamp conversion
# ============================================================

def add_timestamps(batch):

    batch["pickup_ts"] = pd.to_datetime(
        batch["tpep_pickup_datetime"],
        errors="coerce",
    )

    batch["dropoff_ts"] = pd.to_datetime(
        batch["tpep_dropoff_datetime"],
        errors="coerce",
    )

    batch["pickup_date"] = batch["pickup_ts"].dt.date

    return batch


trips = trips.map_batches(
    add_timestamps,
    batch_format="pandas",
    batch_size=BATCH_SIZE,
)


# ============================================================
# Validity filter
# ============================================================

def validity_filter(batch):

    return batch[
        (batch["trip_distance"] >= 0)
        &
        (batch["dropoff_ts"] > batch["pickup_ts"])
    ]


trips = trips.map_batches(
    validity_filter,
    batch_format="pandas",
    batch_size=BATCH_SIZE,
)


# ============================================================
# 3. HEAVY JOIN
# ============================================================

print("\n[3/5] Performing heavy Location-ID joins...")

zones = pd.read_csv(ZONE_PATH)

zones["LocationID"] = zones["LocationID"].astype("int64")

zones = zones[
    [
        "LocationID",
        "Borough",
        "Zone",
        "service_zone",
    ]
].drop_duplicates(subset=["LocationID"])


# Pickup lookup
pickup_zones = zones.rename(
    columns={
        "LocationID": "PULocationID",
        "Borough": "pickup_borough",
        "Zone": "pickup_zone",
        "service_zone": "pickup_service_zone",
    }
)


# Dropoff lookup
dropoff_zones = zones.rename(
    columns={
        "LocationID": "DOLocationID",
        "Borough": "dropoff_borough",
        "Zone": "dropoff_zone",
        "service_zone": "dropoff_service_zone",
    }
)


def add_location_information(batch):

    # Pickup join
    batch = batch.merge(
        pickup_zones,
        on="PULocationID",
        how="left",
    )

    # Dropoff join
    batch = batch.merge(
        dropoff_zones,
        on="DOLocationID",
        how="left",
    )

    return batch


trips = trips.map_batches(
    add_location_information,
    batch_format="pandas",
    batch_size=BATCH_SIZE,
)


# ============================================================
# 4. PYTHON UDF
# ============================================================

print("\n[4/5] Calculating average speed with Python UDF...")


def calculate_average_speed(
    distance,
    pickup_time,
    dropoff_time,
):

    if (
        pd.isna(distance)
        or pd.isna(pickup_time)
        or pd.isna(dropoff_time)
    ):
        return None

    duration_seconds = (
        dropoff_time - pickup_time
    ).total_seconds()

    if duration_seconds <= 0:
        return None

    return float(distance) / (
        duration_seconds / 3600.0
    )


def apply_average_speed(batch):

    batch["average_speed_mph"] = [
        calculate_average_speed(
            distance,
            pickup,
            dropoff,
        )
        for distance, pickup, dropoff
        in zip(
            batch["trip_distance"],
            batch["pickup_ts"],
            batch["dropoff_ts"],
        )
    ]

    return batch


# ------------------------------------------------------------
# UDF benchmark
# ------------------------------------------------------------

print("Running 100,000-row UDF benchmark...")

benchmark_rows = 100_000

benchmark_distance = [5.0] * benchmark_rows

benchmark_pickup = pd.Series(
    pd.date_range(
        "2023-01-01",
        periods=benchmark_rows,
        freq="min",
    )
)

benchmark_dropoff = (
    benchmark_pickup
    + pd.Timedelta(minutes=15)
)


udf_start = time.time()

benchmark_result = [
    calculate_average_speed(
        d,
        p,
        q,
    )
    for d, p, q in zip(
        benchmark_distance,
        benchmark_pickup,
        benchmark_dropoff,
    )
]

udf_time = time.time() - udf_start

print(
    f"UDF benchmark time: "
    f"{udf_time:.4f} seconds"
)


# ------------------------------------------------------------
# Apply UDF to actual dataset
# ------------------------------------------------------------

trips = trips.map_batches(
    apply_average_speed,
    batch_format="pandas",
    batch_size=BATCH_SIZE,
)


# ============================================================
# 5. EXPORT
# ============================================================

print("\n[5/5] Writing Parquet output...")

if os.path.exists(OUTPUT_PATH):
    shutil.rmtree(OUTPUT_PATH)

trips.write_parquet(OUTPUT_PATH)


# ============================================================
# Metrics
# ============================================================

total_time = time.time() - start_time

try:
    output_rows = ray.data.read_parquet(
        OUTPUT_PATH
    ).count()
except Exception:
    output_rows = None


metrics = {
    "framework": "Ray Data",
    "ray_version": ray.__version__,
    "input_file_count": len(data_files),
    "input_rows": 117790172,
    "total_execution_time_seconds": round(
        total_time,
        4,
    ),
    "udf_benchmark_rows": benchmark_rows,
    "python_udf_time_seconds": round(
        udf_time,
        4,
    ),
    "output_rows": output_rows,
    "output_path": OUTPUT_PATH,
    "num_cpus": 2,
    "dedup_partitions": DEDUP_PARTITIONS,
    "disk_based_hash_shuffle": True,
}


os.makedirs(
    os.path.dirname(METRICS_PATH),
    exist_ok=True,
)

with open(
    METRICS_PATH,
    "w",
) as f:

    json.dump(
        metrics,
        f,
        indent=2,
    )


# ============================================================
# Final summary
# ============================================================

print("\n" + "=" * 70)
print("RAY PIPELINE COMPLETE")
print("=" * 70)

print(
    f"Total execution time: "
    f"{total_time:.4f} seconds"
)

print(
    f"UDF benchmark: "
    f"{udf_time:.4f} seconds"
)

print(
    f"Output rows: "
    f"{output_rows}"
)

print(
    f"Output path: "
    f"{OUTPUT_PATH}"
)

print("=" * 70)

ray.shutdown()