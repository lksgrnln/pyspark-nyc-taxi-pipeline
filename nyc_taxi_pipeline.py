import os
import requests
from functools import reduce
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor, as_completed

class NYCTaxiPipeline:
    def __init__(self,
                 base_uri: str = 'https://d37ci6vzurychx.cloudfront.net/trip-data/' ,
                 master_mode: str = 'local[*]',
                 partitions: int = 20,
                 years = None,
                 months = None,
                 aggregation_level: str = 'day',
                 keep_all_columns: bool = False):
        """Initializes the Pipeline Class Parameters.
        Args:
            base_uri (str): The base URI where the NYC Taxi Parquet data resides.
            master_mode (str): The Spark deployment master string (e.g., 'local[*]' for local execution).
            partitions (int): Total shuffle partitions to use during wide transformations (e.g., joins, aggregations).
            years (list): List of integer or string years to process (e.g., [2022, 2023]).
            months (list): List of integer or string months to process (e.g., [1, 2] or ['01', '02']).
        """
        if years is None:
            years = [2021, 2022, 2023, 2024, 2025]
        if months is None:
            months = list(range(1, 13))
        self.base_uri = base_uri
        self.master_mode = master_mode
        self.partitions = partitions
        self.years = years
        self.months = months
        self.aggregation_level = aggregation_level
        self.keep_all_columns = keep_all_columns
        self.spark_session = None

        self.local_data_dir = "/app/data"
        os.makedirs(self.local_data_dir, exist_ok=True)

        self.required_columns = [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
            "total_amount",
            "fare_amount"
        ]

    def start_spark_session(self):
        """Initializes a streamlined Apache Spark Session using default web filesystem protocols."""
        print(f"[INFO] Initializing Spark Session via master: '{self.master_mode}'...")

        self.spark_session = SparkSession.builder \
            .appName(f"NYC-Taxi-Pipeline-{self.aggregation_level}") \
            .master(self.master_mode) \
            .config("spark.driver.memory", "3g") \
            .config("spark.executor.memory", "3g") \
            .config("spark.sql.shuffle.partitions", str(self.partitions)) \
            .config("spark.sql.ansi.enabled", "true") \
            .config("spark.sql.parquet.enableVectorizedReader", "true") \
            .getOrCreate()

        print("[INFO] Spark Session established successfully.")
        return self.spark_session

    def download_single_file(self, filename: str) -> str:
        """Worker function to download a single file from public storage if missing."""
        local_path = os.path.join(self.local_data_dir, filename)
        remote_url = f"{self.base_uri}{filename}"

        if os.path.exists(local_path):
            return local_path

        try:
            with requests.get(remote_url, stream=True) as r:
                if r.status_code == 404:
                    return None
                r.raise_for_status()
                with open(local_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        f.write(chunk)
            print(f"[DOWNLOAD SUCCESS] Finished fetching: {filename}")
            return local_path
        except Exception as e:
            print(f"[ERROR] Failed to download {filename}: {e}")
            return None

    def load_data(self):
        """Loads all paths simultaneously and applies a global type projection."""
        if not self.spark_session:
            raise RuntimeError("Spark session not started. Call start_spark_session() first.")

        filenames = []
        for year in self.years:
            for month in self.months:
                month_str = f"{int(month):02d}"
                filenames.append(f"yellow_tripdata_{year}-{month_str}.parquet")

        print(f"[INFO] Checking local availability for {len(filenames)} files across {len(self.years)} years...")

        local_paths = []

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(self.download_single_file, fname): fname for fname in filenames}
            for future in as_completed(futures):
                path_result = future.result()
                if path_result:
                    local_paths.append(path_result)  # <--- Populating the list

        if not local_paths:
            raise RuntimeError("[CRITICAL] No valid data files found or downloaded.")

        print(f"[INFO] Staged {len(local_paths)} files. Loading and globally normalizing...")

        # 1. We pass the list directly using Python's unpack operator (*)
        # This expands the list into raw arguments for Spark: .parquet(path1, path2, path3...)
        raw_unified_df = self.spark_session.read \
            .option("mergeSchema", "false") \
            .parquet(*local_paths)

        # 2. Apply a single, global type projection pass over everything at once
        print("[INFO] Applying global type projection to resolve schema drift...")
        unified_df = raw_unified_df.select(
            F.col("tpep_pickup_datetime").cast("timestamp"),
            F.col("tpep_dropoff_datetime").cast("timestamp"),
            F.col("trip_distance").cast("double"),
            F.col("total_amount").cast("double"),
            F.col("fare_amount").cast("double")
        )

        print(f"[INFO] Data loaded and unified successfully.")
        return unified_df

    def clean_data(self, df):
        """Applies assignment business logic to filter out corrupt/unplausible data.

        Args:
            df (DataFrame): The raw input Spark DataFrame.
        Returns:
            DataFrame: Cleaned and optimized Spark DataFrame.
        """
        print("[INFO] Beginning data cleaning and validation phase...")

        # 1. Column Selection Drop Switch (Optimization Step)
        if not self.keep_all_columns:
            print(f"[OPTIMIZATION] Dropping unnecessary columns. Retaining: {self.required_columns}")
            df = df.select(*self.required_columns)

        # 2. Mandatory Validation: Filter missing pickup or dropoff timestamps
        df = df.filter(
            F.col("tpep_pickup_datetime").isNotNull() &
            F.col("tpep_dropoff_datetime").isNotNull()
        )

        # 3. Mandatory Validation: Filter out records where dropoff happens before pickup
        df = df.filter(F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime"))

        # 4. Mandatory Validation: Filter out negative or null durations
        # Formula given in task sheet: trip_duration_min = (dropoff - pickup) / 60
        # --- MODERN SPARK COMPATIBLE TIMEDELTA CALCULATION ---
        # F.unix_timestamp safely extracts seconds since epoch for TIMESTAMP_NTZ
        pickup_epoch = F.unix_timestamp(F.col("tpep_pickup_datetime"))
        dropoff_epoch = F.unix_timestamp(F.col("tpep_dropoff_datetime"))

        duration_in_secs = dropoff_epoch - pickup_epoch
        df = df.withColumn("trip_duration_min", duration_in_secs / 60.0)

        # Filter negative or zero durations
        df = df.filter(F.col("trip_duration_min") > 0)

        # 5. Mandatory Validation: Filter out negative distances or negative prices
        # Task explicitly requires filtering negative values for distances and prices
        df = df.filter(
            (F.col("trip_distance") >= 0) &
            (F.col("total_amount") >= 0) &
            (F.col("fare_amount") >= 0)
        )

        # 6. TIME-FENCING OUTLIER PURGE
        # Extracted year must strictly exist within the explicit parameters requested in configuration
        df = df.filter(F.year(F.col("tpep_pickup_datetime")).isin(self.years))

        print("[INFO] Data cleaning complete. Outliers and temporal anomalies successfully purged.")
        return df

    def compute_metrics(self, df):
        """Calculates trip durations and aggregates core/extended metrics in English."""

        print("[INFO] Initiating Metric Computation and Performance Aggregations...")

        # Feature extraction (single pass transformation)
        analyzed_df = df \
            .withColumn("year", F.year(F.col("tpep_pickup_datetime"))) \
            .withColumn("month", F.month(F.col("tpep_pickup_datetime"))) \
            .withColumn("hour", F.hour(F.col("tpep_pickup_datetime"))) \
            .withColumn("day_of_week", F.dayofweek(F.col("tpep_pickup_datetime")))

        # Cache the clean records in RAM
        analyzed_df.cache()

        # --- CORE METRICS ---
        print("\n--- [CORE METRIC] Average Trip Duration per Year ---")
        avg_year_df = analyzed_df.groupBy("year") \
            .agg(F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min")) \
            .sort("year")
        avg_year_df.show(truncate=False)

        print("\n--- [CORE METRIC] Average Trip Duration per Month ---")
        avg_month_df = analyzed_df.groupBy("year", "month") \
            .agg(F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min")) \
            .sort("year", "month")
        avg_month_df.show(len(self.years) * 12, truncate=False)

        print("\n--- [CORE METRIC] Global Overall Average Trip Duration ---")
        overall_avg = analyzed_df.select(F.round(F.avg("trip_duration_min"), 2).alias("global_avg_duration_min"))
        overall_avg.show(truncate=False)

        # --- EXTENDED ANALYSIS ---
        print("\n--- [EXTENDED METRIC] Average Trip Duration per Hour of Day (Rush Hours) ---")
        avg_hour_df = analyzed_df.groupBy("hour") \
            .agg(F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min")) \
            .sort("hour")
        avg_hour_df.show(24, truncate=False)

        print("\n--- [EXTENDED METRIC] Average Trip Duration per Day of Week (1=Sun, 7=Sat) ---")
        avg_dow_df = analyzed_df.groupBy("day_of_week") \
            .agg(F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min")) \
            .sort("day_of_week")
        avg_dow_df.show(truncate=False)

        analyzed_df.unpersist()

    def run(self):
        """Orchestrates the entire pipeline from load to execution."""

        self.start_spark_session()
        raw_df = self.load_data()
        cleaned_df = self.clean_data(raw_df)
        self.compute_metrics(cleaned_df)

        print("[INFO] Pipeline execution completed successfully.")

if __name__ == "__main__":
    pipeline = NYCTaxiPipeline()
    pipeline.run()