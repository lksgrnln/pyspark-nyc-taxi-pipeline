from pyspark.sql import SparkSession
import pyspark.sql.functions as F

class NYCTaxiPipeline:
    def __init__(self,
                 base_s3_uri: str = 's3a://nyc-tlc/trip data/' ,
                 master_mode: str = 'local[*]',
                 partitions: int = 20,
                 years = None,
                 months = None,
                 aggregation_level: str = 'day'):
        """Initializes the Pipeline Class Parameters.
        Args:
            base_s3_uri (str): The root public AWS S3 bucket URI where the Parquet data resides.
            master_mode (str): The Spark deployment master string (e.g., 'local[*]' for local execution).
            partitions (int): Total shuffle partitions to use during wide transformations (e.g., joins, aggregations).
            years (list): List of integer or string years to process (e.g., [2022, 2023]).
            months (list): List of integer or string months to process (e.g., [1, 2] or ['01', '02']).
            aggregation_level (str): Time boundary granularity for metric outputs ('year', 'month', 'day', 'hour').
        """
        if years is None:
            years = [2024]
        if months is None:
            months = [1, 2, 3]
        self.base_s3_uri = base_s3_uri
        self.master_mode = master_mode
        self.partitions = partitions
        self.years = years
        self.months = months
        self.aggregation_level = aggregation_level
        self.spark_session = None

    def start_spark_session(self):
        """Initializes and configures a highly scalable Apache Spark Session.

        Injects necessary Hadoop AWS configurations to stream directly from the public
        NYC TLC open data S3 bucket anonymously without requiring local AWS credentials.

        Returns:
            SparkSession: The active, fully-configured Spark session instance.
        """
        print(f"[INFO] Initializing Spark Session via master: '{self.master_mode}'...")

        self.spark_session = SparkSession.builder \
            .appName(f"NYC-Taxi-Pipeline-{self.aggregation_level}") \
            .master(self.master_mode) \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
            .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider") \
            .config("spark.sql.shuffle.partitions", str(self.partitions)) \
            .config("spark.sql.ansi.enabled", "true") \
            .getOrCreate()

        print("[INFO] Spark Session established successfully.")
        return self.spark_session


    def load_data(self, s3_path_pattern: str):
        """Loads Parquet files and handles schema variations dynamically."""
        pass

    def clean_data(self, df):
        """Applies business logic to filter out corrupt/unplausible data."""
        pass

    def compute_metrics(self, df):
        """Calculates trip durations and aggregates core/extended metrics."""
        pass

    def run(self):
        """Orchestrates the entire pipeline from load to execution."""
        self.start_spark_session()

if __name__ == "__main__":
    pipeline = NYCTaxiPipeline()
    pipeline.run()