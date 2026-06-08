import pytest
from datetime import datetime
import pyspark.sql.functions as F
from nyc_taxi_pipeline import NYCTaxiPipeline  # assuming your script is named pipeline.py


@pytest.fixture(scope="module")
def spark_fixture():
    """Initializes a local Spark instance dedicated to testing."""
    pipeline = NYCTaxiPipeline(master_mode="local[2]")
    spark = pipeline.start_spark_session()
    yield spark
    spark.stop()


def test_compute_metrics_average(spark_fixture):
    # 1. HAND-COMPUTED EXPECTED DATA
    # Setup 5 test samples with explicit pickup and dropoff times.
    # Hand calculation formula for each row: duration = (dropoff - pickup) in minutes
    # Row 1: 10:00 to 10:10 = 10 mins
    # Row 2: 11:00 to 11:15 = 15 mins
    # Row 3: 12:00 to 12:20 = 20 mins
    # Row 4: 13:00 to 13:05 = 5 mins
    # Row 5: 14:00 to 14:30 = 30 mins
    # Total minutes = 10 + 15 + 20 + 5 + 30 = 80 minutes
    # Hand-computed average = 80 / 5 = 16.00 minutes

    expected_hand_computed_avg = 16.00

    data = [
        (datetime(2024, 1, 1, 10, 0, 0), datetime(2024, 1, 1, 10, 10, 0), 2.5, 12.0, 10.0),
        (datetime(2024, 1, 1, 11, 0, 0), datetime(2024, 1, 1, 11, 15, 0), 3.0, 15.5, 13.0),
        (datetime(2024, 1, 1, 12, 0, 0), datetime(2024, 1, 1, 12, 20, 0), 4.1, 18.0, 15.0),
        (datetime(2024, 1, 1, 13, 0, 0), datetime(2024, 1, 1, 13, 5, 0), 1.0, 7.0, 5.0),
        (datetime(2024, 1, 1, 14, 0, 0), datetime(2024, 1, 1, 14, 30, 0), 6.8, 28.0, 24.0)
    ]

    schema = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "total_amount",
        "fare_amount"
    ]

    # Create the Mock DataFrame
    mock_df = spark_fixture.createDataFrame(data, schema)

    # 2. RUN PIPELINE CLEANING LOGIC
    # Initialize pipeline object to gain access to its cleaning & transformation rules
    pipeline = NYCTaxiPipeline(aggregation_mode="avg_per_year", test=True)
    cleaned_df = pipeline.clean_data(mock_df)

    # 3. COMPUTE THE METRIC MANUALLY IN SPARK FOR THE ASSERTION
    # Since your original `compute_metrics` just prints using `.show()`,
    # we isolate the specific calculation step here to assert its numeric accuracy.
    analyzed_df = cleaned_df.withColumn("year", F.year(F.col("tpep_pickup_datetime")))
    result_df = pipeline.compute_metrics(cleaned_df)

    # Collect the calculated row from Spark's memory
    result_row = result_df.filter(F.col("year") == 2024).collect()

    assert len(result_row) == 1, "Should return exactly one aggregated row for the year 2024"

    spark_computed_avg = result_row[0]["avg_duration_min"]

    # 4. THE ULTIMATE ASSERTION
    print(f"\n[TEST LOG] Hand Computed: {expected_hand_computed_avg} | Spark Computed: {spark_computed_avg}")
    assert spark_computed_avg == expected_hand_computed_avg