# pyspark-nyc-taxi-pipeline
Repository for a pipeline of aggregating nyc taxi data.

## How to use: 
Prerequisite is an installed and running docker daemon. First the docker images needs to be build

With running docker daemon: 
```
docker build -t nyc-taxi-pipeline:0.0.1 .   
```

After the image is build the pipeline can be started with default parameters as following:

For windows: 
```
docker run --rm -it -v "${PWD}:/app" nyc-taxi-pipeline:0.0.1
```
For linux:
```
docker run --rm -it -v "$(pwd):/app" nyc-taxi-pipeline:0.0.1
```

For further configurations see documentation of the pipeline class in respective Python file. 

Sample output: 
```
(.venv) PS C:\Users\lukas\PycharmProjects\pyspark-nyc-taxi-pipeline> docker run --rm -it -v "${PWD}:/app" nyc-taxi-pipeline:0.0.1
[INFO] Initializing Spark Session via master: 'local[*]'...
WARNING: Using incubator modules: jdk.incubator.vector
Using Spark's default log4j profile: org/apache/spark/log4j2-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
26/06/08 06:04:01 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
[INFO] Spark Session established successfully.
[INFO] Checking local availability for 60 files across 5 years...
[INFO] Staged 60 files. Loading and globally normalizing...
[INFO] Applying global type projection to resolve schema drift...               
[INFO] Data loaded and unified successfully.
[INFO] Beginning data cleaning and validation phase...
[OPTIMIZATION] Dropping unnecessary columns. Retaining: ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_distance', 'total_amount', 'fare_amount']
[INFO] Data cleaning complete. Outliers and temporal anomalies successfully purged.
[INFO] Initiating Metric Computation and Performance Aggregations...

--- [CORE METRIC] Average Trip Duration per Year ---
26/06/08 06:06:31 WARN MemoryStore: Not enough space to cache rdd_8_15 in memory! (computed 29.2 MiB so far)
26/06/08 06:06:31 WARN MemoryStore: Not enough space to cache rdd_8_11 in memory! (computed 29.5 MiB so far)
26/06/08 06:06:31 WARN BlockManager: Persisting block rdd_8_15 to disk instead.
26/06/08 06:06:31 WARN BlockManager: Persisting block rdd_8_11 to disk instead.
26/06/08 06:06:32 WARN MemoryStore: Not enough space to cache rdd_8_12 in memory! (computed 29.5 MiB so far)
26/06/08 06:06:32 WARN BlockManager: Persisting block rdd_8_12 to disk instead.
26/06/08 06:06:32 WARN MemoryStore: Not enough space to cache rdd_8_13 in memory! (computed 29.3 MiB so far)
26/06/08 06:06:32 WARN BlockManager: Persisting block rdd_8_13 to disk instead.
26/06/08 06:06:32 WARN MemoryStore: Not enough space to cache rdd_8_8 in memory! (computed 29.7 MiB so far)
26/06/08 06:06:32 WARN BlockManager: Persisting block rdd_8_8 to disk instead.
26/06/08 06:06:59 WARN MemoryStore: Not enough space to cache rdd_8_10 in memory! (computed 80.9 MiB so far)
26/06/08 06:06:59 WARN BlockManager: Persisting block rdd_8_10 to disk instead.
26/06/08 06:07:00 WARN MemoryStore: Not enough space to cache rdd_8_9 in memory! (computed 79.9 MiB so far)
26/06/08 06:07:00 WARN BlockManager: Persisting block rdd_8_9 to disk instead.
26/06/08 06:07:02 WARN MemoryStore: Not enough space to cache rdd_8_14 in memory! (computed 80.1 MiB so far)
26/06/08 06:07:02 WARN BlockManager: Persisting block rdd_8_14 to disk instead.
26/06/08 06:08:22 WARN MemoryStore: Not enough space to cache rdd_8_16 in memory! (computed 79.3 MiB so far)
26/06/08 06:08:22 WARN BlockManager: Persisting block rdd_8_16 to disk instead.
26/06/08 06:08:37 WARN MemoryStore: Not enough space to cache rdd_8_14 in memory! (computed 7.7 MiB so far)
26/06/08 06:08:38 WARN MemoryStore: Not enough space to cache rdd_8_11 in memory! (computed 7.8 MiB so far)
26/06/08 06:08:38 WARN MemoryStore: Not enough space to cache rdd_8_13 in memory! (computed 79.9 MiB so far)
26/06/08 06:08:39 WARN MemoryStore: Not enough space to cache rdd_8_12 in memory! (computed 51.3 MiB so far)
26/06/08 06:08:40 WARN MemoryStore: Not enough space to cache rdd_8_10 in memory! (computed 80.9 MiB so far)
26/06/08 06:08:43 WARN MemoryStore: Not enough space to cache rdd_8_15 in memory! (computed 29.2 MiB so far)
26/06/08 06:08:44 WARN MemoryStore: Not enough space to cache rdd_8_20 in memory! (computed 15.0 MiB so far)
26/06/08 06:08:44 WARN BlockManager: Persisting block rdd_8_20 to disk instead.
26/06/08 06:08:45 WARN MemoryStore: Not enough space to cache rdd_8_22 in memory! (computed 7.8 MiB so far)
26/06/08 06:08:45 WARN BlockManager: Persisting block rdd_8_22 to disk instead.
26/06/08 06:08:46 WARN MemoryStore: Not enough space to cache rdd_8_9 in memory! (computed 7.7 MiB so far)
26/06/08 06:08:46 WARN MemoryStore: Not enough space to cache rdd_8_17 in memory! (computed 29.3 MiB so far)
26/06/08 06:08:46 WARN BlockManager: Persisting block rdd_8_17 to disk instead.
26/06/08 06:08:46 WARN MemoryStore: Not enough space to cache rdd_8_21 in memory! (computed 15.0 MiB so far)
26/06/08 06:08:46 WARN BlockManager: Persisting block rdd_8_21 to disk instead.
26/06/08 06:08:47 WARN MemoryStore: Not enough space to cache rdd_8_18 in memory! (computed 29.4 MiB so far)
26/06/08 06:08:47 WARN BlockManager: Persisting block rdd_8_18 to disk instead.
26/06/08 06:08:52 WARN MemoryStore: Not enough space to cache rdd_8_19 in memory! (computed 51.0 MiB so far)
26/06/08 06:08:52 WARN BlockManager: Persisting block rdd_8_19 to disk instead.
26/06/08 06:09:23 WARN MemoryStore: Not enough space to cache rdd_8_23 in memory! (computed 80.3 MiB so far)
26/06/08 06:09:23 WARN BlockManager: Persisting block rdd_8_23 to disk instead.
26/06/08 06:10:04 WARN MemoryStore: Not enough space to cache rdd_8_16 in memory! (computed 79.3 MiB so far)
26/06/08 06:10:37 WARN MemoryStore: Not enough space to cache rdd_8_24 in memory! (computed 80.7 MiB so far)
26/06/08 06:10:37 WARN BlockManager: Persisting block rdd_8_24 to disk instead.
26/06/08 06:10:47 WARN MemoryStore: Not enough space to cache rdd_8_22 in memory! (computed 80.8 MiB so far)
26/06/08 06:10:51 WARN MemoryStore: Not enough space to cache rdd_8_19 in memory! (computed 51.0 MiB so far)
26/06/08 06:10:52 WARN MemoryStore: Not enough space to cache rdd_8_23 in memory! (computed 29.6 MiB so far)
26/06/08 06:10:52 WARN MemoryStore: Not enough space to cache rdd_8_20 in memory! (computed 51.2 MiB so far)
26/06/08 06:10:55 WARN MemoryStore: Not enough space to cache rdd_8_17 in memory! (computed 50.9 MiB so far)
26/06/08 06:10:59 WARN MemoryStore: Not enough space to cache rdd_8_26 in memory! (computed 15.2 MiB so far)
26/06/08 06:10:59 WARN BlockManager: Persisting block rdd_8_26 to disk instead.
26/06/08 06:11:00 WARN MemoryStore: Not enough space to cache rdd_8_27 in memory! (computed 15.0 MiB so far)
26/06/08 06:11:00 WARN BlockManager: Persisting block rdd_8_27 to disk instead.
26/06/08 06:11:01 WARN MemoryStore: Not enough space to cache rdd_8_28 in memory! (computed 14.9 MiB so far)
26/06/08 06:11:01 WARN BlockManager: Persisting block rdd_8_28 to disk instead.
26/06/08 06:11:02 WARN MemoryStore: Not enough space to cache rdd_8_21 in memory! (computed 29.6 MiB so far)
26/06/08 06:11:05 WARN MemoryStore: Not enough space to cache rdd_8_18 in memory! (computed 29.4 MiB so far)
26/06/08 06:11:05 WARN MemoryStore: Not enough space to cache rdd_8_25 in memory! (computed 51.0 MiB so far)
26/06/08 06:11:05 WARN BlockManager: Persisting block rdd_8_25 to disk instead.
26/06/08 06:11:13 WARN MemoryStore: Not enough space to cache rdd_8_31 in memory! (computed 15.2 MiB so far)
26/06/08 06:11:13 WARN BlockManager: Persisting block rdd_8_31 to disk instead.
26/06/08 06:11:14 WARN MemoryStore: Not enough space to cache rdd_8_30 in memory! (computed 29.8 MiB so far)
26/06/08 06:11:14 WARN BlockManager: Persisting block rdd_8_30 to disk instead.
26/06/08 06:11:32 WARN MemoryStore: Not enough space to cache rdd_8_29 in memory! (computed 81.3 MiB so far)
26/06/08 06:11:32 WARN BlockManager: Persisting block rdd_8_29 to disk instead.
26/06/08 06:12:04 WARN MemoryStore: Not enough space to cache rdd_8_24 in memory! (computed 80.7 MiB so far)
26/06/08 06:12:25 WARN MemoryStore: Not enough space to cache rdd_8_32 in memory! (computed 81.7 MiB so far)
26/06/08 06:12:25 WARN BlockManager: Persisting block rdd_8_32 to disk instead.
26/06/08 06:12:27 WARN MemoryStore: Not enough space to cache rdd_8_25 in memory! (computed 7.7 MiB so far)
26/06/08 06:12:31 WARN MemoryStore: Not enough space to cache rdd_8_27 in memory! (computed 81.0 MiB so far)
26/06/08 06:12:32 WARN MemoryStore: Not enough space to cache rdd_8_26 in memory! (computed 80.9 MiB so far)
26/06/08 06:12:33 WARN MemoryStore: Not enough space to cache rdd_8_28 in memory! (computed 80.2 MiB so far)
26/06/08 06:12:33 WARN MemoryStore: Not enough space to cache rdd_8_30 in memory! (computed 81.2 MiB so far)
26/06/08 06:12:35 WARN MemoryStore: Not enough space to cache rdd_8_29 in memory! (computed 81.3 MiB so far)
26/06/08 06:12:45 WARN MemoryStore: Not enough space to cache rdd_8_31 in memory! (computed 81.4 MiB so far)
26/06/08 06:12:51 WARN MemoryStore: Not enough space to cache rdd_8_32 in memory! (computed 81.7 MiB so far)
26/06/08 06:12:59 WARN MemoryStore: Not enough space to cache rdd_8_15 in memory! (computed 14.9 MiB so far)
26/06/08 06:13:11 WARN MemoryStore: Not enough space to cache rdd_8_11 in memory! (computed 15.1 MiB so far)
26/06/08 06:13:12 WARN MemoryStore: Not enough space to cache rdd_8_10 in memory! (computed 51.7 MiB so far)
26/06/08 06:13:12 WARN MemoryStore: Not enough space to cache rdd_8_14 in memory! (computed 51.1 MiB so far)
26/06/08 06:13:12 WARN MemoryStore: Not enough space to cache rdd_8_12 in memory! (computed 51.3 MiB so far)
26/06/08 06:13:13 WARN MemoryStore: Not enough space to cache rdd_8_13 in memory! (computed 51.1 MiB so far)
26/06/08 06:13:17 WARN MemoryStore: Not enough space to cache rdd_8_16 in memory! (computed 79.3 MiB so far)
26/06/08 06:13:34 WARN MemoryStore: Not enough space to cache rdd_8_22 in memory! (computed 29.8 MiB so far)
26/06/08 06:13:34 WARN MemoryStore: Not enough space to cache rdd_8_9 in memory! (computed 189.5 MiB so far)
26/06/08 06:13:34 WARN MemoryStore: Not enough space to cache rdd_8_21 in memory! (computed 51.5 MiB so far)
26/06/08 06:13:34 WARN MemoryStore: Not enough space to cache rdd_8_19 in memory! (computed 29.3 MiB so far)
26/06/08 06:13:44 WARN MemoryStore: Not enough space to cache rdd_8_20 in memory! (computed 122.9 MiB so far)
26/06/08 06:13:45 WARN MemoryStore: Not enough space to cache rdd_8_17 in memory! (computed 123.4 MiB so far)
26/06/08 06:13:45 WARN MemoryStore: Not enough space to cache rdd_8_23 in memory! (computed 123.8 MiB so far)
26/06/08 06:13:55 WARN MemoryStore: Not enough space to cache rdd_8_26 in memory! (computed 15.2 MiB so far)
26/06/08 06:13:57 WARN MemoryStore: Not enough space to cache rdd_8_25 in memory! (computed 51.0 MiB so far)
26/06/08 06:13:57 WARN MemoryStore: Not enough space to cache rdd_8_24 in memory! (computed 191.1 MiB so far)
26/06/08 06:13:58 WARN MemoryStore: Not enough space to cache rdd_8_27 in memory! (computed 15.0 MiB so far)
26/06/08 06:13:58 WARN MemoryStore: Not enough space to cache rdd_8_28 in memory! (computed 29.5 MiB so far)
26/06/08 06:13:59 WARN MemoryStore: Not enough space to cache rdd_8_30 in memory! (computed 7.8 MiB so far)
26/06/08 06:13:59 WARN MemoryStore: Not enough space to cache rdd_8_31 in memory! (computed 15.2 MiB so far)
26/06/08 06:15:17 WARN MemoryStore: Not enough space to cache rdd_8_32 in memory! (computed 193.6 MiB so far)
+----+----------------+                                                         
|year|avg_duration_min|
+----+----------------+
|2021|16.65           |
|2022|17.38           |
|2023|17.51           |
|2024|17.52           |
|2025|17.54           |
+----+----------------+


--- [CORE METRIC] Average Trip Duration per Month ---
26/06/08 06:15:23 WARN MemoryStore: Not enough space to cache rdd_8_3 in memory! (computed 51.1 MiB so far)
26/06/08 06:15:23 WARN MemoryStore: Not enough space to cache rdd_8_5 in memory! (computed 29.3 MiB so far)
26/06/08 06:15:23 WARN MemoryStore: Not enough space to cache rdd_8_0 in memory! (computed 50.9 MiB so far)
26/06/08 06:15:36 WARN MemoryStore: Not enough space to cache rdd_8_9 in memory! (computed 7.7 MiB so far)
26/06/08 06:15:40 WARN MemoryStore: Not enough space to cache rdd_8_12 in memory! (computed 80.3 MiB so far)
26/06/08 06:15:43 WARN MemoryStore: Not enough space to cache rdd_8_14 in memory! (computed 14.9 MiB so far)
26/06/08 06:15:44 WARN MemoryStore: Not enough space to cache rdd_8_10 in memory! (computed 191.3 MiB so far)
26/06/08 06:15:44 WARN MemoryStore: Not enough space to cache rdd_8_11 in memory! (computed 123.8 MiB so far)
26/06/08 06:16:41 WARN MemoryStore: Not enough space to cache rdd_8_19 in memory! (computed 14.8 MiB so far)
26/06/08 06:16:41 WARN MemoryStore: Not enough space to cache rdd_8_16 in memory! (computed 50.8 MiB so far)
26/06/08 06:16:41 WARN MemoryStore: Not enough space to cache rdd_8_18 in memory! (computed 29.4 MiB so far)
26/06/08 06:16:51 WARN MemoryStore: Not enough space to cache rdd_8_15 in memory! (computed 189.2 MiB so far)
26/06/08 06:16:51 WARN MemoryStore: Not enough space to cache rdd_8_17 in memory! (computed 50.9 MiB so far)
26/06/08 06:18:04 WARN MemoryStore: Not enough space to cache rdd_8_21 in memory! (computed 189.6 MiB so far)
26/06/08 06:18:44 WARN MemoryStore: Not enough space to cache rdd_8_22 in memory! (computed 124.3 MiB so far)
26/06/08 06:18:45 WARN MemoryStore: Not enough space to cache rdd_8_23 in memory! (computed 51.4 MiB so far)
26/06/08 06:18:56 WARN MemoryStore: Not enough space to cache rdd_8_26 in memory! (computed 51.7 MiB so far)
26/06/08 06:18:56 WARN MemoryStore: Not enough space to cache rdd_8_24 in memory! (computed 124.4 MiB so far)
26/06/08 06:18:56 WARN MemoryStore: Not enough space to cache rdd_8_25 in memory! (computed 123.6 MiB so far)
26/06/08 06:19:18 WARN MemoryStore: Not enough space to cache rdd_8_28 in memory! (computed 80.2 MiB so far)
26/06/08 06:19:19 WARN MemoryStore: Not enough space to cache rdd_8_31 in memory! (computed 81.4 MiB so far)
26/06/08 06:19:19 WARN MemoryStore: Not enough space to cache rdd_8_29 in memory! (computed 81.3 MiB so far)
26/06/08 06:19:23 WARN MemoryStore: Not enough space to cache rdd_8_30 in memory! (computed 192.7 MiB so far)
26/06/08 06:19:26 WARN MemoryStore: Not enough space to cache rdd_8_32 in memory! (computed 291.1 MiB so far)
+----+-----+----------------+                                                   
|year|month|avg_duration_min|
+----+-----+----------------+
|2021|1    |14.1            |
|2021|2    |15.0            |
|2021|3    |14.82           |
|2021|4    |15.57           |
|2021|5    |15.97           |
|2021|6    |16.89           |
|2021|7    |16.69           |
|2021|8    |16.64           |
|2021|9    |17.56           |
|2021|10   |17.47           |
|2021|11   |17.89           |
|2021|12   |17.52           |
|2022|1    |14.25           |
|2022|2    |15.7            |
|2022|3    |16.36           |
|2022|4    |17.38           |
|2022|5    |18.28           |
|2022|6    |18.14           |
|2022|7    |16.87           |
|2022|8    |17.04           |
|2022|9    |18.76           |
|2022|10   |18.08           |
|2022|11   |18.2            |
|2022|12   |18.39           |
|2023|1    |15.71           |
|2023|2    |16.06           |
|2023|3    |16.93           |
|2023|4    |17.29           |
|2023|5    |18.13           |
|2023|6    |17.76           |
|2023|7    |17.13           |
|2023|8    |17.16           |
|2023|9    |18.82           |
|2023|10   |18.35           |
|2023|11   |18.21           |
|2023|12   |18.22           |
|2024|1    |15.66           |
|2024|2    |16.03           |
|2024|3    |16.73           |
|2024|4    |17.1            |
|2024|5    |18.08           |
|2024|6    |17.63           |
|2024|7    |17.28           |
|2024|8    |17.41           |
|2024|9    |18.69           |
|2024|10   |18.33           |
|2024|11   |17.86           |
|2024|12   |18.75           |
|2025|1    |15.06           |
|2025|2    |15.44           |
|2025|3    |16.1            |
|2025|4    |16.77           |
|2025|5    |18.15           |
|2025|6    |17.67           |
|2025|7    |17.33           |
|2025|8    |17.5            |
|2025|9    |18.84           |
|2025|10   |18.93           |
|2025|11   |18.64           |
|2025|12   |19.23           |
+----+-----+----------------+


--- [CORE METRIC] Global Overall Average Trip Duration ---
26/06/08 06:19:31 WARN MemoryStore: Not enough space to cache rdd_8_7 in memory! (computed 50.8 MiB so far)
26/06/08 06:19:31 WARN MemoryStore: Not enough space to cache rdd_8_2 in memory! (computed 50.5 MiB so far)
26/06/08 06:19:31 WARN MemoryStore: Not enough space to cache rdd_8_4 in memory! (computed 80.0 MiB so far)
26/06/08 06:19:31 WARN MemoryStore: Not enough space to cache rdd_8_0 in memory! (computed 50.9 MiB so far)
26/06/08 06:19:33 WARN MemoryStore: Not enough space to cache rdd_8_9 in memory! (computed 7.7 MiB so far)
26/06/08 06:19:34 WARN MemoryStore: Not enough space to cache rdd_8_10 in memory! (computed 15.1 MiB so far)
26/06/08 06:19:34 WARN MemoryStore: Not enough space to cache rdd_8_11 in memory! (computed 15.1 MiB so far)
26/06/08 06:19:35 WARN MemoryStore: Not enough space to cache rdd_8_12 in memory! (computed 15.0 MiB so far)
26/06/08 06:19:37 WARN MemoryStore: Not enough space to cache rdd_8_14 in memory! (computed 14.9 MiB so far)
26/06/08 06:19:38 WARN MemoryStore: Not enough space to cache rdd_8_15 in memory! (computed 14.9 MiB so far)
26/06/08 06:19:39 WARN MemoryStore: Not enough space to cache rdd_8_16 in memory! (computed 14.9 MiB so far)
26/06/08 06:19:40 WARN MemoryStore: Not enough space to cache rdd_8_17 in memory! (computed 7.7 MiB so far)
26/06/08 06:19:40 WARN MemoryStore: Not enough space to cache rdd_8_18 in memory! (computed 15.1 MiB so far)
26/06/08 06:19:40 WARN MemoryStore: Not enough space to cache rdd_8_19 in memory! (computed 14.8 MiB so far)
26/06/08 06:19:44 WARN MemoryStore: Not enough space to cache rdd_8_21 in memory! (computed 15.0 MiB so far)
26/06/08 06:19:45 WARN MemoryStore: Not enough space to cache rdd_8_23 in memory! (computed 7.7 MiB so far)
26/06/08 06:19:45 WARN MemoryStore: Not enough space to cache rdd_8_22 in memory! (computed 15.2 MiB so far)
26/06/08 06:19:46 WARN MemoryStore: Not enough space to cache rdd_8_24 in memory! (computed 7.8 MiB so far)
26/06/08 06:19:46 WARN MemoryStore: Not enough space to cache rdd_8_25 in memory! (computed 14.9 MiB so far)
26/06/08 06:19:46 WARN MemoryStore: Not enough space to cache rdd_8_26 in memory! (computed 15.2 MiB so far)
26/06/08 06:19:47 WARN MemoryStore: Not enough space to cache rdd_8_28 in memory! (computed 7.7 MiB so far)
26/06/08 06:19:47 WARN MemoryStore: Not enough space to cache rdd_8_29 in memory! (computed 15.2 MiB so far)
26/06/08 06:19:48 WARN MemoryStore: Not enough space to cache rdd_8_31 in memory! (computed 7.9 MiB so far)
26/06/08 06:19:48 WARN MemoryStore: Not enough space to cache rdd_8_30 in memory! (computed 15.1 MiB so far)
26/06/08 06:19:48 WARN MemoryStore: Not enough space to cache rdd_8_32 in memory! (computed 15.3 MiB so far)
+-----------------------+                                                       
|global_avg_duration_min|
+-----------------------+
|17.36                  |
+-----------------------+


--- [EXTENDED METRIC] Average Trip Duration per Hour of Day (Rush Hours) ---
26/06/08 06:19:53 WARN MemoryStore: Not enough space to cache rdd_8_2 in memory! (computed 29.0 MiB so far)
26/06/08 06:19:53 WARN MemoryStore: Not enough space to cache rdd_8_0 in memory! (computed 29.3 MiB so far)
26/06/08 06:19:53 WARN MemoryStore: Not enough space to cache rdd_8_4 in memory! (computed 51.1 MiB so far)
26/06/08 06:19:54 WARN MemoryStore: Not enough space to cache rdd_8_7 in memory! (computed 79.7 MiB so far)
26/06/08 06:19:54 WARN MemoryStore: Not enough space to cache rdd_8_11 in memory! (computed 7.8 MiB so far)
26/06/08 06:20:02 WARN MemoryStore: Not enough space to cache rdd_8_10 in memory! (computed 125.2 MiB so far)
26/06/08 06:20:07 WARN MemoryStore: Not enough space to cache rdd_8_9 in memory! (computed 189.5 MiB so far)
26/06/08 06:20:07 WARN MemoryStore: Not enough space to cache rdd_8_15 in memory! (computed 29.2 MiB so far)
26/06/08 06:20:10 WARN MemoryStore: Not enough space to cache rdd_8_12 in memory! (computed 123.6 MiB so far)
26/06/08 06:20:15 WARN MemoryStore: Not enough space to cache rdd_8_13 in memory! (computed 190.1 MiB so far)
26/06/08 06:20:15 WARN MemoryStore: Not enough space to cache rdd_8_14 in memory! (computed 190.1 MiB so far)
26/06/08 06:20:16 WARN MemoryStore: Not enough space to cache rdd_8_16 in memory! (computed 122.3 MiB so far)
26/06/08 06:20:16 WARN MemoryStore: Not enough space to cache rdd_8_17 in memory! (computed 14.8 MiB so far)
26/06/08 06:20:22 WARN MemoryStore: Not enough space to cache rdd_8_22 in memory! (computed 51.7 MiB so far)
26/06/08 06:20:22 WARN MemoryStore: Not enough space to cache rdd_8_23 in memory! (computed 29.6 MiB so far)
26/06/08 06:20:22 WARN MemoryStore: Not enough space to cache rdd_8_19 in memory! (computed 189.3 MiB so far)
26/06/08 06:20:22 WARN MemoryStore: Not enough space to cache rdd_8_21 in memory! (computed 80.5 MiB so far)
26/06/08 06:20:28 WARN MemoryStore: Not enough space to cache rdd_8_24 in memory! (computed 191.1 MiB so far)
26/06/08 06:20:29 WARN MemoryStore: Not enough space to cache rdd_8_20 in memory! (computed 284.1 MiB so far)
26/06/08 06:20:30 WARN MemoryStore: Not enough space to cache rdd_8_25 in memory! (computed 79.9 MiB so far)
26/06/08 06:20:30 WARN MemoryStore: Not enough space to cache rdd_8_26 in memory! (computed 29.8 MiB so far)
26/06/08 06:20:46 WARN MemoryStore: Not enough space to cache rdd_8_28 in memory! (computed 123.7 MiB so far)
26/06/08 06:20:46 WARN MemoryStore: Not enough space to cache rdd_8_30 in memory! (computed 125.5 MiB so far)
26/06/08 06:20:46 WARN MemoryStore: Not enough space to cache rdd_8_31 in memory! (computed 125.5 MiB so far)
26/06/08 06:20:46 WARN MemoryStore: Not enough space to cache rdd_8_29 in memory! (computed 125.2 MiB so far)
26/06/08 06:20:46 WARN MemoryStore: Not enough space to cache rdd_8_27 in memory! (computed 190.5 MiB so far)
26/06/08 06:20:46 WARN MemoryStore: Not enough space to cache rdd_8_32 in memory! (computed 81.7 MiB so far)
+----+----------------+                                                         
|hour|avg_duration_min|
+----+----------------+
|0   |15.61           |
|1   |14.89           |
|2   |13.85           |
|3   |14.1            |
|4   |15.66           |
|5   |17.07           |
|6   |16.87           |
|7   |16.69           |
|8   |16.79           |
|9   |16.71           |
|10  |17.01           |
|11  |17.5            |
|12  |17.95           |
|13  |18.49           |
|14  |19.66           |
|15  |20.25           |
|16  |20.27           |
|17  |18.84           |
|18  |16.97           |
|19  |15.97           |
|20  |15.63           |
|21  |15.5            |
|22  |15.9            |
|23  |16.06           |
+----+----------------+


--- [EXTENDED METRIC] Average Trip Duration per Day of Week (1=Sun, 7=Sat) ---
26/06/08 06:20:56 WARN MemoryStore: Not enough space to cache rdd_8_7 in memory! (computed 122.9 MiB so far)
26/06/08 06:20:56 WARN MemoryStore: Not enough space to cache rdd_8_2 in memory! (computed 123.2 MiB so far)
26/06/08 06:20:56 WARN MemoryStore: Not enough space to cache rdd_8_4 in memory! (computed 123.9 MiB so far)
26/06/08 06:20:59 WARN MemoryStore: Not enough space to cache rdd_8_12 in memory! (computed 51.3 MiB so far)
26/06/08 06:21:00 WARN MemoryStore: Not enough space to cache rdd_8_14 in memory! (computed 51.1 MiB so far)
26/06/08 06:21:00 WARN MemoryStore: Not enough space to cache rdd_8_10 in memory! (computed 80.9 MiB so far)
26/06/08 06:21:03 WARN MemoryStore: Not enough space to cache rdd_8_15 in memory! (computed 29.2 MiB so far)
26/06/08 06:21:03 WARN MemoryStore: Not enough space to cache rdd_8_11 in memory! (computed 190.3 MiB so far)
26/06/08 06:21:03 WARN MemoryStore: Not enough space to cache rdd_8_13 in memory! (computed 190.1 MiB so far)
26/06/08 06:21:15 WARN MemoryStore: Not enough space to cache rdd_8_21 in memory! (computed 15.0 MiB so far)
26/06/08 06:21:15 WARN MemoryStore: Not enough space to cache rdd_8_20 in memory! (computed 29.5 MiB so far)
26/06/08 06:21:16 WARN MemoryStore: Not enough space to cache rdd_8_18 in memory! (computed 80.0 MiB so far)
26/06/08 06:21:16 WARN MemoryStore: Not enough space to cache rdd_8_19 in memory! (computed 79.9 MiB so far)
26/06/08 06:21:16 WARN MemoryStore: Not enough space to cache rdd_8_22 in memory! (computed 51.7 MiB so far)
26/06/08 06:21:18 WARN MemoryStore: Not enough space to cache rdd_8_16 in memory! (computed 285.2 MiB so far)
26/06/08 06:21:20 WARN MemoryStore: Not enough space to cache rdd_8_23 in memory! (computed 29.6 MiB so far)
26/06/08 06:21:21 WARN MemoryStore: Not enough space to cache rdd_8_17 in memory! (computed 285.2 MiB so far)
26/06/08 06:21:27 WARN MemoryStore: Not enough space to cache rdd_8_29 in memory! (computed 52.0 MiB so far)
26/06/08 06:21:31 WARN MemoryStore: Not enough space to cache rdd_8_30 in memory! (computed 51.8 MiB so far)
26/06/08 06:21:31 WARN MemoryStore: Not enough space to cache rdd_8_27 in memory! (computed 81.0 MiB so far)
26/06/08 06:21:40 WARN MemoryStore: Not enough space to cache rdd_8_28 in memory! (computed 123.7 MiB so far)
26/06/08 06:21:41 WARN MemoryStore: Not enough space to cache rdd_8_25 in memory! (computed 190.4 MiB so far)
26/06/08 06:22:11 WARN MemoryStore: Not enough space to cache rdd_8_32 in memory! (computed 126.0 MiB so far)
+-----------+----------------+                                                  
|day_of_week|avg_duration_min|
+-----------+----------------+
|1          |16.51           |
|2          |17.06           |
|3          |17.43           |
|4          |17.72           |
|5          |18.24           |
|6          |17.78           |
|7          |16.53           |
+-----------+----------------+

[INFO] Pipeline execution completed successfully.
```

## Testing
To run a test on the aggregation computation of the implementation, a synthetic data frame is used to compare the 
function return with hand computed average. Run:
```
docker run --rm -it -v "${PWD}:/app" --entrypoint pytest nyc-taxi-pipeline:0.0.1 /app/test_pipeline.py -v -s 
```
for windows and 
```
docker run --rm -it -v "$(pwd):/app" --entrypoint pytest nyc-taxi-pipeline:0.0.1 /app/test_pipeline.py -v -s 
```
for linux.

## Findings
Over 5 years of aggregation show that the average passenger spends 17 Minutes and 36 seconds in a New York taxi. 
If the average per month throughout the years is perceived the first 2-3 month tend to show lower averages may due 
to holidays where there could be less traffic compared to other months. In 2021 the average of January and February is 
even lower than for later years may due to COVID restrictions. The aggregation per hour over 5 years shows the clear 
trend that the nightly hours have less traveling time compared to rush hours with higher traffic like 
the working period from around 5 am till 17 pm. Also, the time spent aggregated per weekday shows higher duration for 
workdays Monday till Friday. The data contained several anomalies like month containing data points from other month 
then requested due to measuring methods in the taxameters or rides starting on last day of month at aroung 23:50 
and take until the next first day of month. Also, due to clock drifts, missconfigurations or late offline updates
some files contained even rides from other years which are also filtered out in the clean_data function. 

For low power laptops with 8gb of RAM and older processors the runtime is fairly high with local execution. 
Nevertheless, if we use a compute cluster or higher power hardware with more cores and more RAM the pipeline scales 
with the hardware and availability of several machines where pyspark could compute distributed. By dropping obsolete 
data for average duration computation we ensure only relevant data is loaded to memory. We unified the dataframes by 
typecasting all relevant data types to the same in the unified dataframe to overcome schema missmatches between the
years. The pipeline is also highly parameterized to compute only relevant metrics and set RAM size based on the 
available hardware. 

Regarding testing purposes, a pytest check with a small data frame can be utilized with 5 data points where the 
aggregation can be computed by hand and then compare the results to the code return values. Several other checks could 
assure data integrity like checking for filtered out values. Overall each function should get a test function to ensure
proper functionality.
