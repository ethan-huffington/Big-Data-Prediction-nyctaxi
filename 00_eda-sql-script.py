# Databricks notebook source
# MAGIC %md
# MAGIC # S3 Dataset Full Download

# COMMAND ----------

# Retrieve Secrets
dbutils.secrets.setScope("aws-secrets")
access_key = dbutils.secrets.get(scope="aws-secrets", key="aws-access-key")
secret_key = dbutils.secrets.get(scope="aws-secrets", key="aws-secret-key")

# COMMAND ----------

# Set AWS access key and secret key
spark.conf.set("spark.hadoop.fs.s3a.access.key", access_key)
spark.conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)

# Read the Parquet files into a PySpark DataFrame
df = spark.read.parquet("s3a://nyc-tlc/trip data/green_tripdata_2020*.parquet")

# Write the DataFrame to DBFS
df.write.mode("overwrite").parquet("/mnt/data/green_tripdata_2020.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC # Dataset SQL EDA

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG samples;
# MAGIC USE nyctaxi;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM trips LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_rows FROM trips;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE trips;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Feature Inspection

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fare_amount FROM trips;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT trip_distance FROM trips;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     count(DISTINCT pickup_zip),
# MAGIC     count(DISTINCT dropoff_zip)
# MAGIC FROM trips;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(trip_distance) as count,
# MAGIC     AVG(trip_distance) as avg,
# MAGIC     MIN(trip_distance) as min,
# MAGIC     MAX(trip_distance) as max,
# MAGIC     STDDEV(trip_distance) as stddev
# MAGIC FROM trips;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(fare_amount) as count,
# MAGIC     AVG(fare_amount) as avg,
# MAGIC     MIN(fare_amount) as min,
# MAGIC     MAX(fare_amount) as max,
# MAGIC     STDDEV(fare_amount) as stddev
# MAGIC FROM trips;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC        TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_time
# MAGIC FROM trips;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(trip_time) AS total_trips,
# MAGIC     AVG(trip_time) AS average_trip_time,
# MAGIC     MIN(trip_time) AS shortest_trip_time,
# MAGIC     MAX(trip_time) AS longest_trip_time,
# MAGIC     STDDEV(trip_time) AS stddev_trip_time
# MAGIC FROM (
# MAGIC     SELECT TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_time
# MAGIC     FROM trips
# MAGIC ) AS subquery;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick Explore of Databricks Multi-Dimensional Visualizations

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     fare_amount,
# MAGIC     trip_distance,
# MAGIC     TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_time
# MAGIC FROM trips;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     EXTRACT(MONTH FROM trips.tpep_pickup_datetime) AS pickup_month,
# MAGIC     COUNT(*) AS total_trips,
# MAGIC     AVG(trips.fare_amount) AS average_fare
# MAGIC FROM
# MAGIC     trips
# MAGIC GROUP BY
# MAGIC     EXTRACT(MONTH FROM trips.tpep_pickup_datetime);

# COMMAND ----------


