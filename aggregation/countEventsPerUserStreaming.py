# user_event_streaming.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, count, window

# Setup Spark Session
spark = SparkSession.builder \
    .appName("UserEventCountStreaming") \
    .getOrCreate()

# Read Streaming Data from Kafka
# Replace 'localhost:9092' and 'user-events' with your Kafka server and topic
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user-events") \
    .load()

# Kafka 'value' column is binary; convert to string
df = df.selectExpr("CAST(value AS STRING) as raw_line")

# Parse CSV Lines
df_parsed = df.withColumn("timestamp", split(col("raw_line"), ",")[0]) \
              .withColumn("user_id", split(col("raw_line"), ",")[1]) \
              .withColumn("event_type", split(col("raw_line"), ",")[2])

# Optional: filter out malformed lines
df_parsed = df_parsed.filter(col("user_id").isNotNull() & col("timestamp").isNotNull())

# Aggregate Counts Per User

# Global cumulative count per user
user_counts = df_parsed.groupBy("user_id").agg(count("*").alias("event_count"))

# OR: Windowed aggregation (per 1 minute)
windowed_counts = df_parsed.groupBy(
    window(col("timestamp"), "1 minute"),
    col("user_id")
).agg(count("*").alias("event_count"))

# Output Streaming Results
query = user_counts.writeStream \
    .outputMode("complete") \           # 'complete' shows all counts; use 'update' for incremental
    .format("console") \                # Console output for testing
    .option("checkpointLocation", "/tmp/checkpoints") \  # Required for stateful streaming
    .start()

# Wait for termination
query.awaitTermination()
