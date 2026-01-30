
### In Streaming Case

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create Spark session
spark = SparkSession.builder \
    .appName("StreamingJoinExample") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schemas
users_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("email", StringType(), True),
    StructField("timestamp", TimestampType(), True)  # event time
])

purchases_schema = StructType([
    StructField("email", StringType(), True),
    StructField("purchase_amount", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)  # event time
])

#Read streaming data from Kafka
users_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "users") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), users_schema).alias("data")) \
    .select("data.*")

purchases_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "purchases") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), purchases_schema).alias("data")) \
    .select("data.*")

# Perform stream-stream join
# Using watermarks to manage state for late events
joined_stream = users_stream \
    .withWatermark("timestamp", "10 minutes") \
    .join(
        purchases_stream.withWatermark("timestamp", "10 minutes"),
        on="email",
        how="inner"
    )

# Aggregate purchases per user
user_totals = joined_stream.groupBy("user_id") \
    .agg(sum("purchase_amount").alias("total_purchase"))

# Output results to console
query = user_totals.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
