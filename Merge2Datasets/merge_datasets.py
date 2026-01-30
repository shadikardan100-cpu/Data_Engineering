# Interviewer might say something like this:

# “Suppose we have two datasets:

# Dataset A: User email addresses and user IDs from an e-commerce platform (10 million rows).

# Dataset B: User purchase history with email and purchase amount (50 million rows).

# How would you efficiently join these datasets on the email to get a combined view of each user’s total purchase amount?”

# Solution Thinking Process:
# 10M + 50M rows → too large for simple in-memory join on a single machine (unless you have huge RAM).

# Distributed processing is preferred (Spark, Dataflow, BigQuery, Snowflake).


from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName("JoinExample").getOrCreate()

# Load datasets
users = spark.read.parquet("gs://bucket/users.parquet")   # Dataset A
purchases = spark.read.parquet("gs://bucket/purchases.parquet") # Dataset B

# Join on email
joined = users.join(purchases, on="email", how="inner")

# Aggregate total purchase per user
result = joined.groupBy("user_id").agg(sum("purchase_amount").alias("total_purchase"))

result.show()