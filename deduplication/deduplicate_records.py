'''
Deduplicate Millions of Records in PySpark

Description:
This file demonstrates how to deduplicate a dataset with millions of records efficiently using PySpark. 
The deduplication process selects a canonical record per unique key (email) based on business rules: 
1. Newest `updated_at` timestamp wins.
2. If timestamps are equal, a record from a more trusted source wins.

'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, trim, when, desc, row_number
from pyspark.sql.window import Window

# ---------------------------
# Step 1: Create Spark Session
# ---------------------------
spark = SparkSession.builder.appName("DeduplicateRecords").getOrCreate()

# ---------------------------
# Step 2: Sample Input Data
# ---------------------------
data = [
    ("John@gmail.com", "111", 100, "partnerA"),
    ("john@gmail.com", "222", 200, "partnerB"),
    ("mary@gmail.com", "333", 150, "internal"),
]

columns = ["email", "phone", "updated_at", "source"]

df = spark.createDataFrame(data, columns)

# ---------------------------
# Step 3: Normalize Dedup Key
# ---------------------------
# Dedup key is lowercase and stripped email to handle duplicates with case differences

df = df.withColumn("dedup_key", lower(trim(df.email)))

# ---------------------------
# Step 4: Assign Source Priority
# ---------------------------
# More trusted sources get higher priority (used to break ties)
SOURCE_PRIORITY = {"internal": 2, "partnerA": 1, "partnerB": 0}

df = df.withColumn(
    "source_priority",
    when(df.source == "internal", 2)
    .when(df.source == "partnerA", 1)
    .otherwise(0)
)

# ---------------------------
# Step 5: Define Window for Ranking
# ---------------------------
# Partition by dedup_key, order by updated_at (desc) then source_priority (desc)
# This ranks records within each group: top-ranked record is canonical

window = Window.partitionBy("dedup_key").orderBy(desc("updated_at"), desc("source_priority"))

# ---------------------------
# Step 6: Select Canonical Record
# ---------------------------
# Assign row numbers within each group; keep only row_num = 1

deduped_df = (
    df.withColumn("row_num", row_number().over(window))
      .filter("row_num = 1")
      .drop("row_num", "source_priority", "dedup_key")
)

# ---------------------------
# Step 7: Show Result
# ---------------------------
deduped_df.show()
