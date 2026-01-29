## How to Run data_dedup.py:

### From your terminal:

```text
python deduplicate_records.py
```

Spark will start a local Spark session

It will process the data

You should see output like this:
```text
+--------------+-----+----------+---------+
|email         |phone|updated_at|source   |
+--------------+-----+----------+---------+
|john@gmail.com|222  |200       |partnerB|
|mary@gmail.com|333  |150       |internal|
+--------------+-----+----------+---------+
```

### For bigger scripts, you can also use:
```text
spark-submit deduplicate_records.py
```
spark-submit is the standard way to run Spark jobs in production.

For your small example, both python and spark-submit will work.