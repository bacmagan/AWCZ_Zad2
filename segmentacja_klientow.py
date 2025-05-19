batch_counter = {"count": 0}
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

import os

if os.name == "nt" and "HADOOP_HOME" not in os.environ:
    hadoop_path = os.path.join(os.getcwd(), "hadoop")
    os.environ["HADOOP_HOME"] = hadoop_path
    os.environ["PATH"] += os.pathsep + os.path.join(hadoop_path, "bin")


def process_batch(df, batch_id):
    batch_counter["count"] += 1
    print(f"Batch ID: {batch_id}")
    df.show(truncate=False)


def segmentacja_klientow():
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
    import os
    os.makedirs("checkpoint/segments", exist_ok=True)

    spark = SparkSession.builder.appName("SegmentacjaKlientow").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True)
    ])

    stream = (
        spark.readStream
        .schema(schema)
        .json("data/stream")
    )

    windowed = (
        stream
        .withWatermark("timestamp", "1 minute")
        .groupBy(window("timestamp", "5 minutes"), "user_id")
        .agg(collect_set("event_type").alias("eventy"))
        .withColumn("segment", expr("""
            CASE 
                WHEN array_contains(eventy, 'purchase') THEN 'Buyer'
                WHEN array_contains(eventy, 'cart') THEN 'Cart abandoner'
                WHEN array_contains(eventy, 'view') THEN 'Lurker'
                ELSE 'Unknown'
            END
        """))
    )

    query = (
        windowed.writeStream
        .outputMode("update")
        .foreachBatch(process_batch)
        .format("console")
        .start()
    )

    query.awaitTermination()

segmentacja_klientow()

