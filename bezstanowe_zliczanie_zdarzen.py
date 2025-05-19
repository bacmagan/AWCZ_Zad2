batch_counter = {"count": 0}
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

import os

if os.name == "nt" and "HADOOP_HOME" not in os.environ:
    hadoop_path = os.path.join(os.getcwd(), "hadoop")
    os.environ["HADOOP_HOME"] = hadoop_path
    os.environ["PATH"] += os.pathsep + os.path.join(hadoop_path, "bin")


def process_batch(df, batch_id):
    batch_counter["count"] += 1
    print(f"Batch ID: {batch_id}")
    df.show(truncate=False)


def bezstanowe_zliczanie_zdarzen():
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

    spark = SparkSession.builder.appName("RealTimeEcommerce").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True)
    ])

    stream = (spark.readStream
              .schema(schema)
              .json("data/stream"))

    agg1 = (stream
            .groupBy("event_type")
            .agg(count("*").alias("liczba_zdarzen"))
            )

    query = (agg1
             .writeStream
             .outputMode("complete")
             .format("console")
             .foreachBatch(process_batch)
             .start()
             )

    query.awaitTermination()

bezstanowe_zliczanie_zdarzen()

