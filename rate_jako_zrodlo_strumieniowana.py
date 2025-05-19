batch_counter = {"count": 0}
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


def rate_jako_zrodlo_strumieniowana():

    spark = SparkSession.builder.appName("AppendExample").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    rate_df = (spark.readStream
               .format("rate")
               .option("rowsPerSecond", 5)
               .load())

    events = (rate_df
              .withColumn("user_id", expr("concat('u', cast(rand() * 100 as int))"))
              .withColumn("event_type", expr("case when rand() > 0.7 then 'purchase' else 'view' end"))
              )

    query = (events.writeStream
              .foreachBatch(process_batch)
              .start())
    query.awaitTermination()


rate_jako_zrodlo_strumieniowana()
