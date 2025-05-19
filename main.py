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


def filtrowanie_danych_bez_agregacji():

    spark = SparkSession.builder.appName("StreamingDemo").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    rate_df = (spark.readStream
               .format("rate")
               .option("rowsPerSecond", 5)
               .load())

    events = (rate_df
              .withColumn("user_id", expr("concat('u', cast(rand() * 100 as int))"))
              .withColumn("event_type", expr("case when rand() > 0.7 then 'purchase' else 'view' end"))
              )

    purchases = events.filter(col("event_type") == "purchase")

    query = (purchases.writeStream
             .format("console")
             .outputMode("append")
             .foreachBatch(process_batch)
             .start())
    query.awaitTermination()


def zrodlo_plikowe():
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

    query = (stream.writeStream
             .format("console")
             .foreachBatch(process_batch)
             .start())

    query.awaitTermination()


def bezstanowe_zliczanie_zdarzen():
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
    from pyspark.sql.functions import count
    from pyspark.sql import SparkSession

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

def agregacja_w_oknach_czasowych_tumbling():
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

    import os
    os.makedirs("checkpoint/windowed", exist_ok=True)

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


    windowed = (stream
                .withWatermark("timestamp", "1 minute")
                .groupBy(window("timestamp", "5 minutes"), "event_type")
                .count()
                )

    query = (
        windowed.writeStream
            .outputMode("append")
            .foreachBatch(process_batch)
            .format("console")
            .start()
    )

    query.awaitTermination()

def agregacja_w_oknach_czasowych_sliding():
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

    import os
    os.makedirs("checkpoint/windowed", exist_ok=True)

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


    windowed = (stream
                .withWatermark("timestamp", "1 minute")
                .groupBy(window("timestamp", "5 minutes", "1 minute"), "event_type")
                .count()
                )

    query = (
        windowed.writeStream
            .outputMode("update")
            .foreachBatch(process_batch)
            .format("console")
            .start()
    )

    query.awaitTermination()


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


####Wykonania poszczególnych funkcji

#bezstanowe_zliczanie_zdarzen()
#zrodlo_plikowe()
#rate_jako_zrodlo_strumieniowana()
#filtrowanie_danych_bez_agregacji()
#zrodlo_plikowe()
#agregacja_w_oknach_czasowych_tumbling()
#agregacja_w_oknach_czasowych_sliding()
#segmentacja_klientow()