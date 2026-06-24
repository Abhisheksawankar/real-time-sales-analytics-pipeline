from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, count, round
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("SalesPipeline") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
            "org.postgresql:postgresql:42.7.4") \
    .config("spark.sql.shuffle.partitions", "4") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_id",   LongType()),
    StructField("product",    StringType()),
    StructField("region",     StringType()),
    StructField("quantity",   IntegerType()),
    StructField("price",      DoubleType()),
    StructField("timestamp",  DoubleType()),
])

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "sales-events") \
    .option("startingOffsets", "earliest") \
    .load()

events = raw \
    .select(from_json(col("value").cast("string"), schema).alias("d")) \
    .select("d.*") \
    .withColumn("revenue", round(col("quantity") * col("price"), 2))

aggregated = events.groupBy(
    window(col("timestamp").cast("timestamp"), "30 seconds"),
    "product",
    "region"
).agg(
    round(sum("revenue"), 2).alias("total_revenue"),
    count("*").alias("order_count")
# ---- FIX: extract window struct into two flat timestamp columns ----
).select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("product"),
    col("region"),
    col("total_revenue"),
    col("order_count")
)

def write_to_postgres(df, epoch_id):
    if df.count() == 0:
        return
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/sales") \
        .option("dbtable", "sales_aggregates") \
        .option("user", "postgres") \
        .option("password", "secret123") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    print(f"Batch {epoch_id} written to Postgres ✓")

aggregated.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .trigger(processingTime="30 seconds") \
    .start() \
    .awaitTermination()