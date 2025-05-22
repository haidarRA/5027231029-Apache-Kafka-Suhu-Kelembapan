from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, when, window
from pyspark.sql.types import StructType, StringType, FloatType

spark = SparkSession.builder \
    .appName("Monitoring Gudang Format Tabel") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema_suhu = StructType() \
    .add("gudang_id", StringType()) \
    .add("suhu", FloatType())

schema_kelembapan = StructType() \
    .add("gudang_id", StringType()) \
    .add("kelembapan", FloatType())

stream_suhu = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .option("startingOffsets", "latest") \
    .load()

stream_kelembapan = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-kelembapan-gudang") \
    .option("startingOffsets", "latest") \
    .load()

parsed_suhu = stream_suhu.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_suhu).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", current_timestamp()) \
    .withColumn("warning", when(col("suhu") > 80, "Peringatan Suhu Tinggi").otherwise("Aman"))

parsed_kelembapan = stream_kelembapan.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_kelembapan).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", current_timestamp()) \
    .withColumn("warning", when(col("kelembapan") > 70, "Peringatan Kelembapan Tinggi").otherwise("Aman"))

query_suhu = parsed_suhu.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 1000) \
    .trigger(processingTime="1 second") \
    .start()

query_kelembapan = parsed_kelembapan.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 1000) \
    .trigger(processingTime="1 second") \
    .start()

windowed_suhu = parsed_suhu \
    .withWatermark("timestamp", "15 seconds") \
    .groupBy(
        window(col("timestamp"), "10 seconds"),
        col("gudang_id")
    ).agg(
        {"suhu": "last"}
    ).withColumnRenamed("last(suhu)", "suhu")

windowed_kelembapan = parsed_kelembapan \
    .withWatermark("timestamp", "15 seconds") \
    .groupBy(
        window(col("timestamp"), "10 seconds"),
        col("gudang_id")
    ).agg(
        {"kelembapan": "last"}
    ).withColumnRenamed("last(kelembapan)", "kelembapan")

joined_df = windowed_suhu.join(
    windowed_kelembapan,
    on=["window", "gudang_id"]
)

joined_with_warning = joined_df.withColumn(
    "warning",
    when((col("suhu") > 80) & (col("kelembapan") <= 70), "Suhu tinggi, kelembapan normal")
    .when((col("suhu") <= 80) & (col("kelembapan") > 70), "Kelembapan tinggi, suhu aman")
    .when((col("suhu") > 80) & (col("kelembapan") > 70), "Bahaya tinggi! Barang berisiko rusak")
    .otherwise("Aman")
)

query_gabungan = joined_with_warning.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 1000) \
    .trigger(processingTime="10 seconds") \
    .start()

query_suhu.awaitTermination()
query_kelembapan.awaitTermination()
query_gabungan.awaitTermination()