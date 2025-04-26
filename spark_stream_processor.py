from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, min, max, avg, expr
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import os

# Create checkpoint directories
current_dir = os.getcwd()
kafka_checkpoint_dir = os.path.join(current_dir, "checkpoints", "kafka")
mysql_checkpoint_dir = os.path.join(current_dir, "checkpoints", "mysql")

os.makedirs(kafka_checkpoint_dir, exist_ok=True)
os.makedirs(mysql_checkpoint_dir, exist_ok=True)

# Convert paths to file:// URIs
kafka_checkpoint_uri = f"file://{kafka_checkpoint_dir}"
mysql_checkpoint_uri = f"file://{mysql_checkpoint_dir}"

# Create Spark Session with proper configuration
spark = SparkSession.builder \
    .appName("CryptoStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .config("spark.jars", "mysql-connector-java-8.3.0.jar") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.sql.streaming.checkpointLocation", "file:///tmp/spark-checkpoint") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .master("local[*]") \
    .getOrCreate()

# Define schema for the incoming data
schema = StructType() \
    .add("asset_id_base", StringType()) \
    .add("asset_id_quote", StringType()) \
    .add("rate", DoubleType()) \
    .add("time", StringType())

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw_coin_data") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", "10000") \
    .load()

# Parse JSON data
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("time").cast(TimestampType()))

# Window aggregation with watermark
agg_window = json_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window("timestamp", "5 minutes"),
        "asset_id_base", "asset_id_quote"
    ).agg(
        count("rate").alias("update_count"),
        avg("rate").alias("avg_rate"),
        min("rate").alias("min_rate"),
        max("rate").alias("max_rate"),
        expr("sum(rate)").alias("sum_rate")
    )

# Write aggregated data to Kafka
agg_query = agg_window.selectExpr(
    "to_json(struct(*)) AS value"
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "aggregated_data") \
    .option("checkpointLocation", kafka_checkpoint_uri) \
    .outputMode("update") \
    .trigger(processingTime="5 seconds") \
    .start()

# Function to write to MySQL
def write_to_mysql(df, epoch_id):
    try:
        if df is not None and not df.isEmpty():
            df.select(
                col("window.start").alias("start_time"),
                col("window.end").alias("end_time"),
                "asset_id_base", 
                "asset_id_quote",
                "update_count",
                "avg_rate",
                "min_rate",
                "max_rate",
                "sum_rate"
            ).write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/cryptodb") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "stream_agg") \
            .option("user", "coinuser") \
            .option("password", "Coinpass@123") \
            .mode("append") \
            .save()
            print(f"Successfully wrote batch {epoch_id} to MySQL")
    except Exception as e:
        print(f"Error writing to MySQL: {str(e)}")

# Write to MySQL
mysql_query = agg_window.writeStream \
    .foreachBatch(write_to_mysql) \
    .option("checkpointLocation", mysql_checkpoint_uri) \
    .outputMode("update") \
    .trigger(processingTime="5 seconds") \
    .start()

# Add error handling and keep the application running
try:
    spark.streams.awaitAnyTermination()
except Exception as e:
    print(f"An error occurred: {str(e)}")
finally:
    print("Stopping streams gracefully...")
    if agg_query is not None:
        agg_query.stop()
    if mysql_query is not None:
        mysql_query.stop()