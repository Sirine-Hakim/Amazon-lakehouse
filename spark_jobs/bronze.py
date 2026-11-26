from pyspark.sql import SparkSession
from pyspark.sql import functions as F

KAFKA_SERVERS = "kafka:29092"   # adresse interne Docker (pas localhost)
TOPIC = "reviews_raw"
BRONZE_PATH = "/lakehouse/bronze/reviews"
CHECKPOINT = "/lakehouse/_checkpoints/bronze_reviews"

spark = (
    SparkSession.builder.appName("bronze_reviews")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# Bronze = données brutes : JSON conservé tel quel + métadonnées Kafka
bronze = raw.select(
    F.col("key").cast("string").alias("product_key"),
    F.col("value").cast("string").alias("raw_json"),
    F.col("topic"),
    F.col("partition").alias("kafka_partition"),
    F.col("offset").alias("kafka_offset"),
    F.col("timestamp").alias("kafka_timestamp"),
    F.current_timestamp().alias("ingestion_ts"),
)

query = (
    bronze.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)   # traite tout le backlog, puis s'arrête
    .start(BRONZE_PATH)
)
query.awaitTermination()
print("Bronze terminé.")