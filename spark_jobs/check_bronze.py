from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("check_bronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load("/lakehouse/bronze/reviews")
print("Nombre de lignes Bronze :", df.count())
df.groupBy("kafka_partition").count().orderBy("kafka_partition").show()
df.select("product_key", "kafka_offset", "ingestion_ts", "raw_json").show(3, truncate=60)