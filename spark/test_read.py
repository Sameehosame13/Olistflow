from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("TestRead").getOrCreate()
df = spark.read.format("kafka").option("kafka.bootstrap.servers", "kafka:29092").option("subscribe", "orders").option("startingOffsets", "earliest").load()
print("=== RAW KAFKA ORDERS COUNT ===", df.count())
schema = StructType([StructField("order_id", StringType()), StructField("customer_id", StringType())])
parsed = df.selectExpr("CAST(value AS STRING) AS json").select(from_json(col("json"), schema).alias("data")).select("data.*")
print("=== PARSED COUNT ===", parsed.count())
parsed.show(5)
