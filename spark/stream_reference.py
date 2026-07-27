"""
Spark Structured Streaming — Reference Topics
Reads: customers, products, sellers from Kafka
Writes: cleaned Parquet to HDFS
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

KAFKA_BROKER = "kafka:29092"
OUTPUT_BASE = "hdfs://namenode:9000/data/cleaned"
CHECKPOINT_BASE = "hdfs://namenode:9000/checkpoints"


CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType()),
    StructField("customer_unique_id", StringType()),
    StructField("customer_zip_code_prefix", StringType()),
    StructField("customer_city", StringType()),
    StructField("customer_state", StringType()),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType()),
    StructField("product_category_name", StringType()),
    StructField("product_name_lenght", StringType()),
    StructField("product_description_lenght", StringType()),
    StructField("product_photos_qty", StringType()),
    StructField("product_weight_g", StringType()),
    StructField("product_length_cm", StringType()),
    StructField("product_height_cm", StringType()),
    StructField("product_width_cm", StringType()),
])

SELLERS_SCHEMA = StructType([
    StructField("seller_id", StringType()),
    StructField("seller_zip_code_prefix", StringType()),
    StructField("seller_city", StringType()),
    StructField("seller_state", StringType()),
])


TOPIC_CONFIGS = {
    "customers": {
        "schema": CUSTOMERS_SCHEMA,
        "required_columns": ["customer_id"],
        "timestamp_columns": [],
        "numeric_columns": {},
    },
    "products": {
        "schema": PRODUCTS_SCHEMA,
        "required_columns": ["product_id"],
        "timestamp_columns": [],
        "numeric_columns": {
            "product_name_lenght": DoubleType(),
            "product_description_lenght": DoubleType(),
            "product_photos_qty": DoubleType(),
            "product_weight_g": DoubleType(),
            "product_length_cm": DoubleType(),
            "product_height_cm": DoubleType(),
            "product_width_cm": DoubleType(),
        },
    },
    "sellers": {
        "schema": SELLERS_SCHEMA,
        "required_columns": ["seller_id"],
        "timestamp_columns": [],
        "numeric_columns": {},
    },
}


def process_topic(spark, topic_name, config):
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), config["schema"]).alias("data"))
        .select("data.*")
    )

    cleaned = parsed.dropna(subset=config["required_columns"])

    for num_col, num_type in config["numeric_columns"].items():
        cleaned = cleaned.withColumn(num_col, col(num_col).cast(num_type))

    query = (
        cleaned.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", f"{OUTPUT_BASE}/{topic_name}/")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{topic_name}/")
        .queryName(topic_name)
        .start()
    )

    return query


def main():
    spark = (
        SparkSession.builder
        .appName("Olistflow-StreamReference")
        .getOrCreate()
    )

    queries = []
    for topic_name, config in TOPIC_CONFIGS.items():
        query = process_topic(spark, topic_name, config)
        queries.append(query)
        print(f"Started streaming query: {topic_name}")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
