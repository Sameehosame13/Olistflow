from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)

KAFKA_BROKER = "kafka:29092"
OUTPUT_BASE = "hdfs://namenode:9000/data/cleaned"
CHECKPOINT_BASE = "hdfs://namenode:9000/checkpoints"


ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", StringType()),
    StructField("order_approved_at", StringType()),
    StructField("order_delivered_carrier_date", StringType()),
    StructField("order_delivered_customer_date", StringType()),
    StructField("order_estimated_delivery_date", StringType()),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("order_item_id", StringType()),
    StructField("product_id", StringType()),
    StructField("seller_id", StringType()),
    StructField("shipping_limit_date", StringType()),
    StructField("price", StringType()),
    StructField("freight_value", StringType()),
])

ORDER_PAYMENTS_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("payment_sequential", StringType()),
    StructField("payment_type", StringType()),
    StructField("payment_installments", StringType()),
    StructField("payment_value", StringType()),
])



TOPIC_CONFIGS = {
    "orders": {
        "schema": ORDERS_SCHEMA,
        "required_columns": ["order_id", "customer_id"],
        "timestamp_columns": [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        "numeric_columns": {},
        "watermark": ("order_purchase_timestamp", "1 hour"),
    },
    "order_items": {
        "schema": ORDER_ITEMS_SCHEMA,
        "required_columns": ["order_id", "product_id", "seller_id"],
        "timestamp_columns": ["shipping_limit_date"],
        "numeric_columns": {
            "order_item_id": IntegerType(),
            "price": DoubleType(),
            "freight_value": DoubleType(),
        },
        "watermark": None,
    },
    "order_payments": {
        "schema": ORDER_PAYMENTS_SCHEMA,
        "required_columns": ["order_id"],
        "timestamp_columns": [],
        "numeric_columns": {
            "payment_sequential": IntegerType(),
            "payment_installments": IntegerType(),
            "payment_value": DoubleType(),
        },
        "watermark": None,
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

    for ts_col in config["timestamp_columns"]:
        cleaned = cleaned.withColumn(ts_col, col(ts_col).cast(TimestampType()))

    for num_col, num_type in config["numeric_columns"].items():
        cleaned = cleaned.withColumn(num_col, col(num_col).cast(num_type))

    if config["watermark"]:
        wm_col, wm_delay = config["watermark"]
        cleaned = cleaned.withWatermark(wm_col, wm_delay)

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
        .appName("Olistflow-StreamEvents")
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
