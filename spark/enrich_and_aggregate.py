"""
Spark Batch Processing — Gold Layer Aggregations (enrich_and_aggregate.py)

Reads cleaned Parquet tables from HDFS Silver layer (data/cleaned/),
executes grain-aware business joins, computes 3 key performance indicator (KPI)
datamarts, and writes idempotent Parquet output to HDFS Gold layer (data/gold/).

Senior Architectural Notes:
1. Avoids Naive 6-Way Mega-Join: Joining order_items (1:N) directly with order_payments (1:M)
   on order_id creates a Cartesian fanout (N x M) that inflates revenue metrics.
   Instead, each KPI is calculated from its native, uninflated grain.
2. Broadcast Joins: Small reference dimensions (customers, products, sellers) are broadcasted
   to eliminate unnecessary network shuffle during joins.
3. Idempotent Execution: Output format uses mode("overwrite") to ensure idempotent batch runs.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as _sum,
    count,
    countDistinct,
    avg,
    round as _round,
    coalesce,
    lit,
    datediff,
    when
)
from pyspark.sql.functions import broadcast


SILVER_BASE = "hdfs://namenode:9000/data/cleaned"
GOLD_BASE = "hdfs://namenode:9000/data/gold"


def load_silver_tables(spark):
    """Load all 6 cleaned Silver Parquet tables from HDFS."""
    print("Loading Silver Parquet tables...")
    return {
        "orders": spark.read.parquet(f"{SILVER_BASE}/orders/"),
        "order_items": spark.read.parquet(f"{SILVER_BASE}/order_items/"),
        "order_payments": spark.read.parquet(f"{SILVER_BASE}/order_payments/"),
        "customers": spark.read.parquet(f"{SILVER_BASE}/customers/"),
        "products": spark.read.parquet(f"{SILVER_BASE}/products/"),
        "sellers": spark.read.parquet(f"{SILVER_BASE}/sellers/"),
    }


def compute_revenue_by_state_category(tables):
    """
    KPI 1: Revenue & Order Volume by Customer State and Product Category.
    Grain: Item-level aggregation (order_items -> orders -> customers & products).
    """
    print("Computing KPI 1: Revenue by State & Category...")

    orders = tables["orders"].filter(col("order_status") != "canceled")
    order_items = tables["order_items"]
    customers = broadcast(tables["customers"])
    products = broadcast(tables["products"])

    joined = (
        order_items
        .join(orders, on="order_id", how="inner")
        .join(customers, on="customer_id", how="inner")
        .join(products, on="product_id", how="left")
    )

    kpi1 = (
        joined
        .withColumn(
            "category",
            coalesce(col("product_category_name"), lit("unspecified"))
        )
        .groupBy("customer_state", "category")
        .agg(
            count("order_id").alias("total_items_sold"),
            countDistinct("order_id").alias("total_orders"),
            _round(_sum("price"), 2).alias("total_item_revenue"),
            _round(_sum("freight_value"), 2).alias("total_freight_value"),
            _round(_sum(col("price") + col("freight_value")), 2).alias("total_gross_revenue")
        )
        .orderBy(col("total_gross_revenue").desc())
    )

    return kpi1


def compute_delivery_sla_by_state(tables):
    """
    KPI 2: Delivery SLA & Carrier Lead Time Analysis by Customer State.
    Grain: Order-level aggregation (orders -> customers).
    Calculates actual vs estimated delivery time, carrier delay, and SLA compliance %.
    """
    print("Computing KPI 2: Delivery SLA Analysis by State...")

    orders = tables["orders"].filter(
        (col("order_status") == "delivered") &
        col("order_delivered_customer_date").isNotNull() &
        col("order_estimated_delivery_date").isNotNull()
    )
    customers = broadcast(tables["customers"])

    joined = orders.join(customers, on="customer_id", how="inner")

    kpi2 = (
        joined
        .withColumn(
            "actual_delivery_days",
            datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))
        )
        .withColumn(
            "estimated_delivery_days",
            datediff(col("order_estimated_delivery_date"), col("order_purchase_timestamp"))
        )
        .withColumn(
            "delay_days",
            datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date"))
        )
        .withColumn(
            "is_delayed",
            when(col("delay_days") > 0, 1).otherwise(0)
        )
        .groupBy("customer_state")
        .agg(
            count("order_id").alias("total_delivered_orders"),
            _round(avg("actual_delivery_days"), 1).alias("avg_actual_delivery_days"),
            _round(avg("estimated_delivery_days"), 1).alias("avg_estimated_delivery_days"),
            _round(avg("delay_days"), 1).alias("avg_delay_days"),
            _sum("is_delayed").alias("delayed_orders_count"),
            _round(
                (_sum("is_delayed") / count("order_id")) * 100, 2
            ).alias("delay_rate_percentage"),
            _round(
                ((count("order_id") - _sum("is_delayed")) / count("order_id")) * 100, 2
            ).alias("on_time_sla_percentage")
        )
        .orderBy(col("total_delivered_orders").desc())
    )

    return kpi2


def compute_payment_type_breakdown(tables):
    """
    KPI 3: Payment Method Distribution & Value Breakdown by Customer State.
    Grain: Payment-level aggregation (order_payments -> orders -> customers).
    """
    print("Computing KPI 3: Payment Type Breakdown...")

    order_payments = tables["order_payments"]
    orders = tables["orders"]
    customers = broadcast(tables["customers"])

    joined = (
        order_payments
        .join(orders, on="order_id", how="inner")
        .join(customers, on="customer_id", how="inner")
    )

    kpi3 = (
        joined
        .groupBy("customer_state", "payment_type")
        .agg(
            count("order_id").alias("payment_transaction_count"),
            _round(_sum("payment_value"), 2).alias("total_payment_value"),
            _round(avg("payment_value"), 2).alias("avg_transaction_value"),
            _round(avg("payment_installments"), 1).alias("avg_installments")
        )
        .orderBy(col("total_payment_value").desc())
    )

    return kpi3


def write_gold_table(df, table_name):
    """Write DataFrame to HDFS Gold Parquet in overwrite mode for idempotency."""
    path = f"{GOLD_BASE}/{table_name}/"
    print(f"Writing Gold table to {path}...")
    (
        df.write
        .format("parquet")
        .mode("overwrite")
        .save(path)
    )
    print(f"Successfully saved {table_name}.")


def main():
    spark = (
        SparkSession.builder
        .appName("Olistflow-Gold-EnrichAndAggregate")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    try:
        tables = load_silver_tables(spark)

        # 1. KPI 1: Revenue by State & Category
        kpi1_df = compute_revenue_by_state_category(tables)
        write_gold_table(kpi1_df, "kpi_revenue_by_state_category")

        # 2. KPI 2: Delivery SLA by State
        kpi2_df = compute_delivery_sla_by_state(tables)
        write_gold_table(kpi2_df, "kpi_delivery_sla_by_state")

        # 3. KPI 3: Payment Method Breakdown
        kpi3_df = compute_payment_type_breakdown(tables)
        write_gold_table(kpi3_df, "kpi_payment_type_breakdown")

        print("=== Gold Layer Batch Processing Completed Successfully ===")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
