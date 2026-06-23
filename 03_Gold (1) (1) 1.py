# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 GOLD LAYER — Business Aggregations & Summary Tables
# MAGIC **Purpose:** Aggregate Silver data into business-ready summary tables using Star Schema design.
# MAGIC
# MAGIC **Tables Created:**
# MAGIC - `gold.restaurant_performance` (Fact — partitioned by order_month)
# MAGIC - `gold.customer_behavior`
# MAGIC - `gold.delivery_partner_performance`
# MAGIC - `gold.monthly_trends`

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📦 Imports & Setup

# COMMAND ----------

import os
from datetime import datetime

from pyspark.sql import Row, SparkSession, Window
from pyspark.sql.functions import (
    broadcast, col, count, countDistinct, current_timestamp, desc, expr, lag,
    lit, max as spark_max, min as spark_min, quarter, round as spark_round,
    row_number, sum as spark_sum, avg as spark_avg, datediff, to_date, when,
    year, month, dayofmonth, dayofweek, date_format
)
from pyspark.sql.types import NumericType
from pyspark.sql.utils import AnalysisException
from azure.storage.blob import BlobServiceClient

spark = SparkSession.builder.getOrCreate()

# --- SQL connection configuration -----------------------------------------
# NOTE: Do NOT hardcode production passwords in notebooks.
# Use Databricks Secret Scopes or environment variables instead.
SQL_SERVER   = "<your_sql_server>.database.windows.net"
SQL_DATABASE = "<your_database>"
SQL_USER     = "<your_username>"
try:
    SQL_PASSWORD = dbutils.secrets.get("my-scope", "sql-password")
except NameError:
    SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "<REDACTED>")

JDBC_URL = (
    f"jdbc:sqlserver://{SQL_SERVER};"
    f"database={SQL_DATABASE};"
    "encrypt=true;trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
)

# Example: read from SQL Server via JDBC if needed
# df_sql = spark.read.format("jdbc") \
#     .option("url", JDBC_URL) \
#     .option("dbtable", "dbo.your_table") \
#     .option("user", SQL_USER) \
#     .option("password", SQL_PASSWORD) \
#     .load()
# ---------------------------------------------------------------------------

spark.sql("CREATE DATABASE IF NOT EXISTS gold")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
print("✅ Gold database ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📥 Read Silver Tables

# COMMAND ----------

df_orders   = spark.read.format("delta").table("silver.orders")
df_cust     = spark.read.format("delta").table("silver.customers_current")
df_partners = spark.read.format("delta").table("silver.delivery_partners")

print(f"Orders: {df_orders.count()} | Customers: {df_cust.count()} | Partners: {df_partners.count()}")

# COMMAND ----------

# DBTITLE 1,Incremental State Detection
from pyspark.sql.functions import lit

# ── Gold incremental state table ────────────────────────────────────────────
spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.gold_pipeline_state (
        last_gold_run_ts TIMESTAMP
    ) USING DELTA
""")

last_gold_ts = spark.sql(
    "SELECT max(last_gold_run_ts) FROM gold.gold_pipeline_state"
).collect()[0][0]

if last_gold_ts:
    new_months = sorted([
        r["order_month"] for r in
        df_orders.filter(col("ingested_at") > lit(str(last_gold_ts)))
                 .select("order_month").distinct().collect()
    ])
    affected_customers = [
        r["customer_id"] for r in
        df_orders.filter(col("order_month").isin(new_months))
                 .select("customer_id").distinct().collect()
    ]
    affected_partners = [
        r["partner_id"] for r in
        df_orders.filter(col("order_month").isin(new_months))
                 .select("partner_id").distinct().collect()
    ]
    FULL_LOAD = False
    print(f"📅 Incremental run | New months: {new_months}")
    print(f"   👤 Affected customers: {len(affected_customers)} | 🚴 Affected partners: {len(affected_partners)}")
else:
    new_months = sorted([r["order_month"] for r in df_orders.select("order_month").distinct().collect()])
    affected_customers = None
    affected_partners  = None
    FULL_LOAD = True
    print(f"🆕 First Gold run (full load) | Months: {new_months}")

# COMMAND ----------

# DBTITLE 1,Star Schema Design
# MAGIC %md
# MAGIC ---
# MAGIC ## 🌟 STAR SCHEMA DESIGN
# MAGIC
# MAGIC Gold layer uses a **Star Schema** — a central **Fact table** surrounded by **Dimension tables**:
# MAGIC
# MAGIC ```
# MAGIC                  dim_customer
# MAGIC                       │
# MAGIC dim_partner ──── fact_orders_summary ──── dim_restaurant
# MAGIC                       │
# MAGIC                  monthly_trends
# MAGIC ```
# MAGIC
# MAGIC | Table | Type | Contains |
# MAGIC |-------|------|----------|
# MAGIC | `fact_orders` | Fact | Order metrics + FK refs to all dims |
# MAGIC | `dim_customer` | Dimension | Customer attributes |
# MAGIC | `dim_restaurant` | Dimension | Restaurant attributes |
# MAGIC | `dim_delivery_partner` | Dimension | Partner attributes |
# MAGIC | `dim_date` | Dimension | Date spine (year, month, quarter, weekend) |
# MAGIC | `orders_enriched` | Joined Schema | Fully denormalized (broadcast join) |
# MAGIC | `restaurant_performance` | Aggregated Fact | Revenue, orders, ratings per restaurant/month |
# MAGIC | `customer_behavior` | Aggregated Fact | Cumulative customer metrics |
# MAGIC | `delivery_partner_performance` | Aggregated Fact | Partner-level metrics |
# MAGIC | `monthly_trends` | Aggregated Fact | Time-series MoM aggregates |
# MAGIC
# MAGIC **Why NOT Z-ordered in Gold?**
# MAGIC Gold tables are already aggregated — they're small compared to Silver. Query patterns here are month-level aggregations and full scans, not point lookups. Partitioning by `order_month` is sufficient for query pruning.

# COMMAND ----------

# DBTITLE 1,Dimension & Fact Tables Header
# MAGIC %md
# MAGIC ---
# MAGIC ## 🏛️ DIMENSION TABLES, FACT TABLE & ENRICHED SCHEMA
# MAGIC
# MAGIC | Table | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `gold.dim_customer` | Dimension | Customer attributes from Silver |
# MAGIC | `gold.dim_restaurant` | Dimension | Restaurant attributes from Bronze |
# MAGIC | `gold.dim_delivery_partner` | Dimension | Partner attributes from Silver |
# MAGIC | `gold.dim_date` | Dimension | Date spine derived from order dates |
# MAGIC | `gold.fact_orders` | Fact | Order metrics with FK references to all dims |
# MAGIC | `gold.orders_enriched` | Joined Schema | Fully denormalized fact + all dims (broadcast join) |
# MAGIC
# MAGIC > **Broadcast joins** are used when joining small dim tables (≤1500 rows) to the large fact table to avoid shuffle.

# COMMAND ----------

# DBTITLE 1,dim_customer

# ── Dimension: Customer ────────────────────────────────────────────────────
df_dim_customer = (
    spark.table("silver.customers_current")
    .select(
        col("customer_id"),
        col("customer_name"),
        col("email"),
        col("phone"),
        col("city"),
        col("loyalty_status"),
        col("registration_date") if "registration_date" in spark.table("silver.customers_current").columns else lit(None).cast("string").alias("registration_date")
    )
    .withColumn("dim_created_at", current_timestamp())
    .dropDuplicates(["customer_id"])
)

df_dim_customer.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable("gold.dim_customer")

print(f"✅ gold.dim_customer written | rows: {df_dim_customer.count()} | cols: {len(df_dim_customer.columns)}")
display(spark.table("gold.dim_customer").limit(5))

# COMMAND ----------

# DBTITLE 1,dim_restaurant
# ── Dimension: Restaurant ─────────────────────────────────────────────────
df_bronze_rest = spark.table("bronze.restaurants")

df_dim_restaurant = (
    df_bronze_rest
    .select(
        col("restaurant_id"),
        col("restaurant_name"),
        col("city") if "city" in df_bronze_rest.columns else lit(None).cast("string").alias("city"),
        col("cuisine").alias("cuisine_type") if "cuisine" in df_bronze_rest.columns else lit(None).cast("string").alias("cuisine_type"),
        col("rating").alias("restaurant_rating") if "rating" in df_bronze_rest.columns else lit(None).cast("double").alias("restaurant_rating")
    )
    .withColumn("dim_created_at", current_timestamp())
    .dropDuplicates(["restaurant_id"])
)

df_dim_restaurant.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable("gold.dim_restaurant")

print(f"✅ gold.dim_restaurant written | rows: {df_dim_restaurant.count()} | cols: {len(df_dim_restaurant.columns)}")
display(spark.table("gold.dim_restaurant").limit(5))

# COMMAND ----------

# DBTITLE 1,dim_delivery_partner
# ── Dimension: Delivery Partner ───────────────────────────────────────────
df_dim_partner = (
    spark.table("silver.delivery_partners")
    .select(
        col("partner_id"),
        col("partner_name") if "partner_name" in spark.table("silver.delivery_partners").columns else col("name").alias("partner_name"),
        col("vehicle_type"),
        col("city"),
        col("rating").alias("partner_rating") if "rating" in spark.table("silver.delivery_partners").columns else lit(None).cast("double").alias("partner_rating")
    )
    .withColumn("dim_created_at", current_timestamp())
    .dropDuplicates(["partner_id"])
)

df_dim_partner.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable("gold.dim_delivery_partner")

print(f"✅ gold.dim_delivery_partner written | rows: {df_dim_partner.count()} | cols: {len(df_dim_partner.columns)}")
display(spark.table("gold.dim_delivery_partner").limit(5))

# COMMAND ----------

# DBTITLE 1,dim_date

# ── Dimension: Date spine from all order dates ─────────────────────────────
df_dim_date = (
    df_orders
    .select(to_date(col("order_date")).alias("order_date"))
    .dropDuplicates(["order_date"])
    .filter(col("order_date").isNotNull())
    .withColumn("year",         year(col("order_date")))
    .withColumn("month",        month(col("order_date")))
    .withColumn("day",          dayofmonth(col("order_date")))
    .withColumn("quarter",      quarter(col("order_date")))
    .withColumn("day_of_week",  dayofweek(col("order_date")))
    .withColumn("month_name",   date_format(col("order_date"), "MMMM"))
    .withColumn("day_name",     date_format(col("order_date"), "EEEE"))
    .withColumn("is_weekend",   when(dayofweek(col("order_date")).isin(1, 7), True).otherwise(False))
    .orderBy("order_date")
)

df_dim_date.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable("gold.dim_date")

print(f"✅ gold.dim_date written | rows: {df_dim_date.count()} | cols: {len(df_dim_date.columns)}")
display(spark.table("gold.dim_date").limit(5))

# COMMAND ----------

# DBTITLE 1,fact_orders

# ── Fact Table: Orders ─────────────────────────────────────────────────────
# Contains order metrics + FK references to all dimension tables
df_fact_orders = (
    df_orders
    .select(
        col("order_id"),
        col("customer_id"),              # FK → gold.dim_customer
        col("restaurant_id"),            # FK → gold.dim_restaurant
        col("partner_id"),               # FK → gold.dim_delivery_partner
        to_date(col("order_date")).alias("order_date"),  # FK → gold.dim_date
        col("order_month"),
        expr("try_cast(order_amount AS DOUBLE)").alias("order_amount"),
        col("payment_amount"),
        expr("try_cast(delivery_time AS DOUBLE)").alias("delivery_time"),
        col("rating"),
        col("promo_code") if "promo_code" in df_orders.columns else lit(None).cast("string").alias("promo_code"),
        col("delivery_mode") if "delivery_mode" in df_orders.columns else lit(None).cast("string").alias("delivery_mode"),
        col("ingested_at")
    )
)

df_fact_orders.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("order_month") \
    .saveAsTable("gold.fact_orders")

print(f"✅ gold.fact_orders written | rows: {df_fact_orders.count()} | cols: {len(df_fact_orders.columns)}")
display(spark.table("gold.fact_orders").limit(5))

# COMMAND ----------

# DBTITLE 1,orders_enriched (Broadcast Join)
from pyspark.sql.functions import broadcast

# ── Fully Joined Schema: fact + all dims via BROADCAST JOIN ────────────────
# Rename 'city' and 'dim_created_at' in each dim before joining
# to avoid AMBIGUOUS_REFERENCE errors (all 3 dims have both columns)

df_fact  = spark.table("gold.fact_orders")

df_dcust = (
    spark.table("gold.dim_customer")
    .withColumnRenamed("city",           "customer_city")
    .withColumnRenamed("dim_created_at", "cust_dim_ts")
)
df_drest = (
    spark.table("gold.dim_restaurant")
    .withColumnRenamed("city",           "restaurant_city")
    .withColumnRenamed("dim_created_at", "rest_dim_ts")
)
df_dpart = (
    spark.table("gold.dim_delivery_partner")
    .withColumnRenamed("city",           "partner_city")
    .withColumnRenamed("dim_created_at", "part_dim_ts")
)
df_ddate = spark.table("gold.dim_date")

df_orders_enriched = (
    df_fact
    .join(broadcast(df_dcust), on="customer_id",  how="left")   # broadcast: ~1500 rows
    .join(broadcast(df_drest), on="restaurant_id", how="left")   # broadcast: small
    .join(broadcast(df_dpart), on="partner_id",    how="left")   # broadcast: ~500 rows
    .join(broadcast(df_ddate), on="order_date",    how="left")   # broadcast: ~90 dates
    .select(
        # ── Order keys & metrics ──
        col("order_id"),
        col("order_date"),
        col("order_month"),
        col("order_amount"),
        col("payment_amount"),
        col("delivery_time"),
        col("rating"),
        col("promo_code"),
        col("delivery_mode"),
        # ── Customer dim ──
        col("customer_id"),
        col("customer_name"),
        col("customer_city"),
        col("loyalty_status"),
        # ── Restaurant dim ──
        col("restaurant_id"),
        col("restaurant_name"),
        col("cuisine_type"),
        col("restaurant_city"),
        # ── Partner dim ──
        col("partner_id"),
        col("partner_name"),
        col("vehicle_type"),
        col("partner_city"),
        # ── Date dim ──
        col("year"),
        col("month_name"),
        col("day_name"),
        col("quarter"),
        col("is_weekend")
    )
)

df_orders_enriched.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("order_month") \
    .saveAsTable("gold.orders_enriched")

print(f"✅ gold.orders_enriched written (broadcast join) | rows: {df_orders_enriched.count()} | cols: {len(df_orders_enriched.columns)}")
display(spark.table("gold.orders_enriched").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📊 SUMMARY TABLE 1: gold.restaurant_performance

# COMMAND ----------

# DBTITLE 1,Gold Table 1: restaurant_performance
# Mode helper — most frequent value per group
def mode_col(df, group_col, value_col, alias_name):
    """Return the most frequent non-null value for each group key."""
    w = Window.partitionBy(group_col).orderBy(desc("_cnt"))
    return (
        df.filter(col(value_col).isNotNull())
        .groupBy(group_col, value_col)
        .agg(count("*").alias("_cnt"))
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .select(group_col, col(value_col).alias(alias_name))
    )

# Mode promo_code and delivery_mode per restaurant+month
df_promo_mode = mode_col(
    df_orders.withColumn("grp", col("restaurant_id").cast("string")),
    "restaurant_id", "promo_code", "most_used_promo_code"
)

df_rest_perf = (
    df_orders
    .groupBy("restaurant_id", "order_month")
    .agg(
        count("*").alias("total_orders"),
        spark_round(spark_sum("order_amount"), 2).alias("total_revenue"),
        spark_round(spark_avg("order_amount"), 2).alias("avg_order_value"),
        spark_round(spark_avg("rating"), 2).alias("avg_rating"),
        spark_round(spark_avg("delivery_time"), 2).alias("avg_delivery_time_minutes"),
        countDistinct("customer_id").alias("total_unique_customers")
    )
    .join(df_promo_mode, on="restaurant_id", how="left")
)

# Incremental: overwrite only new/changed month partitions
df_rest_perf_inc = df_rest_perf.filter(col("order_month").isin(new_months))
if FULL_LOAD:
    df_rest_perf_inc.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("order_month").saveAsTable("gold.restaurant_performance")
    print("✅ gold.restaurant_performance created (full load)")
else:
    if not new_months:
        print("ℹ️  gold.restaurant_performance — no new months to process, skipping.")
    else:
        replace_cond = " OR ".join([f"order_month = {m}" for m in new_months])
        df_rest_perf_inc.write.format("delta").mode("overwrite").option("replaceWhere", replace_cond).saveAsTable("gold.restaurant_performance")
        print(f"✅ gold.restaurant_performance updated (replaceWhere | months={new_months})")
print(f"   Total rows: {spark.table('gold.restaurant_performance').count()}")
display(spark.table("gold.restaurant_performance").orderBy(col("total_revenue").desc()).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 👤 SUMMARY TABLE 2: gold.customer_behavior

# COMMAND ----------

# DBTITLE 1,Gold Table 2: customer_behavior
df_most_rest  = mode_col(df_orders, "customer_id", "restaurant_id", "most_ordered_restaurant")
df_most_promo = mode_col(df_orders, "customer_id", "promo_code",    "most_used_promo")

df_cust_base = (
    df_orders
    .groupBy("customer_id")
    .agg(
        count("*").alias("total_orders"),
        spark_round(spark_sum("order_amount"), 2).alias("total_spend"),
        spark_round(spark_avg("order_amount"), 2).alias("avg_order_value"),
        spark_round(spark_avg("rating"), 2).alias("avg_rating_given"),
        spark_min("order_date").alias("first_order_date"),
        spark_max("order_date").alias("last_order_date"),
        countDistinct("order_month").alias("months_active")
    )
    .withColumn("customer_lifetime_days",
        datediff(col("last_order_date"), col("first_order_date"))
    )
    .withColumn("order_frequency_per_month",
        spark_round(col("total_orders") / col("months_active"), 2)
    )
)

df_cust_behavior = (
    df_cust_base
    .join(df_most_rest,  on="customer_id", how="left")
    .join(df_most_promo, on="customer_id", how="left")
    .join(df_cust.select("customer_id", "loyalty_status", "city"), on="customer_id", how="left")
)

if FULL_LOAD:
    df_cust_behavior.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.customer_behavior")
    print("✅ gold.customer_behavior created (full load)")
else:
    # Recompute cumulative metrics only for affected customers → MERGE INTO
    df_cust_inc = (
        df_cust_base.filter(col("customer_id").isin(affected_customers))
        .join(df_most_rest,  on="customer_id", how="left")
        .join(df_most_promo, on="customer_id", how="left")
        .join(df_cust.select("customer_id", "loyalty_status", "city"), on="customer_id", how="left")
    )
    df_cust_inc.createOrReplaceTempView("_cust_behavior_updates")
    spark.sql("""
        MERGE INTO gold.customer_behavior tgt
        USING _cust_behavior_updates src ON tgt.customer_id = src.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"✅ gold.customer_behavior updated via MERGE ({len(affected_customers)} customers)")
print(f"   Total rows: {spark.table('gold.customer_behavior').count()}")
display(spark.table("gold.customer_behavior").orderBy(col("total_orders").desc()).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🚴 SUMMARY TABLE 3: gold.delivery_partner_performance

# COMMAND ----------

# DBTITLE 1,Gold Table 3: delivery_partner_performance
df_most_city = mode_col(df_orders, "partner_id", "restaurant_id", "most_delivered_restaurant")

df_dp_perf = (
    df_orders
    .groupBy("partner_id")
    .agg(
        count("*").alias("total_deliveries"),
        spark_round(spark_avg("delivery_time"), 2).alias("avg_delivery_time"),
        spark_round(spark_avg("rating"), 2).alias("avg_rating_received")
    )
    .join(df_most_city, on="partner_id", how="left")
    .join(df_partners.select("partner_id", "vehicle_type", "city"), on="partner_id", how="left")
)

if FULL_LOAD:
    df_dp_perf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.delivery_partner_performance")
    print("✅ gold.delivery_partner_performance created (full load)")
else:
    # Recompute only for affected partners → MERGE INTO
    df_orders_aff = df_orders.filter(col("partner_id").isin(affected_partners))
    df_dp_inc = (
        df_orders_aff
        .groupBy("partner_id")
        .agg(
            count("*").alias("total_deliveries"),
            spark_round(spark_avg("delivery_time"), 2).alias("avg_delivery_time"),
            spark_round(spark_avg("rating"), 2).alias("avg_rating_received")
        )
        .join(mode_col(df_orders_aff, "partner_id", "restaurant_id", "most_delivered_restaurant"), on="partner_id", how="left")
        .join(df_partners.select("partner_id", "vehicle_type", "city"), on="partner_id", how="left")
    )
    df_dp_inc.createOrReplaceTempView("_dp_perf_updates")
    spark.sql("""
        MERGE INTO gold.delivery_partner_performance tgt
        USING _dp_perf_updates src ON tgt.partner_id = src.partner_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"✅ gold.delivery_partner_performance updated via MERGE ({len(affected_partners)} partners)")
print(f"   Total rows: {spark.table('gold.delivery_partner_performance').count()}")
display(spark.table("gold.delivery_partner_performance").orderBy(col("total_deliveries").desc()).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📅 SUMMARY TABLE 4: gold.monthly_trends

# COMMAND ----------

# DBTITLE 1,Gold Table 4: monthly_trends
from pyspark.sql.functions import lag
from pyspark.sql import Window as W

w_month = W.orderBy("order_month")

df_monthly = (
    df_orders
    .groupBy("order_month")
    .agg(
        count("*").alias("total_orders"),
        spark_round(spark_sum("order_amount"), 2).alias("total_revenue"),
        spark_round(spark_avg("order_amount"), 2).alias("avg_order_value"),
        spark_round(spark_avg("rating"), 2).alias("avg_rating"),
        countDistinct("customer_id").alias("total_customers"),
        countDistinct("restaurant_id").alias("total_restaurants")
    )
    .withColumn("revenue_mom_change",
        col("total_revenue") - lag("total_revenue", 1).over(w_month)
    )
    .withColumn("orders_mom_change",
        col("total_orders") - lag("total_orders", 1).over(w_month)
    )
)

# Incremental: overwrite only new/changed month partitions
if FULL_LOAD:
    df_monthly.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("order_month").saveAsTable("gold.monthly_trends")
    print("✅ gold.monthly_trends created (full load)")
else:
    if not new_months:
        print("ℹ️  gold.monthly_trends — no new months to process, skipping.")
    else:
        df_monthly_inc = df_monthly.filter(col("order_month").isin(new_months))
        replace_cond = " OR ".join([f"order_month = {m}" for m in new_months])
        df_monthly_inc.write.format("delta").mode("overwrite").option("replaceWhere", replace_cond).saveAsTable("gold.monthly_trends")
        print(f"✅ gold.monthly_trends updated (replaceWhere | months={new_months})")
display(spark.table("gold.monthly_trends").orderBy("order_month"))

# COMMAND ----------

# DBTITLE 1,Update Gold Pipeline State
# ── Record this run's timestamp so next run knows where to start ─────────────

gold_run_ts = datetime.now()
spark.createDataFrame([Row(last_gold_run_ts=gold_run_ts)]) \
     .write.format("delta").mode("append").saveAsTable("gold.gold_pipeline_state")

print(f"✅ Gold pipeline state updated | last_gold_run_ts = {gold_run_ts}")
print(f"💡 Next run will only process months with ingested_at > {gold_run_ts}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🕰️ SLOW CHANGING DIMENSIONS (SCD)
# MAGIC
# MAGIC | SCD Type | Strategy | Use Case |
# MAGIC |----------|----------|----------|
# MAGIC | **Type 1** | Overwrite old value | No history needed (e.g., fixing a typo) |
# MAGIC | **Type 2** | Add new row; keep old with end_date | Full history needed (e.g., customer city changes) |
# MAGIC | **Type 3** | Add `prev_value` column | Only previous value matters |
# MAGIC
# MAGIC Our `customers.csv` uses **SCD Type 2** with columns: `start_date`, `end_date`, `current_flag`.
# MAGIC
# MAGIC **Point-in-time query** — find a customer's status on 2025-02-15:
# MAGIC ```sql
# MAGIC SELECT * FROM silver.customers_history
# MAGIC WHERE customer_id = 'CUST001'
# MAGIC   AND start_date <= '2025-02-15'
# MAGIC   AND (end_date >= '2025-02-15' OR end_date IS NULL)
# MAGIC ```
# MAGIC
# MAGIC **Current record query:**
# MAGIC ```sql
# MAGIC SELECT * FROM silver.customers_history WHERE current_flag = 'Y'
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🗂️ PARTITIONING STRATEGY
# MAGIC
# MAGIC | Layer | Partition Column | Reason |
# MAGIC |-------|-----------------|--------|
# MAGIC | Bronze | None | Raw ingestion — no filtering needed yet |
# MAGIC | Silver.orders | `order_month` | Most queries filter by month |
# MAGIC | Gold | `order_month` | Aggregation queries always group by month |
# MAGIC
# MAGIC **Partition Pruning:** When you run `WHERE order_month = 2`, Spark reads only the `order_month=2/` directory, skipping months 1 and 3 entirely. This can reduce scan cost by 60–70% for monthly reports.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ CI/CD DATA QUALITY CHECKS

# COMMAND ----------

# DBTITLE 1,CI/CD Data Quality Checks
from pyspark.sql.functions import col

def check_nulls(table_name, cols):
    """Check that critical columns have 0 nulls."""
    table_df = spark.table(table_name)
    for c in cols:
        n = table_df.filter(col(c).isNull()).count()
        if n > 0:
            raise ValueError(f"[NULL CHECK] {table_name}.{c} has {n} null values")
    print(f"✅ NULL CHECK passed for {table_name}")

def check_duplicates(table_name, key_cols):
    """Check no duplicate composite key."""
    table_df = spark.table(table_name)
    total = table_df.count()
    distinct = table_df.select(*key_cols).distinct().count()
    if total != distinct:
        raise ValueError(f"[DUP CHECK] {table_name} has {total - distinct} duplicate rows on {key_cols}")
    print(f"✅ DUPLICATE CHECK passed for {table_name}")

def check_rating_threshold(table_name, threshold=2.0):
    """Warn if any month's avg_rating drops below threshold."""
    table_df = spark.table(table_name)
    low = table_df.filter(col("avg_rating") < threshold)
    if low.count() > 0:
        print(f"⚠️  [THRESHOLD WARNING] {table_name} has months with avg_rating < {threshold}:")
        display(low)
    else:
        print(f"✅ THRESHOLD CHECK passed for {table_name}")

def check_id_format(table_name, id_col, pattern):
    """Check all IDs match expected regex pattern."""
    table_df = spark.table(table_name)
    bad = table_df.filter(~table_df[id_col].rlike(pattern)).count()
    if bad > 0:
        raise ValueError(f"[FORMAT CHECK] {table_name}.{id_col} has {bad} values not matching '{pattern}'")
    print(f"✅ FORMAT CHECK passed for {table_name}.{id_col}")

def check_null_percentage(table_name, max_pct=30.0):
    """BLOCK pipeline if any numeric column exceeds max_pct% nulls — data must NOT reach Gold."""
    table_df = spark.table(table_name)
    total = table_df.count()
    numeric_cols = [f.name for f in table_df.schema.fields if isinstance(f.dataType, NumericType)]
    failed_cols = []
    for c in numeric_cols:
        null_pct = table_df.filter(col(c).isNull()).count() / total * 100
        if null_pct > max_pct:
            failed_cols.append(f"  • {c}: {null_pct:.1f}% nulls (threshold: {max_pct}%)")
    if failed_cols:
        raise ValueError(
            f"❌ [NULL PCT GATE] {table_name} BLOCKED — columns exceed {max_pct}% null threshold:\n"
            + "\n".join(failed_cols)
            + "\n⛔ Fix nulls in Silver before pushing to Gold."
        )
    print(f"✅ NULL PCT GATE passed for {table_name}")

# --- RUN ALL CHECKS ---
print("\n🔍 Running Gold Data Quality Checks...\n")

check_nulls("gold.restaurant_performance", ["restaurant_id", "order_month", "total_orders", "total_revenue"])
check_duplicates("gold.restaurant_performance", ["restaurant_id", "order_month"])
check_rating_threshold("gold.restaurant_performance", threshold=2.0)
check_id_format("gold.restaurant_performance", "restaurant_id", r'^REST\d+$')
check_null_percentage("gold.restaurant_performance", max_pct=30.0)

check_nulls("gold.customer_behavior", ["customer_id", "total_orders", "total_spend"])
check_null_percentage("gold.customer_behavior", max_pct=30.0)

print("\n🎉 ALL GOLD DATA QUALITY CHECKS COMPLETE")

# COMMAND ----------

# DBTITLE 1,Export Gold Tables to Parquet — ADLS
# MAGIC %md
# MAGIC ---
# MAGIC ## 📦 STORE ALL TABLES IN ADLS + PARQUET EXPORT
# MAGIC
# MAGIC **Flow:**
# MAGIC 1. Bronze Delta tables → `food-data-store/bronze/`
# MAGIC 2. Silver Delta tables → `food-data-store/silver/`
# MAGIC 3. Gold Delta tables → `food-data-store/gold/`
# MAGIC 4. Gold tables as Parquet → `food-data-store/gold-parquet/`

# COMMAND ----------

# DBTITLE 1,Export Gold Tables to Parquet — ADLS
# ─────────────────────────────────────────────────────────────────────
# FLOW:
#   STEP 1 — Store Bronze Delta tables   → food-data-store/bronze/
#   STEP 2 — Store Silver Delta tables   → food-data-store/silver/
#   STEP 3 — Store Gold Delta tables     → food-data-store/gold/
#   STEP 4 — Export Gold as Parquet      → food-data-store/gold-parquet/
#
# LEGACY_PASSTHROUGH blocks native abfss:// writes.
# Workaround: write to DBFS first, then upload via azure-storage-blob SDK.
# ─────────────────────────────────────────────────────────────────────
from azure.storage.blob import BlobServiceClient
import os

STORAGE_ACCOUNT = "datalakedegroup1"
CONTAINER       = "food-data-store"
ACCESS_KEY      = "<REDACTED>"
DBFS_BASE       = "/dbfs/tmp/food-data/adls-export"

# ── Helper: recursively upload all files in a directory to ADLS ───────────
def upload_dir(source_dir, blob_prefix, blob_client):
    """Upload all files from source_dir to the given blob prefix."""
    file_count = 0
    for root, _, files in os.walk(source_dir):
        for fname in files:
            local_path = os.path.join(root, fname)
            rel_path = os.path.relpath(local_path, source_dir)
            blob_name = f"{blob_prefix}/{rel_path}".replace("\\", "/")
            with open(local_path, "rb") as file_obj:
                blob_client.upload_blob(name=blob_name, data=file_obj, overwrite=True)
            file_count += 1
    return file_count

container_client = BlobServiceClient(
    account_url=f"https://{STORAGE_ACCOUNT}.blob.core.windows.net",
    credential=ACCESS_KEY
).get_container_client(CONTAINER)

export_log = []

# ── STEPS 1–3: Store Bronze + Silver + Gold as Delta in ADLS ─────────────
DELTA_TABLES = [
    # Bronze → food-data-store/bronze/{table}/
    ("bronze", "bronze.orders_jan"),
    ("bronze", "bronze.orders_feb"),
    ("bronze", "bronze.orders_mar"),
    ("bronze", "bronze.customers"),
    ("bronze", "bronze.delivery_partners"),
    ("bronze", "bronze.restaurants"),
    ("bronze", "bronze.orders_stream"),
    ("bronze", "bronze.bronze_ingestion_audit"),
    ("bronze", "bronze.quarantine"),
    # Silver → food-data-store/silver/{table}/
    ("silver", "silver.orders"),
    ("silver", "silver.customers_current"),
    ("silver", "silver.customers_history"),
    ("silver", "silver.delivery_partners"),
    ("silver", "silver.pipeline_state"),
    # Gold → food-data-store/gold/{table}/
    ("gold", "gold.dim_customer"),
    ("gold", "gold.dim_restaurant"),
    ("gold", "gold.dim_delivery_partner"),
    ("gold", "gold.dim_date"),
    ("gold", "gold.fact_orders"),
    ("gold", "gold.orders_enriched"),
    ("gold", "gold.restaurant_performance"),
    ("gold", "gold.customer_behavior"),
    ("gold", "gold.delivery_partner_performance"),
    ("gold", "gold.monthly_trends"),
    ("gold", "gold.gold_pipeline_state"),
]

print("📦 STEP 1–3: Storing Bronze + Silver + Gold Delta tables in ADLS...\n")
for adls_folder, tbl in DELTA_TABLES:
    _, tbl_name = tbl.split(".")
    dbfs_path   = f"dbfs:/tmp/food-data/adls-export/{adls_folder}/{tbl_name}"
    local_dir   = f"{DBFS_BASE}/{adls_folder}/{tbl_name}"
    try:
        delta_df = spark.table(tbl)
        row_count = delta_df.count()
        # Write Delta to DBFS (preserves _delta_log/ + data files)
        delta_df.write.format("delta").mode("overwrite").save(dbfs_path)
        # Upload ALL files (data + _delta_log) recursively to ADLS
        files_n = upload_dir(local_dir, f"{adls_folder}/{tbl_name}", container_client)
        export_log.append((adls_folder, tbl_name, row_count, f"✅ DELTA ({files_n} files)"))
        print(f"  ✅ [{adls_folder:<6}] {tbl_name:<35} rows={row_count:>6}  files={files_n}")
    except (AnalysisException, OSError) as exc:
        export_log.append((adls_folder, tbl_name, 0, f"❌ {exc}"))
        print(f"  ❌ [{adls_folder:<6}] {tbl_name:<35} ERROR: {exc}")

# ── STEP 4: Export Gold as Parquet → food-data-store/gold-parquet/ ─────────
GOLD_PARQUET = [
    "gold.dim_customer", "gold.dim_restaurant",
    "gold.dim_delivery_partner", "gold.dim_date",
    "gold.fact_orders", "gold.orders_enriched",
    "gold.restaurant_performance", "gold.customer_behavior",
    "gold.delivery_partner_performance", "gold.monthly_trends",
]

print("\n📄 STEP 4: Exporting Gold tables as Parquet → ADLS gold-parquet/...\n")
for tbl in GOLD_PARQUET:
    _, tbl_name = tbl.split(".")
    dbfs_path   = f"dbfs:/tmp/food-data/adls-export/gold-parquet/{tbl_name}"
    local_dir   = f"{DBFS_BASE}/gold-parquet/{tbl_name}"
    try:
        parquet_df = spark.table(tbl)
        row_count = parquet_df.count()
        parquet_df.write.format("parquet").mode("overwrite").save(dbfs_path)
        uploaded = 0
        for fname in os.listdir(local_dir):
            if fname.endswith(".parquet"):
                with open(os.path.join(local_dir, fname), "rb") as file_obj:
                    container_client.upload_blob(
                        name=f"gold-parquet/{tbl_name}/{fname}", data=file_obj, overwrite=True
                    )
                uploaded += 1
        export_log.append(("gold-parquet", tbl_name, row_count, f"✅ PARQUET ({uploaded} files)"))
        print(f"  ✅ [parquet] {tbl_name:<35} rows={row_count:>6}  files={uploaded}")
    except (AnalysisException, OSError) as exc:
        export_log.append(("gold-parquet", tbl_name, 0, f"❌ {exc}"))
        print(f"  ❌ [parquet] {tbl_name:<35} ERROR: {exc}")
# ── Final Summary ────────────────────────────────────────────────────────────────
print("\n📊 Final Summary:")
print(f"  {'Layer':<14} {'OK':>4}  {'Failed':>6}  {'Total Rows':>12}")
print("  " + "-" * 42)
for layer in ["bronze", "silver", "gold", "gold-parquet"]:
    rows    = [r for r in export_log if r[0] == layer]
    ok      = sum(1 for r in rows if "✅" in r[3])
    fail    = sum(1 for r in rows if "❌" in r[3])
    t_rows  = sum(r[2] for r in rows if "✅" in r[3])
    print(f"  {layer:<14} {ok:>4}  {fail:>6}  {t_rows:>12,}")

failed_tables = [r for r in export_log if "❌" in r[3]]
if failed_tables:
    print(f"\n⚠️  {len(failed_tables)} table(s) had errors. Others completed successfully.")
else:
    print("\n✅ All done! Your ADLS container structure:")
    print(f"   {CONTAINER}/")
    print(f"   ├── bronze/        ← Bronze Delta tables ({len([t for t in DELTA_TABLES if t[0]=='bronze'])} tables)")
    print(f"   ├── silver/        ← Silver Delta tables ({len([t for t in DELTA_TABLES if t[0]=='silver'])} tables)")
    print(f"   ├── gold/          ← Gold Delta tables   ({len([t for t in DELTA_TABLES if t[0]=='gold'])} tables)")
    print(f"   └── gold-parquet/  ← Gold as Parquet     ({len(GOLD_PARQUET)} tables) → for SQL DB via linked service")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ⚙️ DATABRICKS JOB SETUP
# MAGIC
# MAGIC ### Setting Up a 3-Task Pipeline Job
# MAGIC
# MAGIC 1. Go to **Workflows** → **Create Job**
# MAGIC 2. Add **Task 1: Bronze**
# MAGIC    - Type: Notebook
# MAGIC    - Notebook path: `/Shared/01_Bronze`
# MAGIC    - Parameters: `source_month = all`
# MAGIC 3. Add **Task 2: Silver** (depends on Task 1)
# MAGIC    - Type: Notebook
# MAGIC    - Notebook path: `/Shared/02_Silver`
# MAGIC    - Parameters: `process_month = all`
# MAGIC 4. Add **Task 3: Gold** (depends on Task 2)
# MAGIC    - Type: Notebook
# MAGIC    - Notebook path: `/Shared/03_Gold`
# MAGIC
# MAGIC ### Cluster Configuration (Recommended)
# MAGIC - **Runtime:** Databricks Runtime 13.3 LTS (includes Delta Lake 2.4)
# MAGIC - **Node type:** `i3.xlarge` or `Standard_DS3_v2`
# MAGIC - **Workers:** 2–4 (auto-scaling recommended)
# MAGIC - **Spot instances:** Enable for cost savings on non-production runs
# MAGIC
# MAGIC ### Widget Parameterization from Job UI
# MAGIC Widgets defined with `dbutils.widgets` in notebooks are exposed as **job parameters**. When running via a Databricks Job, you can override widget defaults from the UI or API:
# MAGIC ```json
# MAGIC { "source_month": "jan", "process_month": "jan" }
# MAGIC ```
# MAGIC This allows a single notebook to serve both full loads (`all`) and targeted backfills (`jan`) without code changes.

# COMMAND ----------

# DBTITLE 1,Time Travel Header
# MAGIC %md
# MAGIC ---
# MAGIC ## ⏱️ DELTA LAKE TIME TRAVEL — Gold Layer
# MAGIC
# MAGIC Every Gold write (full load, replaceWhere, MERGE INTO) creates a new version.
# MAGIC Use time travel to compare business KPIs across pipeline runs.

# COMMAND ----------

# DBTITLE 1,Time Travel 1: All Gold Table Histories
# Version history for all 4 Gold tables
for tbl in ["gold.restaurant_performance", "gold.customer_behavior",
            "gold.delivery_partner_performance", "gold.monthly_trends"]:
    print(f"\n📜 {tbl}:")
    display(spark.sql(f"DESCRIBE HISTORY {tbl}"))

# COMMAND ----------

# DBTITLE 1,Time Travel 2: Version 0 vs Current Gold KPIs
# Compare total revenue per month: initial load vs current
df_gold_compare = spark.sql("""
    SELECT
        curr.order_month,
        ROUND(curr.total_revenue, 2)            AS current_revenue,
        ROUND(v0.total_revenue, 2)              AS initial_revenue,
        ROUND(curr.total_revenue
              - v0.total_revenue, 2)            AS revenue_diff
    FROM (
        SELECT order_month, SUM(total_revenue) AS total_revenue
        FROM gold.restaurant_performance GROUP BY order_month
    ) curr
    JOIN (
        SELECT order_month, SUM(total_revenue) AS total_revenue
        FROM gold.restaurant_performance VERSION AS OF 0 GROUP BY order_month
    ) v0 ON curr.order_month = v0.order_month
    ORDER BY curr.order_month
""")
print("🔍 Gold restaurant_performance: current vs version 0")
display(df_gold_compare)

# COMMAND ----------

# DBTITLE 1,Time Travel 3: Restore Example
# ⚠️ RESTORE is a write operation — commented out for safety
# Uncomment to roll back a Gold table to a previous version:

# spark.sql("RESTORE TABLE gold.restaurant_performance TO VERSION AS OF 0")
# spark.sql("RESTORE TABLE gold.monthly_trends TO TIMESTAMP AS OF '2026-06-19 10:00:00'")

# Safe dry-run preview
df_restore_preview = spark.sql("""
    SELECT 'Version 0' AS label, COUNT(*) AS rows FROM gold.restaurant_performance VERSION AS OF 0
    UNION ALL
    SELECT 'Current',            COUNT(*) AS rows FROM gold.restaurant_performance
""")
print("🔍 Row count: current vs version 0")
display(df_restore_preview)
