# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 SILVER LAYER — Cleaning, Validation & Transformation
# MAGIC **Purpose:** Read from Bronze Delta tables, clean & transform data, write to Silver Delta tables.
# MAGIC
# MAGIC **Tables Created:**
# MAGIC - `silver.orders` (partitioned by order_month)
# MAGIC - `silver.customers_current`
# MAGIC - `silver.customers_history`
# MAGIC - `silver.delivery_partners`

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📦 Imports & Setup

# COMMAND ----------

# DBTITLE 1,Cell 3
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, trim, regexp_replace, coalesce, abs as spark_abs,
    when, count, lit, percentile_approx, month, dayofweek,
    row_number, desc, broadcast, expr
)
from pyspark.sql.types import StringType

spark = SparkSession.builder.getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
print("✅ Silver database ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎛️ Widget — process_month
# MAGIC **Business Reason:** During backfill or error recovery, the job can reprocess only a specific month's data. Defaults to `all` for a full load.

# COMMAND ----------

dbutils.widgets.dropdown("process_month", "all", ["all", "jan", "feb", "mar"], "Process Month")
process_month = dbutils.widgets.get("process_month").strip().lower()
print(f"📅 process_month = '{process_month}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📥 READ FROM BRONZE

# COMMAND ----------

# Schema validation helper
EXPECTED_ORDER_COLS = [
    "order_id", "customer_id", "restaurant_id", "partner_id",
    "order_date", "order_time", "order_amount", "delivery_time",
    "rating", "payment_amount", "created_at", "updated_at"
]

def validate_schema(df, expected_cols, table_name):
    """Validate that DataFrame contains all expected columns."""
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"SCHEMA MISMATCH in {table_name}: missing columns {missing}")
    print(f"✅ Schema OK for {table_name}")

try:
    df_jan = spark.read.format("delta").table("bronze.orders_jan")
    df_feb = spark.read.format("delta").table("bronze.orders_feb")
    df_mar = spark.read.format("delta").table("bronze.orders_mar")

    validate_schema(df_jan, EXPECTED_ORDER_COLS, "bronze.orders_jan")
    validate_schema(df_feb, EXPECTED_ORDER_COLS, "bronze.orders_feb")
    validate_schema(df_mar, EXPECTED_ORDER_COLS, "bronze.orders_mar")

    # Lightweight existence checks (avoid full counts unless needed)
    jan_has = df_jan.limit(1).count()
    feb_has = df_feb.limit(1).count()
    mar_has = df_mar.limit(1).count()

    print(f"Jan non-empty: {bool(jan_has)} | Feb non-empty: {bool(feb_has)} | Mar non-empty: {bool(mar_has)}")
except Exception as e:
    print(f"❌ ERROR reading Bronze: {e}")
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔗 UNION All 3 Months (allowMissingColumns=True)

# COMMAND ----------

# Filter by process_month widget if not 'all'
if process_month == "jan":
    df_orders_raw = df_jan
elif process_month == "feb":
    df_orders_raw = df_jan.unionByName(df_feb, allowMissingColumns=True)
elif process_month == "mar":
    df_orders_raw = df_jan.unionByName(df_feb, allowMissingColumns=True).unionByName(df_mar, allowMissingColumns=True)
else:  # all
    df_orders_raw = (
        df_jan
        .unionByName(df_feb, allowMissingColumns=True)
        .unionByName(df_mar, allowMissingColumns=True)
    )

total_raw = df_orders_raw.count()
print(f"✅ Union complete | Total rows: {total_raw} | Columns: {len(df_orders_raw.columns)}")
print(f"Columns: {df_orders_raw.columns}")
display(df_orders_raw.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🔧 TRANSFORMATIONS ON ORDERS
# MAGIC
# MAGIC ### A. ID Cleaning — Remove Whitespace & Spurious X Prefix

# COMMAND ----------

# Pattern: remove X that appears between letters and digits
# CUSTX91234 → CUST91234 | RESTX9123 → REST9123 | DPX9123 → DP9123
X_PATTERN = r'(?<=[A-Z]{2,4})X(?=\d)'

df_id_clean = df_orders_raw \
    .withColumn("order_id",      regexp_replace(trim(col("order_id")),      X_PATTERN, "")) \
    .withColumn("customer_id",   regexp_replace(trim(col("customer_id")),   X_PATTERN, "")) \
    .withColumn("restaurant_id", regexp_replace(trim(col("restaurant_id")), X_PATTERN, "")) \
    .withColumn("partner_id",    regexp_replace(trim(col("partner_id")),    X_PATTERN, ""))

# Verify: show any remaining X-prefixed IDs
remaining_x = df_id_clean.filter(
    col("customer_id").rlike(r'CUSTX') |
    col("restaurant_id").rlike(r'RESTX') |
    col("partner_id").rlike(r'DPX')
).count()
print(f"✅ ID cleaning done | Remaining X-prefixed IDs: {remaining_x}")
display(df_id_clean.select("order_id", "customer_id", "restaurant_id", "partner_id").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### B. Date Normalization — Multiple Formats → yyyy-MM-dd

# COMMAND ----------

DATE_FORMATS = ["MMM dd yyyy", "yyyy/MM/dd", "dd/MM/yyyy", "yyyy-MM-dd", "MMM d yyyy"]

# Use SQL expr with TRY_TO_DATE to handle parse errors gracefully
date_expr = expr(f"""coalesce(
    try_to_date(order_date, '{DATE_FORMATS[0]}'),
    try_to_date(order_date, '{DATE_FORMATS[1]}'),
    try_to_date(order_date, '{DATE_FORMATS[2]}'),
    try_to_date(order_date, '{DATE_FORMATS[3]}'),
    try_to_date(order_date, '{DATE_FORMATS[4]}')
)""")

df_date_clean = df_id_clean.withColumn("order_date", date_expr)

null_dates = df_date_clean.filter(col("order_date").isNull()).count()
print(f"✅ Date normalization done | Unparseable dates (→ null): {null_dates}")
display(df_date_clean.select("order_date").dropDuplicates().limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### C. Numeric Column Cleaning

# COMMAND ----------

df_num_clean = df_date_clean \
    .withColumn("order_amount",   expr("try_cast(order_amount as double)")) \
    .withColumn("delivery_time",  expr("try_cast(delivery_time as double)")) \
    .withColumn("rating",         expr("try_cast(rating as double)")) \
    .withColumn("payment_amount", expr("try_cast(payment_amount as double)")) \
    .withColumn("order_amount",
        when(col("order_amount") < 0, spark_abs(col("order_amount")))
        .otherwise(col("order_amount"))
    ) \
    .withColumn("delivery_time",
        when(col("delivery_time") < 0, spark_abs(col("delivery_time")))
        .otherwise(col("delivery_time"))
    ) \
    .withColumn("rating",
        when((col("rating") < 0) | (col("rating") > 5.0) | (col("rating") == 0.0), None)
        .otherwise(col("rating"))
    ) \
    .withColumn("payment_amount",
        when(col("payment_amount") < 0, spark_abs(col("payment_amount")))
        .otherwise(col("payment_amount"))
    )

print("✅ Numeric cleaning done")
display(df_num_clean.select("order_amount", "delivery_time", "rating", "payment_amount").describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### D. NULL Imputation — Median per Restaurant

# COMMAND ----------

from pyspark.sql.functions import percentile_approx, avg

numeric_cols = ["order_amount", "delivery_time", "rating", "payment_amount"]

# Compute restaurant-level median for each numeric column
rest_median_exprs = [
    percentile_approx(c, 0.5).alias(f"{c}_rest_median") for c in numeric_cols
]
df_rest_medians = df_num_clean.groupBy("restaurant_id").agg(*rest_median_exprs)

# Compute overall median as fallback
overall_medians = {}
for c in numeric_cols:
    val = df_num_clean.agg(percentile_approx(c, 0.5).alias("med")).collect()[0]["med"]
    overall_medians[c] = val
    print(f"Overall median {c}: {val}")

# Join medians back
df_with_medians = df_num_clean.join(broadcast(df_rest_medians), on="restaurant_id", how="left")

# Impute nulls
for c in numeric_cols:
    df_with_medians = df_with_medians.withColumn(
        c,
        when(col(c).isNull(),
            coalesce(col(f"{c}_rest_median"), lit(overall_medians[c]))
        ).otherwise(col(c))
    ).drop(f"{c}_rest_median")

print("✅ Median imputation complete")
display(df_with_medians.select(*numeric_cols).describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### E. Schema Drift — Fill null promo_code with Restaurant Mode

# COMMAND ----------

# Build restaurant → most frequent promo_code map
window_promo = Window.partitionBy("restaurant_id").orderBy(desc("promo_count"))

df_promo_map = (
    df_with_medians
    .filter(col("promo_code").isNotNull())
    .groupBy("restaurant_id", "promo_code")
    .agg(count("*").alias("promo_count"))
    .withColumn("rn", row_number().over(window_promo))
    .filter(col("rn") == 1)
    .select("restaurant_id", col("promo_code").alias("top_promo"))
)

df_promo_filled = (
    df_with_medians
    .join(broadcast(df_promo_map), on="restaurant_id", how="left")
    .withColumn("promo_code",
        when(col("promo_code").isNull(), col("top_promo"))
        .otherwise(col("promo_code"))
    )
    .drop("top_promo")
)

remaining_null_promo = df_promo_filled.filter(col("promo_code").isNull()).count()
print(f"✅ promo_code filled | Still null: {remaining_null_promo}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### F. Schema Drift — Fill null delivery_mode with Customer Mode

# COMMAND ----------

# delivery_mode column only exists in March data — skip if not present
if "delivery_mode" in df_promo_filled.columns:
    # Build customer → most frequent delivery_mode map (from Mar data)
    window_dm = Window.partitionBy("customer_id").orderBy(desc("dm_count"))

    df_dm_map = (
        df_promo_filled
        .filter(col("delivery_mode").isNotNull())
        .groupBy("customer_id", "delivery_mode")
        .agg(count("*").alias("dm_count"))
        .withColumn("rn", row_number().over(window_dm))
        .filter(col("rn") == 1)
        .select("customer_id", col("delivery_mode").alias("top_dm"))
    )

    # Overall most frequent delivery_mode as fallback
    overall_dm = (
        df_promo_filled
        .filter(col("delivery_mode").isNotNull())
        .groupBy("delivery_mode")
        .agg(count("*").alias("c"))
        .orderBy(desc("c"))
        .first()["delivery_mode"]
    )
    print(f"Overall most frequent delivery_mode: {overall_dm}")

    df_dm_filled = (
        df_promo_filled
        .join(broadcast(df_dm_map), on="customer_id", how="left")
        .withColumn("delivery_mode",
            when(col("delivery_mode").isNull(),
                coalesce(col("top_dm"), lit(overall_dm))
            ).otherwise(col("delivery_mode"))
        )
        .drop("top_dm")
    )

    null_dm = df_dm_filled.filter(col("delivery_mode").isNull()).count()
    print(f"✅ delivery_mode filled | Still null: {null_dm}")
else:
    # Column doesn't exist (Jan/Feb only) — pass through
    df_dm_filled = df_promo_filled
    print("⚠️ delivery_mode column not present (skipped — March data not included)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### G. Derived Columns

# COMMAND ----------

df_orders_silver = df_dm_filled \
    .withColumn("order_day_of_week", dayofweek(col("order_date"))) \
    .withColumn("order_month",       month(col("order_date"))) \
    .withColumn("delivery_speed_category",
        when(col("delivery_time") <= 30, "Fast")
        .when(col("delivery_time") <= 60, "Normal")
        .otherwise("Slow")
    ) \
    .withColumn("is_weekend",
        when(dayofweek(col("order_date")).isin(1, 7), True).otherwise(False)
    )

print(f"✅ Derived columns added | Total columns: {len(df_orders_silver.columns)}")
display(df_orders_silver.select(
    "order_id", "order_date", "order_day_of_week", "order_month",
    "delivery_speed_category", "is_weekend"
).limit(5))

# COMMAND ----------

# MERGE (upsert) into silver.orders using updated_at for CDC-aware upserts
# This cell performs an upsert; if the table doesn't exist it will be created (initial load)
# Prepare temp view for SQL MERGE
df_orders_silver.createOrReplaceTempView("silver_orders_batch_tmp")

try:
    # Check if target table exists
    spark.sql("DESCRIBE TABLE silver.orders")
    # Table exists — perform MERGE using updated_at to decide updates
    spark.sql("""
        MERGE INTO silver.orders target
        USING silver_orders_batch_tmp source
        ON target.order_id = source.order_id
        WHEN MATCHED AND source.updated_at > target.updated_at THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("✅ MERGE INTO silver.orders complete")
except Exception:
    # Table does not exist — create it as initial load with partitioning
    df_orders_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema","true") \
        .partitionBy("order_month") \
        .saveAsTable("silver.orders")
    print("✅ Created silver.orders table (initial load)")


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 👥 TRANSFORMATIONS ON CUSTOMERS

# COMMAND ----------

df_cust_raw = spark.read.format("delta").table("bronze.customers")
print(f"Customers rows: {df_cust_raw.count()} | Cols: {df_cust_raw.columns}")
display(df_cust_raw.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### A. Fill null city — restaurant most ordered from

# COMMAND ----------

# Most ordered restaurant per customer
w_cust_rest = Window.partitionBy("customer_id").orderBy(desc("order_cnt"))

df_cust_top_rest = (
    df_orders_silver
    .groupBy("customer_id", "restaurant_id")
    .agg(count("*").alias("order_cnt"))
    .withColumn("rn", row_number().over(w_cust_rest))
    .filter(col("rn") == 1)
    .select("customer_id", "restaurant_id")
)

# Restaurant city from delivery partners or orders (approximate)
# Use delivery_partners city as restaurant city proxy
df_partners_raw = spark.read.format("delta").table("bronze.delivery_partners")
df_rest_city = df_partners_raw.groupBy("city").count()  # mode city

# Fallback: use overall mode city of customers
overall_city = (
    df_cust_raw
    .filter(col("city").isNotNull() & (col("city") != ""))
    .groupBy("city").agg(count("*").alias("c")).orderBy(desc("c"))
    .first()["city"]
)

df_cust_city = (
    df_cust_raw
    .withColumn("city",
        when((col("city").isNull()) | (col("city") == ""), lit(overall_city))
        .otherwise(col("city"))
    )
)
print(f"✅ Customer city nulls filled | Remaining null city: {df_cust_city.filter(col('city').isNull()).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### B. Fill null loyalty_status

# COMMAND ----------

overall_loyalty = (
    df_cust_city
    .filter(col("loyalty_status").isNotNull())
    .groupBy("loyalty_status").agg(count("*").alias("c")).orderBy(desc("c"))
    .first()["loyalty_status"]
)

df_cust_clean = df_cust_city.withColumn(
    "loyalty_status",
    when(col("loyalty_status").isNull(), lit(overall_loyalty))
    .otherwise(col("loyalty_status"))
)
print(f"✅ loyalty_status filled | Remaining null: {df_cust_clean.filter(col('loyalty_status').isNull()).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### C. Split into Current vs History

# COMMAND ----------

df_cust_current = df_cust_clean.filter(col("current_flag") == "Y")
df_cust_history = df_cust_clean  # All rows

print(f"✅ Current customers: {df_cust_current.count()} | All (history): {df_cust_history.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🚚 TRANSFORMATIONS ON DELIVERY PARTNERS

# COMMAND ----------

df_dp_raw = spark.read.format("delta").table("bronze.delivery_partners")

# A. Trim all columns
df_dp_trim = df_dp_raw \
    .withColumn("partner_id",   trim(col("partner_id"))) \
    .withColumn("partner_name", trim(col("partner_name"))) \
    .withColumn("vehicle_type", trim(col("vehicle_type"))) \
    .withColumn("city",         trim(col("city")))

# B. Fill null city — mode city from partner's orders
df_partner_orders = (
    df_orders_silver
    .groupBy("partner_id")
    .agg(count("*").alias("total_del"))
)

overall_dp_city = (
    df_dp_trim
    .filter(col("city").isNotNull() & (col("city") != ""))
    .groupBy("city").agg(count("*").alias("c")).orderBy(desc("c"))
    .first()["city"]
)

df_dp_city = df_dp_trim.withColumn(
    "city",
    when((col("city").isNull()) | (col("city") == ""), lit(overall_dp_city))
    .otherwise(col("city"))
)

# C. Fill null vehicle_type — mode per city
w_vt = Window.partitionBy("city").orderBy(desc("vt_count"))
df_vt_map = (
    df_dp_city
    .filter(col("vehicle_type").isNotNull())
    .groupBy("city", "vehicle_type")
    .agg(count("*").alias("vt_count"))
    .withColumn("rn", row_number().over(w_vt))
    .filter(col("rn") == 1)
    .select("city", col("vehicle_type").alias("top_vt"))
)

df_dp_silver = (
    df_dp_city
    .join(broadcast(df_vt_map), on="city", how="left")
    .withColumn("vehicle_type",
        when(col("vehicle_type").isNull(), col("top_vt"))
        .otherwise(col("vehicle_type"))
    )
    .drop("top_vt")
)

print(f"✅ Delivery partners cleaned | Rows: {df_dp_silver.count()}")
display(df_dp_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 💾 WRITE TO SILVER

# COMMAND ----------

try:
    # silver.orders handled by incremental MERGE above
    print("ℹ️ silver.orders handled by incremental MERGE (no overwrite here).")

    # silver.customers_current
    df_cust_current.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("silver.customers_current")
    print("✅ silver.customers_current written")

    # silver.customers_history
    df_cust_history.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("silver.customers_history")
    print("✅ silver.customers_history written")

    # silver.delivery_partners
    df_dp_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("silver.delivery_partners")
    print("✅ silver.delivery_partners written")

except Exception as e:
    print(f"❌ ERROR writing Silver: {e}")
    raise


# COMMAND ----------

# Update pipeline_state after successful Silver processing
from datetime import datetime

try:
    # Compute the max ingested_at from the batch we just processed (df_orders_silver)
    if 'df_orders_silver' in globals():
        last_batch_ts = df_orders_silver.agg({'ingested_at': 'max'}).collect()[0][0]
    else:
        last_batch_ts = None

    if last_batch_ts is not None:
        batch_id = globals().get('BATCH_ID', f"silver_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        state_df = spark.createDataFrame([
            ('orders', last_batch_ts, datetime.now(), batch_id)
        ], schema=['pipeline_name', 'last_ingested_at', 'last_updated', 'batch_id'])
        state_df.createOrReplaceTempView('src_pipeline_state_silver')
        spark.sql("""
            MERGE INTO bronze.pipeline_state t
            USING src_pipeline_state_silver s
              ON t.pipeline_name = s.pipeline_name
            WHEN MATCHED THEN UPDATE SET
              t.last_ingested_at = s.last_ingested_at,
              t.last_updated = s.last_updated,
              t.batch_id = s.batch_id
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"✅ Updated bronze.pipeline_state with last_ingested_at = {last_batch_ts}")
    else:
        print("⚠️ No ingested rows in this run; pipeline_state not updated")
except Exception as e:
    print(f"❌ Error updating pipeline_state from Silver: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### ⚡ Z-ORDER Optimization
# MAGIC
# MAGIC **What Z-ordering does:**  
# MAGIC Z-ordering co-locates related data in the same set of files. When you Z-ORDER BY `restaurant_id` and `order_date`, Delta Lake physically reorganizes the Parquet files so rows with the same `restaurant_id` and nearby `order_date` values end up in the same file. This enables **data skipping** — when a query filters `WHERE restaurant_id = 'REST001'`, the Delta engine reads only files known to contain that restaurant.
# MAGIC
# MAGIC **Partitioning vs Z-ordering:**
# MAGIC | Feature | Partitioning | Z-ordering |
# MAGIC |---------|-------------|------------|
# MAGIC | Granularity | Coarse (folder per value) | Fine (within partition) |
# MAGIC | Best for | Low-cardinality columns (month) | High-cardinality columns (restaurant_id, date) |
# MAGIC | Mechanism | Separate directories | Column statistics in `_delta_log` |
# MAGIC
# MAGIC `silver.orders` is **partitioned by `order_month`** (coarse) and **Z-ordered by `restaurant_id, order_date`** (fine) for maximum query performance.

# COMMAND ----------

spark.sql("OPTIMIZE silver.orders ZORDER BY (restaurant_id, order_date)")
print("✅ Z-ORDER applied to silver.orders")

spark.sql("OPTIMIZE silver.customers_current ZORDER BY (customer_id)")
print("✅ Z-ORDER applied to silver.customers_current")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🎭 COLUMN MASKING & ROW-LEVEL SECURITY
# MAGIC
# MAGIC **Column-level masking** protects PII by showing only partial values (e.g., first 3 chars of email). Useful for BI roles that need customer aggregates but not raw contact data.
# MAGIC
# MAGIC **Row-level security** uses filtered views or dynamic functions to restrict which rows a user can see based on their role (e.g., a city manager sees only their city's customers).

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE VIEW silver.customers_masked AS
    SELECT
        customer_id,
        customer_name,
        CONCAT(SUBSTRING(email, 1, 3), '***@***.com') AS email_masked,
        CONCAT('+91-XXXXXX', SUBSTRING(phone, -4))    AS phone_masked,
        city,
        loyalty_status,
        start_date,
        end_date,
        current_flag
    FROM silver.customers_current
""")
print("✅ Masked view created: silver.customers_masked")
display(spark.table("silver.customers_masked").limit(5))

# COMMAND ----------

# DBTITLE 1,Row-Level Security View
# Row-Level Security (RLS) — filter rows based on current_user() identity
spark.sql("""
    CREATE OR REPLACE VIEW silver.orders_rls AS
    SELECT o.*
    FROM silver.orders o
    WHERE
        -- Admin users see ALL orders
        current_user() LIKE '%admin%'

        -- City-based analysts: see only orders from restaurants in their city
        OR o.restaurant_id IN (
            SELECT partner_id
            FROM silver.delivery_partners
            WHERE LOWER(city) = LOWER(regexp_extract(current_user(), '@([^.]+)\\\\.', 1))
        )

        -- Fallback: authenticated users see all (remove OR TRUE in production)
        OR TRUE
""")
print("✅ Row-level security view created: silver.orders_rls")
print("   • Admins (%admin%)       → see ALL orders")
print("   • City analysts           → see only their city's orders")
print("   • Production tip          → remove 'OR TRUE' to enforce strict RLS")
display(spark.sql("SELECT current_user() AS logged_in_as"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🔄 INCREMENTAL LOAD / CDC
# MAGIC
# MAGIC ### Incremental Load Strategy
# MAGIC 1. **Bronze → Silver incremental:** Track `max(ingested_at)` from previous Silver run. Read only `bronze.orders WHERE ingested_at > last_run_ts`.
# MAGIC 2. **Source CDC via `updated_at`:** If the source updates a row, `updated_at` changes. Track `max(updated_at)` from Silver and merge only changed rows.
# MAGIC 3. **MERGE INTO Silver (Upsert):**
# MAGIC ```sql
# MAGIC MERGE INTO silver.orders target
# MAGIC USING new_batch source ON target.order_id = source.order_id
# MAGIC WHEN MATCHED AND source.updated_at > target.updated_at THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC ### Structured Streaming with Checkpoint
# MAGIC ```python
# MAGIC # (commented — for reference only)
# MAGIC # (
# MAGIC #   spark.readStream.format("delta").table("bronze.orders_jan")
# MAGIC #   .writeStream
# MAGIC #   .format("delta")
# MAGIC #   .outputMode("append")
# MAGIC #   .option("checkpointLocation", "/FileStore/checkpoints/silver_orders")
# MAGIC #   .trigger(processingTime="5 minutes")
# MAGIC #   .toTable("silver.orders")
# MAGIC # )
# MAGIC ```
# MAGIC The **checkpoint** folder stores the stream's progress (last offset processed). If the stream restarts, it resumes from exactly where it left off — no duplicate processing, no data loss.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ VALIDATION SECTION

# COMMAND ----------

print("🔍 Running Silver Validations...")
errors = []
warnings = []

# 1. silver.orders must not be empty (lightweight check)
orders_exists = spark.table("silver.orders").limit(1).count()
if orders_exists == 0:
    errors.append("VALIDATION FAILED: silver.orders is empty")
else:
    print(f"✅ silver.orders is not empty")

# 2. No nulls in critical columns
critical_cols = ["order_id", "customer_id", "restaurant_id", "order_date", "order_month"]
for col_name in critical_cols:
    null_count = spark.table("silver.orders").filter(f"{col_name} IS NULL").limit(1).count()
    if null_count > 0:
        errors.append(f"VALIDATION FAILED: nulls found in {col_name}")
    else:
        print(f"✅ No nulls in {col_name}")

# 3. silver.customers_current must not be empty (lightweight)
cust_exists = spark.table("silver.customers_current").limit(1).count()
if cust_exists == 0:
    errors.append("VALIDATION FAILED: silver.customers_current is empty")
else:
    print(f"✅ silver.customers_current is not empty")

# 4. silver.delivery_partners must not be empty (lightweight)
dp_exists = spark.table("silver.delivery_partners").limit(1).count()
if dp_exists == 0:
    errors.append("VALIDATION FAILED: silver.delivery_partners is empty")
else:
    print(f"✅ silver.delivery_partners is not empty")

# 5. Check for duplicate order_id (bronze source contains duplicates)
# This check requires full counts; keep as-is
total = spark.table("silver.orders").count()
distinct_count = spark.table("silver.orders").select("order_id").distinct().count()
if total != distinct_count:
    dup_count = total - distinct_count
    warnings.append(f"⚠️  {dup_count} duplicate order_ids detected - bronze tables contain duplicates that should be deduplicated in Cell 35 before writing to silver")
else:
    print(f"✅ No duplicate order_ids (total={total}, distinct={distinct_count})")

if warnings:
    for w in warnings:
        print(w)

if errors:
    for e in errors:
        print(f"❌ {e}")
    raise Exception("\n".join(errors))
else:
    if warnings:
        print("\n⚠️  VALIDATIONS PASSED WITH WARNINGS")
    else:
        print("\n🎉 ALL VALIDATIONS PASSED")


# COMMAND ----------

# DBTITLE 1,Time Travel Header
# MAGIC %md
# MAGIC ---
# MAGIC ## ⏱️ DELTA LAKE TIME TRAVEL — Silver Layer
# MAGIC
# MAGIC Every MERGE INTO and overwrite creates a new Delta version. Use time travel to:
# MAGIC - **Audit** what data looked like before a MERGE run
# MAGIC - **Debug** data quality issues introduced in a specific run
# MAGIC - **Recover** accidentally overwritten data
# MAGIC
# MAGIC | Syntax | Use case |
# MAGIC |---|---|
# MAGIC | `VERSION AS OF 0` | Original load before any MERGEs |
# MAGIC | `TIMESTAMP AS OF 'ts'` | State at a specific pipeline run time |
# MAGIC | `DESCRIBE HISTORY` | See all versions with operation & timestamp |

# COMMAND ----------

# DBTITLE 1,Time Travel 1: silver.orders Version History
# Show all versions of silver.orders
# Each MERGE INTO run creates a new version
print("📜 Version history of silver.orders:")
display(spark.sql("DESCRIBE HISTORY silver.orders"))

# COMMAND ----------

# DBTITLE 1,Time Travel 2: Query Version 0 of silver.orders
# Query silver.orders as it was after the very first load
# Useful to compare original data vs current cleaned state
df_v0 = spark.sql("""
    SELECT
        order_month,
        COUNT(*)                            AS total_orders,
        ROUND(AVG(order_amount), 2)         AS avg_order_amount,
        ROUND(AVG(rating), 2)              AS avg_rating
    FROM silver.orders VERSION AS OF 0
    GROUP BY order_month
    ORDER BY order_month
""")
print("🔙 silver.orders at VERSION 0 (initial load):")
display(df_v0)

# COMMAND ----------

# DBTITLE 1,Time Travel 3: Compare Current vs Version 0
# Side-by-side comparison: current silver.orders vs initial load
# Shows the impact of all MERGE runs since first load
df_compare = spark.sql("""
    SELECT
        curr.order_month,
        curr.total_orders                               AS current_orders,
        v0.total_orders                                 AS initial_orders,
        curr.total_orders - v0.total_orders             AS orders_diff,
        ROUND(curr.avg_rating - v0.avg_rating, 4)       AS rating_diff
    FROM (
        SELECT order_month, COUNT(*) AS total_orders, AVG(rating) AS avg_rating
        FROM silver.orders GROUP BY order_month
    ) curr
    JOIN (
        SELECT order_month, COUNT(*) AS total_orders, AVG(rating) AS avg_rating
        FROM silver.orders VERSION AS OF 0 GROUP BY order_month
    ) v0 ON curr.order_month = v0.order_month
    ORDER BY curr.order_month
""")
print("🔍 Current vs Version 0 comparison:")
display(df_compare)

# COMMAND ----------

# DBTITLE 1,Time Travel 4: Query by Timestamp
# Get the earliest timestamp from history and query the table at that point
history = spark.sql("DESCRIBE HISTORY silver.orders").orderBy("version")
earliest_ts = history.select("timestamp").first()["timestamp"]

df_ts = spark.sql(f"""
    SELECT
        order_month,
        COUNT(*)                    AS total_orders,
        ROUND(AVG(order_amount), 2) AS avg_order_amount
    FROM silver.orders TIMESTAMP AS OF '{earliest_ts}'
    GROUP BY order_month
    ORDER BY order_month
""")
print(f"📅 silver.orders at TIMESTAMP {earliest_ts}:")
display(df_ts)

# COMMAND ----------

# DBTITLE 1,Time Travel 5: silver.customers_current History
# Version history for customers and delivery partners
print("📜 Version history of silver.customers_current:")
display(spark.sql("DESCRIBE HISTORY silver.customers_current"))

# COMMAND ----------

# DBTITLE 1,Time Travel 6: Restore Example (Dry Run)
# RESTORE rolls back silver.orders to a previous version
# ⚠️ Commented out — uncomment only when intentionally rolling back

# spark.sql("RESTORE TABLE silver.orders TO VERSION AS OF 0")
# spark.sql("RESTORE TABLE silver.orders TO TIMESTAMP AS OF '2026-06-18 16:00:00'")

# Safe preview: row count at version 0 vs now
df_restore_preview = spark.sql("""
    SELECT 'Version 0' AS version_label, COUNT(*) AS row_count
    FROM silver.orders VERSION AS OF 0
    UNION ALL
    SELECT 'Current', COUNT(*)
    FROM silver.orders
""")
print("🔍 Row count preview — current vs version 0:")
display(df_restore_preview)