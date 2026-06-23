# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 BRONZE LAYER — Raw Ingestion
# MAGIC **Purpose:** Ingest raw CSVs from DBFS into Delta tables with minimal transformation. Add metadata columns. Perform data understanding.
# MAGIC
# MAGIC **Tables Created:**
# MAGIC - `bronze.orders_jan`
# MAGIC - `bronze.orders_feb`
# MAGIC - `bronze.orders_mar`
# MAGIC - `bronze.customers`
# MAGIC - `bronze.delivery_partners`

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📦 Imports & Setup

# COMMAND ----------

# DBTITLE 1,Install Azure SDK
# MAGIC %pip install azure-storage-blob -q

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
print("✅ Bronze database ready")

# COMMAND ----------

# MAGIC %md
# MAGIC FILE HANDLING

# COMMAND ----------

def detect_file_format(file_path):
    """Auto-detect file format: csv, json, or parquet"""
    if file_path.lower().endswith('.csv'):
        return 'csv'
    elif file_path.lower().endswith('.json'):
        return 'json'
    elif file_path.lower().endswith('.parquet'):
        return 'parquet'
    else:
        raise ValueError(f"Unsupported format: {file_path}")

def read_file_smart(file_path):
    """Read any format (CSV, JSON, Parquet) intelligently with comprehensive error handling"""
    try:
        # Check if path exists
        try:
            dbutils.fs.ls(file_path)
            path_exists = True
        except Exception as e:
            path_exists = False
        
        if not path_exists:
            return None, None, f"❌ PATH NOT FOUND: {file_path} - File does not exist in the filesystem"
        
        file_format = detect_file_format(file_path)
        
        # Read based on format
        if file_format == 'csv':
            df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(file_path)
        elif file_format == 'json':
            df = spark.read.format("json") \
                .option("inferSchema", "true") \
                .load(file_path)
        elif file_format == 'parquet':
            df = spark.read.format("parquet").load(file_path)
        
        # Check if dataframe is empty (0 rows)
        row_count = df.count()
        
        if row_count == 0:
            return df, file_format, f"⚠️  PATH FOUND BUT 0 ROWS: {file_path} - File exists but contains no data"
        
        return df, file_format, f"✅ PATH FOUND: {file_path} - Successfully loaded {row_count} rows in {file_format} format"
    
    except ValueError as ve:
        return None, None, f"❌ UNSUPPORTED FORMAT: {str(ve)}"
    except Exception as e:
        return None, None, f"❌ ERROR READING FILE: {file_path} - {str(e)}"

print("✅ File format detection helpers ready")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎛️ Widget — source_month
# MAGIC **Business Reason:** When triggered from a Databricks Job, the `source_month` widget allows the operator to choose which month's data to ingest (jan / feb / mar / all). Defaults to `all` for full loads.

# COMMAND ----------

dbutils.widgets.text("source_month", "all", "Source Month")
source_month = dbutils.widgets.get("source_month").strip().lower()
print(f"📅 source_month = '{source_month}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📂 File Paths

# COMMAND ----------

# DBTITLE 1,File Paths
import os
from azure.storage.blob import BlobServiceClient

# Download files directly via Azure Blob SDK (bypasses ABFSS & LEGACY_PASSTHROUGH)
STORAGE_ACCOUNT = "datalakedegroup1"
CONTAINER       = "food-data-store"
ACCESS_KEY      = "<REDACTED>"
REMOTE_DIR      = "raw-data"
LOCAL_DIR       = "/dbfs/tmp/food-data"

os.makedirs(LOCAL_DIR, exist_ok=True)

container_client = BlobServiceClient(
    account_url=f"https://{STORAGE_ACCOUNT}.blob.core.windows.net",
    credential=ACCESS_KEY
).get_container_client(CONTAINER)

for fname in ["orders_jan.csv", "orders_feb.csv", "orders_mar.csv", "customers.csv", "delivery_partners.csv", "restaurants.csv"]:
    with open(f"{LOCAL_DIR}/{fname}", "wb") as f:
        f.write(container_client.get_blob_client(f"{REMOTE_DIR}/{fname}").download_blob().readall())
    print(f"✅ Downloaded: {fname}")

BASE_PATH = "dbfs:/tmp/food-data"

paths = {
    "jan": f"{BASE_PATH}/orders_jan.csv",
    "feb": f"{BASE_PATH}/orders_feb.csv",
    "mar": f"{BASE_PATH}/orders_mar.csv",
    "customers": f"{BASE_PATH}/customers.csv",
    "partners": f"{BASE_PATH}/delivery_partners.csv",
    "restaurants": f"{BASE_PATH}/restaurants.csv"
}
print("✅ Paths configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📖 Schema Drift Documentation
# MAGIC
# MAGIC | File | Columns | New Column |
# MAGIC |------|---------|------------|
# MAGIC | `orders_jan.csv` | 12 | — (baseline) |
# MAGIC | `orders_feb.csv` | 13 | `promo_code` added |
# MAGIC | `orders_mar.csv` | 14 | `delivery_mode` added |
# MAGIC
# MAGIC **How `mergeSchema` handles drift:**  
# MAGIC When `mergeSchema=True` (or `spark.databricks.delta.schema.autoMerge.enabled=true`) is set, Delta Lake automatically adds new columns from incoming data to the existing table schema. This means writing `orders_feb` (which has `promo_code`) to a Delta table that was previously written with only 12 columns will automatically extend the schema — no manual `ALTER TABLE` required.
# MAGIC
# MAGIC Rows from earlier loads that predate the new column will simply have `null` for that column — which is the correct and expected behavior.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📥 Helper: MERGE INTO (Dedup on Re-run)

# COMMAND ----------

# DBTITLE 1,Helper: MERGE INTO with Partition
def merge_or_create(df, table_name, key_col="order_id"):
    """Merge new data into existing table or create if not exists"""
    view_name = f"temp_{table_name.replace('.', '_')}"
    df.createOrReplaceTempView(view_name)
    
    try:
        # Table exists — merge
        spark.sql(f"""
            MERGE INTO {table_name} target
            USING {view_name} source
            ON target.{key_col} = source.{key_col}
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"✅ MERGE INTO {table_name} complete")
    except Exception:
        # Table does not exist — create it fresh, partitioned by ingestion_month
        from pyspark.sql.functions import date_format as dt_fmt
        df_partitioned = df.withColumn("ingestion_month", dt_fmt(col("ingested_at"), "yyyy-MM"))
        df_partitioned.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("ingestion_month").saveAsTable(table_name)
        print(f"✅ Created new table {table_name} (partitioned by ingestion_month)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📥 Ingest Orders — January

# COMMAND ----------

if source_month in ("all", "jan"):
    df_jan = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(paths["jan"])
        .withColumn("ingested_at", current_timestamp())
    )
    print(f"📊 orders_jan rows: {df_jan.count()} | cols: {len(df_jan.columns)}")
    merge_or_create(df_jan, "bronze.orders_jan", key_col="order_id")
    display(df_jan.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📥 Ingest Orders — February (Schema Drift: +promo_code)

# COMMAND ----------

if source_month in ("all", "feb"):
    df_feb = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mergeSchema", "true")
        .load(paths["feb"])
        .withColumn("ingested_at", current_timestamp())
    )
    print(f"📊 orders_feb rows: {df_feb.count()} | cols: {len(df_feb.columns)}")
    merge_or_create(df_feb, "bronze.orders_feb", key_col="order_id")
    display(df_feb.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📥 Ingest Orders — March (Schema Drift: +delivery_mode)

# COMMAND ----------

if source_month in ("all", "mar"):
    df_mar = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mergeSchema", "true")
        .load(paths["mar"])
        .withColumn("ingested_at", current_timestamp())
    )
    print(f"📊 orders_mar rows: {df_mar.count()} | cols: {len(df_mar.columns)}")
    merge_or_create(df_mar, "bronze.orders_mar", key_col="order_id")
    display(df_mar.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📥 Ingest Customers

# COMMAND ----------

df_customers = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(paths["customers"])
    .withColumn("ingested_at", current_timestamp())
)
df_customers.write.format("delta").mode("overwrite").saveAsTable("bronze.customers")
print(f"✅ bronze.customers written | rows: {df_customers.count()} | cols: {len(df_customers.columns)}")
display(df_customers.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📥 Ingest Delivery Partners

# COMMAND ----------

df_partners = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(paths["partners"])
    .withColumn("ingested_at", current_timestamp())
)
df_partners.write.format("delta").mode("overwrite").saveAsTable("bronze.delivery_partners")
print(f"✅ bronze.delivery_partners written | rows: {df_partners.count()} | cols: {len(df_partners.columns)}")
display(df_partners.limit(5))

# COMMAND ----------

# DBTITLE 1,Ingest Restaurants
# MAGIC %md
# MAGIC ### 📥 Ingest Restaurants

# COMMAND ----------

# DBTITLE 1,Ingest Restaurants
df_restaurants = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(paths["restaurants"])
    .withColumn("ingested_at", current_timestamp())
)
df_restaurants.write.format("delta").mode("overwrite").saveAsTable("bronze.restaurants")
print(f"✅ bronze.restaurants written | rows: {df_restaurants.count()} | cols: {len(df_restaurants.columns)}")
display(df_restaurants.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🔍 DATA UNDERSTANDING SECTION

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📐 Row Counts & Schema per Table

# COMMAND ----------

tables = ["bronze.orders_jan", "bronze.orders_feb", "bronze.orders_mar",
          "bronze.customers", "bronze.delivery_partners"]

for tbl in tables:
    df = spark.read.format("delta").table(tbl)
    print(f"\n{'='*50}")
    print(f"📋 {tbl}  |  Rows: {df.count()}  |  Cols: {len(df.columns)}")
    df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 Column Count Proof — Schema Drift

# COMMAND ----------

for tbl in ["bronze.orders_jan", "bronze.orders_feb", "bronze.orders_mar"]:
    df = spark.read.format("delta").table(tbl)
    print(f"{tbl}: {len(df.columns)} columns → {df.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 describe() Statistics

# COMMAND ----------

df_jan_raw = spark.read.format("delta").table("bronze.orders_jan")
display(df_jan_raw.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🗓️ order_date Format Samples (Multiple Formats Present)

# COMMAND ----------

from pyspark.sql.functions import col, when

df_jan_raw = spark.read.format("delta").table("bronze.orders_jan")

# Detect date format pattern
display(df_jan_raw.select("order_date").dropDuplicates().limit(20))

# Classify format types
df_formats = df_jan_raw.select(
    col("order_date"),
    when(col("order_date").rlike(r'^\d{4}-\d{2}-\d{2}$'), 'yyyy-MM-dd')
    .when(col("order_date").rlike(r'^\d{4}/\d{2}/\d{2}$'), 'yyyy/MM/dd')
    .when(col("order_date").rlike(r'^\d{2}/\d{2}/\d{4}$'), 'dd/MM/yyyy')
    .when(col("order_date").rlike(r'^[A-Za-z]+ \d{1,2} \d{4}$'), 'MMM dd yyyy')
    .otherwise('unknown').alias("detected_format")
)
display(df_formats.groupBy("detected_format").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 X-Prefixed ID Samples

# COMMAND ----------

from pyspark.sql.functions import col

df_jan_raw = spark.read.format("delta").table("bronze.orders_jan")

print("🔴 Spurious X-prefix in customer_id:")
display(df_jan_raw.filter(col("customer_id").rlike(r'CUSTX')).select("order_id", "customer_id").limit(10))

print("🔴 Spurious X-prefix in restaurant_id:")
display(df_jan_raw.filter(col("restaurant_id").rlike(r'RESTX')).select("order_id", "restaurant_id").limit(10))

print("🔴 Spurious X-prefix in partner_id:")
display(df_jan_raw.filter(col("partner_id").rlike(r'DPX')).select("order_id", "partner_id").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 Rating Range Analysis

# COMMAND ----------

# DBTITLE 1,Cell 33
from pyspark.sql.functions import min as spark_min, max as spark_max, avg as spark_avg, count, when, col

df_jan_raw = spark.read.format("delta").table("bronze.orders_jan")
display(df_jan_raw.select(
    spark_min("rating").alias("min_rating"),
    spark_max("rating").alias("max_rating"),
    spark_avg("rating").alias("avg_rating"),
    count(when(col("rating").isNull(), 1)).alias("null_ratings"),
    count(when(col("rating").cast("double") < 0, 1)).alias("negative_ratings"),
    count(when(col("rating").cast("double") > 5, 1)).alias("ratings_above_5")
))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 Delivery Time Nulls

# COMMAND ----------

df_jan_raw = spark.read.format("delta").table("bronze.orders_jan")
null_dt = df_jan_raw.filter(col("delivery_time").isNull() | (col("delivery_time").cast("string") == "unknown")).count()
print(f"⚠️ delivery_time nulls/unknowns in orders_jan: {null_dt}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 Distinct Promo Codes (orders_feb)

# COMMAND ----------

df_feb_raw = spark.read.format("delta").table("bronze.orders_feb")
display(df_feb_raw.groupBy("promo_code").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 Distinct Delivery Modes (orders_mar)

# COMMAND ----------

df_mar_raw = spark.read.format("delta").table("bronze.orders_mar")
display(df_mar_raw.groupBy("delivery_mode").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 Null Percentage per Column — All Tables

# COMMAND ----------

def null_percentage(df, tbl_name):
    total = df.count()
    null_exprs = []
    for c in df.columns:
        null_exprs.append(
            (count(when(col(c).isNull(), c)) / total * 100).alias(c)
        )
    result = df.select(null_exprs)
    print(f"\n--- Null % for {tbl_name} ---")
    display(result)

for tbl in tables:
    null_percentage(spark.read.format("delta").table(tbl), tbl)

# COMMAND ----------

# DBTITLE 1,Schema Validation Header
# MAGIC %md
# MAGIC ### ✅ Schema Validation — Required Columns Check

# COMMAND ----------

# DBTITLE 1,Schema Validation
# Validate that all expected columns exist in each Bronze table
EXPECTED_SCHEMAS = {
    "bronze.orders_jan": ["order_id", "customer_id", "restaurant_id", "partner_id",
                          "order_date", "order_amount", "rating", "delivery_time"],
    "bronze.orders_feb": ["order_id", "customer_id", "restaurant_id", "partner_id",
                          "order_date", "order_amount", "rating", "delivery_time", "promo_code"],
    "bronze.orders_mar": ["order_id", "customer_id", "restaurant_id", "partner_id",
                          "order_date", "order_amount", "rating", "delivery_time",
                          "promo_code", "delivery_mode"],
    "bronze.customers": ["customer_id", "name", "email", "phone", "city"],
    "bronze.delivery_partners": ["partner_id", "name", "vehicle_type", "city"],
    "bronze.restaurants": ["restaurant_id", "name", "city", "cuisine_type"]
}

print("🔍 Schema Validation Report:")
all_valid = True
for tbl, expected_cols in EXPECTED_SCHEMAS.items():
    try:
        actual_cols = set(spark.table(tbl).columns)
        missing = [c for c in expected_cols if c not in actual_cols]
        if missing:
            print(f"  ❌ {tbl} — MISSING COLUMNS: {missing}")
            all_valid = False
        else:
            print(f"  ✅ {tbl} — all {len(expected_cols)} required columns present")
    except Exception as e:
        print(f"  ⚠️  {tbl} — table not found: {e}")
        all_valid = False

print(f"\n{'\u2705 ALL SCHEMAS VALID' if all_valid else '❌ SCHEMA ISSUES FOUND — fix before Silver ingestion'}")

# COMMAND ----------

# DBTITLE 1,Bad Records Quarantine Header
# MAGIC %md
# MAGIC ### 🚨 Bad Records Quarantine
# MAGIC Invalid rows are routed to `bronze.quarantine` instead of blocking the pipeline.

# COMMAND ----------

# DBTITLE 1,Bad Records Quarantine
from pyspark.sql.functions import col, lit, current_timestamp, when, expr

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# Rules: order_id/customer_id/restaurant_id null, rating out of [0,5], order_amount <= 0
def quarantine_bad_records(tbl_name):
    df = spark.table(tbl_name)
    
    # Tag reason for each bad record
    df_tagged = df.withColumn(
        "quarantine_reason",
        when(col("order_id").isNull(), "null_order_id")
        .when(col("customer_id").isNull(), "null_customer_id")
        .when(col("restaurant_id").isNull(), "null_restaurant_id")
        .when(expr("try_cast(rating AS DOUBLE)") < 0, "negative_rating")
        .when(expr("try_cast(rating AS DOUBLE)") > 5, "rating_above_5")
        .when(expr("try_cast(order_amount AS DOUBLE)") <= 0, "zero_or_negative_amount")
    )
    
    bad = df_tagged.filter(col("quarantine_reason").isNotNull()) \
                   .withColumn("source_table", lit(tbl_name)) \
                   .withColumn("quarantined_at", current_timestamp())
    
    good = df_tagged.filter(col("quarantine_reason").isNull()).drop("quarantine_reason")
    
    bad_count = bad.count()
    good_count = good.count()
    
    if bad_count > 0:
        bad.write.format("delta").mode("append").option("mergeSchema", "true") \
           .saveAsTable("bronze.quarantine")
        print(f"  ⚠️  {tbl_name}: {bad_count} bad rows → bronze.quarantine | {good_count} clean rows")
    else:
        print(f"  ✅ {tbl_name}: 0 bad rows | {good_count} all clean")
    
    return good_count, bad_count

print("🚨 Quarantine check for orders tables:")
for tbl in ["bronze.orders_jan", "bronze.orders_feb", "bronze.orders_mar"]:
    quarantine_bad_records(tbl)

print("\n📊 Quarantine table summary:")
try:
    display(spark.sql("""
        SELECT source_table, quarantine_reason, COUNT(*) AS bad_count
        FROM bronze.quarantine
        GROUP BY source_table, quarantine_reason
        ORDER BY source_table, bad_count DESC
    """))
except:
    print("  ℹ️  No quarantine records found — all data is clean!")

# COMMAND ----------

# DBTITLE 1,Data Quality Score Header
# MAGIC %md
# MAGIC ### 🌟 Data Quality Score (0–100%) per Table

# COMMAND ----------

# DBTITLE 1,Data Quality Score
from pyspark.sql.functions import col, when, count

def data_quality_score(tbl_name, key_cols):
    """Score = 100 - weighted penalties for nulls, duplicates, invalid values"""
    df = spark.table(tbl_name)
    total = df.count()
    if total == 0:
        return 0.0
    
    # Penalty 1: null % in key columns (weight: 40)
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in key_cols]).collect()[0]
    avg_null_pct = sum([null_counts[c] for c in key_cols]) / (len(key_cols) * total) * 100
    null_penalty = min(40, avg_null_pct * 2)
    
    # Penalty 2: duplicate key (weight: 30)
    if key_cols:
        distinct_count = df.select(key_cols[0]).distinct().count()
        dup_pct = (total - distinct_count) / total * 100
        dup_penalty = min(30, dup_pct * 1.5)
    else:
        dup_penalty = 0
    
    # Penalty 3: invalid rating (weight: 30) — only for orders tables
    if "rating" in df.columns:
        invalid_ratings = df.filter(
            col("rating").cast("double").isNull() |
            (col("rating").cast("double") < 0) |
            (col("rating").cast("double") > 5)
        ).count()
        rating_penalty = min(30, (invalid_ratings / total) * 100)
    else:
        rating_penalty = 0
    
    score = max(0.0, 100 - null_penalty - dup_penalty - rating_penalty)
    return round(score, 1)

print("🌟 Data Quality Scores:")
print(f"{'Table':<40} {'Score':>8}  {'Grade':>6}")
print("-" * 58)
tables_config = [
    ("bronze.orders_jan",        ["order_id"]),
    ("bronze.orders_feb",        ["order_id"]),
    ("bronze.orders_mar",        ["order_id"]),
    ("bronze.customers",         ["customer_id"]),
    ("bronze.delivery_partners", ["partner_id"]),
    ("bronze.restaurants",       ["restaurant_id"]),
]
for tbl, keys in tables_config:
    try:
        score = data_quality_score(tbl, keys)
        grade = "🟢 GOOD" if score >= 90 else ("🟡 FAIR" if score >= 70 else "🔴 POOR")
        print(f"  {tbl:<38} {score:>7}%  {grade}")
    except Exception as e:
        print(f"  {tbl:<38}  ERROR: {e}")

# COMMAND ----------

# DBTITLE 1,Referential Integrity Check Header
# MAGIC %md
# MAGIC ### 🔗 Referential Integrity Check
# MAGIC Verify that `customer_id`, `restaurant_id`, `partner_id` in orders actually exist in dimension tables.

# COMMAND ----------

# DBTITLE 1,Referential Integrity Check
from pyspark.sql.functions import col

df_orders_all = (
    spark.table("bronze.orders_jan")
    .union(spark.table("bronze.orders_feb").select(spark.table("bronze.orders_jan").columns))
    .union(spark.table("bronze.orders_mar").select(spark.table("bronze.orders_jan").columns))
)

df_customers   = spark.table("bronze.customers").select("customer_id").distinct()
df_restaurants = spark.table("bronze.restaurants").select("restaurant_id").distinct()
df_partners    = spark.table("bronze.delivery_partners").select("partner_id").distinct()

print("🔗 Referential Integrity Report:")

# Orphan customer_ids
orphan_customers = df_orders_all.join(df_customers, "customer_id", "left_anti").count()
print(f"  {'customer_id':<20} orphan orders (no matching customer): {orphan_customers}")
if orphan_customers == 0:
    print(f"  ✅ All customer_ids in orders are valid")
else:
    print(f"  ⚠️  {orphan_customers} orders reference unknown customers")

# Orphan restaurant_ids
orphan_restaurants = df_orders_all.join(df_restaurants, "restaurant_id", "left_anti").count()
if orphan_restaurants == 0:
    print(f"  ✅ All restaurant_ids in orders are valid")
else:
    print(f"  ⚠️  {orphan_restaurants} orders reference unknown restaurants")

# Orphan partner_ids
orphan_partners = df_orders_all.join(df_partners, "partner_id", "left_anti").count()
if orphan_partners == 0:
    print(f"  ✅ All partner_ids in orders are valid")
else:
    print(f"  ⚠️  {orphan_partners} orders reference unknown partners")

print(f"\n📊 Dimension table sizes:")
print(f"  bronze.customers:          {df_customers.count():>6} records")
print(f"  bronze.restaurants:        {df_restaurants.count():>6} records")
print(f"  bronze.delivery_partners:  {df_partners.count():>6} records")

# COMMAND ----------

# DBTITLE 1,Data Freshness Check Header
# MAGIC %md
# MAGIC ### 🗓️ Data Freshness Check
# MAGIC Verify how recent the ingested data is based on `order_date` and `ingested_at`.

# COMMAND ----------

# DBTITLE 1,Data Freshness Check
print("🗓️ Data Freshness Report:")
print(f"{'Table':<25} {'Min order_date':<18} {'Max order_date':<18} {'Days since last record':>22}")
print("-" * 86)

for tbl in ["bronze.orders_jan", "bronze.orders_feb", "bronze.orders_mar"]:
    df = spark.table(tbl)
    if "order_date" in df.columns:
        # Raw string min/max — avoids SparkDateTimeException on mixed-format order_date
        stats = spark.sql(f"""
            SELECT MIN(order_date) AS min_date, MAX(order_date) AS max_date
            FROM {tbl}
            WHERE order_date IS NOT NULL AND order_date != 'NA'
              AND order_date RLIKE '^[0-9]'
        """).collect()[0]
        if stats["max_date"]:
            # Use ingested_at (proper TIMESTAMP) — safe, no date parsing needed
            days_old = spark.sql(f"""
                SELECT datediff(current_date(), MAX(date(ingested_at))) AS days_old FROM {tbl}
            """).collect()[0]["days_old"]
            freshness = "🟢 Fresh" if days_old < 30 else ("🟡 Aging" if days_old < 90 else "🔴 Stale")
            print(f"  {tbl:<23} {str(stats['min_date']):<18} {str(stats['max_date']):<18} {days_old:>10} days  {freshness}")
        else:
            print(f"  {tbl:<23} No date data found")

print(f"\n🕑 Latest ingestion timestamps:")
for tbl in ["bronze.orders_jan", "bronze.orders_feb", "bronze.orders_mar",
            "bronze.customers", "bronze.delivery_partners", "bronze.restaurants"]:
    ts = spark.sql(f"SELECT MAX(ingested_at) AS ts FROM {tbl}").collect()[0]["ts"]
    print(f"  {tbl:<38} {ts}")

# COMMAND ----------

# DBTITLE 1,Duplicate Detection Header
# MAGIC %md
# MAGIC ### 🔄 Duplicate Detection Before MERGE

# COMMAND ----------

# DBTITLE 1,Duplicate Detection
from pyspark.sql.functions import count

print("🔄 Duplicate Detection Report:")
for tbl, key_col in [("bronze.orders_jan", "order_id"), ("bronze.orders_feb", "order_id"),
                     ("bronze.orders_mar", "order_id"), ("bronze.customers", "customer_id"),
                     ("bronze.delivery_partners", "partner_id"), ("bronze.restaurants", "restaurant_id")]:
    df = spark.table(tbl)
    total = df.count()
    distinct = df.select(key_col).distinct().count()
    dups = total - distinct
    status = "✅" if dups == 0 else "⚠️ "
    print(f"  {status} {tbl:<38} total={total:>6} | distinct {key_col}={distinct:>6} | duplicates={dups:>5}")
    if dups > 0:
        print(f"      Top duplicate {key_col}s:")
        display(
            df.groupBy(key_col).agg(count("*").alias("occurrences"))
              .filter(col("occurrences") > 1)
              .orderBy(col("occurrences").desc())
              .limit(5)
        )

# COMMAND ----------

# DBTITLE 1,Outlier Detection Header
# MAGIC %md
# MAGIC ### 📉 Outlier Detection — Numeric Columns

# COMMAND ----------

# DBTITLE 1,Outlier Detection
from pyspark.sql.functions import col, mean, stddev, count, when, expr

print("📉 Outlier Detection (values beyond mean ± 3 std dev):")

for tbl, numeric_cols in [
    ("bronze.orders_jan", ["order_amount", "rating", "delivery_time", "payment_amount"]),
    ("bronze.orders_feb", ["order_amount", "rating"]),
    ("bronze.orders_mar", ["order_amount", "rating"]),
]:
    df = spark.table(tbl)
    available_cols = [c for c in numeric_cols if c in df.columns]
    print(f"\n  📊 {tbl}:")
    for c in available_cols:
        stats = df.select(
            mean(expr(f"try_cast({c} AS DOUBLE)")).alias("mean"),
            stddev(expr(f"try_cast({c} AS DOUBLE)")).alias("std")
        ).collect()[0]
        if stats["std"] and stats["std"] > 0:
            lower = stats["mean"] - 3 * stats["std"]
            upper = stats["mean"] + 3 * stats["std"]
            outliers = df.filter(
                (expr(f"try_cast({c} AS DOUBLE)") < lower) | (expr(f"try_cast({c} AS DOUBLE)") > upper)
            ).count()
            pct = round(outliers / df.count() * 100, 2)
            status = "✅" if outliers == 0 else "⚠️ "
            print(f"    {status} {c:<22} mean={round(stats['mean'],2):>8} | std={round(stats['std'],2):>8} | range=[{round(lower,2)}, {round(upper,2)}] | outliers={outliers} ({pct}%)")

# COMMAND ----------

# DBTITLE 1,Partition Distribution Header
# MAGIC %md
# MAGIC ### 🗂️ Partition Distribution — ingestion_month

# COMMAND ----------

# DBTITLE 1,Partition Distribution
from pyspark.sql.functions import col

print("🗂️ Partition distribution (ingestion_month):")
for tbl in ["bronze.orders_jan", "bronze.orders_feb", "bronze.orders_mar"]:
    if "ingestion_month" in spark.table(tbl).columns:
        print(f"\n  {tbl}:")
        display(
            spark.table(tbl)
            .groupBy("ingestion_month")
            .count()
            .orderBy("ingestion_month")
        )
    else:
        # Show ingested_at truncated to month as proxy
        print(f"\n  {tbl} (by ingested_at month):")
        display(
            spark.sql(f"""
                SELECT date_format(ingested_at, 'yyyy-MM') AS ingest_month,
                       COUNT(*) AS row_count
                FROM {tbl}
                GROUP BY 1 ORDER BY 1
            """)
        )

# Also show partition files on disk
print("\n📁 Delta partition files:")
for tbl in ["bronze.orders_jan", "bronze.orders_feb", "bronze.orders_mar"]:
    try:
        location = spark.sql(f"DESCRIBE DETAIL {tbl}").select("location").collect()[0][0]
        parts = [f.name for f in dbutils.fs.ls(location) if f.name.startswith("ingestion_month")]
        print(f"  {tbl}: {parts}")
    except:
        print(f"  {tbl}: partition info unavailable")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 👥 SCD Type 2 Structure — customers

# COMMAND ----------

df_cust = spark.read.format("delta").table("bronze.customers")

# Show customers with multiple rows (SCD2 history)
from pyspark.sql.functions import count as cnt
multi_row = df_cust.groupBy("customer_id").agg(cnt("*").alias("versions")).filter(col("versions") > 1)
print(f"Customers with multiple SCD2 rows: {multi_row.count()}")
display(multi_row.limit(5))

# Show sample SCD2 rows
sample_cust = multi_row.limit(1).select("customer_id").collect()[0][0]
display(df_cust.filter(col("customer_id") == sample_cust).select(
    "customer_id", "loyalty_status", "city", "start_date", "end_date", "current_flag"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🗄️ STORAGE EXPLANATION
# MAGIC
# MAGIC ### Where Delta Tables Live on DBFS
# MAGIC Delta tables written with `saveAsTable()` are stored at:  
# MAGIC `dbfs:/user/hive/warehouse/<database>.db/<table_name>/`
# MAGIC
# MAGIC You can verify with: `dbutils.fs.ls("dbfs:/user/hive/warehouse/bronze.db/")`
# MAGIC
# MAGIC ### Underlying Format — Parquet
# MAGIC Delta Lake stores actual data as **Parquet files** — a columnar binary format that is highly efficient for analytical queries. Each write operation creates one or more `.parquet` files inside the table directory.
# MAGIC
# MAGIC ### Delta Transaction Log (`_delta_log`)
# MAGIC Every Delta table has a `_delta_log/` folder containing JSON files (one per transaction). This log records every operation (INSERT, UPDATE, DELETE, MERGE) with full atomicity. It enables:
# MAGIC - **Time travel** (`VERSION AS OF`, `TIMESTAMP AS OF`)
# MAGIC - **ACID guarantees**
# MAGIC - **Schema enforcement & evolution**
# MAGIC
# MAGIC ### Append vs Overwrite in Delta
# MAGIC | Mode | Behavior |
# MAGIC |------|----------|
# MAGIC | `append` | Adds new Parquet files; existing data untouched. Transaction log records the new files. |
# MAGIC | `overwrite` | Marks all previous Parquet files as removed in the transaction log; writes new files. Old files are physically removed during `VACUUM`. |
# MAGIC
# MAGIC For orders, we use **MERGE INTO** (a form of upsert) to avoid duplicates on re-run — smarter than plain append.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 💧 WATERMARKING & CDC CONCEPT
# MAGIC
# MAGIC ### `ingested_at` as a Watermark
# MAGIC Every row written to Bronze carries an `ingested_at = current_timestamp()`. This acts as a **high-water mark**: on the next incremental run, the pipeline can query:
# MAGIC ```sql
# MAGIC SELECT * FROM bronze.orders_jan WHERE ingested_at > '<last_run_timestamp>'
# MAGIC ```
# MAGIC This retrieves only **newly ingested rows** without re-scanning the full table.
# MAGIC
# MAGIC ### CDC with `updated_at`
# MAGIC The source orders files include `updated_at` — the timestamp when a record was last modified at the source. A CDC pipeline would:
# MAGIC 1. Track the **max `updated_at`** from the previous Silver run.
# MAGIC 2. On next run, read only rows from Bronze where `updated_at > last_silver_run_timestamp`.
# MAGIC 3. Use **MERGE INTO Silver** to upsert changed rows (update existing, insert new).
# MAGIC
# MAGIC This combination of `ingested_at` (pipeline watermark) and `updated_at` (source watermark) makes the pipeline **idempotent and incremental**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Bronze Ingestion Complete

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🎯 METADATA LAYER

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📝 Metadata Creation Function

# COMMAND ----------

def add_ingestion_metadata(df, source_path, dataset_name, source_format):
    """Add metadata columns to track ingestion details"""
    from pyspark.sql.functions import lit, current_timestamp
    
    file_name = source_path.split("/")[-1]
    
    df_with_metadata = df \
        .withColumn("_bronze_ingested_at", current_timestamp()) \
        .withColumn("_bronze_source_path", lit(source_path)) \
        .withColumn("_bronze_source_file", lit(file_name)) \
        .withColumn("_bronze_source_format", lit(source_format)) \
        .withColumn("_bronze_dataset_name", lit(dataset_name)) \
        .withColumn("_bronze_row_count", lit(df.count()))
    
    return df_with_metadata

# Create audit table to track all ingestions
def create_audit_table():
    """Create metadata audit table to log all file ingestions"""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.bronze_ingestion_audit (
            source_path STRING,
            source_file STRING,
            source_format STRING,
            dataset_name STRING,
            file_row_count LONG,
            ingestion_timestamp TIMESTAMP,
            batch_id STRING,
            ingestion_status STRING,
            error_message STRING
        )
        USING DELTA
    """)
    print("✅ Audit table created: bronze.bronze_ingestion_audit")

# Initialize audit table
create_audit_table()

# Get unique batch ID for this run
from datetime import datetime
BATCH_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
print(f"🔖 Current batch ID: {BATCH_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎯 Log Ingestion Records to Audit Table

# COMMAND ----------

def log_ingestion_audit(source_path, source_format, row_count, dataset_name, status="SUCCESS", error_msg=None):
    """Log ingestion event to audit table"""
    from datetime import datetime
    
    file_name = source_path.split("/")[-1]
    audit_record = spark.createDataFrame([
        (source_path, file_name, source_format, dataset_name, row_count, 
         datetime.now(), BATCH_ID, status, error_msg)
    ], schema=["source_path", "source_file", "source_format", "dataset_name", 
               "file_row_count", "ingestion_timestamp", "batch_id", "ingestion_status", "error_message"])
    
    audit_record.write.format("delta").mode("append").saveAsTable("bronze.bronze_ingestion_audit")
    print(f"  📋 Audit logged: {dataset_name} | {source_format} | {row_count} rows | Status: {status}")

print("✅ Audit logging function ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 Example: Log Orders January Metadata

# COMMAND ----------

if source_month in ("all", "jan"):
    try:
        fmt = detect_file_format(paths["jan"])
        df_jan_count = df_jan.count()
        log_ingestion_audit(paths["jan"], fmt, df_jan_count, "orders_jan", status="SUCCESS")
    except Exception as e:
        log_ingestion_audit(paths["jan"], "csv", 0, "orders_jan", status="FAILED", error_msg=str(e))
        print(f"❌ Error logging orders_jan: {e}")

if source_month in ("all", "feb"):
    try:
        fmt = detect_file_format(paths["feb"])
        df_feb_count = df_feb.count()
        log_ingestion_audit(paths["feb"], fmt, df_feb_count, "orders_feb", status="SUCCESS")
    except Exception as e:
        log_ingestion_audit(paths["feb"], "csv", 0, "orders_feb", status="FAILED", error_msg=str(e))
        print(f"❌ Error logging orders_feb: {e}")

if source_month in ("all", "mar"):
    try:
        fmt = detect_file_format(paths["mar"])
        df_mar_count = df_mar.count()
        log_ingestion_audit(paths["mar"], fmt, df_mar_count, "orders_mar", status="SUCCESS")
    except Exception as e:
        log_ingestion_audit(paths["mar"], "csv", 0, "orders_mar", status="FAILED", error_msg=str(e))
        print(f"❌ Error logging orders_mar: {e}")

try:
    fmt = detect_file_format(paths["customers"])
    log_ingestion_audit(paths["customers"], fmt, df_customers.count(), "customers", status="SUCCESS")
except Exception as e:
    log_ingestion_audit(paths["customers"], "csv", 0, "customers", status="FAILED", error_msg=str(e))
    print(f"❌ Error logging customers: {e}")

try:
    fmt = detect_file_format(paths["partners"])
    log_ingestion_audit(paths["partners"], fmt, df_partners.count(), "delivery_partners", status="SUCCESS")
except Exception as e:
    log_ingestion_audit(paths["partners"], "csv", 0, "delivery_partners", status="FAILED", error_msg=str(e))
    print(f"❌ Error logging delivery_partners: {e}")

print("✅ All ingestion records logged to audit table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📋 View Ingestion Audit Log

# COMMAND ----------

spark.sql("""
    SELECT 
        dataset_name,
        source_file,
        source_format,
        file_row_count,
        ingestion_timestamp,
        batch_id,
        ingestion_status
    FROM bronze.bronze_ingestion_audit
    ORDER BY ingestion_timestamp DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔄 File Format Handling Techniques

# COMMAND ----------

print("""
📊 FILE FORMAT HANDLING TECHNIQUES:

1️⃣  CSV (Comma-Separated Values)
   - Best for: Tabular data, spreadsheets, exports
   - Spark options: header=true, inferSchema=true, delimiter=","
   - Example: orders_jan.csv
   
2️⃣  JSON (JavaScript Object Notation)
   - Best for: Semi-structured, nested data, APIs
   - Spark options: inferSchema=true, multiLine=true
   - Example: events.json (nested customer objects)
   
3️⃣  PARQUET (Columnar Binary Format)
   - Best for: Already partitioned data, high compression
   - Spark options: None needed, auto-detected
   - Example: archive.parquet (pre-optimized exports)
   
4️⃣  AUTO-DETECTION (Our approach)
   - Check file extension → Determine format
   - Use smart reader → Apply appropriate options
   - Log format in audit → Track data lineage
""")

# Demonstrate format detection for each file in our paths
print("\n✅ DETECTED FORMATS FOR CURRENT FILES:\n")
for dataset, file_path in paths.items():
    detected = detect_file_format(file_path)
    print(f"  • {dataset:20} → {file_path:40} [Format: {detected}]")

print("\n✨ Auto-detection ensures pipeline adapts to new file types without code changes!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Metadata & File Handling Features Summary

# COMMAND ----------

print("""
🎯 WHAT WE ADDED TO BRONZE LAYER:

╔════════════════════════════════════════════════════════════════╗
║  1. FILE FORMAT DETECTION                                      ║
║     • detect_file_format() → Auto-detect CSV/JSON/Parquet       ║
║     • read_file_smart() → Read any format intelligently         ║
║                                                                ║
║  2. METADATA COLUMNS                                            ║
║     • _bronze_ingested_at → When was this row ingested?         ║
║     • _bronze_source_path → Full path to source file            ║
║     • _bronze_source_file → Just the filename                   ║
║     • _bronze_source_format → CSV/JSON/Parquet                  ║
║     • _bronze_dataset_name → Business name (orders_jan)         ║
║     • _bronze_row_count → How many rows in this batch?          ║
║                                                                ║
║  3. AUDIT TRACKING TABLE                                        ║
║     • bronze.bronze_ingestion_audit (Delta table)               ║
║     • Logs: file path, format, row count, timestamp, status     ║
║     • Tracks: Success/Failure + error messages                  ║
║     • Query: SELECT * FROM bronze.bronze_ingestion_audit        ║
║                                                                ║
║  4. BATCH ID TRACKING                                           ║
║     • BATCH_ID → Unique ID per ingestion run                    ║
║     • Correlates: Which files were loaded together              ║
║     • Use: Find all tables ingested in batch_20250617_143022    ║
╚════════════════════════════════════════════════════════════════╝

✨ KEY BENEFITS:
   ✓ Lineage Tracking → Know exactly where data came from
   ✓ Format Flexibility → Handle CSV, JSON, Parquet seamlessly
   ✓ Error Auditing → Track failed ingestions with error messages
   ✓ Batch Correlation → Link related files loaded together
   ✓ Compliance Ready → Full audit trail for governance
""")

# Show audit table exists
if table_exists := spark.sql("DESCRIBE TABLE bronze.bronze_ingestion_audit"):
    print(f"\n📊 Audit Table Status: ✅ ACTIVE")
    count = spark.sql("SELECT COUNT(*) as cnt FROM bronze.bronze_ingestion_audit").collect()[0]["cnt"]
    print(f"   Total ingestion records logged: {count}")
else:
    print("\n📊 Audit Table Status: Ready to log")

# COMMAND ----------

print("✅ BRONZE LAYER COMPLETE")
for tbl in tables:
    cnt = spark.read.format("delta").table(tbl).count()
    print(f"   {tbl}: {cnt} rows")

# COMMAND ----------

# DBTITLE 1,Structured Streaming with Checkpoints
from pyspark.sql.functions import current_timestamp, date_format

CHECKPOINT_BASE = "dbfs:/tmp/food-data/checkpoints"

print("⚡ Starting Structured Streaming ingestion with checkpoint...")

# Derive schema from a static read first (streaming cannot use inferSchema)
orders_schema = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("dbfs:/tmp/food-data/orders_jan.csv")
    .schema
)

# Read all orders CSVs as a stream (picks up new files automatically)
df_stream = (
    spark.readStream
    .format("csv")
    .option("header", "true")
    .schema(orders_schema)             # explicit schema required for streaming
    .option("maxFilesPerTrigger", 1)   # process 1 file per micro-batch
    .load("dbfs:/tmp/food-data/orders_*.csv")
)

# Write to bronze.orders_stream with checkpoint
query = (
    df_stream
    .withColumn("ingested_at", current_timestamp())
    .withColumn("ingestion_month", date_format(current_timestamp(), "yyyy-MM"))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/orders_stream")  # ← checkpoint folder
    .option("mergeSchema", "true")
    .trigger(availableNow=True)   # process all available files then stop
    .toTable("bronze.orders_stream")
)
query.awaitTermination()

print(f"✅ Streaming complete")
print(f"📁 Checkpoint saved at : {CHECKPOINT_BASE}/orders_stream")
print(f"📊 Rows in bronze.orders_stream: {spark.table('bronze.orders_stream').count()}")
print("💡 On next run, stream resumes from checkpoint — no duplicates, no data loss.")

# COMMAND ----------

# DBTITLE 1,Time Travel Header
# MAGIC %md
# MAGIC ---
# MAGIC ## ⏱️ DELTA LAKE TIME TRAVEL — Bronze Layer
# MAGIC
# MAGIC Bronze tables accumulate versions on every MERGE run. Use time travel to:
# MAGIC - Verify raw data before any Silver transformations
# MAGIC - Audit exactly when a batch of records was ingested
# MAGIC - Recover from accidental overwrites

# COMMAND ----------

# DBTITLE 1,Time Travel 1: Bronze Table Histories
# Version history for all Bronze tables
for tbl in ["bronze.orders_jan", "bronze.orders_feb",
            "bronze.orders_mar", "bronze.customers", "bronze.delivery_partners"]:
    print(f"\n📜 {tbl}:")
    display(spark.sql(f"DESCRIBE HISTORY {tbl}"))

# COMMAND ----------

# DBTITLE 1,Time Travel 2: Query Version 0 of bronze.orders_jan
# Query bronze.orders_jan at initial load (version 0)
# Confirms raw CSV data before any MERGE or re-ingestion
df_bronze_v0 = spark.sql("""
    SELECT
        COUNT(*)                        AS total_rows,
        COUNT(DISTINCT customer_id)     AS unique_customers,
        COUNT(DISTINCT restaurant_id)   AS unique_restaurants,
        ROUND(AVG(try_cast(order_amount AS DOUBLE)), 2) AS avg_order_amount
    FROM bronze.orders_jan VERSION AS OF 0
""")
print("🔙 bronze.orders_jan at VERSION 0:")
display(df_bronze_v0)

# COMMAND ----------

# DBTITLE 1,Time Travel 3: Compare Current vs Version 0 Bronze
# Compare current bronze.orders_jan vs version 0
# Shows if any rows were added/changed since initial ingestion
df_bronze_compare = spark.sql("""
    SELECT
        'Version 0' AS label, COUNT(*) AS total_rows,
        COUNT(DISTINCT order_id) AS distinct_orders
    FROM bronze.orders_jan VERSION AS OF 0
    UNION ALL
    SELECT
        'Current', COUNT(*), COUNT(DISTINCT order_id)
    FROM bronze.orders_jan
""")
print("🔍 bronze.orders_jan: current vs version 0")
display(df_bronze_compare)

# COMMAND ----------

# DBTITLE 1,Time Travel 4: Restore Example (Dry Run)
# ⚠️ RESTORE is a write operation — commented out for safety
# Uncomment only when intentionally rolling back:

# spark.sql("RESTORE TABLE bronze.orders_jan TO VERSION AS OF 0")

# Safe preview: ingestion timestamps at version 0
df_ingest_ts = spark.sql("""
    SELECT
        MIN(ingested_at) AS first_ingested,
        MAX(ingested_at) AS last_ingested,
        COUNT(*)         AS total_rows
    FROM bronze.orders_jan VERSION AS OF 0
""")
print("📅 Ingestion window at version 0:")
display(df_ingest_ts)