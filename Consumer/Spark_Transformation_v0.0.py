"""
Spark Structured Streaming - Full IoT Insights Pipeline

Reads:    Kafka INPUT_TOPIC ("farm_sensors") with JSON sensor events
Produces: Enriched events -> Kafka OUTPUT_EVENTS_TOPIC ("farm_insights")
          Trend insights (5min) -> Kafka OUTPUT_TRENDS_TOPIC ("farm_trends")
          KPI aggregates (daily, weekly) -> Kafka OUTPUT_KPIS_TOPIC ("farm_kpis")
          Parquet archives for historical/batch analysis -> PARQUET_BASE_PATH

Features implemented:
- Per-event enrichment (env_health_score, ph_status, salinity_status, flags)
- Windowed aggregates (5min sliding, 1h sliding, 1d tumbling)
- Delta & trend detection (lag on windowed aggregates)
- Outlier detection using window mean/std (Z-score) & IQR approximation (via percentiles not available in streaming)
- Sensor reliability score (error ratio + variance heuristics)
- Top-N sensors by anomaly frequency within sliding windows
- Writes results to Kafka and Parquet for dashboard / downstream ML
"""

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, avg as _avg, stddev as _stddev,
    count as _count, lit, when, expr, to_json, struct, row_number, abs
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, BooleanType
)
from pyspark.sql.window import Window

# ===========================
# CONFIG - تعديل حسب الحاجة
# ===========================
KAFKA_BOOTSTRAP = "localhost:9092"
INPUT_TOPIC = "farmSensors"
OUTPUT_EVENTS_TOPIC = "farmInsights"   # enriched per-event
OUTPUT_TRENDS_TOPIC = "farmTrends"     # 5min trend insights
OUTPUT_KPIS_TOPIC = "farmKpis"         # daily/week KPIs (periodic)
HOME_PATH = "/home/mostafa"
PARQUET_BASE_PATH = f"{HOME_PATH}/spark_project_data/farm_iot_parquet"
CHECKPOINT_BASE = f"{HOME_PATH}/spark_project_data/checkpoints/farm_iot_full_pipeline"
PROCESSING_TRIGGER = "30 seconds"

# ===========================
# SCHEMA (matches your simulator)
# ===========================
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("date", StringType()),
    StructField("time", StringType()),
    StructField("season", StringType()),
    StructField("day_period", StringType()),
    StructField("daytime", BooleanType()),
    StructField("soil_temperature_c", FloatType()),
    StructField("air_temperature_c", FloatType()),
    StructField("soil_humidity_percent", FloatType()),
    StructField("air_humidity_percent", FloatType()),
    StructField("soil_ph", FloatType()),
    StructField("soil_salinity_ds_m", FloatType()),
    StructField("light_intensity_lux", FloatType()),
    StructField("water_level_percent", FloatType()),
    StructField("location", StringType()),
    StructField("is_error", BooleanType())
])

# ===========================
# Spark session
# ===========================
spark = (
    SparkSession.builder
    .appName("FarmIoTFullPipeline")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ===========================
# Read from Kafka
# ===========================
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    raw.select(from_json(col("value").cast("string"), schema).alias("data"))
       .select("data.*")
       .withColumn("event_ts", to_timestamp(col("timestamp")))
)
# Add watermark for window operations (tolerate slightly late data)
parsed = parsed.withWatermark("event_ts", "1 hour")

# ===========================
# 1) Per-event enrichment: flags, scores, derived cols
# ===========================
events_enriched = (
    parsed
    # differences
    .withColumn("temp_diff_air_soil", col("air_temperature_c") - col("soil_temperature_c"))
    .withColumn("humidity_diff_air_soil", col("air_humidity_percent") - col("soil_humidity_percent"))

    # pH classification
    .withColumn("ph_status",
        when(col("soil_ph").isNull(), lit("Unknown"))
        .when(col("soil_ph") < 6.0, lit("Acidic"))
        .when(col("soil_ph") > 8.0, lit("Alkaline"))
        .otherwise(lit("Normal"))
    )

    # salinity classification
    .withColumn("salinity_status",
        when(col("soil_salinity_ds_m") < 2.0, lit("Low"))
        .when((col("soil_salinity_ds_m") >= 2.0) & (col("soil_salinity_ds_m") < 4.0), lit("Moderate"))
        .otherwise(lit("High"))
    )

    # anomalies rules (rule-based)
    .withColumn("is_anomaly_temp",
        when((col("soil_temperature_c") > 40) | (col("air_temperature_c") > 40) | (col("soil_temperature_c") < -20), lit(1)).otherwise(lit(0))
    )
    .withColumn("is_anomaly_humidity",
        when((col("soil_humidity_percent") < 30) | (col("soil_humidity_percent") > 100), lit(1)).otherwise(lit(0))
    )
    .withColumn("is_sensor_error", when(col("is_error") == True, lit(1)).otherwise(lit(0)))

    # immediate environmental indicators
    .withColumn("needs_watering", when(col("soil_humidity_percent") < 30, lit(1)).otherwise(lit(0)))
    .withColumn("possible_overheating", when((col("soil_temperature_c") > 40) | (col("air_temperature_c") > 40), lit(1)).otherwise(lit(0)))
    .withColumn("ph_not_optimal", when((col("soil_ph") < 6) | (col("soil_ph") > 8), lit(1)).otherwise(lit(0)))

    # environmental health score (0-100) - tune weights per crop later
    .withColumn("env_health_score",
        (lit(100)
         - (expr("abs(soil_ph - 7.0) * 12"))   # pH distance weight
         - (col("soil_salinity_ds_m") * 6)     # salinity weight
         - when(col("soil_humidity_percent") < 30, lit(12)).otherwise(lit(0)) )
    )
)

# ===========================
# 2) Windowed aggregates for trends & spatial analysis
#    - 5 minutes sliding (step 1 minute) for short-term trends
#    - 1 hour sliding (step 5 min) for mid-term
#    - 1 day tumbling for KPIs
# ===========================
agg_5m = (
    events_enriched
    .groupBy(window(col("event_ts"), "5 minutes", "1 minute"), col("location"))
    .agg(
        _avg("soil_temperature_c").alias("avg_soil_temp_5m"),
        _avg("soil_humidity_percent").alias("avg_soil_humidity_5m"),
        _avg("soil_salinity_ds_m").alias("avg_salinity_5m"),
        _avg("env_health_score").alias("avg_env_health_score_5m"),
        _stddev("soil_temperature_c").alias("std_soil_temp_5m"),
        _stddev("soil_humidity_percent").alias("std_soil_humidity_5m"),
        _count(when(col("is_anomaly_temp") == 1, True)).alias("anomaly_temp_count_5m"),
        _count(when(col("is_anomaly_humidity") == 1, True)).alias("anomaly_humidity_count_5m"),
        _count(when(col("is_sensor_error") == 1, True)).alias("error_count_5m"),
        _count("*").alias("records_5m")
    )
)

agg_1h = (
    events_enriched
    .groupBy(window(col("event_ts"), "1 hour", "5 minutes"), col("location"))
    .agg(
        _avg("soil_temperature_c").alias("avg_soil_temp_1h"),
        _avg("soil_humidity_percent").alias("avg_soil_humidity_1h"),
        _avg("soil_salinity_ds_m").alias("avg_salinity_1h"),
        _avg("env_health_score").alias("avg_env_health_score_1h"),
        _stddev("soil_temperature_c").alias("std_soil_temp_1h"),
        _count(when(col("is_sensor_error") == 1, True)).alias("error_count_1h"),
        _count("*").alias("records_1h"),
        _count(when(col("is_anomaly_temp") == 1, True)).alias("anomaly_temp_count_1h"),
    )
)

agg_1d = (
    events_enriched
    .groupBy(window(col("event_ts"), "1 day"), col("location"))
    .agg(
        _avg("env_health_score").alias("avg_env_health_score_1d"),
        _avg("soil_humidity_percent").alias("avg_soil_humidity_1d"),
        _avg("soil_temperature_c").alias("avg_soil_temp_1d"),
        _avg("soil_salinity_ds_m").alias("avg_salinity_1d"),
        _count(when(col("needs_watering") == 1, True)).alias("needs_watering_count_1d"),
        _count(when(col("possible_overheating") == 1, True)).alias("overheat_count_1d"),
        _count(when(col("is_sensor_error") == 1, True)).alias("error_count_1d"),
        _count("*").alias("records_1d")
    )
)

# ===========================
# 3) Trend detection: compare current 5m window to previous 5m (lag)
#    We'll compute deltas via window-level lag using row_number partition trick.
# ===========================
# Prepare ordered 5m aggregates with window start time
agg5_time = agg_5m.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("location"),
    "avg_soil_temp_5m", "avg_soil_humidity_5m", "avg_salinity_5m",
    "std_soil_temp_5m", "std_soil_humidity_5m",
    "anomaly_temp_count_5m", "anomaly_humidity_count_5m", "error_count_5m", "records_5m"
)

# use event-time ordering per location by window_start


# ===========================
# 4) Outlier detection (Z-score) - mark rows in events_enriched where value deviates from window mean by > k * std
#    We'll join event-level stream with 5-min aggregates to compute z-score per event
# ===========================
# First, create compact agg5 stream keyed by location & window range
agg5_compact = agg5_time.select(
    col("location"),
    col("window_start"),
    col("window_end"),
    "avg_soil_temp_5m", "std_soil_temp_5m",
    "avg_soil_humidity_5m", "std_soil_humidity_5m"
)
agg5_compact = agg5_compact.withWatermark("window_start", "10 minutes")
# Join events with their 5-min window aggregates (stream-stream join on time range)
# Approach: join on location and event_ts between window_start and window_end using range join pattern.
events_with_window = events_enriched.join(
    agg5_compact,
    (events_enriched.location == agg5_compact.location) &
    (events_enriched.event_ts >= agg5_compact.window_start) &
    (events_enriched.event_ts < agg5_compact.window_end),
    how="left"
).drop(agg5_compact.location)

# Compute Z-score outlier flags
k_temp = 3.0
k_hum = 3.0
events_outliers = events_with_window.withColumn(
    "z_temp",
    when(col("std_soil_temp_5m").isNull(), lit(0.0))
    .otherwise( (col("soil_temperature_c") - col("avg_soil_temp_5m")) / col("std_soil_temp_5m") )
).withColumn(
    "z_hum",
    when(col("std_soil_humidity_5m").isNull(), lit(0.0))
    .otherwise( (col("soil_humidity_percent") - col("avg_soil_humidity_5m")) / col("std_soil_humidity_5m") )
).withColumn(
    "is_outlier_temp_z", when(abs(col("z_temp")) > k_temp, lit(1)).otherwise(lit(0))
).withColumn(
    "is_outlier_hum_z", when(abs(col("z_hum")) > k_hum, lit(1)).otherwise(lit(0))
)

# ===========================
# 5) Sensor reliability score per location (sliding window)
#    simple heuristic: reliability = 100 * (1 - alpha*error_ratio - beta*variance_ratio)
# ===========================
# We'll compute per-location error ratio and variance_ratio using 1h window
reliability_1h = (
    agg_1h
    .select("window", "location", "avg_soil_temp_1h", "avg_soil_humidity_1h", "avg_salinity_1h", "std_soil_temp_1h", "error_count_1h", "records_1h")
    .withColumn("error_ratio_1h", col("error_count_1h") / col("records_1h"))
    .withColumn("variance_ratio_temp", col("std_soil_temp_1h") / (col("avg_soil_temp_1h") + lit(0.0001)))
    .withColumn("sensor_reliability_score",
        (lit(100) - (col("error_ratio_1h") * lit(100) * lit(2.0)) - (col("variance_ratio_temp") * lit(50)))
    )
)

# ===========================
# 6) Top 5 sensors by anomaly frequency (sliding 1 day window or 1h for demo)
# ===========================
top_anomalies_1h = (
    agg_1h
    .withColumn("anomaly_total_1h", col("anomaly_temp_count_1h") + lit(0))   # ensure column exists
    .select(col("location"), col("anomaly_temp_count_1h"), col("records_1h"))
    .withColumn("anomaly_rate_1h", col("anomaly_temp_count_1h") / col("records_1h"))
)

# For Top N we can rank per microbatch (non-perfect in streaming but acceptable)

# ===========================
# 7) KPI: daily/week KPIs streaming (tumbling windows)
#    - Average soil health score per day/week
#    - Number of alerts per zone
#    - % of time soil was dry (needs_watering ratio)
#    - % normal vs abnormal readings
# ===========================
kpi_daily = (
    events_enriched
    .groupBy(window(col("event_ts"), "1 day"), col("location"))
    .agg(
        _avg("env_health_score").alias("avg_env_health_score_day"),
        (_count(when(col("needs_watering") == 1, True)) / _count("*")).alias("pct_time_dry"),
        _count(when(col("is_anomaly_temp") == 1, True)).alias("anomaly_count_day"),
        _count(when(col("is_sensor_error") == 1, True)).alias("error_count_day"),
        _count("*").alias("records_day")
    )
)

kpi_weekly = (
    events_enriched
    .groupBy(window(col("event_ts"), "7 days"), col("location"))
    .agg(
        _avg("env_health_score").alias("avg_env_health_score_week"),
        (_count(when(col("needs_watering") == 1, True)) / _count("*")).alias("pct_time_dry_week"),
        _count(when(col("is_anomaly_temp") == 1, True)).alias("anomaly_count_week"),
        _count("*").alias("records_week")
    )
)

# Derive an overall farm health grade (A/B/C/D) based on avg_env_health_score thresholds
kpi_daily_grade = kpi_daily.withColumn(
    "farm_health_grade",
    when(col("avg_env_health_score_day") >= 80, lit("A"))
    .when(col("avg_env_health_score_day") >= 60, lit("B"))
    .when(col("avg_env_health_score_day") >= 40, lit("C"))
    .otherwise(lit("D"))
)

# ===========================
# 8) Output sinks
#    - events_outliers -> enriched events + outlier flags -> Kafka (OUTPUT_EVENTS_TOPIC)
#    - agg5_with_lag trends -> Kafka (OUTPUT_TRENDS_TOPIC)
#    - kpi_daily & kpi_weekly -> Kafka (OUTPUT_KPIS_TOPIC) & Parquet for history
#    - reliability_1h -> Parquet (sensor reliability)
#    - top5 -> console (and could be pushed to Kafka)
# ===========================

def process_top_anomalies_batch(batch_df, batch_id):
    """
    Function to process each micro-batch of aggregated anomalies
    to calculate Top-N (which is not supported directly on a stream).
    """
    print(f"\n--- Processing Top 5 Anomalies Batch: {batch_id} ---")
    
    # 1. Define window (NOW ALLOWED inside a batch)
    w_rank_batch = Window.orderBy(col("anomaly_rate_1h").desc_nulls_last())

    # 2. Calculate Top 5 for THIS batch
    top5_batch = (
        batch_df
        .withColumn("rank", row_number().over(w_rank_batch))
        .filter(col("rank") <= 5)
        .select("location", "anomaly_rate_1h", "rank")
    )

    # 3. Show results to console
    top5_batch.show(truncate=False)
    
# === نهاية الدالة ===


# Prepare event-level JSON for Kafka (enriched + outlier flags)
events_to_kafka = events_outliers.select(
    to_json(struct(*[c for c in events_outliers.columns])).alias("value")
)

events_kafka_q = (
    events_to_kafka.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", OUTPUT_EVENTS_TOPIC)
    .option("checkpointLocation", CHECKPOINT_BASE + "/events_to_kafka")
    .outputMode("append")
    .start()
)
# --- Sink جديد: بناء الـ Data Lake (Bronze/Silver Layer) ---
# حفظ كل الأحداث المُعالجة (enriched) في Delta Lake
events_lake_q = (
    events_outliers.writeStream
    .format("delta")  # استخدام Delta Lake بدلاً من Parquet
    .outputMode("append")
    .option("path", f"{PARQUET_BASE_PATH}/delta_lake/all_events") # مسار الـ Data Lake
    .option("checkpointLocation", CHECKPOINT_BASE + "/events_to_delta_lake")
    .start()
)
# --- 8a) تعريفات الـ SQL Data Warehouse (الطبقة الذهبية) ---
# (قم بتعديل هذه القيم لـ SQL Server أو PostgreSQL أو MySQL)

DB_URL = "jdbc:postgresql://localhost:5432/farm_dwh"  # مثال لـ PostgreSQL
DB_PROPERTIES = {
    "user": "spark_user",              # اسم مستخدم قاعدة البيانات
    "password": "spark_password",      # كلمة المرور
    "driver": "org.postgresql.Driver"  # الـ Driver class
}
KPI_TABLE_NAME = "daily_farm_kpis"      # اسم الجدول الذي سيتم إنشاؤه/الكتابة فيه

def write_kpis_to_sql_batch(batch_df, batch_id):
    """
    Function to process each micro-batch of aggregated KPIs (Gold Layer)
    and write them to the SQL Data Warehouse.
    """
    print(f"--- 🚀 Writing KPI Batch {batch_id} to SQL DWH ({KPI_TABLE_NAME}) ---")
    
    try:
        # نقوم بمعالجة الـ DataFrame داخل الـ Batch 
        # تماماً كما كنا نفعل مع ملفات Parquet (لإزالة عمود "window")
        processed_batch_df = (
            batch_df
            .withColumn("window_start", col("window").start)
            .withColumn("window_end", col("window").end)
            .drop("window")
            .select("window_start", "window_end", "location", 
                    "avg_env_health_score_day", "pct_time_dry", 
                    "anomaly_count_day", "error_count_day", 
                    "records_day", "farm_health_grade")
        )

        # كتابة الـ Batch إلى قاعدة البيانات
        (processed_batch_df
         .write
         .jdbc(url=DB_URL,
               table=KPI_TABLE_NAME,
               mode="append",  # إضافة البيانات الجديدة
               properties=DB_PROPERTIES)
        )
        
        print(f"--- ✅ Batch {batch_id} successfully written to {KPI_TABLE_NAME} ---")
    
    except Exception as e:
        # طباعة الخطأ، ولكن السماح للـ Stream بالاستمرار
        print(f"--- 🔥 Error writing Batch {batch_id} to SQL: {e} ---")

# === نهاية الدالة ===

# Trend insights (5m) -> include delta & labels
trends_for_kafka = agg5_time.select(
    to_json(struct(
        col("window_start").alias("window_start"),
        col("window_end").alias("window_end"),
        col("location"),
        col("avg_soil_temp_5m"),
        col("avg_soil_humidity_5m"),
        col("anomaly_temp_count_5m"),
        col("error_count_5m")
    )).alias("value")
)

trends_kafka_q = (
    trends_for_kafka.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", OUTPUT_TRENDS_TOPIC)
    .option("checkpointLocation", CHECKPOINT_BASE + "/trends_to_kafka")
    .outputMode("update")
    .start()
)

# KPIs daily -> Kafka + Parquet archive
kpi_daily_kafka = kpi_daily_grade.select(
    to_json(struct(
        col("window").start.alias("window_start"),
        col("window").end.alias("window_end"),
        col("location"),
        col("avg_env_health_score_day"),
        col("pct_time_dry"),
        col("anomaly_count_day"),
        col("error_count_day"),
        col("records_day"),
        col("farm_health_grade")
    )).alias("value")
)

kpi_daily_q = (
    kpi_daily_kafka.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", OUTPUT_KPIS_TOPIC)
    .option("checkpointLocation", CHECKPOINT_BASE + "/kpi_daily_kafka")
    .outputMode("update")
    .start()
)

# Also archive daily KPIs to Parquet for dashboard/historical
# --- 8b) Sink الـ SQL Data Warehouse (الطبقة الذهبية) ---
# هذا هو الـ Sink الجديد الذي يرسل الـ KPIs إلى SQL
kpi_daily_sql_q = (
    kpi_daily_grade  # <-- الـ DataFrame الخاص بالـ Gold Layer
    .writeStream
    .outputMode("update")  # نستخدم 'update' لأنها مخرجات Tumble window
    .foreachBatch(write_kpis_to_sql_batch) # استدعاء الدالة المخصصة
    .option("checkpointLocation", CHECKPOINT_BASE + "/kpi_daily_sql_dwh")
    .start()
)
print("✅ SQL DWH Sink (kpi_daily_sql_q) started.")


# --- 8c) Sink ملفات Parquet (لـ KPIs) ---
# !!! (اختياري) يمكنك حذف هذا الـ Sink أو التعليق عليه
# لأنه تم استبداله بـ kpi_daily_sql_q
# kpi_daily_parquet_q = (
#     kpi_daily_parquet.writeStream
#     .format("parquet")
#     .option("path", f"{PARQUET_BASE_PATH}/kpi_daily")
#     .option("checkpointLocation", CHECKPOINT_BASE + "/kpi_daily_parquet")
#     .outputMode("append")
#     .start()
# )
# print("⚠️ (Optional) Parquet DWH Sink (kpi_daily_parquet_q) started.")

# Reliability (1h) -> parquet for monitoring
reliability_parquet_q = (
    reliability_1h
    .withColumn("window_start", col("window").start)
    .withColumn("window_end", col("window").end)
    .drop("window")
    .writeStream
    .format("parquet")
    .option("path", f"{PARQUET_BASE_PATH}/reliability_1h")
    .option("checkpointLocation", CHECKPOINT_BASE + "/reliability_parquet")
    .outputMode("append")
    .start()
)

# Top5 anomalies -> console (for demo); can be pushed to Kafka similarly
top5_q = (
    top_anomalies_1h.writeStream # Note: We write the *input* DF
    .foreachBatch(process_top_anomalies_batch) # Call our new function
    .outputMode("update") # Use 'update' mode for aggregations
    .option("checkpointLocation", CHECKPOINT_BASE + "/top5_console")
    .start()
)

# Final: console sample for debugging events
console_events_q = (
    events_outliers.select("event_ts", "location", "soil_temperature_c", "soil_humidity_percent",
                           "is_outlier_temp_z", "is_outlier_hum_z", "needs_watering",
                           "possible_overheating", "env_health_score")
    .writeStream
    .format("console")
    .option("truncate", False)
    .outputMode("append")
    .start()
)

print("Streaming started. Topics ->", OUTPUT_EVENTS_TOPIC, OUTPUT_TRENDS_TOPIC, OUTPUT_KPIS_TOPIC)
spark.streams.awaitAnyTermination()
