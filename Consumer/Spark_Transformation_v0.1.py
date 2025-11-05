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
import pandas as pd
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, avg as _avg, stddev as _stddev,
    count as _count, lit, when, expr, to_json, struct, row_number, abs, session_window
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, BooleanType, IntegerType
)
from pyspark.sql.window import Window as SparkWindow
import pyspark.sql.functions as F
from pyspark.sql.pandas.functions import pandas_udf
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
# (استخدم نفس تعريفات DB_URL و DB_PROPERTIES من الأسفل)
DB_URL = "jdbc:postgresql://localhost:5432/farm_dwh"
DB_PROPERTIES = {
    "user": "spark_user",
    "password": "spark_password",
    "driver": "org.postgresql.Driver"
}
KPI_TABLE_NAME = "daily_farm_kpis"      # اسم الجدول الذي سيتم إنشاؤه/الكتابة فيه
DIM_TABLE_NAME = "dim_location"
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
# ML Model UDF Definition
# ===========================

# 1. تعريف الأعمدة التي سيتلقاها النموذج
# (يجب أن تتطابق مع ما تدرب عليه النموذج)
features_for_model = struct(
    col("soil_temperature_c").alias("soil_temp"),
    col("soil_humidity_percent").alias("soil_hum"),
    col("soil_ph"),
    col("light_intensity_lux")
)

# 2. تعريف الدالة (محاكاة)

def predict_anomaly(series: pd.Series) -> pd.Series:
    # --- بداية المحاكاة ---
    # (في الإنتاج، استبدل هذا باستدعاء النموذج الفعلي)
    # e.g., features_df = pd.DataFrame(series.tolist())
    # return model.predict(features_df)
    
    # محاكاة بسيطة: اعتبرها anomaly إذا كانت الحرارة عالية والرطوبة منخفضة جداً
    def check_row(x):
        if x is None: return 0
        try:
            # (نفترض أن x هو dict أو struct)
            if x['soil_temp'] > 35 and x['soil_hum'] < 40:
                return 1 # (Anomaly)
            else:
                return 0 # (Normal)
        except (TypeError, KeyError):
            return 0 # (Handle Nulls/Errors)

    return series.apply(check_row)
    # --- نهاية المحاكاة ---

# 3. تسجيل الدالة كـ Pandas UDF
ml_anomaly_udf = pandas_udf(predict_anomaly, returnType=IntegerType())

print("✅ ML Anomaly Detection UDF is defined.")




# ===========================
# (جديد) 1b) Write Dimension Table to DWH (One-Time)
# ===========================

location_dim_data = [
    (1, "القاهرة، مصر", "Tomatoes", 30.0444, 31.2357),
    (2, "الإسكندرية، مصر", "Cucumbers", 31.2001, 29.9187)
    # (أضف مواقعك الأخرى هنا)
]
location_dim_schema = StructType([
    StructField("location_id", IntegerType(), False), # Primary Key
    StructField("location_name", StringType(), True),
    StructField("crop_type", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True)
])


print(f"--- 🔄 Writing Dimension Table '{DIM_TABLE_NAME}' to DWH... ---")


# 1. إنشاء الـ DataFrame الثابت
location_dim_df = spark.createDataFrame(
    data=location_dim_data,
    schema=location_dim_schema
)

# 2. كتابة الجدول (سيقوم بإعادة إنشائه بالكامل)
try:
    (location_dim_df
     .write
     .jdbc(url=DB_URL,
           table=DIM_TABLE_NAME,
           mode="overwrite",# <-- هام: نستخدم overwrite لإنشائه
           properties=DB_PROPERTIES)
    )
    print(f"--- ✅ Successfully created/overwritten '{DIM_TABLE_NAME}' in DWH. ---")
except Exception as e:
    print(f"--- 🔥 Error writing Dimension Table: {e} ---")
    # (في الإنتاج، قد ترغب في إيقاف البرنامج إذا فشل)
    pass


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
# 1a) Stream-Static Join (Data Enrichment)
# ===========================

# 1. إنشاء DataFrame ثابت (Dimension Table)
# (في نظام حقيقي، ستقرأ هذا الجدول من قاعدة بيانات أو Data Lake)

dim_location_dwh = (
    spark.read
    .jdbc(url=DB_URL,
          table=DIM_TABLE_NAME,
          properties=DB_PROPERTIES)
)
print("✅ Dimension Table (dim_location) loaded from DWH for Join.")

# 2. تنفيذ الـ Join لإثراء الـ Stream
parsed_enriched_static = parsed.join(
    dim_location_dwh,
    parsed.location == dim_location_dwh.location_name,
    how="left"
).drop("location_name") # 'location' موجود بالفعل من الـ stream

print("✅ Stream-Static Join (Enrichment) is configured.")

# ===========================
# 1b) Advanced Rule-based Cleaning (تعديل 2)
# ===========================
# (أضف هذا الكود لتنظيف البيانات قبل التحويلات)
events_cleaned = (
    parsed_enriched_static  # <-- المصدر هو الـ DataFrame المخصب
    # 1. تحويل القيم غير المنطقية إلى NULL
    .withColumn("soil_temp_cleaned",
    when((col("soil_temperature_c") > 60) | (col("soil_temperature_c") < -10), lit(None))
    .otherwise(col("soil_temperature_c"))
    )
    .withColumn("soil_ph_cleaned",
        when((col("soil_ph") > 14) | (col("soil_ph") < 0), lit(None))
        .otherwise(col("soil_ph"))
    )
    # 2. استبدال الأعمدة الأصلية
    .drop("soil_temperature_c", "soil_ph")
    .withColumnRenamed("soil_temp_cleaned", "soil_temperature_c")
    .withColumnRenamed("soil_ph_cleaned", "soil_ph")
)
print("✅ Rule-based Cleaning is configured.")


# ===========================
# 1c) Per-event enrichment: flags, scores, derived cols
# ===========================
events_enriched = (
    events_cleaned
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

    .withColumn("crop_type", col("crop_type"))
    .withColumn("latitude", col("latitude"))
    .withColumn("longitude", col("longitude"))
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

events_with_window = events_enriched.alias("events").join( # <-- (1) إضافة Alias للجانب الأيسر
    agg5_compact.alias("agg"), # (2) الـ Alias للجانب الأيمن
    (col("events.location") == col("agg.location")) & # <-- (3) استخدام Alias الجانب الأيسر
    (col("events.event_ts") >= col("agg.window_start")) &
    (col("events.event_ts") < col("agg.window_end")),
    how="left"
).drop(col("agg.location"))
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
# 7b) Sessionization for 'needs_watering' events
# ===========================
# (يمكنك تغيير هذا لأي حدث، مثل 'possible_overheating')

# 1. فلترة الأحداث التي تمثل مشكلة (e.g., needs_watering)
dry_events = (
    events_enriched  # <-- نستخدم المخرجات من الخطوة 1c
    .filter(col("needs_watering") == 1)
    .select("event_ts", "location", "soil_humidity_percent")
)

# 2. تطبيق Session Window (بفجوة 15 دقيقة)
# (أي حدث "جفاف" يأتي بعد 15 دقيقة من آخر حدث، يبدأ جلسة جديدة)
dry_sessions = (
    dry_events
    .withWatermark("event_ts", "20 minutes")
    .groupBy(
        session_window(col("event_ts"), "15 minutes"),
        col("location")
    )
    .agg(
        F.avg("soil_humidity_percent").alias("avg_humidity_during_dry_session"),
        F.count("*").alias("event_count_in_session"),
        F.max("event_ts").alias("session_end_time"),
        F.min("event_ts").alias("session_start_time")
    )
    .withColumn("session_duration_minutes",
                (col("session_end_time").cast("long") - col("session_start_time").cast("long")) / 60
    )
    # فلترة الجلسات القصيرة جداً (أقل من دقيقتين مثلاً)
    .filter(col("session_duration_minutes") > 2) 
)
print("✅ Sessionization (dry_sessions) is configured.")


# (أضف هذه الدالة قبل قسم 8) Output sinks)

def write_trends_to_kafka(batch_df, batch_id):
    """
    Calculates 5-min deltas (trends) using lag() within the micro-batch
    and writes the results to Kafka.
    """
    print(f"--- 📈 Processing Trends Batch {batch_id} ---")
    
    # 1. تعريف Window.partitionBy() للـ Batch
    window_spec_lag = (
        SparkWindow
        .partitionBy("location")
        .orderBy("window_start")
    )

    # 2. حساب القيم السابقة (Lag)
    batch_with_lag = (
        batch_df
        .withColumn("prev_avg_soil_temp_5m", 
                    F.lag("avg_soil_temp_5m").over(window_spec_lag))
        .withColumn("prev_avg_soil_humidity_5m", 
                    F.lag("avg_soil_humidity_5m").over(window_spec_lag))
    )
    # 3. حساب الـ Delta (التغير)
    batch_with_delta = (
        batch_with_lag
        .withColumn("delta_temp_5m",
                    (col("avg_soil_temp_5m") - col("prev_avg_soil_temp_5m")))
        .withColumn("delta_humidity_5m",
                    (col("avg_soil_humidity_5m") - col("prev_avg_soil_humidity_5m")))
        .fillna(0.0, subset=["delta_temp_5m", "delta_humidity_5m"]) # ملء أول قيمة (Null) بـ 0
    )

    # 4. اختيار الأعمدة وتحويلها لـ JSON
    final_trends_batch = batch_with_delta.select(
        to_json(struct(
            col("window_start"),
            col("window_end"),
            col("location"),
            col("avg_soil_temp_5m"),
            col("delta_temp_5m"), # <-- الإضافة الجديدة
            col("avg_soil_humidity_5m"),
            col("delta_humidity_5m"), # <-- الإضافة الجديدة
            col("anomaly_temp_count_5m"),
            col("error_count_5m")
        )).alias("value")
    )

    # 5. كتابة الـ Batch إلى Kafka
    try:
        (final_trends_batch
         .write
         .format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
         .option("topic", OUTPUT_TRENDS_TOPIC)
         .save()) # نستخدم .save() داخل foreachBatch
        print(f"--- ✅ Trends Batch {batch_id} successfully written to Kafka ---")
    except Exception as e:
        print(f"--- 🔥 Error writing Trends Batch {batch_id} to Kafka: {e} ---")

# === نهاية الدالة ===



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

def process_top_anomalies_batch(batch_df, batch_id):
    """
    Function to process each micro-batch of aggregated anomalies
    to calculate Top-N (which is not supported directly on a stream).
    """
    print(f"\n--- Processing Top 5 Anomalies Batch: {batch_id} ---")
    
    # 1. Define window (NOW ALLOWED inside a batch)
    w_rank_batch = SparkWindow.orderBy(col("anomaly_rate_1h").desc_nulls_last())

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


FACT_TABLE_NAME = "fact_sensor_events"
SESSIONS_TABLE_NAME = "farm_dry_sessions"

def write_events_to_sql_batch(batch_df, batch_id):
    """
    Writes the fully enriched event-level data (Silver/Gold Layer)
    to the Fact Table in the DWH.
    """
    print(f"--- 🚀 Writing Fact Table Batch {batch_id} to SQL DWH ({FACT_TABLE_NAME}) ---")
    
    try:
        # 1. اختيار الأعمدة المطلوبة للـ Fact Table
        # (نختار المقاييس + المفاتيح الأجنبية)
        fact_data = batch_df.select(
            "event_ts",
            "location_id", # <-- المفتاح الأجنبي لجدول الأبعاد
            "season",
            "day_period",
            "soil_temperature_c",
            "air_temperature_c",
            "soil_humidity_percent",
            "air_humidity_percent",
            "soil_ph",
            "soil_salinity_ds_m",
            "light_intensity_lux",
            "water_level_percent",
            "env_health_score",
            "is_outlier_temp_z",
            "is_outlier_hum_z",
            "ml_anomaly_score",
            "is_sensor_error"
        )
        
        # 2. كتابة الـ Batch إلى قاعدة البيانات
        (fact_data
         .write
         .jdbc(url=DB_URL,
               table=FACT_TABLE_NAME,
               mode="append",  # إضافة البيانات الجديدة
               properties=DB_PROPERTIES)
        )
        
        print(f"--- ✅ Fact Batch {batch_id} successfully written to {FACT_TABLE_NAME} ---")
    
    except Exception as e:
        print(f"--- 🔥 Error writing Fact Batch {batch_id} to SQL: {e} ---")

# === نهاية الدالة ===

def write_sessions_to_sql_batch(batch_df, batch_id):
    """
    Writes the aggregated session data (e.g., dry spells)
    to the DWH.
    """
    print(f"--- 📊 Writing Sessions Batch {batch_id} to SQL DWH ({SESSIONS_TABLE_NAME}) ---")
    
    try:
        # 1. معالجة الـ DataFrame (استخراج أوقات البدء/النهاية)
        processed_batch_df = (
            batch_df
            .withColumn("session_start", col("session_window").start)
            .withColumn("session_end", col("session_window").end)
            .drop("session_window")
            .select(
                "session_start",
                "session_end",
                "location",
                "avg_humidity_during_dry_session",
                "event_count_in_session",
                "session_duration_minutes"
            )
        )

        # 2. كتابة الـ Batch إلى قاعدة البيانات
        (processed_batch_df
         .write
         .jdbc(url=DB_URL,
               table=SESSIONS_TABLE_NAME,
               mode="append",  # إضافة البيانات الجديدة
               properties=DB_PROPERTIES)
        )
        
        print(f"--- ✅ Sessions Batch {batch_id} successfully written to {SESSIONS_TABLE_NAME} ---")
    
    except Exception as e:
        print(f"--- 🔥 Error writing Sessions Batch {batch_id} to SQL: {e} ---")

# === نهاية الدالة ===

# ===========================
# 8) Output sinks
#    - events_outliers -> enriched events + outlier flags -> Kafka (OUTPUT_EVENTS_TOPIC)
#    - agg5_with_lag trends -> Kafka (OUTPUT_TRENDS_TOPIC)
#    - kpi_daily & kpi_weekly -> Kafka (OUTPUT_KPIS_TOPIC) & Parquet for history
#    - reliability_1h -> Parquet (sensor reliability)
#    - top5 -> console (and could be pushed to Kafka)
# ===========================


events_final_ml = events_outliers.withColumn(
    "ml_anomaly_score", ml_anomaly_udf(features_for_model)
)


# Prepare event-level JSON for Kafka (enriched + outlier flags)
events_to_kafka = events_final_ml.select(
    to_json(struct(*[c for c in events_final_ml.columns])).alias("value")
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
    events_final_ml.writeStream
    .format("delta")
    .outputMode("append")
    .option("path", f"{PARQUET_BASE_PATH}/delta_lake/all_events") # مسار الـ Data Lake
    .option("checkpointLocation", CHECKPOINT_BASE + "/events_to_delta_lake")
    .start()
)

events_sql_dwh_q = (
    events_final_ml  # <-- الـ DataFrame النهائي بعد الـ ML
    .writeStream
    .outputMode("append")
    .foreachBatch(write_events_to_sql_batch) # استدعاء الدالة الجديدة
    .option("checkpointLocation", CHECKPOINT_BASE + "/events_fact_table_sql_dwh")
    .start()
)
print("✅ Fact Table SQL DWH Sink (events_sql_dwh_q) started.")


#
# Trend insights (5m) -> include delta & labels
#trends_for_kafka = agg5_time.select(
#    to_json(struct(
#        col("window_start").alias("window_start"),
#        col("window_end").alias("window_end"),
#        col("location"),
#        col("avg_soil_temp_5m"),
#        col("avg_soil_humidity_5m"),
#        col("anomaly_temp_count_5m"),
#        col("error_count_5m")
#    )).alias("value")
#)
#
# trends_kafka_q = (
#     trends_for_kafka.writeStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
#     .option("topic", OUTPUT_TRENDS_TOPIC)
#     .option("checkpointLocation", CHECKPOINT_BASE + "/trends_to_kafka")
#     .outputMode("update")
#     .start()
# )

trends_delta_q = (
    agg5_time  # <-- مخرج التجميعات (5 دقائق)
    .writeStream
    .outputMode("update")
    .foreachBatch(write_trends_to_kafka) # استدعاء الدالة الجديدة
    .option("checkpointLocation", CHECKPOINT_BASE + "/trends_delta_kafka")
    .start()
)
print("✅ Trends (Delta) Sink (trends_delta_q) started.")

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

# (أضف هذا الكود في قسم 8) Output sinks)

# --- 8d) Sink لجلسات الجفاف (Sessionization) ---
# dry_sessions_q = (
#     dry_sessions
#     .writeStream
#     .format("console") # <-- يمكن تغييره إلى Kafka أو Parquet
#     .outputMode("update") # 'update' أو 'complete' مناسب للجلسات
#     .option("truncate", False)
#     .option("checkpointLocation", CHECKPOINT_BASE + "/dry_sessions")
#     .start()
# )
# print("✅ Dry Sessions Sink (dry_sessions_q) started.")

dry_sessions_q = (
    dry_sessions
    .writeStream
    .outputMode("append") # 'update' هو الصحيح للجلسات
    .foreachBatch(write_sessions_to_sql_batch) # <-- استدعاء دالة الـ DWH
    .option("checkpointLocation", CHECKPOINT_BASE + "/dry_sessions_sql_dwh") # <-- تغيير المسار
    .start()
)
print("✅ Dry Sessions SQL DWH Sink (dry_sessions_q) started.")

# Final: console sample for debugging events
console_events_q = (
    events_final_ml.select("event_ts", "location", "soil_temperature_c", "soil_humidity_percent",
                           "is_outlier_temp_z", "is_outlier_hum_z", "needs_watering",
                           "possible_overheating", "env_health_score","ml_anomaly_score")
    .writeStream
    .format("console")
    .option("truncate", False)
    .outputMode("append")
    .start()
)

print("Streaming started. Topics ->", OUTPUT_EVENTS_TOPIC, OUTPUT_TRENDS_TOPIC, OUTPUT_KPIS_TOPIC)
spark.streams.awaitAnyTermination()
