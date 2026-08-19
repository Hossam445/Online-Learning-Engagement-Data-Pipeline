from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType)

# HDFS paths (namenode is the Hadoop service name in docker-compose).
RAW_HDFS = "hdfs://namenode:8020/data/online-learning/raw/"
OUTPUT_HDFS = "hdfs://namenode:8020/output/online-learning/"

# 1. Create the Spark session.
spark = SparkSession.builder.appName("OnlineLearningAnalysis").getOrCreate()

# Schema of the Online Learning Engagement Dataset (18 columns).
# Flume ingested the CSV without its header row, so header=False + explicit schema.
schema = StructType([
    StructField("student_id", IntegerType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("country", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("internet_speed_mbps", DoubleType(), True),
    StructField("study_hours_weekly", DoubleType(), True),
    StructField("login_frequency_weekly", IntegerType(), True),
    StructField("avg_session_duration_min", DoubleType(), True),
    StructField("video_watch_time_min", DoubleType(), True),
    StructField("assignments_submitted", IntegerType(), True),
    StructField("forum_posts", IntegerType(), True),
    StructField("quiz_attempts", IntegerType(), True),
    StructField("avg_quiz_score", DoubleType(), True),
    StructField("attendance_rate", DoubleType(), True),
    StructField("engagement_score", DoubleType(), True),
    StructField("final_grade", DoubleType(), True),
    StructField("dropout", IntegerType(), True),
])

# 2. Read the data ingested by Flume from HDFS.
print("Reading raw data from HDFS:", RAW_HDFS)
df = spark.read.option("header", False).schema(schema).csv(RAW_HDFS)
print("Loaded {} student records.".format(df.count()))
df.show(5)

# 3. Analysis 1 - Average engagement score by device type.
device_analysis = (
    df.groupBy("device_type")
      .agg(F.round(F.avg("engagement_score"), 2).alias("avg_engagement_score"))
      .orderBy(F.desc("avg_engagement_score"))
)
print("=== Analysis 1: Average engagement score by device type ===")
device_analysis.show()
device_analysis.write.mode("overwrite").parquet(OUTPUT_HDFS + "device_analysis")

# 4. Analysis 2 - Average final grade by country.
country_analysis = (
    df.groupBy("country")
      .agg(F.round(F.avg("final_grade"), 2).alias("avg_final_grade"))
      .orderBy(F.desc("avg_final_grade"))
)
print("=== Analysis 2: Average final grade by country ===")
country_analysis.show()
country_analysis.write.mode("overwrite").parquet(OUTPUT_HDFS + "country_analysis")

# 5. Analysis 3 - Study hours (weekly) vs average engagement.
#    Bin students into low (<5h), medium (5-15h) and high (>=15h) study groups.
study_engagement = (
    df.withColumn(
        "study_group",
        F.when(F.col("study_hours_weekly") < 5, "low (<5h)")
         .when(F.col("study_hours_weekly") < 15, "medium (5-15h)")
         .otherwise("high (>=15h)"),
    )
    .groupBy("study_group")
    .agg(
        F.round(F.avg("engagement_score"), 2).alias("avg_engagement_score"),
        F.count("*").alias("students"),
    )
    .orderBy(F.desc("avg_engagement_score"))
)
print("=== Analysis 3: Study hours (weekly) vs average engagement ===")
study_engagement.show()
study_engagement.write.mode("overwrite").parquet(OUTPUT_HDFS + "study_engagement")

# 6. Analysis 4 - Student activity summary.
student_activity = (
    df.select(
        "student_id",
        "login_frequency_weekly",
        "avg_session_duration_min",
        "video_watch_time_min",
        "assignments_submitted",
        "forum_posts",
    )
    .orderBy("student_id")
)
print("=== Analysis 4: Student activity summary ===")
student_activity.show(10)
student_activity.write.mode("overwrite").parquet(OUTPUT_HDFS + "student_activity")

print("All analyses written to HDFS under:", OUTPUT_HDFS)

# 7. Close the Spark session.
spark.stop()