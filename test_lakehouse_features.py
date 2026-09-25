import os, sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

builder = SparkSession.builder \
    .appName("Delta_Features_Test") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

silver_path = "./lakehouse/silver/sales_clean_delta"

# 1. ดูประวัติ History / Commit Log ของ Delta Table
print("=== 📜 DELTA TABLE HISTORY ===")
delta_table = DeltaTable.forPath(spark, silver_path)
delta_table.history().select("version", "timestamp", "operation", "operationParameters").show(truncate=False)

# 2. ทำ Time Travel ย้อนดูข้อมูล Version 0
print("=== ⏳ TIME TRAVEL (READ VERSION 0) ===")
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(silver_path)
print(f"จำนวนแถวใน Version 0: {df_v0.count()} แถว")