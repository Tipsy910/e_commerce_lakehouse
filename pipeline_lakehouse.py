import pandas as pd
from ucimlrepo import fetch_ucirepo
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, round, sum as _sum, count
import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
print("Using python:", sys.executable)
# ---------------------------------------------------------
# 0. ดึงข้อมูลตรงจาก UCI Repository
# ---------------------------------------------------------
print("กำลังดึงข้อมูล Online Retail จาก UCI Repository...")
online_retail = fetch_ucirepo(id=352)

# .data.features ไม่รวมคอลัมน์ ID (InvoiceNo, StockCode)
# ต้องรวมเข้ากับ .data.ids ก่อน ถึงจะได้ทุกคอลัมน์ที่ต้องใช้
pdf_raw = pd.concat([online_retail.data.ids, online_retail.data.features], axis=1)

print("คอลัมน์ทั้งหมดที่ได้:", list(pdf_raw.columns))

# สุ่มดึงมา 5,000 แถวเพื่อความรวดเร็วในการทดสอบ
pdf_sample = pdf_raw.sample(n=5000, random_state=42)

# ---------------------------------------------------------
# 1. สร้าง SparkSession
# ---------------------------------------------------------
spark = SparkSession.builder \
    .appName("ECommerce_Medallion_Pipeline") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# ---------------------------------------------------------
# 🥉 BRONZE LAYER (Raw Ingestion)
# ---------------------------------------------------------
print("\n=== 🥉 BRONZE LAYER ===")
# แปลง Pandas DataFrame เป็น Spark DataFrame
df_bronze = spark.createDataFrame(pdf_sample)
print(f"จำนวนแถวข้อมูลดิบใน Bronze: {df_bronze.count()} แถว")
df_bronze.printSchema()

# ---------------------------------------------------------
# 🥈 SILVER LAYER (Data Cleaning & Transformation)
# ---------------------------------------------------------
print("\n=== 🥈 SILVER LAYER ===")
df_silver = df_bronze \
    .filter(col("CustomerID").isNotNull()) \
    .filter((col("Quantity") > 0) & (col("UnitPrice") > 0)) \
    .withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "M/d/yyyy H:m")) \
    .withColumn("TotalAmount", round(col("Quantity") * col("UnitPrice"), 2))

print(f"จำนวนแถวหลังทำ Cleaning ใน Silver: {df_silver.count()} แถว")
df_silver.select("InvoiceNo", "StockCode", "Quantity", "UnitPrice", "TotalAmount", "CustomerID").show(5)

# ---------------------------------------------------------
# 🥇 GOLD LAYER (Business Aggregations / Metrics)
# ---------------------------------------------------------
print("\n=== 🥇 GOLD LAYER ===")

# ตัวอย่างที่ 1: สรุปยอดขายและจำนวนออเดอร์รายประเทศ (Country Sales Summary)
df_gold_country = df_silver.groupBy("Country") \
    .agg(
        round(_sum("TotalAmount"), 2).alias("TotalRevenue"),
        count("InvoiceNo").alias("TotalOrders")
    ) \
    .orderBy(col("TotalRevenue").desc())

print("--- สรุปยอดขายรายประเทศ (Top Countries) ---")
df_gold_country.show(5)

# ตัวอย่างที่ 2: สินค้าขายดี 5 อันดับแรก (Top Best-Selling Products)
df_gold_top_products = df_silver.groupBy("StockCode", "Description") \
    .agg(
        _sum("Quantity").alias("TotalQuantitySold"),
        round(_sum("TotalAmount"), 2).alias("TotalRevenue")
    ) \
    .orderBy(col("TotalQuantitySold").desc())

print("--- 5 อันดับสินค้าขายดีที่สุด ---")
df_gold_top_products.show(5)

print("\nทำ Medallion Pipeline สำเร็จครบทุก Layer เรียบร้อยแล้วครับ! 🎉")

print("\n=== 📤 EXPORTING GOLD LAYER TO CSV ===")

# สร้างโฟลเดอร์สำหรับเก็บผลลัพธ์ Gold Layer
output_dir = "./lakehouse/gold"
os.makedirs(output_dir, exist_ok=True)

# 1. Export สรุปยอดขายรายประเทศ
country_csv_path = os.path.join(output_dir, "gold_country_sales.csv")
df_gold_country.toPandas().to_csv(country_csv_path, index=False)
print(f"บันทึกไฟล์: {country_csv_path}")

# 2. Export 5 อันดับสินค้าขายดี
top_products_csv_path = os.path.join(output_dir, "gold_top_products.csv")
df_gold_top_products.toPandas().to_csv(top_products_csv_path, index=False)
print(f"บันทึกไฟล์: {top_products_csv_path}")