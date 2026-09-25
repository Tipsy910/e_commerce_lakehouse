# 🌊 Online Retail Data Lakehouse (Medallion Architecture)

โปรเจกต์ Data Lakehouse แบบ Medallion Architecture (Bronze -> Silver -> Gold) สำหรับประมวลผลข้อมูล E-Commerce โดยใช้ PySpark ร่วมกับ ucimlrepo และส่งออกผลลัพธ์ไปทำ Dashboard บน Apache Superset

## 🏗️ Architecture Design

1. **Data Source:** UCI Machine Learning Repository (`Online Retail`, ID: 352)
2. **🥉 Bronze Layer:** ดึงข้อมูลดิบผ่าน `ucimlrepo` API และนำเข้าสู่ Spark DataFrame
3. **🥈 Silver Layer:** ทำ Data Cleaning (กรอง Null/Value ติดลบ, แปลง Timestamp, สร้างคอลัมน์ `TotalAmount`)
4. **🥇 Gold Layer:** คำนวณ Business Aggregations (ยอดขายรายประเทศ & 5 อันดับสินค้าขายดี) และ Export เป็น CSV
5. **📊 Visualization:** นำเสนอผลลัพธ์ผ่าน Apache Superset Dashboard

## 🛠️ Tech Stack

* **Language:** Python 3.12+
* **Engine:** PySpark
* **Data Ingestion:** `ucimlrepo`, `pandas`
* **Package Manager:** `uv`
* **BI Tool:** Apache Superset

## 🚀 How to Run

1. ติดตั้ง Dependencies ด้วย `uv`:
   ```bash
   uv sync
2. รัน Medallion Pipeline:
python pipeline_medallion.py
