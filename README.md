# Parquet vs Iceberg Lab

A simple data engineering lab project demonstrating the key differences between traditional **Apache Parquet** file operations and **Apache Iceberg** table format operations using **PySpark**.

This project is designed for learning modern data lakehouse concepts and understanding why Iceberg is useful for handling updates, deletes, and table versioning.

---

## Project Summary

This lab compares two approaches for storing and managing data:

### 1. Apache Parquet
A traditional columnar file format.

In this demo:
- Load healthcare dataset into Spark
- Write data to Parquet
- Simulate delete by filtering rows and overwriting the entire dataset

### 2. Apache Iceberg
A modern table format for data lakes.

In this demo:
- Load healthcare dataset into Spark
- Insert data into Iceberg table
- Delete records using SQL DELETE
- Track snapshots/history automatically

---

## File Descriptions
## basic_iceberg.py

Demonstrates core Iceberg features:

Create SparkSession with Iceberg catalog
Create Iceberg table
Insert initial records
Append new records
Update records using MERGE INTO
View snapshot history
<img width="877" height="646" alt="image" src="https://github.com/user-attachments/assets/b7422b92-2c5b-4c12-9f00-5b9c1599bb13" />

## parquet_vs_iceberg.py
Compares Parquet and Iceberg side-by-side.

Parquet Flow
Load CSV into Spark DataFrame
Save as Parquet
Delete rows by filtering
Overwrite Parquet files
<img width="1917" height="875" alt="image" src="https://github.com/user-attachments/assets/4e4c6bfb-10b3-4dab-aa73-a545886836ab" />
<img width="1918" height="855" alt="image" src="https://github.com/user-attachments/assets/8809a62e-b35a-4707-898f-145d469f1977" />

---

## Technologies Used

- Python
- PySpark
- Apache Spark
- Apache Parquet
- Apache Iceberg
- Java
- SQL

---

## Project Structure

```bash
iceberg/
│
├── healthcare_dataset.csv
├── basic_iceberg.py
├── parquet_vs_iceberg.py
├── jars/
│   └── iceberg-spark-runtime.jar
└── README.md
