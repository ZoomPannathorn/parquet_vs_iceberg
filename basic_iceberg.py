"""
basic_iceberg.py
===================
Basic PySpark + Apache Iceberg operations:
  - Create a table
  - Insert / append data
  - Read data
  - Upsert (MERGE INTO)
  - List snapshots
"""

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


PROJECT_DIR = Path(__file__).resolve().parent
JAR_PATH = PROJECT_DIR / "jars" / "iceberg-spark-runtime.jar"
WAREHOUSE_DIR = PROJECT_DIR / "warehouse"
SYSTEM_HADOOP_HOME = Path(r"C:\wh")
HADOOP_HOME = SYSTEM_HADOOP_HOME if SYSTEM_HADOOP_HOME.exists() else PROJECT_DIR / "hadoop"
JAVA_HOME = Path(r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot")

if JAVA_HOME.exists():
    os.environ["JAVA_HOME"] = str(JAVA_HOME)
    os.environ["PATH"] = f"{JAVA_HOME / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"

if HADOOP_HOME.exists():
    os.environ["HADOOP_HOME"] = str(HADOOP_HOME)
    os.environ["hadoop.home.dir"] = str(HADOOP_HOME)
    os.environ["PATH"] = f"{HADOOP_HOME / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"

if not JAR_PATH.exists():
    raise FileNotFoundError(f"Missing Iceberg runtime jar: {JAR_PATH}")

WAREHOUSE_DIR.mkdir(exist_ok=True)


spark = (
    SparkSession.builder
    .appName("Iceberg Basic Demo")
    .master("local[*]")
    .config("spark.driver.extraClassPath", str(JAR_PATH))
    .config("spark.executor.extraClassPath", str(JAR_PATH))
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE_DIR.as_uri())
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("SparkSession created with Iceberg catalog\n")


schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("order_date", StringType(), True),
])

initial_orders = [
    (1001, "C001", "Laptop", 1, 999.99, "pending", "2024-01-10"),
    (1002, "C002", "Headphones", 2, 149.99, "shipped", "2024-01-11"),
    (1003, "C003", "Keyboard", 1, 79.99, "delivered", "2024-01-12"),
    (1004, "C001", "Mouse", 1, 29.99, "pending", "2024-01-13"),
    (1005, "C004", "Monitor", 2, 399.99, "processing", "2024-01-14"),
]

df_orders = spark.createDataFrame(initial_orders, schema)

spark.sql("CREATE NAMESPACE IF NOT EXISTS local.ecommerce")
spark.sql("DROP TABLE IF EXISTS local.ecommerce.orders")

(
    df_orders.writeTo("local.ecommerce.orders")
    .partitionedBy("status")
    .tableProperty("write.format.default", "parquet")
    .create()
)

print("Iceberg table created (snapshot #1 - initial load)")
spark.sql("SELECT * FROM local.ecommerce.orders ORDER BY order_id").show()

new_orders = [
    (1006, "C005", "Webcam", 1, 89.99, "pending", "2024-01-15"),
    (1007, "C002", "USB Hub", 3, 24.99, "shipped", "2024-01-15"),
]

spark.createDataFrame(new_orders, schema).writeTo("local.ecommerce.orders").append()

print("Appended 2 new orders (snapshot #2)")
print(f"   Total rows now: {spark.table('local.ecommerce.orders').count()}\n")

spark.createDataFrame(
    [(1001, "shipped"), (1004, "shipped")],
    ["order_id", "new_status"],
).createOrReplaceTempView("status_updates")

spark.sql(
    """
    MERGE INTO local.ecommerce.orders AS t
    USING status_updates AS s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN
        UPDATE SET t.status = s.new_status
    """
)

print("MERGE INTO completed - orders 1001 and 1004 updated to 'shipped' (snapshot #3)")
spark.sql(
    """
    SELECT order_id, customer_id, product, status
    FROM local.ecommerce.orders
    WHERE order_id IN (1001, 1004)
    ORDER BY order_id
    """
).show()

print("Snapshot history:")
spark.sql(
    """
    SELECT snapshot_id, committed_at, operation
    FROM local.ecommerce.orders.snapshots
    ORDER BY committed_at
    """
).show(truncate=False)

print("Done.\n")
spark.stop()
