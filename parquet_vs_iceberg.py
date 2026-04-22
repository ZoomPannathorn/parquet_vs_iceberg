import os
import shutil
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


PROJECT_DIR = Path(__file__).resolve().parent
DATA_PATH = PROJECT_DIR / "healthcare_dataset.csv"
PARQUET_PATH = PROJECT_DIR / "parquet_output.parquet"
PARQUET_TMP_PATH = PROJECT_DIR / "parquet_output_tmp.parquet"
WAREHOUSE_DIR = PROJECT_DIR / "warehouse"
JAR_PATH = PROJECT_DIR / "jars" / "iceberg-spark-runtime.jar"
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

if not DATA_PATH.exists():
    raise FileNotFoundError(f"Missing dataset: {DATA_PATH}")

if not JAR_PATH.exists():
    raise FileNotFoundError(f"Missing Iceberg runtime jar: {JAR_PATH}")

WAREHOUSE_DIR.mkdir(exist_ok=True)


spark = (
    SparkSession.builder
    .appName("Parquet vs Iceberg")
    .master("local[*]")
    .config("spark.driver.extraClassPath", str(JAR_PATH))
    .config("spark.executor.extraClassPath", str(JAR_PATH))
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE_DIR.as_uri())
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n========== LOAD DATA ==========")
df = spark.read.csv(str(DATA_PATH), header=True, inferSchema=True)
df.show(5)

print("\n========== PARQUET ==========")
pandas_df = pd.read_csv(DATA_PATH)

legacy_parquet_dir = PROJECT_DIR / "parquet_output"
legacy_parquet_tmp_dir = PROJECT_DIR / "parquet_output_tmp"
if legacy_parquet_dir.exists():
    shutil.rmtree(legacy_parquet_dir)
if legacy_parquet_tmp_dir.exists():
    shutil.rmtree(legacy_parquet_tmp_dir)
if PARQUET_PATH.exists():
    PARQUET_PATH.unlink()
if PARQUET_TMP_PATH.exists():
    PARQUET_TMP_PATH.unlink()

pandas_df.to_parquet(PARQUET_PATH, index=False)

print("Initial Parquet Data:")
print(pd.read_parquet(PARQUET_PATH).head())

df_deleted = pandas_df[pandas_df["Age"] > 30]

df_deleted.to_parquet(PARQUET_TMP_PATH, index=False)

if PARQUET_PATH.exists():
    PARQUET_PATH.unlink()

PARQUET_TMP_PATH.replace(PARQUET_PATH)

print("After Delete in Parquet:")
print(pd.read_parquet(PARQUET_PATH).head())

print("\n========== ICEBERG ==========")
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
spark.sql("DROP TABLE IF EXISTS local.db.healthcare")

df.writeTo("local.db.healthcare").create()

print("Initial Iceberg Data:")
spark.sql("SELECT * FROM local.db.healthcare LIMIT 5").show()

spark.sql("DELETE FROM local.db.healthcare WHERE Age <= 30")

print("After Delete in Iceberg:")
spark.sql("SELECT * FROM local.db.healthcare LIMIT 5").show()

spark.stop()
