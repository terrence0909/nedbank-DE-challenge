"""
Bronze layer: Ingest raw source data into Delta Parquet tables.
"""
import os
import yaml
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_timestamp

os.environ["SPARK_HOME"] = "/usr/local/lib/python3.11/site-packages/pyspark"


def load_config(path: str = "/data/config/pipeline_config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze-ingest")
        .master("local[2]")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.parquet.compression.codec", "uncompressed")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def ingest_csv(spark, input_path: str, output_path: str, run_timestamp: str):
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")  # keep everything as string in Bronze
        .csv(input_path)
    )
    df = df.withColumn(
        "ingestion_timestamp",
        to_timestamp(lit(run_timestamp))
    )
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(output_path)
    )
    print(f"[Bronze] {input_path}: wrote {df.count()} rows → {output_path}")


def ingest_jsonl(spark, input_path: str, output_path: str, run_timestamp: str):
    df = spark.read.json(input_path)
    df = df.withColumn(
        "ingestion_timestamp",
        to_timestamp(lit(run_timestamp))
    )
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(output_path)
    )
    print(f"[Bronze] {input_path}: wrote {df.count()} rows → {output_path}")


def run_ingestion():
    cfg = load_config()
    spark = get_spark()

    # Single timestamp for the entire ingestion run
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    ingest_csv(
        spark,
        input_path    = cfg["input"]["accounts_path"],
        output_path   = cfg["output"]["bronze_path"] + "/accounts",
        run_timestamp = run_ts,
    )
    ingest_csv(
        spark,
        input_path    = cfg["input"]["customers_path"],
        output_path   = cfg["output"]["bronze_path"] + "/customers",
        run_timestamp = run_ts,
    )
    ingest_jsonl(
        spark,
        input_path    = cfg["input"]["transactions_path"],
        output_path   = cfg["output"]["bronze_path"] + "/transactions",
        run_timestamp = run_ts,
    )

    spark.stop()


if __name__ == "__main__":
    run_ingestion()