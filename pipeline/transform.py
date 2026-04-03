"""
Silver layer: Clean, type-cast, deduplicate, and link Bronze data.
"""
import os
import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType

os.environ["SPARK_HOME"] = "/usr/local/lib/python3.11/site-packages/pyspark"


def load_config(path: str = "/data/config/pipeline_config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver-transform")
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


def transform_customers(df: DataFrame) -> DataFrame:
    return (
        df
        # Cast types
        .withColumn("risk_score", F.col("risk_score").cast(IntegerType()))
        # Standardise date format
        .withColumn("dob", F.to_date(F.col("dob"), "yyyy-MM-dd"))
        # Deduplicate on primary key, keep first occurrence
        .dropDuplicates(["customer_id"])
        # Drop raw ingestion metadata — Silver is clean data only
        .drop("_source")
    )


def transform_accounts(df: DataFrame) -> DataFrame:
    return (
        df
        # Rename FK to match Gold layer spec
        .withColumnRenamed("customer_ref", "customer_id")
        # Cast types
        .withColumn("credit_limit",
                    F.col("credit_limit").cast(DecimalType(18, 2)))
        .withColumn("current_balance",
                    F.col("current_balance").cast(DecimalType(18, 2)))
        # Standardise dates
        .withColumn("open_date",
                    F.to_date(F.col("open_date"), "yyyy-MM-dd"))
        .withColumn("last_activity_date",
                    F.to_date(F.col("last_activity_date"), "yyyy-MM-dd"))
        # Deduplicate on primary key
        .dropDuplicates(["account_id"])
        .drop("_source")
    )


def transform_transactions(spark: SparkSession, df: DataFrame) -> DataFrame:
    # Handle merchant_subcategory — absent in Stage 1, present in Stage 2+
    # Add as null column if missing so downstream code never breaks
    if "merchant_subcategory" not in df.columns:
        df = df.withColumn("merchant_subcategory", F.lit(None).cast("string"))

    # Flatten nested location and metadata structs
    df = (
        df
        .withColumn("location_province",
                    F.col("location.province"))
        .withColumn("location_city",
                    F.col("location.city"))
        .withColumn("location_coordinates",
                    F.col("location.coordinates"))
        .withColumn("device_id",
                    F.col("metadata.device_id"))
        .withColumn("session_id",
                    F.col("metadata.session_id"))
        .withColumn("retry_flag",
                    F.col("metadata.retry_flag"))
        .drop("location", "metadata")
    )

    # Combine date + time into a single timestamp
    df = df.withColumn(
        "transaction_timestamp",
        F.to_timestamp(
            F.concat_ws(" ", F.col("transaction_date"), F.col("transaction_time")),
            "yyyy-MM-dd HH:mm:ss"
        )
    )

    # Cast amount to decimal
    df = df.withColumn("amount", F.col("amount").cast(DecimalType(18, 2)))

    # Standardise date column
    df = df.withColumn(
        "transaction_date",
        F.to_date(F.col("transaction_date"), "yyyy-MM-dd")
    )

    # Deduplicate on transaction_id — keep the record with latest timestamp
    # (defensive for Stage 2 duplicate injection)
    window = (
        __import__("pyspark.sql.window", fromlist=["Window"])
        .Window.partitionBy("transaction_id")
        .orderBy(F.col("transaction_timestamp").desc())
    )
    df = (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num", "_source")
    )

    # Add dq_flag as null for Stage 1 (no DQ issues)
    df = df.withColumn("dq_flag", F.lit(None).cast("string"))

    return df


def run_transformation():
    cfg = load_config()
    spark = get_spark()

    bronze = cfg["output"]["bronze_path"]
    silver = cfg["output"]["silver_path"]

    # --- Customers ---
    customers_df = spark.read.format("delta").load(f"{bronze}/customers")
    customers_silver = transform_customers(customers_df)
    (
        customers_silver.write
        .format("delta")
        .mode("overwrite")
        .save(f"{silver}/customers")
    )
    print(f"[Silver] customers: {customers_silver.count()} rows")

    # --- Accounts ---
    accounts_df = spark.read.format("delta").load(f"{bronze}/accounts")
    accounts_silver = transform_accounts(accounts_df)
    (
        accounts_silver.write
        .format("delta")
        .mode("overwrite")
        .save(f"{silver}/accounts")
    )
    print(f"[Silver] accounts: {accounts_silver.count()} rows")

    # --- Transactions ---
    transactions_df = spark.read.format("delta").load(f"{bronze}/transactions")
    transactions_silver = transform_transactions(spark, transactions_df)
    (
        transactions_silver.write
        .format("delta")
        .mode("overwrite")
        .save(f"{silver}/transactions")
    )
    print(f"[Silver] transactions: {transactions_silver.count()} rows")

    spark.stop()


if __name__ == "__main__":
    run_transformation()