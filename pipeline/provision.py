"""
Gold layer: Build dimensional model from Silver tables.
Produces: fact_transactions, dim_accounts, dim_customers
"""
import os
import yaml
from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType
from pyspark.sql.window import Window

os.environ["SPARK_HOME"] = "/usr/local/lib/python3.11/site-packages/pyspark"


def load_config(path: str = "/data/config/pipeline_config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("gold-provision")
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


def make_surrogate_key(col_name: str) -> F.Column:
    """
    Deterministic surrogate key via sha2 hash → cast to BIGINT.
    Stable across re-runs on the same input data (spec requirement).
    """
    return F.conv(F.sha2(F.col(col_name), 256).substr(1, 15), 16, 10) \
             .cast(LongType())


def build_dim_customers(customers_df: DataFrame) -> DataFrame:
    pipeline_run_date = date.today()

    age_band = F.when(
        F.floor(
            F.datediff(F.lit(pipeline_run_date), F.col("dob")) / 365.25
        ) >= 65, "65+"
    ).when(
        F.floor(F.datediff(F.lit(pipeline_run_date), F.col("dob")) / 365.25) >= 56, "56-65"
    ).when(
        F.floor(F.datediff(F.lit(pipeline_run_date), F.col("dob")) / 365.25) >= 46, "46-55"
    ).when(
        F.floor(F.datediff(F.lit(pipeline_run_date), F.col("dob")) / 365.25) >= 36, "36-45"
    ).when(
        F.floor(F.datediff(F.lit(pipeline_run_date), F.col("dob")) / 365.25) >= 26, "26-35"
    ).when(
        F.floor(F.datediff(F.lit(pipeline_run_date), F.col("dob")) / 365.25) >= 18, "18-25"
    ).otherwise(None)

    return (
        customers_df
        .withColumn("customer_sk", make_surrogate_key("customer_id"))
        .withColumn("age_band", age_band)
        .select(
            "customer_sk",   # 1
            "customer_id",   # 2
            "gender",        # 3
            "province",      # 4
            "income_band",   # 5
            "segment",       # 6
            "risk_score",    # 7
            "kyc_status",    # 8
            "age_band",      # 9
        )
    )


def build_dim_accounts(accounts_df: DataFrame) -> DataFrame:
    # customer_ref was already renamed to customer_id in Silver
    return (
        accounts_df
        .withColumn("account_sk", make_surrogate_key("account_id"))
        .select(
            "account_sk",        # 1
            "account_id",        # 2
            "customer_id",       # 3  ← GAP-026: required for validation query 2
            "account_type",      # 4
            "account_status",    # 5
            "open_date",         # 6
            "product_tier",      # 7
            "digital_channel",   # 8
            "credit_limit",      # 9
            "current_balance",   # 10
            "last_activity_date" # 11
        )
    )


def build_fact_transactions(
    transactions_df: DataFrame,
    dim_accounts: DataFrame,
    dim_customers: DataFrame,
) -> DataFrame:

    # Bring in account_sk and customer_id via account_id join
    account_lookup = dim_accounts.select(
        "account_id", "account_sk", "customer_id"
    )

    # Bring in customer_sk via customer_id join
    customer_lookup = dim_customers.select(
        "customer_id", "customer_sk"
    )

    fact = (
        transactions_df
        # Join to get account_sk and customer_id
        .join(account_lookup, on="account_id", how="inner")
        # Join to get customer_sk
        .join(customer_lookup, on="customer_id", how="inner")
        # Surrogate key for this fact row
        .withColumn("transaction_sk", make_surrogate_key("transaction_id"))
        # Standardise currency — Stage 1 is always ZAR but be defensive
        .withColumn(
            "currency",
            F.when(
                F.upper(F.col("currency")).isin(
                    ["ZAR", "R", "RANDS", "710", "ZAR"]
                ),
                F.lit("ZAR")
            ).otherwise(F.col("currency"))
        )
        .select(
            "transaction_sk",        # 1
            "transaction_id",        # 2
            "account_sk",            # 3
            "customer_sk",           # 4
            "transaction_date",      # 5
            "transaction_timestamp", # 6
            "transaction_type",      # 7
            "merchant_category",     # 8
            "merchant_subcategory",  # 9  ← null in Stage 1, fine
            "amount",                # 10
            "currency",              # 11
            "channel",               # 12
            F.col("location_province").alias("province"),  # 13
            "dq_flag",               # 14
            "ingestion_timestamp",   # 15
        )
    )

    return fact


def run_provisioning():
    cfg = load_config()
    spark = get_spark()

    silver = cfg["output"]["silver_path"]
    gold   = cfg["output"]["gold_path"]

    # Load Silver tables
    customers_df    = spark.read.format("delta").load(f"{silver}/customers")
    accounts_df     = spark.read.format("delta").load(f"{silver}/accounts")
    transactions_df = spark.read.format("delta").load(f"{silver}/transactions")

    # Build dims first — facts depend on their surrogate keys
    dim_customers = build_dim_customers(customers_df)
    dim_accounts  = build_dim_accounts(accounts_df)
    fact_transactions = build_fact_transactions(
        transactions_df, dim_accounts, dim_customers
    )

    # Write Gold tables
    (
        dim_customers.write
        .format("delta").mode("overwrite")
        .save(f"{gold}/dim_customers")
    )
    print(f"[Gold] dim_customers: {dim_customers.count()} rows")

    (
        dim_accounts.write
        .format("delta").mode("overwrite")
        .save(f"{gold}/dim_accounts")
    )
    print(f"[Gold] dim_accounts: {dim_accounts.count()} rows")

    (
        fact_transactions.write
        .format("delta").mode("overwrite")
        .save(f"{gold}/fact_transactions")
    )
    print(f"[Gold] fact_transactions: {fact_transactions.count()} rows")

    spark.stop()


if __name__ == "__main__":
    run_provisioning()