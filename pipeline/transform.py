"""
Silver layer: Clean, type-cast, deduplicate, and link Bronze data.
Stage 2: Full DQ handling with config-driven rules and dq_report.json
"""
import os
import yaml
import json
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType
from pyspark.sql.window import Window

os.environ["SPARK_HOME"] = "/usr/local/lib/python3.11/site-packages/pyspark"


def load_config() -> dict:
    """Load pipeline configuration from YAML"""
    possible_paths = [
        "/data/config/pipeline_config.yaml",
        "/app/config/pipeline_config.yaml",
        "config/pipeline_config.yaml"
    ]
    for path in possible_paths:
        if os.path.exists(path):
            with open(path) as f:
                return yaml.safe_load(f)
    raise FileNotFoundError("pipeline_config.yaml not found")


def load_dq_rules() -> dict:
    """Load DQ rules from YAML file"""
    possible_paths = [
        "/data/config/dq_rules.yaml",
        "/app/config/dq_rules.yaml",
        "config/dq_rules.yaml"
    ]
    for path in possible_paths:
        if os.path.exists(path):
            with open(path) as f:
                return yaml.safe_load(f)
    raise FileNotFoundError("dq_rules.yaml not found")


def get_spark() -> SparkSession:
    """Get or create Spark session with optimized settings for 2GB constraint"""
    return (
        SparkSession.builder
        .appName("silver-transform")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.parquet.compression.codec", "uncompressed")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )


def normalize_currency(df: DataFrame, rules: dict) -> DataFrame:
    """Normalize currency variants to ZAR"""
    txn_rules = rules.get("transactions", {})
    currency_rules = txn_rules.get("currency_normalization", {})
    target = currency_rules.get("target", "ZAR")
    variants = currency_rules.get("variants", [])
    dq_code = currency_rules.get("dq_code", "CURRENCY_VARIANT")
    
    if not variants:
        return df
    
    # Build mapping
    for variant in variants:
        df = df.withColumn(
            "currency",
            F.when(F.col("currency") == variant, target).otherwise(F.col("currency"))
        )
    
    # Add DQ flag for variants
    df = df.withColumn(
        "dq_currency_variant",
        F.when(F.col("currency").isin(variants), F.lit(dq_code))
    )
    
    return df


def parse_dates(df: DataFrame, rules: dict, table_name: str) -> DataFrame:
    """Parse multiple date formats"""
    table_rules = rules.get(table_name, {})
    date_checks = table_rules.get("date_format_checks", [])
    
    for check in date_checks:
        col_name = check.get("column") if isinstance(check, dict) else check
        formats = check.get("formats", ["yyyy-MM-dd"]) if isinstance(check, dict) else ["yyyy-MM-dd"]
        dq_code = check.get("dq_code", "DATE_FORMAT") if isinstance(check, dict) else "DATE_FORMAT"
        
        if col_name not in df.columns:
            continue
        
        parsed_col = None
        for fmt in formats:
            if parsed_col is None:
                parsed_col = F.to_date(F.col(col_name), fmt)
            else:
                parsed_col = F.when(parsed_col.isNull(), F.to_date(F.col(col_name), fmt)).otherwise(parsed_col)
        
        df = df.withColumn(f"{col_name}_parsed", parsed_col)
        df = df.withColumn(
            col_name,
            F.coalesce(F.col(f"{col_name}_parsed"), F.col(col_name))
        ).drop(f"{col_name}_parsed")
        
        df = df.withColumn(
            f"dq_date_{col_name}",
            F.when(parsed_col.isNull() & F.col(col_name).isNotNull(), F.lit(dq_code))
        )
    
    return df


def apply_null_checks(df: DataFrame, rules: dict, table_name: str) -> DataFrame:
    """Apply null checks from rules"""
    table_rules = rules.get(table_name, {})
    null_checks = table_rules.get("null_checks", [])
    
    for check in null_checks:
        column = check["column"]
        dq_code = check.get("dq_code", "NULL_REQUIRED")
        
        df = df.withColumn(
            f"dq_null_{column}",
            F.when(F.col(column).isNull(), F.lit(dq_code))
        )
    
    return df


def apply_type_checks(df: DataFrame, rules: dict, table_name: str) -> DataFrame:
    """Apply type checks from rules"""
    table_rules = rules.get(table_name, {})
    type_checks = table_rules.get("type_checks", {})
    
    for column, check in type_checks.items():
        if column not in df.columns:
            continue
        
        expected = check.get("expected_type")
        dq_code = check.get("dq_code", "TYPE_MISMATCH")
        
        if expected == "decimal":
            original = F.col(column)
            df = df.withColumn(f"{column}_casted", F.col(column).cast(DecimalType(18, 2)))
            df = df.withColumn(
                column,
                F.coalesce(F.col(f"{column}_casted"), original)
            ).drop(f"{column}_casted")
            
            df = df.withColumn(
                f"dq_type_{column}",
                F.when(F.col(column).isNull() & original.isNotNull(), F.lit(dq_code))
            )
        
        elif expected == "integer":
            original = F.col(column)
            df = df.withColumn(column, F.col(column).cast(IntegerType()))
            df = df.withColumn(
                f"dq_type_{column}",
                F.when(F.col(column).isNull() & original.isNotNull(), F.lit(dq_code))
            )
    
    return df


def apply_domain_checks(df: DataFrame, rules: dict, table_name: str) -> DataFrame:
    """Apply domain/allowed values checks"""
    table_rules = rules.get(table_name, {})
    domain_checks = table_rules.get("domain_checks", [])
    
    for check in domain_checks:
        column = check.get("column")
        allowed = check.get("allowed", [])
        dq_code = check.get("dq_code", "INVALID_VALUE")
        
        if column in df.columns and allowed:
            df = df.withColumn(
                f"dq_domain_{column}",
                F.when(~F.col(column).isin(allowed), F.lit(dq_code))
            )
    
    return df


def handle_duplicates(df: DataFrame, rules: dict, table_name: str) -> tuple:
    """Deduplicate based on rules"""
    table_rules = rules.get(table_name, {})
    dup_rules = table_rules.get("duplicate_checks", {})
    
    if not dup_rules:
        return df, 0
    
    keys = dup_rules.get("keys", [])
    keep = dup_rules.get("keep", "first")
    
    if not keys:
        return df, 0
    
    original_count = df.count()
    
    if keep == "latest" and "transaction_timestamp" in df.columns:
        window = Window.partitionBy(*keys).orderBy(F.col("transaction_timestamp").desc())
    else:
        window = Window.partitionBy(*keys).orderBy(F.col(keys[0]).asc())
    
    df = df.withColumn("_row_num", F.row_number().over(window))
    df_deduped = df.filter(F.col("_row_num") == 1).drop("_row_num")
    
    duplicates_removed = original_count - df_deduped.count()
    
    return df_deduped, duplicates_removed


def flag_orphaned_transactions(df: DataFrame, accounts_df: DataFrame, rules: dict) -> DataFrame:
    """Flag transactions with orphaned account_id"""
    txn_rules = rules.get("transactions", {})
    orphaned_rules = txn_rules.get("orphaned_checks", {})
    dq_code = orphaned_rules.get("dq_code", "ORPHANED_ACCOUNT")
    
    if not orphaned_rules:
        return df
    
    valid_accounts = accounts_df.select("account_id").distinct()
    valid_accounts = valid_accounts.withColumn("valid_account", F.lit(True))
    
    df = df.join(valid_accounts, on="account_id", how="left")
    df = df.withColumn(
        "dq_orphaned",
        F.when(F.col("valid_account").isNull(), F.lit(dq_code))
    ).drop("valid_account")
    
    return df


def combine_dq_flags(df: DataFrame) -> DataFrame:
    """Combine all dq_* columns into a single JSON dq_flag column"""
    dq_columns = [c for c in df.columns if c.startswith("dq_")]
    
    if not dq_columns:
        return df.withColumn("dq_flag", F.lit(None).cast("string"))
    
    # Build JSON with all non-null violations
    dq_struct = F.struct(*[F.col(c).alias(c.replace("dq_", "")) for c in dq_columns])
    df = df.withColumn("dq_violations", F.to_json(dq_struct))
    
    has_violation = F.greatest(*[F.col(c).isNotNull().cast("int") for c in dq_columns]) == 1
    df = df.withColumn(
        "dq_flag",
        F.when(has_violation, F.col("dq_violations")).otherwise(F.lit(None))
    ).drop("dq_violations")
    
    for col in dq_columns:
        df = df.drop(col)
    
    return df


def transform_customers(df: DataFrame, rules: dict) -> DataFrame:
    """Transform customers with all DQ checks"""
    result = df.dropDuplicates(["customer_id"]).drop("_source")
    
    result = apply_type_checks(result, rules, "customers")
    result = parse_dates(result, rules, "customers")
    result = apply_null_checks(result, rules, "customers")
    result = apply_domain_checks(result, rules, "customers")
    result = combine_dq_flags(result)
    
    return result


def transform_accounts(df: DataFrame, rules: dict) -> tuple:
    """Transform accounts with all DQ checks"""
    result = (
        df
        .withColumnRenamed("customer_ref", "customer_id")
        .dropDuplicates(["account_id"])
        .drop("_source")
    )
    
    result = apply_type_checks(result, rules, "accounts")
    result = parse_dates(result, rules, "accounts")
    result = apply_null_checks(result, rules, "accounts")
    
    null_accounts_count = result.filter(F.col("account_id").isNull()).count()
    result_clean = result.filter(F.col("account_id").isNotNull())
    result_clean = combine_dq_flags(result_clean)
    
    return result_clean, null_accounts_count


def transform_transactions(df: DataFrame, accounts_df: DataFrame, rules: dict) -> DataFrame:
    """Transform transactions with all DQ checks"""
    # Handle merchant_subcategory (new in Stage 2)
    if "merchant_subcategory" not in df.columns:
        df = df.withColumn("merchant_subcategory", F.lit(None).cast("string"))
    
    # Flatten nested structs
    if "location" in df.columns:
        df = df.withColumn("location_province", F.col("location.province")) \
               .withColumn("location_city", F.col("location.city")) \
               .withColumn("location_coordinates", F.col("location.coordinates")) \
               .drop("location")
    
    if "metadata" in df.columns:
        df = df.withColumn("device_id", F.col("metadata.device_id")) \
               .withColumn("session_id", F.col("metadata.session_id")) \
               .withColumn("retry_flag", F.col("metadata.retry_flag")) \
               .drop("metadata")
    
    # Apply transformations
    df = normalize_currency(df, rules)
    df = parse_dates(df, rules, "transactions")
    
    # Build transaction timestamp
    df = df.withColumn(
        "transaction_timestamp",
        F.to_timestamp(F.concat_ws(" ", F.col("transaction_date"), F.col("transaction_time")), "yyyy-MM-dd HH:mm:ss")
    )
    
    df = apply_type_checks(df, rules, "transactions")
    df, duplicates_removed = handle_duplicates(df, rules, "transactions")
    df = flag_orphaned_transactions(df, accounts_df, rules)
    df = apply_null_checks(df, rules, "transactions")
    df = combine_dq_flags(df)
    
    df._duplicates_removed = duplicates_removed
    return df


def generate_dq_report(silver_data: dict, start_time: datetime, rules: dict) -> dict:
    """Generate dq_report.json matching the required schema"""
    
    def calc_pct(affected, total):
        if total == 0:
            return 0.00
        return round((affected / total) * 100, 2)
    
    raw_txn = silver_data["transactions_raw"]
    raw_acc = silver_data["accounts_raw"]
    raw_cust = silver_data["customers_raw"]
    txn_df = silver_data.get("transactions_df")
    
    def count_issue(issue_code):
        if issue_code == "DUPLICATE_DEDUPED":
            return silver_data.get("duplicates_removed", 0)
        if txn_df is not None:
            return txn_df.filter(F.col("dq_flag").contains(issue_code)).count()
        return 0
    
    dq_issues = []
    
    dup_count = silver_data.get("duplicates_removed", 0)
    if dup_count > 0:
        dq_issues.append({
            "issue_type": "DUPLICATE_DEDUPED",
            "records_affected": dup_count,
            "percentage_of_total": calc_pct(dup_count, raw_txn),
            "handling_action": "DEDUPLICATED_KEEP_LATEST",
            "records_in_output": silver_data["transactions_count"]
        })
    
    orphaned_count = count_issue("ORPHANED_ACCOUNT")
    if orphaned_count > 0:
        dq_issues.append({
            "issue_type": "ORPHANED_ACCOUNT",
            "records_affected": orphaned_count,
            "percentage_of_total": calc_pct(orphaned_count, raw_txn),
            "handling_action": "QUARANTINED",
            "records_in_output": 0
        })
    
    type_count = count_issue("TYPE_MISMATCH")
    if type_count > 0:
        dq_issues.append({
            "issue_type": "TYPE_MISMATCH",
            "records_affected": type_count,
            "percentage_of_total": calc_pct(type_count, raw_txn),
            "handling_action": "CAST_TO_DECIMAL",
            "records_in_output": type_count
        })
    
    date_count = count_issue("DATE_FORMAT")
    if date_count > 0:
        dq_issues.append({
            "issue_type": "DATE_FORMAT",
            "records_affected": date_count,
            "percentage_of_total": calc_pct(date_count, raw_txn),
            "handling_action": "NORMALISED_DATE",
            "records_in_output": date_count
        })
    
    currency_count = count_issue("CURRENCY_VARIANT")
    if currency_count > 0:
        dq_issues.append({
            "issue_type": "CURRENCY_VARIANT",
            "records_affected": currency_count,
            "percentage_of_total": calc_pct(currency_count, raw_txn),
            "handling_action": "NORMALISED_CURRENCY",
            "records_in_output": currency_count
        })
    
    null_acc_count = silver_data.get("null_accounts_quarantined", 0)
    if null_acc_count > 0:
        dq_issues.append({
            "issue_type": "NULL_ACCOUNT_ID",
            "records_affected": null_acc_count,
            "percentage_of_total": calc_pct(null_acc_count, raw_acc),
            "handling_action": "EXCLUDED_NULL_PK",
            "records_in_output": 0
        })
    
    return {
        "$schema": "nedbank-de-challenge/dq-report/v1",
        "run_timestamp": start_time.isoformat() + "Z",
        "stage": "2",
        "source_record_counts": {
            "accounts_raw": raw_acc,
            "transactions_raw": raw_txn,
            "customers_raw": raw_cust
        },
        "dq_issues": dq_issues,
        "gold_layer_record_counts": {
            "fact_transactions": silver_data["transactions_count"],
            "dim_accounts": silver_data["accounts_count"],
            "dim_customers": silver_data["customers_count"]
        },
        "execution_duration_seconds": silver_data.get("duration_seconds", 0)
    }


def run_transformation():
    """Main transformation orchestration"""
    cfg = load_config()
    dq_rules = load_dq_rules()
    spark = get_spark()
    start_time = datetime.utcnow()
    
    bronze = cfg["output"]["bronze_path"]
    silver = cfg["output"]["silver_path"]
    dq_report_path = cfg["output"]["dq_report_path"]
    
    # Read Bronze tables
    customers_df = spark.read.format("delta").load(f"{bronze}/customers")
    accounts_df = spark.read.format("delta").load(f"{bronze}/accounts")
    transactions_df = spark.read.format("delta").load(f"{bronze}/transactions")
    
    # Raw counts
    raw_customers = customers_df.count()
    raw_accounts = accounts_df.count()
    raw_transactions = transactions_df.count()
    
    print(f"[Silver] Raw - customers: {raw_customers}, accounts: {raw_accounts}, transactions: {raw_transactions}")
    
    # Transform
    customers_silver = transform_customers(customers_df, dq_rules)
    accounts_silver, null_accounts_count = transform_accounts(accounts_df, dq_rules)
    transactions_silver = transform_transactions(transactions_df, accounts_df, dq_rules)
    
    # Final counts
    cust_count = customers_silver.count()
    acc_count = accounts_silver.count()
    txn_count = transactions_silver.count()
    duplicates_removed = getattr(transactions_silver, '_duplicates_removed', 0)
    
    # Write Silver tables
    customers_silver.write.format("delta").mode("overwrite").save(f"{silver}/customers")
    accounts_silver.write.format("delta").mode("overwrite").save(f"{silver}/accounts")
    transactions_silver.write.format("delta").mode("overwrite").save(f"{silver}/transactions")
    
    print(f"[Silver] Output - customers: {cust_count}, accounts: {acc_count}, transactions: {txn_count}")
    print(f"[Silver] Stats - quarantined accounts: {null_accounts_count}, duplicates removed: {duplicates_removed}")
    
    # Generate report
    duration_seconds = int((datetime.utcnow() - start_time).total_seconds())
    silver_data = {
        "customers_raw": raw_customers,
        "accounts_raw": raw_accounts,
        "transactions_raw": raw_transactions,
        "customers_count": cust_count,
        "accounts_count": acc_count,
        "transactions_count": txn_count,
        "transactions_df": transactions_silver,
        "duplicates_removed": duplicates_removed,
        "null_accounts_quarantined": null_accounts_count,
        "duration_seconds": duration_seconds
    }
    
    report = generate_dq_report(silver_data, start_time, dq_rules)
    
    with open(dq_report_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"[Silver] DQ report written to {dq_report_path}")
    print(f"[Silver] Complete in {duration_seconds}s")
    
    spark.stop()


if __name__ == "__main__":
    run_transformation()