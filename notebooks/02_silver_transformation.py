"""
Microsoft Fabric — Notebook 02: Silver Transformation
Lakehouse: healthcare_lakehouse | Layer: Silver
Clean, validate, deduplicate, and MERGE Bronze → Silver
Author: Manaswini Chittepu | Senior Data Engineer
"""

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from notebookutils import mssparkutils
from delta.tables import DeltaTable
import json, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Silver_Transform")

BRONZE_TABLE  = "bronze_claims_raw"
SILVER_TABLE  = "silver_claims"
QUARANTINE    = "quarantine_invalid_claims"
BATCH_DATE    = mssparkutils.env.get_user_variable("batch_date", "2025-01-15")
VALIDITY_SLA  = 95.0   # percent

print(f"⚪ Silver Transformation | batch_date={BATCH_DATE}")


# ── Cell 1: Read incremental Bronze batch ────────────────────
bronze_df = (
    spark.read
    .format("delta")
    .table(BRONZE_TABLE)
    .filter(
        (F.col("ingestion_date") == BATCH_DATE) &
        (F.col("is_corrupt") == False)
    )
)
print(f"   Bronze records for {BATCH_DATE}: {bronze_df.count():,}")


# ── Cell 2: Type casting & standardization ───────────────────
def clean_and_cast(df):
    return (
        df
        .withColumn("service_date",
            F.coalesce(
                F.to_date("service_date", "yyyy-MM-dd"),
                F.to_date("service_date", "MM/dd/yyyy"),
                F.to_date("service_date", "M/d/yyyy"),
                F.to_date("service_date", "yyyyMMdd"),
            )
        )
        .withColumn("billed_amount",
            F.regexp_replace("billed_amount", r"[$,\s]", "").cast(DoubleType()))
        .withColumn("allowed_amount",
            F.regexp_replace("allowed_amount", r"[$,\s]", "").cast(DoubleType()))
        .withColumn("paid_amount",
            F.regexp_replace("paid_amount", r"[$,\s]", "").cast(DoubleType()))
        .withColumn("claim_status",   F.upper(F.trim("claim_status")))
        .withColumn("provider_npi",   F.lpad(F.trim("provider_npi"), 10, "0"))
        .withColumn("icd10_code",     F.upper(F.trim("icd10_code")))
        .withColumn("cpt_code",       F.trim("cpt_code"))
        .withColumn("member_id",      F.trim("member_id"))
        .withColumn("payer_id",       F.upper(F.trim("payer_id")))
        .withColumn("line_of_business", F.upper(F.trim("line_of_business")))
        .withColumn("network_flag",   F.upper(F.trim("network_flag")))
        # Derived columns
        .withColumn("payment_rate_pct",
            F.round(F.col("paid_amount") / F.nullif(F.col("billed_amount"), 0) * 100, 2))
        .withColumn("adjustment_amount",
            F.round(F.col("billed_amount") - F.col("paid_amount"), 2))
        .withColumn("service_year",   F.year("service_date"))
        .withColumn("service_month",  F.month("service_date"))
        .withColumn("service_ym",     F.date_format("service_date", "yyyy-MM"))
    )

cleaned_df = clean_and_cast(bronze_df)


# ── Cell 3: Deduplication ────────────────────────────────────
window_dedup = Window.partitionBy("claim_id").orderBy(F.desc("ingestion_ts"))
deduped_df = (
    cleaned_df
    .withColumn("_rnk", F.row_number().over(window_dedup))
    .filter(F.col("_rnk") == 1)
    .drop("_rnk", "is_corrupt", "_corrupt_record", "batch_id")
)
dupes = cleaned_df.count() - deduped_df.count()
print(f"   Duplicates removed: {dupes:,}")


# ── Cell 4: Data Quality Rules ───────────────────────────────
VALID_STATUSES = {"APPROVED", "DENIED", "PENDING", "ADJUSTED", "VOIDED"}

def apply_dq(df):
    flag_exprs = {
        "NULL_CLAIM_ID":       F.col("claim_id").isNull(),
        "NULL_MEMBER_ID":      F.col("member_id").isNull(),
        "INVALID_STATUS":      ~F.col("claim_status").isin(list(VALID_STATUSES)),
        "NEGATIVE_AMOUNT":     (F.col("billed_amount") < 0) | (F.col("paid_amount") < 0),
        "FUTURE_DATE":         F.col("service_date") > F.current_date(),
        "NULL_SERVICE_DATE":   F.col("service_date").isNull(),
        "PAID_EXCEEDS_BILLED": F.col("paid_amount") > F.col("billed_amount"),
        "INVALID_NPI_LENGTH":  F.length("provider_npi") != 10,
    }
    for flag, expr in flag_exprs.items():
        df = df.withColumn(f"_flag_{flag}", expr.cast("int"))

    flag_cols = [c for c in df.columns if c.startswith("_flag_")]
    df = (
        df
        .withColumn("dq_error_count", sum(F.col(c) for c in flag_cols))
        .withColumn("is_valid", F.col("dq_error_count") == 0)
        .withColumn("dq_flags",
            F.to_json(F.array_compact(F.array(*[
                F.when(F.col(c) == 1, F.lit(c.replace("_flag_",""))).otherwise(F.lit(None))
                for c in flag_cols
            ])))
        )
        .drop(*flag_cols, "dq_error_count")
    )
    return df

validated_df = apply_dq(deduped_df)

total   = validated_df.count()
valid   = validated_df.filter(F.col("is_valid")).count()
invalid = total - valid
validity_pct = round(valid / total * 100, 2) if total > 0 else 0

print(f"   DQ | total={total:,} valid={valid:,} invalid={invalid:,} rate={validity_pct}%")

if validity_pct < VALIDITY_SLA:
    print(f"⚠️  WARNING: DQ rate {validity_pct}% below {VALIDITY_SLA}% SLA — investigate!")


# ── Cell 5: Quarantine bad records ───────────────────────────
if invalid > 0:
    (
        validated_df
        .filter(~F.col("is_valid"))
        .withColumn("quarantine_ts",     F.current_timestamp())
        .withColumn("quarantine_reason", F.col("dq_flags"))
        .write.format("delta").mode("append")
        .saveAsTable(QUARANTINE)
    )
    print(f"🚨 {invalid:,} records quarantined to: {QUARANTINE}")


# ── Cell 6: Prepare Silver records ───────────────────────────
silver_df = (
    validated_df
    .filter(F.col("is_valid"))
    .withColumn("silver_updated_at", F.current_timestamp())
    .withColumn("silver_batch_date", F.lit(BATCH_DATE).cast("date"))
)

# V-Order optimization hint (Fabric-specific)
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize",  "1073741824")


# ── Cell 7: MERGE into Silver (SCD Type 1 upsert) ────────────
silver_df.createOrReplaceTempView("silver_updates")

# Check if table exists
tables = [t.name for t in spark.catalog.listTables()]
if SILVER_TABLE in tables:
    print(f"💾 MERGE into existing {SILVER_TABLE}...")
    spark.sql(f"""
        MERGE INTO {SILVER_TABLE} AS target
        USING silver_updates AS source
        ON target.claim_id = source.claim_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
else:
    print(f"💾 Creating new {SILVER_TABLE}...")
    (
        silver_df.write.format("delta")
        .mode("overwrite")
        .partitionBy("service_year", "service_month")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_TABLE)
    )

print(f"✅ Silver MERGE complete.")


# ── Cell 8: V-Order optimize ─────────────────────────────────
print("⚡ Applying V-Order optimization (Direct Lake performance)...")
spark.sql(f"""
    OPTIMIZE {SILVER_TABLE}
    WHERE silver_batch_date = '{BATCH_DATE}'
    ZORDER BY (payer_id, service_date, claim_status, member_id)
""")
print("✅ Optimization complete — ready for Direct Lake.")


# ── Cell 9: Return stats ──────────────────────────────────────
stats = {
    "batch_date":    BATCH_DATE,
    "total":         total,
    "valid":         valid,
    "invalid":       invalid,
    "validity_pct":  validity_pct,
    "dupes_removed": dupes,
    "status":        "SUCCESS" if validity_pct >= VALIDITY_SLA else "DQ_WARNING",
}
print(f"\n📊 Silver Summary:\n{json.dumps(stats, indent=2)}")
mssparkutils.notebook.exit(json.dumps(stats))
