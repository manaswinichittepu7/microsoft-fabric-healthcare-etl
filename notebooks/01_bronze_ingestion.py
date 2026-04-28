"""
Microsoft Fabric — Notebook 01: Bronze Ingestion
Lakehouse: healthcare_lakehouse | Layer: Bronze
Ingest raw claims from ADLS shortcut / Event Hub into Bronze Delta tables

"""

# ── Cell 1: Imports & Config ──────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import *
from notebookutils import mssparkutils
import json, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Bronze_Ingestion")

# Parameters (injected by Data Factory pipeline activity)
RAW_SOURCE_PATH  = mssparkutils.notebook.exit if False else \
    "Files/raw/claims/"          # Lakehouse Files section (ADLS shortcut)
BRONZE_TABLE     = "bronze_claims_raw"
CHECKPOINT_TABLE = "bronze_ingestion_log"
BATCH_DATE       = mssparkutils.env.get_user_variable("batch_date", "2025-01-15")

print(f"🔵 Bronze Ingestion | batch_date={BATCH_DATE}")


# ── Cell 2: Raw Schema ────────────────────────────────────────
RAW_SCHEMA = StructType([
    StructField("claim_id",       StringType(), True),
    StructField("member_id",      StringType(), True),
    StructField("provider_npi",   StringType(), True),
    StructField("service_date",   StringType(), True),
    StructField("icd10_code",     StringType(), True),
    StructField("cpt_code",       StringType(), True),
    StructField("billed_amount",  StringType(), True),
    StructField("allowed_amount", StringType(), True),
    StructField("paid_amount",    StringType(), True),
    StructField("claim_status",   StringType(), True),
    StructField("payer_id",       StringType(), True),
    StructField("line_of_business", StringType(), True),
    StructField("network_flag",   StringType(), True),
])


# ── Cell 3: Read Raw Files ────────────────────────────────────
print(f"📥 Reading raw claims from: {RAW_SOURCE_PATH}{BATCH_DATE}/")

raw_df = (
    spark.read
    .schema(RAW_SCHEMA)
    .option("header", "true")
    .option("multiLine", "false")
    .option("mode", "PERMISSIVE")          # don't fail on bad rows
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .csv(f"{RAW_SOURCE_PATH}{BATCH_DATE}/")
)

total_raw = raw_df.count()
print(f"   Raw records read: {total_raw:,}")


# ── Cell 4: Add Metadata ──────────────────────────────────────
bronze_df = (
    raw_df
    .withColumn("ingestion_date",    F.lit(BATCH_DATE).cast("date"))
    .withColumn("ingestion_ts",      F.current_timestamp())
    .withColumn("source_file",       F.input_file_name())
    .withColumn("batch_id",          F.lit(f"batch_{BATCH_DATE.replace('-','')}"))
    .withColumn("is_corrupt",
        F.col("_corrupt_record").isNotNull()
        if "_corrupt_record" in raw_df.columns
        else F.lit(False)
    )
)

corrupt_count = bronze_df.filter(F.col("is_corrupt")).count()
if corrupt_count > 0:
    logger.warning(f"⚠️  {corrupt_count:,} corrupt records found — tagged for review")


# ── Cell 5: Write to Bronze Delta Table ──────────────────────
print(f"💾 Writing Bronze Delta table: {BRONZE_TABLE}")

(
    bronze_df
    .write
    .format("delta")
    .mode("append")                         # append-only — Bronze is immutable
    .option("mergeSchema", "true")          # handle schema evolution
    .partitionBy("ingestion_date")
    .saveAsTable(BRONZE_TABLE)
)
print(f"✅ Bronze write complete.")


# ── Cell 6: Optimize for incremental reads ───────────────────
print("⚡ Optimizing Bronze table...")
spark.sql(f"OPTIMIZE {BRONZE_TABLE} WHERE ingestion_date = '{BATCH_DATE}'")
print("✅ Optimize complete.")


# ── Cell 7: Log run to checkpoint table ──────────────────────
log_entry = spark.createDataFrame([{
    "batch_date":       BATCH_DATE,
    "records_ingested": total_raw,
    "corrupt_records":  corrupt_count,
    "source_path":      f"{RAW_SOURCE_PATH}{BATCH_DATE}/",
    "status":           "SUCCESS",
    "logged_at":        str(F.current_timestamp()),
}])

(
    log_entry.write.format("delta")
    .mode("append")
    .saveAsTable(CHECKPOINT_TABLE)
)

print(f"\n📊 Bronze Summary | date={BATCH_DATE} | records={total_raw:,} | corrupt={corrupt_count}")
mssparkutils.notebook.exit(json.dumps({
    "batch_date":       BATCH_DATE,
    "records_ingested": total_raw,
    "corrupt_records":  corrupt_count,
    "status":           "SUCCESS"
}))
