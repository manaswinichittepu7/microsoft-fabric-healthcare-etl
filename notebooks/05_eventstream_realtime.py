"""
Microsoft Fabric — Notebook 05: Real-Time Eventstream Ingestion
Fabric Eventstream → KQL Database → Bronze Delta (Structured Streaming)
Processes live claim submissions with sub-second latency

"""

from pyspark.sql import functions as F
from pyspark.sql.types import *
from notebookutils import mssparkutils
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Eventstream_RT")

# Fabric Eventstream exposes an Event Hub-compatible endpoint
EVENTHUB_NAMESPACE  = mssparkutils.credentials.getSecret("kv-healthcare", "eh-namespace")
EVENTHUB_NAME       = "healthcare-claims-stream"
EVENTHUB_CONN_STR   = mssparkutils.credentials.getSecret("kv-healthcare", "eh-connection-string")
BRONZE_STREAM_TABLE = "bronze_claims_realtime"
CHECKPOINT_PATH     = "Files/checkpoints/realtime_bronze/"

print("⚡ Starting Real-Time Eventstream ingestion...")

# ── Schema for incoming claim events ─────────────────────────
CLAIM_EVENT_SCHEMA = StructType([
    StructField("claim_id",       StringType(), True),
    StructField("member_id",      StringType(), True),
    StructField("provider_npi",   StringType(), True),
    StructField("service_date",   StringType(), True),
    StructField("icd10_code",     StringType(), True),
    StructField("cpt_code",       StringType(), True),
    StructField("billed_amount",  DoubleType(), True),
    StructField("allowed_amount", DoubleType(), True),
    StructField("paid_amount",    DoubleType(), True),
    StructField("claim_status",   StringType(), True),
    StructField("payer_id",       StringType(), True),
    StructField("event_type",     StringType(), True),  # SUBMIT / UPDATE / ADJUDICATE
    StructField("submission_ts",  StringType(), True),
])

# ── Read from Event Hub (Eventstream endpoint) ────────────────
eh_conf = {
    "eventhubs.connectionString":
        sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(EVENTHUB_CONN_STR),
    "eventhubs.name":              EVENTHUB_NAME,
    "eventhubs.startingPosition":  '{"offset":"@latest","seqNo":-1,"enqueuedTime":null,"isInclusive":true}',
    "eventhubs.maxEventsPerTrigger": 10000,
}

raw_stream = (
    spark.readStream
    .format("eventhubs")
    .options(**eh_conf)
    .load()
    .select(
        F.col("enqueuedTime").alias("event_time"),
        F.col("body").cast("string").alias("payload"),
        F.col("partition").alias("partition_id"),
        F.col("offset").alias("eh_offset"),
    )
)

# ── Parse JSON payload ────────────────────────────────────────
parsed_stream = (
    raw_stream
    .withColumn("data", F.from_json("payload", CLAIM_EVENT_SCHEMA))
    .select(
        F.col("data.*"),
        F.col("event_time"),
        F.col("partition_id"),
        F.current_timestamp().alias("ingestion_ts"),
        F.to_date(F.current_timestamp()).alias("ingestion_date"),
    )
    .withColumn("service_date",
        F.coalesce(
            F.to_date("service_date", "yyyy-MM-dd"),
            F.to_date("service_date", "MM/dd/yyyy"),
        )
    )
    .withColumn("claim_status", F.upper(F.trim("claim_status")))
    .withColumn("provider_npi", F.lpad(F.trim("provider_npi"), 10, "0"))
)

# ── Write stream to Bronze Delta (Lakehouse table) ────────────
query = (
    parsed_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")   # micro-batch every 10s
    .option("mergeSchema", "true")
    .partitionBy("ingestion_date")
    .toTable(BRONZE_STREAM_TABLE)
)

logger.info(f"✅ Streaming query started: {query.name}")
logger.info(f"   Writing to: {BRONZE_STREAM_TABLE}")
logger.info("   Trigger: every 10 seconds")

# Await termination (Fabric will manage lifecycle via pipeline)
query.awaitTermination()
