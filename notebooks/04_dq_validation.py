"""
Microsoft Fabric — Notebook 04: Data Quality Validation & Alerting
Runs post-load DQ checks on Silver and Gold tables
Publishes results; triggers Data Activator alert on SLA breach
Author: Manaswini Chittepu | Senior Data Engineer
"""

from pyspark.sql import functions as F
from notebookutils import mssparkutils
import json, datetime, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DQ_Validation")

BATCH_DATE  = mssparkutils.env.get_user_variable("batch_date", "2025-01-15")
DQ_LOG_TABLE = "dq_run_log"

print(f"🔍 DQ Validation | batch_date={BATCH_DATE}")

results = {}


# ── Rule 1: Silver row count vs Bronze ───────────────────────
bronze_count = spark.sql(f"""
    SELECT COUNT(*) as cnt FROM bronze_claims_raw
    WHERE ingestion_date = '{BATCH_DATE}' AND is_corrupt = false
""").collect()[0]["cnt"]

silver_count = spark.sql(f"""
    SELECT COUNT(*) as cnt FROM silver_claims
    WHERE silver_batch_date = '{BATCH_DATE}'
""").collect()[0]["cnt"]

silver_coverage = round(silver_count / max(bronze_count, 1) * 100, 2)
results["silver_coverage_pct"] = silver_coverage
results["bronze_count"]        = bronze_count
results["silver_count"]        = silver_count
print(f"   Silver coverage: {silver_coverage}% ({silver_count:,}/{bronze_count:,})")


# ── Rule 2: Silver validity rate ─────────────────────────────
validity = spark.sql(f"""
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) as valid,
        ROUND(SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as validity_pct
    FROM silver_claims
    WHERE silver_batch_date = '{BATCH_DATE}'
""").collect()[0]

results["validity_pct"] = validity["validity_pct"]
print(f"   Silver validity: {validity['validity_pct']}%")


# ── Rule 3: No future service dates ──────────────────────────
future_dates = spark.sql(f"""
    SELECT COUNT(*) as cnt FROM silver_claims
    WHERE service_date > CURRENT_DATE()
    AND silver_batch_date = '{BATCH_DATE}'
""").collect()[0]["cnt"]

results["future_dates_count"] = future_dates
results["future_dates_pass"]  = future_dates == 0
print(f"   Future dates: {future_dates} (pass={future_dates == 0})")


# ── Rule 4: No duplicate claim_ids in Silver ─────────────────
dup_check = spark.sql(f"""
    SELECT COUNT(*) - COUNT(DISTINCT claim_id) as dupes
    FROM silver_claims
    WHERE silver_batch_date = '{BATCH_DATE}'
""").collect()[0]["dupes"]

results["duplicate_claim_ids"] = int(dup_check)
results["no_duplicates_pass"]  = dup_check == 0
print(f"   Duplicate claim_ids: {dup_check} (pass={dup_check == 0})")


# ── Rule 5: Gold table freshness ─────────────────────────────
gold_tables = ["gold_daily_claims_kpi", "gold_provider_scorecard",
               "gold_member_360", "gold_diagnosis_trending"]
gold_freshness = {}
for tbl in gold_tables:
    try:
        latest = spark.sql(f"SELECT MAX(gold_updated_at) as ts FROM {tbl}").collect()[0]["ts"]
        age_mins = round((datetime.datetime.now() - latest).total_seconds() / 60, 1) if latest else 9999
        gold_freshness[tbl] = age_mins
        print(f"   {tbl}: last updated {age_mins} mins ago")
    except Exception as e:
        gold_freshness[tbl] = -1
        logger.warning(f"Could not check freshness for {tbl}: {e}")

results["gold_freshness_mins"] = gold_freshness


# ── Rule 6: Referential checks ───────────────────────────────
orphan_providers = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM silver_claims s
    WHERE silver_batch_date = '{BATCH_DATE}'
    AND NOT EXISTS (
        SELECT 1 FROM gold_provider_scorecard p
        WHERE p.provider_npi = s.provider_npi
    )
""").collect()[0]["cnt"]
results["orphan_provider_records"] = int(orphan_providers)


# ── Evaluate overall DQ status ────────────────────────────────
sla_violations = []
if results["validity_pct"] < 95.0:
    sla_violations.append(f"VALIDITY_BELOW_95: {results['validity_pct']}%")
if results["future_dates_count"] > 0:
    sla_violations.append(f"FUTURE_DATES: {results['future_dates_count']}")
if results["duplicate_claim_ids"] > 0:
    sla_violations.append(f"DUPLICATES: {results['duplicate_claim_ids']}")
if results["silver_coverage_pct"] < 90.0:
    sla_violations.append(f"LOW_COVERAGE: {results['silver_coverage_pct']}%")

overall_status = "PASS" if not sla_violations else "FAIL"
results["overall_status"]   = overall_status
results["sla_violations"]   = sla_violations
results["batch_date"]       = BATCH_DATE
results["evaluated_at"]     = str(datetime.datetime.now())

print(f"\n{'✅' if overall_status == 'PASS' else '🚨'} DQ Status: {overall_status}")
if sla_violations:
    for v in sla_violations:
        print(f"   ❌ {v}")


# ── Write DQ log ─────────────────────────────────────────────
log_df = spark.createDataFrame([{
    "batch_date":         BATCH_DATE,
    "overall_status":     overall_status,
    "validity_pct":       float(results["validity_pct"]),
    "silver_coverage_pct": float(results["silver_coverage_pct"]),
    "future_dates_count": int(results["future_dates_count"]),
    "duplicate_count":    int(results["duplicate_claim_ids"]),
    "sla_violations":     json.dumps(sla_violations),
    "logged_at":          str(datetime.datetime.now()),
}])

(
    log_df.write.format("delta").mode("append")
    .saveAsTable(DQ_LOG_TABLE)
)

print(f"\n📊 DQ Results written to {DQ_LOG_TABLE}")
mssparkutils.notebook.exit(json.dumps(results))
