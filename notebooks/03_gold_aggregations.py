"""
Microsoft Fabric — Notebook 03: Gold Aggregations
Lakehouse: healthcare_lakehouse | Layer: Gold
Build analytics-ready aggregation tables from Silver
Author: Manaswini Chittepu | Senior Data Engineer
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from notebookutils import mssparkutils
import json, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Gold_Aggregations")

SILVER_TABLE = "silver_claims"
BATCH_DATE   = mssparkutils.env.get_user_variable("batch_date", "2025-01-15")

print(f"🟡 Gold Aggregations | batch_date={BATCH_DATE}")

silver_df = (
    spark.read.format("delta").table(SILVER_TABLE)
    .filter(F.col("is_valid") == True)
)
print(f"   Silver valid records: {silver_df.count():,}")


# ── Gold Table 1: Daily Claims KPI ───────────────────────────
def build_daily_kpi(df):
    return (
        df.groupBy(
            F.to_date("silver_updated_at").alias("report_date"),
            "payer_id", "claim_status", "line_of_business", "network_flag"
        ).agg(
            F.count("claim_id").alias("claim_count"),
            F.countDistinct("member_id").alias("unique_members"),
            F.countDistinct("provider_npi").alias("unique_providers"),
            F.sum("billed_amount").alias("total_billed"),
            F.sum("allowed_amount").alias("total_allowed"),
            F.sum("paid_amount").alias("total_paid"),
            F.avg("paid_amount").alias("avg_paid_per_claim"),
            F.percentile_approx("paid_amount", 0.5).alias("median_paid"),
            F.percentile_approx("paid_amount", 0.95).alias("p95_paid"),
            F.sum(F.when(F.col("claim_status") == "DENIED", 1).otherwise(0)).alias("denied_count"),
            F.round(
                F.sum(F.when(F.col("claim_status") == "DENIED", 1).otherwise(0)).cast("double")
                / F.count("claim_id") * 100, 2
            ).alias("denial_rate_pct"),
            F.round(
                F.sum("paid_amount") / F.nullif(F.sum("billed_amount"), 0) * 100, 2
            ).alias("payment_rate_pct"),
        )
        .withColumn("gold_updated_at", F.current_timestamp())
    )


# ── Gold Table 2: Provider Scorecard ─────────────────────────
def build_provider_scorecard(df):
    return (
        df.groupBy(
            "provider_npi", "service_year", "service_month", "payer_id"
        ).agg(
            F.count("claim_id").alias("total_claims"),
            F.countDistinct("member_id").alias("patients_served"),
            F.sum("billed_amount").alias("total_billed"),
            F.sum("paid_amount").alias("total_paid"),
            F.avg("paid_amount").alias("avg_claim_value"),
            F.round(
                F.sum(F.when(F.col("claim_status") == "DENIED", 1).otherwise(0)).cast("double")
                / F.count("claim_id") * 100, 2
            ).alias("denial_rate_pct"),
            F.countDistinct("icd10_code").alias("unique_diagnoses"),
            F.countDistinct("cpt_code").alias("unique_procedures"),
            F.collect_set("icd10_code").alias("icd10_codes_used"),
        )
        .withColumn("cost_efficiency_score",
            F.round(F.col("total_paid") / F.nullif(F.col("total_billed"), 0) * 100, 1))
        .withColumn("provider_tier",
            F.when(F.col("denial_rate_pct") < 5, "PREFERRED")
            .when(F.col("denial_rate_pct") < 15, "STANDARD")
            .otherwise("REVIEW"))
        .withColumn("gold_updated_at", F.current_timestamp())
    )


# ── Gold Table 3: Member 360 ─────────────────────────────────
def build_member_360(df):
    return (
        df.groupBy("member_id", "payer_id", "line_of_business")
        .agg(
            F.count("claim_id").alias("lifetime_claims"),
            F.sum("billed_amount").alias("lifetime_billed"),
            F.sum("paid_amount").alias("lifetime_paid"),
            F.min("service_date").alias("first_service_date"),
            F.max("service_date").alias("last_service_date"),
            F.countDistinct("provider_npi").alias("unique_providers_seen"),
            F.countDistinct("icd10_code").alias("unique_diagnoses"),
            F.sum(F.when(F.col("claim_status") == "DENIED", 1).otherwise(0)).alias("denied_claims"),
            F.collect_set("network_flag").alias("network_usage"),
        )
        .withColumn("days_as_patient",
            F.datediff("last_service_date", "first_service_date"))
        .withColumn("avg_cost_per_episode",
            F.round(F.col("lifetime_paid") / F.nullif(F.col("lifetime_claims"), 0), 2))
        .withColumn("risk_tier",
            F.when(
                (F.col("lifetime_claims") > 50) | (F.col("lifetime_paid") > 100000), "HIGH"
            ).when(
                (F.col("lifetime_claims") > 20) | (F.col("lifetime_paid") > 30000), "MEDIUM"
            ).otherwise("LOW"))
        .withColumn("gold_updated_at", F.current_timestamp())
    )


# ── Gold Table 4: Diagnosis Trending ─────────────────────────
def build_diagnosis_trending(df):
    window_prev = Window.partitionBy("icd10_code", "payer_id")\
        .orderBy("service_year", "service_month")\
        .rowsBetween(Window.unboundedPreceding, -1)
    return (
        df.groupBy("icd10_code", "service_year", "service_month", "payer_id")
        .agg(
            F.count("claim_id").alias("claim_count"),
            F.countDistinct("member_id").alias("patients_affected"),
            F.sum("paid_amount").alias("total_paid"),
            F.avg("paid_amount").alias("avg_cost_per_episode"),
            F.sum(F.when(F.col("claim_status") == "DENIED", 1).otherwise(0)).alias("denied_count"),
        )
        .withColumn("cost_per_patient",
            F.round(F.col("total_paid") / F.col("patients_affected"), 2))
        .withColumn("prev_month_claims",
            F.lag("claim_count", 1).over(
                Window.partitionBy("icd10_code", "payer_id")
                .orderBy("service_year", "service_month")))
        .withColumn("mom_claim_growth_pct",
            F.round(
                (F.col("claim_count") - F.col("prev_month_claims"))
                / F.nullif(F.col("prev_month_claims"), 0) * 100, 2))
        .withColumn("gold_updated_at", F.current_timestamp())
    )


# ── Write all Gold tables ─────────────────────────────────────
gold_tables = {
    "gold_daily_claims_kpi":      build_daily_kpi(silver_df),
    "gold_provider_scorecard":    build_provider_scorecard(silver_df),
    "gold_member_360":            build_member_360(silver_df),
    "gold_diagnosis_trending":    build_diagnosis_trending(silver_df),
}

for name, df in gold_tables.items():
    print(f"💾 Writing {name}...")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(name)
    )
    # Optimize each Gold table for Direct Lake
    spark.sql(f"OPTIMIZE {name}")
    print(f"   ✅ {name} → {df.count():,} rows")

print(f"\n✅ All Gold tables complete for batch_date={BATCH_DATE}")
mssparkutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "batch_date": BATCH_DATE,
    "gold_tables": list(gold_tables.keys())
}))
