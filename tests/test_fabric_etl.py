"""
Microsoft Fabric ETL — Unit & Integration Tests
Uses pytest + pyspark local session for notebook logic validation

"""

import pytest
import json
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("FabricETL_Tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def sample_bronze_data(spark):
    schema = StructType([
        StructField("claim_id",      StringType(), True),
        StructField("member_id",     StringType(), True),
        StructField("provider_npi",  StringType(), True),
        StructField("service_date",  StringType(), True),
        StructField("billed_amount", StringType(), True),
        StructField("paid_amount",   StringType(), True),
        StructField("claim_status",  StringType(), True),
        StructField("payer_id",      StringType(), True),
        StructField("icd10_code",    StringType(), True),
        StructField("cpt_code",      StringType(), True),
        StructField("allowed_amount",StringType(), True),
        StructField("line_of_business", StringType(), True),
        StructField("network_flag",  StringType(), True),
    ])
    data = [
        ("CLM001", "MBR001", "1234567890", "2025-01-10", "$1500.00", "$1200.00", "approved", "PYR001", "E11.9",  "99213", "$1400.00", "COMMERCIAL", "IN"),
        ("CLM002", "MBR002", "9876543210", "01/05/2025",  "500",      "400",      "DENIED",   "PYR002", "J06.9",  "99202", "480",      "MEDICARE",   "OUT"),
        ("CLM001", "MBR001", "1234567890", "2025-01-10", "$1500.00", "$1200.00", "APPROVED", "PYR001", "E11.9",  "99213", "$1400.00", "COMMERCIAL", "IN"),  # dup
        ("CLM003", None,     "1111111111", "2025-01-08", "800",      "700",      "PENDING",  "PYR001", "Z00.00", "99386", "780",      "COMMERCIAL", "IN"),  # null member
        ("CLM004", "MBR004", "2222222222", "2025-02-01", "-100",     "-50",      "ADJUSTED", "PYR003", "M79.3",  "97110", "-90",      "MEDICAID",   "IN"),  # negative
    ]
    return spark.createDataFrame(data, schema)


# ─────────────────────────────────────────────────────────────
# SILVER TRANSFORMATION TESTS
# ─────────────────────────────────────────────────────────────

class TestCleanAndCast:

    def test_date_parsing_iso(self, spark, sample_bronze_data):
        """ISO format yyyy-MM-dd parses correctly."""
        df = sample_bronze_data.filter(F.col("claim_id") == "CLM001")
        result = df.withColumn("parsed", F.to_date("service_date", "yyyy-MM-dd"))
        assert result.first()["parsed"] == date(2025, 1, 10)

    def test_date_parsing_mm_dd_yyyy(self, spark, sample_bronze_data):
        """MM/dd/yyyy format parses correctly."""
        df = sample_bronze_data.filter(F.col("claim_id") == "CLM002")
        result = df.withColumn("parsed",
            F.coalesce(
                F.to_date("service_date", "yyyy-MM-dd"),
                F.to_date("service_date", "MM/dd/yyyy"),
            )
        )
        assert result.first()["parsed"] == date(2025, 1, 5)

    def test_amount_stripping_currency_symbol(self, spark, sample_bronze_data):
        """Dollar sign and commas stripped before casting to double."""
        df = sample_bronze_data.filter(F.col("claim_id") == "CLM001")
        result = df.withColumn("amt",
            F.regexp_replace("billed_amount", r"[$,\s]", "").cast(DoubleType()))
        assert result.first()["amt"] == 1500.0

    def test_claim_status_uppercased(self, spark, sample_bronze_data):
        """Claim status is uppercased and trimmed."""
        df = sample_bronze_data.filter(F.col("claim_id") == "CLM001")
        result = df.withColumn("status", F.upper(F.trim("claim_status")))
        assert result.first()["status"] == "APPROVED"

    def test_npi_zero_padded(self, spark, sample_bronze_data):
        """NPI is zero-padded to 10 digits."""
        short_npi_data = [("CLM005", "MBR005", "12345", "2025-01-01", "100", "80",
                           "APPROVED", "PYR001", "A00.0", "99201", "90", "COM", "IN")]
        schema = sample_bronze_data.schema
        df = spark.createDataFrame(short_npi_data, schema)
        result = df.withColumn("npi", F.lpad(F.trim("provider_npi"), 10, "0"))
        assert result.first()["npi"] == "0000012345"


class TestDeduplication:

    def test_dedup_removes_duplicate_claim_ids(self, spark, sample_bronze_data):
        """CLM001 appears twice — dedup should keep only one."""
        from pyspark.sql.window import Window
        df = sample_bronze_data.withColumn("ingestion_ts", F.current_timestamp())
        window = Window.partitionBy("claim_id").orderBy(F.desc("ingestion_ts"))
        deduped = (
            df.withColumn("_rnk", F.row_number().over(window))
            .filter(F.col("_rnk") == 1)
        )
        clm001_count = deduped.filter(F.col("claim_id") == "CLM001").count()
        assert clm001_count == 1

    def test_dedup_preserves_unique_records(self, spark, sample_bronze_data):
        """Non-duplicate records are fully preserved."""
        from pyspark.sql.window import Window
        df = sample_bronze_data.withColumn("ingestion_ts", F.current_timestamp())
        window = Window.partitionBy("claim_id").orderBy(F.desc("ingestion_ts"))
        deduped = (
            df.withColumn("_rnk", F.row_number().over(window))
            .filter(F.col("_rnk") == 1)
        )
        # CLM001 deduped → 4 unique: CLM001, CLM002, CLM003, CLM004
        assert deduped.count() == 4


# ─────────────────────────────────────────────────────────────
# DATA QUALITY RULE TESTS
# ─────────────────────────────────────────────────────────────

class TestDataQualityRules:

    def test_null_member_flagged(self, spark, sample_bronze_data):
        """CLM003 has null member_id — should be flagged NULL_MEMBER_ID."""
        df = sample_bronze_data.filter(F.col("claim_id") == "CLM003")
        df = df.withColumn("_flag", F.col("member_id").isNull())
        assert df.first()["_flag"] == True

    def test_negative_amount_flagged(self, spark, sample_bronze_data):
        """CLM004 has negative amounts — should be flagged."""
        df = sample_bronze_data.filter(F.col("claim_id") == "CLM004")
        df = df.withColumn("amt", F.col("billed_amount").cast(DoubleType()))
        df = df.withColumn("_flag_neg", F.col("amt") < 0)
        assert df.first()["_flag_neg"] == True

    def test_valid_record_passes_all_rules(self, spark):
        """A perfect record should have zero DQ flags."""
        schema = StructType([
            StructField("claim_id", StringType()), StructField("member_id", StringType()),
            StructField("provider_npi", StringType()), StructField("service_date", DateType()),
            StructField("billed_amount", DoubleType()), StructField("paid_amount", DoubleType()),
            StructField("claim_status", StringType()),
        ])
        data = [("CLM999", "MBR999", "1234567890", date(2025, 1, 1), 500.0, 400.0, "APPROVED")]
        df = spark.createDataFrame(data, schema)

        df = df \
            .withColumn("f1", F.col("claim_id").isNull().cast("int")) \
            .withColumn("f2", F.col("member_id").isNull().cast("int")) \
            .withColumn("f3", (~F.col("claim_status").isin(["APPROVED","DENIED","PENDING","ADJUSTED","VOIDED"])).cast("int")) \
            .withColumn("f4", (F.col("billed_amount") < 0).cast("int")) \
            .withColumn("f5", (F.col("paid_amount") > F.col("billed_amount")).cast("int"))

        total_flags = df.withColumn("total", F.col("f1")+F.col("f2")+F.col("f3")+F.col("f4")+F.col("f5"))
        assert total_flags.first()["total"] == 0

    def test_validity_rate_calculation(self, spark, sample_bronze_data):
        """Validity rate should be calculable from is_valid column."""
        df = sample_bronze_data \
            .withColumn("member_id_clean", F.trim("member_id")) \
            .withColumn("is_valid", F.col("member_id").isNotNull())

        total = df.count()
        valid = df.filter(F.col("is_valid")).count()
        rate  = round(valid / total * 100, 2)

        assert 0 <= rate <= 100
        assert rate == 80.0   # 4/5 records have non-null member_id


# ─────────────────────────────────────────────────────────────
# GOLD AGGREGATION TESTS
# ─────────────────────────────────────────────────────────────

class TestGoldAggregations:

    @pytest.fixture
    def clean_silver(self, spark):
        schema = StructType([
            StructField("claim_id",      StringType()), StructField("member_id",    StringType()),
            StructField("provider_npi",  StringType()), StructField("service_date", DateType()),
            StructField("billed_amount", DoubleType()), StructField("paid_amount",  DoubleType()),
            StructField("claim_status",  StringType()), StructField("payer_id",     StringType()),
            StructField("icd10_code",    StringType()), StructField("service_year", IntegerType()),
            StructField("service_month", IntegerType()),
        ])
        data = [
            ("C1", "M1", "NPI1111111", date(2025,1,5),  1000.0, 800.0, "APPROVED", "P1", "E11.9", 2025, 1),
            ("C2", "M2", "NPI1111111", date(2025,1,6),  500.0,  0.0,   "DENIED",   "P1", "J06.9", 2025, 1),
            ("C3", "M1", "NPI2222222", date(2025,1,7),  750.0,  600.0, "APPROVED", "P2", "Z00.0", 2025, 1),
        ]
        return spark.createDataFrame(data, schema)

    def test_daily_kpi_claim_count(self, spark, clean_silver):
        result = (
            clean_silver.groupBy("payer_id")
            .agg(F.count("claim_id").alias("claim_count"))
        )
        p1 = result.filter(F.col("payer_id") == "P1").first()["claim_count"]
        assert p1 == 2

    def test_provider_denial_rate(self, spark, clean_silver):
        result = (
            clean_silver.groupBy("provider_npi")
            .agg(
                F.count("claim_id").alias("total"),
                F.sum(F.when(F.col("claim_status") == "DENIED", 1).otherwise(0)).alias("denied"),
            )
            .withColumn("denial_rate",
                F.round(F.col("denied") / F.col("total") * 100, 2))
        )
        npi1 = result.filter(F.col("provider_npi") == "NPI1111111").first()
        assert npi1["denial_rate"] == 50.0

    def test_member_360_risk_tier(self, spark, clean_silver):
        member_df = (
            clean_silver.groupBy("member_id")
            .agg(F.count("claim_id").alias("claims"), F.sum("paid_amount").alias("paid"))
            .withColumn("risk_tier",
                F.when((F.col("claims") > 50) | (F.col("paid") > 100000), "HIGH")
                .when((F.col("claims") > 20) | (F.col("paid") > 30000), "MEDIUM")
                .otherwise("LOW"))
        )
        m1 = member_df.filter(F.col("member_id") == "M1").first()
        assert m1["risk_tier"] == "LOW"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
