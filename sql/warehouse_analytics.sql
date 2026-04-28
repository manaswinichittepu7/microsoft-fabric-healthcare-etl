-- ══════════════════════════════════════════════════════════════
-- Microsoft Fabric Warehouse DDL
-- Healthcare Claims Analytics — Gold Layer SQL Objects
-- Author: Manaswini Chittepu | Senior Data Engineer
-- ══════════════════════════════════════════════════════════════

-- ── 1. Lakehouse shortcut external tables ──────────────────────
-- These point to Gold Delta tables in the Lakehouse via shortcut
-- No data duplication — Warehouse reads directly from OneLake

CREATE TABLE IF NOT EXISTS [dbo].[gold_daily_claims_kpi]
AS SELECT * FROM [healthcare_lakehouse].[dbo].[gold_daily_claims_kpi]
WHERE 1 = 0;  -- schema-only; data served via Lakehouse shortcut


-- ── 2. Analytical Views ────────────────────────────────────────

-- View: Executive Claims Dashboard
CREATE OR ALTER VIEW [dbo].[vw_exec_claims_dashboard] AS
SELECT
    report_date,
    payer_id,
    line_of_business,
    SUM(claim_count)          AS total_claims,
    SUM(unique_members)       AS unique_members,
    SUM(total_billed)         AS total_billed,
    SUM(total_paid)           AS total_paid,
    ROUND(SUM(total_paid) * 100.0 / NULLIF(SUM(total_billed), 0), 2)
                              AS overall_payment_rate_pct,
    SUM(denied_count)         AS total_denied,
    ROUND(SUM(denied_count) * 100.0 / NULLIF(SUM(claim_count), 0), 2)
                              AS overall_denial_rate_pct,
    AVG(avg_paid_per_claim)   AS avg_paid_per_claim
FROM [healthcare_lakehouse].[dbo].[gold_daily_claims_kpi]
GROUP BY report_date, payer_id, line_of_business;
GO


-- View: Provider Tier Summary
CREATE OR ALTER VIEW [dbo].[vw_provider_tier_summary] AS
SELECT
    provider_npi,
    service_year,
    MAX(provider_tier)          AS current_tier,
    SUM(total_claims)           AS ytd_claims,
    SUM(patients_served)        AS ytd_patients,
    SUM(total_paid)             AS ytd_paid,
    AVG(denial_rate_pct)        AS avg_denial_rate_pct,
    AVG(cost_efficiency_score)  AS avg_cost_efficiency,
    COUNT(DISTINCT payer_id)    AS payer_count
FROM [healthcare_lakehouse].[dbo].[gold_provider_scorecard]
GROUP BY provider_npi, service_year;
GO


-- View: High-Risk Members
CREATE OR ALTER VIEW [dbo].[vw_high_risk_members] AS
SELECT
    member_id,
    payer_id,
    line_of_business,
    risk_tier,
    lifetime_claims,
    lifetime_paid,
    lifetime_billed,
    ROUND(lifetime_paid * 100.0 / NULLIF(lifetime_billed, 0), 2) AS lifetime_payment_rate,
    unique_providers_seen,
    unique_diagnoses,
    denied_claims,
    ROUND(denied_claims * 100.0 / NULLIF(lifetime_claims, 0), 2) AS member_denial_rate,
    first_service_date,
    last_service_date,
    days_as_patient,
    avg_cost_per_episode
FROM [healthcare_lakehouse].[dbo].[gold_member_360]
WHERE risk_tier IN ('HIGH', 'MEDIUM')
  AND lifetime_claims > 5;
GO


-- View: Month-over-Month Diagnosis Trend
CREATE OR ALTER VIEW [dbo].[vw_diagnosis_mom_trend] AS
WITH ranked AS (
    SELECT
        icd10_code,
        payer_id,
        service_year,
        service_month,
        claim_count,
        patients_affected,
        total_paid,
        cost_per_patient,
        mom_claim_growth_pct,
        ROW_NUMBER() OVER (
            PARTITION BY service_year, service_month
            ORDER BY claim_count DESC
        ) AS rank_by_volume
    FROM [healthcare_lakehouse].[dbo].[gold_diagnosis_trending]
)
SELECT * FROM ranked WHERE rank_by_volume <= 50;   -- top 50 diagnoses per month
GO


-- ── 3. Stored Procedures ───────────────────────────────────────

-- Proc: Refresh materialized KPI snapshot
CREATE OR ALTER PROCEDURE [dbo].[sp_refresh_kpi_snapshot]
    @report_date DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @d DATE = COALESCE(@report_date, CAST(GETDATE() AS DATE));

    -- Upsert daily snapshot
    MERGE [dbo].[kpi_snapshot] AS target
    USING (
        SELECT
            @d                        AS snapshot_date,
            payer_id,
            SUM(claim_count)          AS total_claims,
            SUM(total_billed)         AS total_billed,
            SUM(total_paid)           AS total_paid,
            AVG(denial_rate_pct)      AS avg_denial_rate,
            GETUTCDATE()              AS refreshed_at
        FROM [healthcare_lakehouse].[dbo].[gold_daily_claims_kpi]
        WHERE report_date = @d
        GROUP BY payer_id
    ) AS src
    ON target.snapshot_date = src.snapshot_date
    AND target.payer_id      = src.payer_id
    WHEN MATCHED THEN UPDATE SET
        total_claims      = src.total_claims,
        total_billed      = src.total_billed,
        total_paid        = src.total_paid,
        avg_denial_rate   = src.avg_denial_rate,
        refreshed_at      = src.refreshed_at
    WHEN NOT MATCHED THEN INSERT VALUES (
        src.snapshot_date, src.payer_id, src.total_claims,
        src.total_billed, src.total_paid, src.avg_denial_rate, src.refreshed_at
    );

    SELECT @@ROWCOUNT AS rows_affected;
END;
GO


-- ── 4. Row-Level Security ──────────────────────────────────────
-- Restrict payer_id visibility by workspace role

CREATE OR ALTER FUNCTION [dbo].[fn_payer_rls_predicate]
    (@payer_id NVARCHAR(50))
RETURNS TABLE
WITH SCHEMABINDING
AS RETURN
    SELECT 1 AS [result]
    WHERE
        -- Admins see all payers
        IS_MEMBER('healthcare_admin') = 1
        -- Payer-specific roles see only their data
        OR @payer_id = SESSION_CONTEXT(N'current_payer_id');
GO

CREATE SECURITY POLICY [dbo].[payer_rls_policy]
ADD FILTER PREDICATE [dbo].[fn_payer_rls_predicate]([payer_id])
ON [healthcare_lakehouse].[dbo].[gold_daily_claims_kpi]
WITH (STATE = ON);
GO
