# 🟣 Microsoft Fabric — Healthcare ETL Pipeline



## 🏗️ What's Built

This pipeline processes **healthcare insurance claims** through the full Medallion Architecture (Bronze → Silver → Gold) using Microsoft Fabric's unified SaaS platform. Every component lives in a single Fabric workspace — no infrastructure to manage.

### Data Flow

```
EHR / FHIR APIs
      │
      ├── Batch ──▶ Data Factory Pipeline ──▶ ADLS Shortcut ──▶
      │                                                         │
      └── Stream ─▶ Eventstream (Event Hub) ──────────────────▶│
                                                                ▼
                                                   ┌─────────────────────┐
                                                   │   OneLake Lakehouse  │
                                                   │                     │
                                                   │  Bronze (raw)       │
                                                   │      ↓ Notebook 02  │
                                                   │  Silver (clean+DQ)  │
                                                   │      ↓ Notebook 03  │
                                                   │  Gold (aggregated)  │
                                                   └──────────┬──────────┘
                                                              │
                                              ┌───────────────┼───────────────┐
                                              ▼               ▼               ▼
                                       Fabric Warehouse  Power BI DL    Data Activator
                                       (T-SQL, RLS)      (Direct Lake)   (Alerts)
```

\---

## 📁 Project Structure

```
microsoft-fabric-etl/
├── notebooks/
│   ├── 01\_bronze\_ingestion.py        # Ingest raw CSV/JSON into Bronze Delta
│   ├── 02\_silver\_transformation.py   # Clean, DQ, MERGE into Silver
│   ├── 03\_gold\_aggregations.py       # 4 Gold analytics tables
│   ├── 04\_dq\_validation.py           # Post-load DQ checks \& SLA alerting
│   └── 05\_eventstream\_realtime.py    # Structured Streaming from Eventstream
├── pipelines/
│   └── pl\_healthcare\_etl\_master.json # Master Data Factory pipeline (ADF JSON)
├── sql/
│   └── warehouse\_analytics.sql       # Views, procedures, RLS (Fabric Warehouse)
├── tests/
│   └── test\_fabric\_etl.py            # pytest unit tests (local Spark)
├── configs/
│   └── workspace\_settings.json       # Workspace, Lakehouse, Spark config
├── docs/
│   └── fabric\_architecture.png       # Architecture diagram
└── README.md
```

\---

## 🔵 Notebooks

|#|Notebook|Layer|Description|
|-|-|-|-|
|01|`bronze\_ingestion`|Bronze|Read raw CSV/JSON from ADLS shortcut, add metadata, append-only write with partition pruning|
|02|`silver\_transformation`|Silver|Type casting, dedup (window fn), 8-rule DQ engine, quarantine, MERGE upsert, V-Order optimize|
|03|`gold\_aggregations`|Gold|4 aggregation tables: daily KPI, provider scorecard, member 360, diagnosis trending|
|04|`dq\_validation`|Audit|Post-load DQ: coverage check, validity rate, future dates, duplicate IDs, Gold freshness|
|05|`eventstream\_realtime`|Bronze|Structured Streaming from Event Hub via Fabric Eventstream, 10s micro-batch|

\---

## ✅ Key Features

|Feature|Implementation|
|-|-|
|**Unified Storage**|OneLake — single copy, all engines (Spark, SQL, Power BI) read same Delta tables|
|**Batch Ingestion**|Data Factory Pipelines + branching on success/failure|
|**Real-time Streaming**|Eventstream → Structured Streaming → Bronze Delta|
|**Data Quality**|8-rule DQ engine with `dq\_flags` JSON column + quarantine table|
|**CDC / Upsert**|Spark SQL `MERGE` statement on Silver table (SCD Type 1)|
|**BI Performance**|V-Order + ZORDER optimization → Power BI Direct Lake (zero import)|
|**Analytics**|Fabric Warehouse T-SQL views + Row-Level Security by payer\_id|
|**Alerting**|Data Activator triggers on DQ SLA breach + email via Office 365 activity|
|**Governance**|Microsoft Purview lineage + sensitivity labels (PHI) + column masking|
|**Testing**|pytest suite with local Spark — 15 unit tests covering all transformation logic|

\---

## 🚀 Quick Start

```bash
# 1. Clone
git clone https://github.com/manaswini-chittepu/microsoft-fabric-etl
cd microsoft-fabric-etl

# 2. Upload notebooks to Fabric workspace
#    Fabric UI → Workspace → New → Notebook → Import .py

# 3. Create Lakehouse
#    Fabric UI → New → Lakehouse → name: healthcare\_lakehouse

# 4. Import Data Factory pipeline
#    Fabric UI → New → Data Pipeline → Import JSON → pl\_healthcare\_etl\_master.json

# 5. Configure workspace settings
#    Update configs/workspace\_settings.json with your workspace\_id + lakehouse\_id

# 6. Run SQL objects in Fabric Warehouse
#    Fabric UI → Warehouse → New SQL Query → paste warehouse\_analytics.sql

# 7. Run tests locally
pip install pytest pyspark==3.4.0
pytest tests/ -v
```

\---

## 📊 Gold Tables

|Table|Granularity|Key Metrics|
|-|-|-|
|`gold\_daily\_claims\_kpi`|Day × Payer × Status|claim count, billed/paid, denial rate, payment rate|
|`gold\_provider\_scorecard`|Provider × Year-Month|denial rate, cost efficiency, provider tier|
|`gold\_member\_360`|Member lifetime|risk tier, lifetime cost, unique diagnoses|
|`gold\_diagnosis\_trending`|ICD-10 × Month|MoM claim growth, cost per patient|

\---

## ⚡ Performance Highlights

|Metric|Value|
|-|-|
|Notebook cold start|< 90s (Starter Pool)|
|Silver MERGE throughput|\~2M records/min on F64|
|V-Order benefit|30–50% faster Power BI Direct Lake reads|
|Streaming latency|< 15s end-to-end (Eventstream → Bronze)|
|DQ rule evaluation|\~30s for 1M records on 4-node cluster|

\---

## 🛠️ Tech Stack

`Microsoft Fabric` · `OneLake` · `Fabric Lakehouse` · `Fabric Data Factory` · `Eventstream` · `Spark Notebooks` · `Fabric Warehouse` · `Power BI Direct Lake` · `Data Activator` · `Microsoft Purview` · `PySpark 3.4` · `Delta Lake` · `T-SQL` · `pytest`

\---

## 👩‍💻 Author

**Manaswini Chittepu** —  Data Engineer | Optum (UnitedHealth Group)

* 🏅 Microsoft Fabric Analytics Engineer Associate (DP-700)



