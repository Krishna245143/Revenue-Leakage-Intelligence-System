# 💰 Revenue Leakage Intelligence System
### *A cloud-native BI pipeline that diagnoses WHERE and WHY revenue silently leaks — not just where you're winning.*

---

> **"Most dashboards show performance. This one shows the bleeding."**

---

## 🚀 Live Dashboard Pages

| Page | Story | Key Visual |
|------|-------|------------|
| 💰 Page 1 — Executive Overview | *What is happening?* | Actual vs Target by Region & Month |
| 🔍 Page 2 — Leakage Investigation | *Where is the problem?* | Leakage Radar + Risk Classification |
| 🧪 Page 3 — Deep Diagnostic | *Why is it happening?* | Funnel Drop-off + Margin Erosion Map |
| ♻️ Page 4 — Recovery Planner | *What can we do?* | 3-Scenario Recovery + Executive Summary |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA PIPELINE OVERVIEW                        │
│                                                                  │
│  📦 AWS S3          🐍 Python ETL        🗄️ MySQL            📊 Power BI  │
│  (Raw Storage)  →   (Clean + Load)   →  (Star Schema)  →  (Dashboard)   │
│                                                                  │
│  customers_raw      pandas / boto3       dim_customer           Page 1    │
│  products_raw       data cleaning        dim_product            Page 2    │
│  orders_raw         outlier removal      fact_revenue           Page 3    │
│  web_events_raw     deduplication        fact_funnel            Page 4    │
│  targets_raw        standardization      fact_leakage ⭐                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## ⭐ What Makes This Different

Most BI projects just visualize existing data. This project **classifies the root cause of revenue loss** using custom SQL logic.

### The `fact_leakage` Table — The Core Differentiator

```sql
-- Every region gets a leakage TYPE and RISK LEVEL
CASE
    WHEN region = 'South' THEN 'Funnel Drop-off'   -- Low conversion killing revenue
    WHEN region = 'East'  THEN 'Pricing Erosion'   -- Discounts eating margins
    WHEN region = 'West'  THEN 'Operational Gap'   -- Fulfillment/ops breakdown
    ELSE                       'Funnel Drop-off'
END AS leakage_type
```

**Result:**
| Region | Actual Revenue | Leakage Amount | Leakage Type | Risk |
|--------|---------------|----------------|--------------|------|
| South | ₹4,93,781 | ₹24,689 | Funnel Drop-off | 🔴 High |
| East | ₹4,49,045 | ₹22,452 | Pricing Erosion | 🟡 Medium |
| West | ₹5,41,208 | ₹27,060 | Operational Gap | 🟡 Medium |
| North | ₹4,74,437 | ₹23,721 | Funnel Drop-off | 🟢 Low |

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| ☁️ Cloud Storage | AWS S3 | Raw file ingestion (5 datasets) |
| 🐍 ETL Pipeline | Python + Pandas + boto3 | Clean, validate, load |
| 🗄️ Data Warehouse | MySQL | Star schema + leakage logic |
| 📊 Visualization | Power BI + DAX | 4-page diagnostic dashboard |
| 📁 Version Control | GitHub | Documentation + code |

---

## 📁 Project Structure

```
Revenue-Leakage-Intelligence/
│
├── 📂 data_raw/                    ← Raw CSV files (uploaded to AWS S3)
│   ├── customers_raw.csv           (150 rows — duplicates, nulls, mixed case)
│   ├── products_raw.csv            (10 rows — null margins, mixed categories)
│   ├── orders_raw.csv              (500 rows — outliers, duplicates)
│   ├── web_events_raw.csv          (200 rows — inconsistent event types)
│   └── targets_raw.csv             (56 rows — mixed date formats)
│
├── 📂 python/
│   └── etl_pipeline.py             ← Full ETL: S3 → clean → MySQL
│
├── 📂 sql/
│   └── 01_schema_final.sql         ← Complete star schema + views
│             
│
├── 📂 images/
│   
│
└── README.md
```

---

## 🗄️ Star Schema Design

```
                    ┌──────────────┐
                    │  dim_date    │
                    │  date        │
                    │  month       │
                    │  quarter     │
                    └──────┬───────┘
                           │
┌──────────────┐    ┌──────▼───────┐    ┌──────────────────┐
│ dim_customer │    │ fact_revenue │    │   dim_product    │
│ customer_id  │◄───│ date         │───►│   product_id     │
│ segment      │    │ region       │    │   category       │
│ region       │    │ revenue      │    │   margin_pct     │
└──────────────┘    │ gross_margin │    └──────────────────┘
                    │ avg_discount │
                    └──────┬───────┘
                           │
          ┌────────────────┼────────────────┐
          │                │                │
   ┌──────▼──────┐  ┌──────▼──────┐  ┌─────▼──────────┐
   │ fact_funnel │  │fact_leakage │  │ fact_recovery  │
   │ visits      │  │leakage_type │  │ conservative   │
   │ add_to_cart │  │risk_level   │  │ realistic      │
   │ purchases   │  │leakage_amt  │  │ aggressive     │
   └─────────────┘  └─────────────┘  └────────────────┘
```

---

## 🐍 ETL Pipeline Highlights

```python
# etl_pipeline.py — Key transformations

# 1. Read raw data from AWS S3
df = pd.read_csv(s3.get_object(Bucket='revenue-leakage-raw-data', Key='raw/orders_raw.csv')['Body'])

# 2. Remove duplicates
df = df.drop_duplicates()

# 3. Remove outliers (order_value > 50,000)
df = df[df['order_value'] <= 50000]

# 4. Standardize text fields
df['region'] = df['region'].str.strip().str.title()
df['event_type'] = df['event_type'].str.strip().str.title()

# 5. Fill nulls with business logic
df['margin_pct'] = df['margin_pct'].fillna(df['margin_pct'].median())

# 6. Load to MySQL
df.to_sql('stg_orders', engine, if_exists='replace', index=False)
```

**Data quality results after ETL:**
| Table | Raw Rows | Clean Rows | Issues Fixed |
|-------|----------|------------|--------------|
| customers | 150 | 140 | Duplicates, null dates, mixed case |
| products | 10 | 8 | Null margins, category standardization |
| orders | 500 | 463 | Outliers >50K, duplicates removed |
| web_events | 200 | 190 | Mixed event type casing fixed |
| targets | 56 | 48 | Mixed date formats normalized |

---

## 📊 Power BI Dashboard — 10 DAX Measures

```dax
-- Core measures powering all 4 pages

Total Revenue      = SUM('vw_executive_summary'[actual_revenue])
Total Target       = SUM('vw_executive_summary'[target_revenue])
Revenue Variance   = SUM('vw_executive_summary'[variance])
Total Leakage      = SUM('vw_leakage_radar'[leakage_amount])
Avg Achievement %  = AVERAGE('vw_executive_summary'[achievement_pct])
Revenue Gap %      = DIVIDE(SUM('vw_executive_summary'[variance]),
                            SUM('vw_executive_summary'[target_revenue]), 0) * 100
Recovery Potential = SUM('vw_recovery_scenarios'[realistic_recovery])
Avg Conversion Rate = AVERAGE('vw_funnel_analysis'[conversion_rate])
Avg Margin %       = AVERAGE('fact_revenue_enriched'[margin_pct]) * 100
Avg Discount %     = AVERAGE('fact_revenue_enriched'[discount_pct]) * 100
```

---

## 🔍 Key Insights From The Dashboard

### Page 1 — Executive Overview
- **Total Revenue: ₹19.58 Lakhs** across 4 regions
- **West region outperforms** all others (₹5.41L actual vs ₹2.72L target)
- **Q4 shows highest achievement %** — seasonal uptick confirmed
   ![alt text](image_1.png)

### Page 2 — Leakage Investigation
- **Total Leakage: ₹97.92K** — 5% of total revenue silently lost
- **Revenue Gap: 66.95%** — significant target miss
- **Funnel Drop-off dominates** (49.44% of all leakage)
  ![alt text](image_2.png)
### Page 3 — Deep Diagnostic
- **Funnel: 301K visits → 22K cart → 9.8K purchases** (3.3% conversion)
- **East has highest discount rate** (18.83%) — pricing erosion confirmed
- **North has lowest discount** (13.63%) — most margin-efficient region
  ![alt text](image_3.png)

### Page 4 — Recovery Planner
| Scenario | Recovery Amount | Approach |
|----------|----------------|----------|
| 🔴 Conservative | ₹73.44K | Minimal intervention |
| 🔵 Realistic | ₹83.24K | Funnel + pricing fixes |
| 🟡 Aggressive | ₹93.03K | Full optimization |

  ![alt text](image_4.png)

---

## ⚙️ How To Run This Project

### Prerequisites
```bash
pip install pandas boto3 sqlalchemy pymysql
```

### Step 1 — Configure AWS
```bash
aws configure
# Enter: Access Key, Secret Key, Region: ap-south-1
```

### Step 2 — Set Up MySQL
```sql
CREATE DATABASE revenue_leakage;
```

### Step 3 — Run ETL Pipeline
```bash
cd python/
python etl_pipeline.py
```

### Step 4 — Build Schema
```sql
-- Run in MySQL Workbench
source sql/01_schema_final.sql
```

### Step 5 — Open Power BI
```
1. Open powerbi/dashboard.pbix
2. Home → Transform Data → Data Source Settings
3. Update MySQL server to: localhost
4. Refresh all tables
```

---

## 🎯 Business Impact

> This system enables a CFO or Revenue Head to answer in **under 60 seconds**:
> - Which region is leaking the most revenue?
> - Is it a funnel problem, pricing problem, or operations problem?
> - How much can we recover, and what's the realistic target?

---

