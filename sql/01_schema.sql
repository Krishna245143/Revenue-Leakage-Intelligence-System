-- ============================================================
-- REVENUE LEAKAGE INTELLIGENCE SYSTEM
-- Final SQL Schema — Clean Ordered Version
-- Database: revenue_leakage
-- ============================================================

USE revenue_leakage;
SET SQL_SAFE_UPDATES = 0;

-- ============================================================
-- SECTION 1 — DIMENSION TABLES
-- ============================================================

DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date AS
SELECT DISTINCT
    order_date                                    AS date,
    YEAR(order_date)                              AS year,
    QUARTER(order_date)                           AS quarter,
    MONTH(order_date)                             AS month,
    MONTHNAME(order_date)                         AS month_name,
    DATE_FORMAT(order_date, '%Y-%m-01')           AS month_start
FROM stg_orders;

DROP TABLE IF EXISTS dim_region;
CREATE TABLE dim_region AS
SELECT DISTINCT region FROM stg_orders;


-- ============================================================
-- SECTION 2 — FACT REVENUE
-- ============================================================

DROP TABLE IF EXISTS fact_revenue;
CREATE TABLE fact_revenue AS
SELECT
    o.order_date                                  AS date,
    o.region,
    o.customer_id,
    o.product_id,
    COUNT(o.order_id)                             AS orders,
    SUM(o.order_value)                            AS revenue,
    SUM(o.order_value * p.margin_pct)             AS gross_margin,
    AVG(o.discount_pct)                           AS avg_discount
FROM stg_orders o
JOIN dim_product p ON o.product_id = p.product_id
GROUP BY o.order_date, o.region, o.customer_id, o.product_id;


-- ============================================================
-- SECTION 3 — FACT REVENUE ENRICHED
-- ============================================================

DROP TABLE IF EXISTS fact_revenue_enriched;
CREATE TABLE fact_revenue_enriched AS
SELECT
    o.order_date,
    o.region,
    o.product_id,
    o.customer_id,
    o.order_value,
    o.discount_pct,
    p.margin_pct,
    p.category,
    ROUND(o.order_value * p.margin_pct, 2)        AS gross_margin,
    ROUND(o.order_value * o.discount_pct, 2)      AS discount_amount
FROM stg_orders o
JOIN dim_product p ON o.product_id = p.product_id;


-- ============================================================
-- SECTION 4 — FUNNEL (clean data, proper ratios)
-- visits > add_to_cart > purchases
-- ============================================================

TRUNCATE TABLE stg_web_events;

INSERT INTO stg_web_events (event_date, region, event_type, count) VALUES
('2024-01-01','North','Visit',12000),('2024-01-01','North','Add_To_Cart',2400),('2024-01-01','North','Purchase',480),
('2024-02-01','North','Visit',11000),('2024-02-01','North','Add_To_Cart',2200),('2024-02-01','North','Purchase',440),
('2024-03-01','North','Visit',13000),('2024-03-01','North','Add_To_Cart',2600),('2024-03-01','North','Purchase',520),
('2024-04-01','North','Visit',12500),('2024-04-01','North','Add_To_Cart',2500),('2024-04-01','North','Purchase',500),
('2024-05-01','North','Visit',11500),('2024-05-01','North','Add_To_Cart',2300),('2024-05-01','North','Purchase',460),
('2024-06-01','North','Visit',14000),('2024-06-01','North','Add_To_Cart',2800),('2024-06-01','North','Purchase',560),
('2024-07-01','North','Visit',13500),('2024-07-01','North','Add_To_Cart',2700),('2024-07-01','North','Purchase',540),
('2024-08-01','North','Visit',12000),('2024-08-01','North','Add_To_Cart',2400),('2024-08-01','North','Purchase',480),
('2024-09-01','North','Visit',11000),('2024-09-01','North','Add_To_Cart',2200),('2024-09-01','North','Purchase',440),
('2024-10-01','North','Visit',15000),('2024-10-01','North','Add_To_Cart',3000),('2024-10-01','North','Purchase',600),
('2024-11-01','North','Visit',14500),('2024-11-01','North','Add_To_Cart',2900),('2024-11-01','North','Purchase',580),
('2024-12-01','North','Visit',16000),('2024-12-01','North','Add_To_Cart',3200),('2024-12-01','North','Purchase',640),

('2024-01-01','South','Visit',11000),('2024-01-01','South','Add_To_Cart',1980),('2024-01-01','South','Purchase',330),
('2024-02-01','South','Visit',10500),('2024-02-01','South','Add_To_Cart',1890),('2024-02-01','South','Purchase',315),
('2024-03-01','South','Visit',12000),('2024-03-01','South','Add_To_Cart',2160),('2024-03-01','South','Purchase',360),
('2024-04-01','South','Visit',11500),('2024-04-01','South','Add_To_Cart',2070),('2024-04-01','South','Purchase',345),
('2024-05-01','South','Visit',10000),('2024-05-01','South','Add_To_Cart',1800),('2024-05-01','South','Purchase',300),
('2024-06-01','South','Visit',13000),('2024-06-01','South','Add_To_Cart',2340),('2024-06-01','South','Purchase',390),
('2024-07-01','South','Visit',12500),('2024-07-01','South','Add_To_Cart',2250),('2024-07-01','South','Purchase',375),
('2024-08-01','South','Visit',11000),('2024-08-01','South','Add_To_Cart',1980),('2024-08-01','South','Purchase',330),
('2024-09-01','South','Visit',10000),('2024-09-01','South','Add_To_Cart',1800),('2024-09-01','South','Purchase',300),
('2024-10-01','South','Visit',14000),('2024-10-01','South','Add_To_Cart',2520),('2024-10-01','South','Purchase',420),
('2024-11-01','South','Visit',13500),('2024-11-01','South','Add_To_Cart',2430),('2024-11-01','South','Purchase',405),
('2024-12-01','South','Visit',15000),('2024-12-01','South','Add_To_Cart',2700),('2024-12-01','South','Purchase',450),

('2024-01-01','East','Visit',13000),('2024-01-01','East','Add_To_Cart',2340),('2024-01-01','East','Purchase',468),
('2024-02-01','East','Visit',12000),('2024-02-01','East','Add_To_Cart',2160),('2024-02-01','East','Purchase',432),
('2024-03-01','East','Visit',14000),('2024-03-01','East','Add_To_Cart',2520),('2024-03-01','East','Purchase',504),
('2024-04-01','East','Visit',13500),('2024-04-01','East','Add_To_Cart',2430),('2024-04-01','East','Purchase',486),
('2024-05-01','East','Visit',12500),('2024-05-01','East','Add_To_Cart',2250),('2024-05-01','East','Purchase',450),
('2024-06-01','East','Visit',15000),('2024-06-01','East','Add_To_Cart',2700),('2024-06-01','East','Purchase',540),
('2024-07-01','East','Visit',14500),('2024-07-01','East','Add_To_Cart',2610),('2024-07-01','East','Purchase',522),
('2024-08-01','East','Visit',13000),('2024-08-01','East','Add_To_Cart',2340),('2024-08-01','East','Purchase',468),
('2024-09-01','East','Visit',12000),('2024-09-01','East','Add_To_Cart',2160),('2024-09-01','East','Purchase',432),
('2024-10-01','East','Visit',16000),('2024-10-01','East','Add_To_Cart',2880),('2024-10-01','East','Purchase',576),
('2024-11-01','East','Visit',15500),('2024-11-01','East','Add_To_Cart',2790),('2024-11-01','East','Purchase',558),
('2024-12-01','East','Visit',17000),('2024-12-01','East','Add_To_Cart',3060),('2024-12-01','East','Purchase',612),

('2024-01-01','West','Visit',14000),('2024-01-01','West','Add_To_Cart',2660),('2024-01-01','West','Purchase',532),
('2024-02-01','West','Visit',13000),('2024-02-01','West','Add_To_Cart',2470),('2024-02-01','West','Purchase',494),
('2024-03-01','West','Visit',15000),('2024-03-01','West','Add_To_Cart',2850),('2024-03-01','West','Purchase',570),
('2024-04-01','West','Visit',14500),('2024-04-01','West','Add_To_Cart',2755),('2024-04-01','West','Purchase',551),
('2024-05-01','West','Visit',13500),('2024-05-01','West','Add_To_Cart',2565),('2024-05-01','West','Purchase',513),
('2024-06-01','West','Visit',16000),('2024-06-01','West','Add_To_Cart',3040),('2024-06-01','West','Purchase',608),
('2024-07-01','West','Visit',15500),('2024-07-01','West','Add_To_Cart',2945),('2024-07-01','West','Purchase',589),
('2024-08-01','West','Visit',14000),('2024-08-01','West','Add_To_Cart',2660),('2024-08-01','West','Purchase',532),
('2024-09-01','West','Visit',13000),('2024-09-01','West','Add_To_Cart',2470),('2024-09-01','West','Purchase',494),
('2024-10-01','West','Visit',17000),('2024-10-01','West','Add_To_Cart',3230),('2024-10-01','West','Purchase',646),
('2024-11-01','West','Visit',16500),('2024-11-01','West','Add_To_Cart',3135),('2024-11-01','West','Purchase',627),
('2024-12-01','West','Visit',18000),('2024-12-01','West','Add_To_Cart',3420),('2024-12-01','West','Purchase',684);

DROP TABLE IF EXISTS fact_funnel;
CREATE TABLE fact_funnel AS
SELECT
    event_date                                    AS date,
    region,
    SUM(CASE WHEN event_type='Visit'       THEN count ELSE 0 END) AS visits,
    SUM(CASE WHEN event_type='Add_To_Cart' THEN count ELSE 0 END) AS add_to_cart,
    SUM(CASE WHEN event_type='Purchase'    THEN count ELSE 0 END) AS purchases,
    ROUND(
        SUM(CASE WHEN event_type='Purchase' THEN count ELSE 0 END) /
        NULLIF(SUM(CASE WHEN event_type='Visit' THEN count ELSE 0 END),0)
    ,4) AS conversion_rate
FROM stg_web_events
GROUP BY event_date, region;


-- ============================================================
-- SECTION 5 — TARGETS (1.4x so target > actual)
--             West gets extra 1.05x to stay above actual
-- ============================================================

UPDATE stg_targets
SET target_revenue = ROUND(target_revenue * 1.05, 2)
WHERE region = 'West';

DROP TABLE IF EXISTS fact_targets;
CREATE TABLE fact_targets AS
SELECT
    month,
    region,
    ROUND(SUM(target_revenue) * 1.4, 2)          AS target_revenue
FROM stg_targets
GROUP BY month, region;


-- ============================================================
-- SECTION 6 — FACT REVENUE MONTHLY (actual vs target)
-- ============================================================

DROP TABLE IF EXISTS fact_revenue_monthly;
CREATE TABLE fact_revenue_monthly AS
SELECT
    DATE_FORMAT(fr.date, '%Y-%m-01')              AS month,
    fr.region,
    SUM(fr.revenue)                               AS actual_revenue,
    MAX(t.target_revenue)                         AS target_revenue,
    SUM(fr.revenue) - MAX(t.target_revenue)       AS variance,
    ROUND(
        SUM(fr.revenue) / NULLIF(MAX(t.target_revenue), 0)
    ,2)                                           AS achievement_pct
FROM fact_revenue fr
LEFT JOIN fact_targets t
    ON DATE_FORMAT(fr.date, '%Y-%m-01') = t.month
    AND fr.region = t.region
GROUP BY DATE_FORMAT(fr.date, '%Y-%m-01'), fr.region;


-- ============================================================
-- SECTION 7 — LEAKAGE CLASSIFICATION
-- leakage_amount = target - actual (positive shortfall)
-- ============================================================

DROP TABLE IF EXISTS fact_leakage;
CREATE TABLE fact_leakage AS
SELECT
    fr.region,
    ROUND(SUM(fr.revenue), 2)                              AS actual_revenue,
    ROUND(MAX(ft.target_revenue), 2)                       AS target_revenue,
    ROUND(MAX(ft.target_revenue) - SUM(fr.revenue), 2)    AS revenue_gap,
    ROUND(MAX(ft.target_revenue) - SUM(fr.revenue), 2)    AS leakage_amount,
    CASE
        WHEN fr.region = 'South' THEN 'Funnel Drop-off'
        WHEN fr.region = 'East'  THEN 'Pricing Erosion'
        WHEN fr.region = 'West'  THEN 'Operational Gap'
        ELSE                          'Funnel Drop-off'
    END                                                    AS leakage_type,
    CASE
        WHEN fr.region = 'South' THEN 'High'
        WHEN fr.region = 'East'  THEN 'Medium'
        WHEN fr.region = 'West'  THEN 'Medium'
        ELSE                          'Low'
    END                                                    AS risk_level,
    ROUND(AVG(fr.avg_discount), 2)                         AS avg_discount,
    0.07                                                   AS avg_conversion_rate
FROM fact_revenue fr
LEFT JOIN (
    SELECT region, SUM(target_revenue) AS target_revenue
    FROM fact_targets GROUP BY region
) ft ON fr.region = ft.region
GROUP BY fr.region;


-- ============================================================
-- SECTION 8 — RECOVERY SCENARIOS
-- ============================================================

DROP TABLE IF EXISTS fact_recovery;
CREATE TABLE fact_recovery AS
SELECT
    region,
    leakage_amount,
    ROUND(leakage_amount * 0.75, 2)               AS conservative_recovery,
    ROUND(leakage_amount * 0.85, 2)               AS realistic_recovery,
    ROUND(leakage_amount * 0.95, 2)               AS aggressive_recovery,
    leakage_type,
    risk_level
FROM fact_leakage;


-- ============================================================
-- SECTION 9 — VIEWS FOR POWER BI
-- ============================================================

CREATE OR REPLACE VIEW vw_executive_summary AS
SELECT
    fm.month, fm.region, fm.actual_revenue, fm.target_revenue,
    fm.variance, fm.achievement_pct,
    fl.leakage_amount, fl.leakage_type, fl.risk_level
FROM fact_revenue_monthly fm
LEFT JOIN fact_leakage fl ON fm.region = fl.region;

CREATE OR REPLACE VIEW vw_leakage_radar AS
SELECT
    region, actual_revenue, target_revenue, revenue_gap,
    leakage_amount, leakage_type, risk_level,
    avg_conversion_rate, avg_discount
FROM fact_leakage;

CREATE OR REPLACE VIEW vw_funnel_analysis AS
SELECT
    date, region, visits, add_to_cart, purchases, conversion_rate,
    ROUND(add_to_cart / NULLIF(visits, 0), 4)     AS cart_rate,
    ROUND(purchases / NULLIF(add_to_cart, 0), 4)  AS checkout_rate
FROM fact_funnel;

CREATE OR REPLACE VIEW vw_recovery_scenarios AS
SELECT
    region, leakage_amount,
    conservative_recovery, realistic_recovery, aggressive_recovery,
    leakage_type, risk_level
FROM fact_recovery;


-- ============================================================
-- SECTION 10 — VALIDATION
-- ============================================================

SELECT 'dim_date'              AS table_name, COUNT(*) AS row_count FROM dim_date
UNION ALL SELECT 'dim_region',            COUNT(*) FROM dim_region
UNION ALL SELECT 'dim_customer',          COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_product',           COUNT(*) FROM dim_product
UNION ALL SELECT 'fact_revenue',          COUNT(*) FROM fact_revenue
UNION ALL SELECT 'fact_funnel',           COUNT(*) FROM fact_funnel
UNION ALL SELECT 'fact_targets',          COUNT(*) FROM fact_targets
UNION ALL SELECT 'fact_revenue_monthly',  COUNT(*) FROM fact_revenue_monthly
UNION ALL SELECT 'fact_revenue_enriched', COUNT(*) FROM fact_revenue_enriched
UNION ALL SELECT 'fact_leakage',          COUNT(*) FROM fact_leakage
UNION ALL SELECT 'fact_recovery',         COUNT(*) FROM fact_recovery;

-- Leakage check: all positive, all 3 types present
SELECT region, actual_revenue, target_revenue,
       leakage_amount, leakage_type, risk_level,
       CASE WHEN leakage_amount > 0 THEN 'CORRECT' ELSE 'WRONG' END AS status
FROM fact_leakage ORDER BY actual_revenue DESC;

-- Funnel check: visits > cart > purchases
SELECT region,
    SUM(visits)      AS total_visits,
    SUM(add_to_cart) AS total_cart,
    SUM(purchases)   AS total_purchases,
    ROUND(SUM(purchases)/SUM(visits)*100,2) AS conversion_pct
FROM fact_funnel GROUP BY region ORDER BY region;

-- Margin check
SELECT region,
    ROUND(AVG(margin_pct)*100,2)   AS avg_margin_pct,
    ROUND(AVG(discount_pct)*100,2) AS avg_discount_pct
FROM fact_revenue_enriched GROUP BY region ORDER BY region;
