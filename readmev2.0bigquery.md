# ═══════════════════════════════════════════
# CARBONSIGHT — COMPLETE BIGQUERY SETUP
# Run these one by one in Cloud Shell
# ═══════════════════════════════════════════

# 1. SET YOUR PROJECT
gcloud config set project medusa-store-demo

# ═══════════════════════════════════════════
# 2. CREATE DATASET
# ═══════════════════════════════════════════

bq mk \
  --dataset \
  --location=asia-south1 \
  --description="CarbonSight Sustainability Dataset" \
  medusa-store-demo:sustainability_ds

# ═══════════════════════════════════════════
# 3. CREATE ALL TABLES
# ═══════════════════════════════════════════

# Operations (CSV upload target)
bq query --use_legacy_sql=false '
CREATE TABLE IF NOT EXISTS sustainability_ds.operations (
  product_id   STRING,
  units_sold   INT64,
  energy_kwh   FLOAT64,
  transport_km FLOAT64,
  record_date  DATE,
  tenant_id    STRING
)'

# Products master
bq query --use_legacy_sql=false '
CREATE TABLE IF NOT EXISTS sustainability_ds.products (
  product_id     STRING,
  product_name   STRING,
  category       STRING,
  avg_weight_kg  FLOAT64
)'

# Utility bills
bq query --use_legacy_sql=false '
CREATE TABLE IF NOT EXISTS sustainability_ds.utility_bills (
  bill_id       STRING,
  bill_type     STRING,
  amount        FLOAT64,
  units         FLOAT64,
  region        STRING,
  month         DATE,
  estimated_co2 FLOAT64,
  upload_time   TIMESTAMP,
  display_date  STRING,
  tenant_id     STRING
)'

# Upload log
bq query --use_legacy_sql=false '
CREATE TABLE IF NOT EXISTS sustainability_ds.upload_log (
  upload_id   STRING,
  upload_time TIMESTAMP,
  file_name   STRING,
  rows_loaded INT64,
  status      STRING
)'

# App settings (emission factors)
bq query --use_legacy_sql=false '
CREATE OR REPLACE TABLE sustainability_ds.app_settings (
  setting_type STRING,
  factor_key   STRING,
  factor_value FLOAT64,
  updated_at   TIMESTAMP,
  updated_by   STRING
)'

# Simulation scenarios
bq query --use_legacy_sql=false '
CREATE OR REPLACE TABLE sustainability_ds.simulation_scenarios (
  scenario_id  STRING,
  name         STRING,
  description  STRING,
  levers_json  STRING,
  result_json  STRING,
  created_at   TIMESTAMP
)'

# Emission factors reference table
bq query --use_legacy_sql=false '
CREATE TABLE IF NOT EXISTS sustainability_ds.emission_factors (
  region        STRING,
  activity_type STRING,
  factor        FLOAT64,
  year          INT64,
  reference     STRING,
  created_at    TIMESTAMP
)'

# ═══════════════════════════════════════════
# 4. CREATE VIEWS
# ═══════════════════════════════════════════

# bill_emissions view — calculates CO2 from utility_bills
bq query --use_legacy_sql=false '
CREATE OR REPLACE VIEW sustainability_ds.bill_emissions AS
SELECT
  month,
  region,
  bill_type,
  SUM(
    CASE
      WHEN bill_type = "electricity" THEN units * 0.82
      WHEN bill_type = "fuel"        THEN units * 2.68
      WHEN bill_type = "courier"     THEN units * 0.18
      ELSE 0
    END
  ) AS estimated_co2
FROM sustainability_ds.utility_bills
GROUP BY month, region, bill_type
ORDER BY month'

# product_catalogue view
bq query --use_legacy_sql=false '
CREATE OR REPLACE VIEW sustainability_ds.product_catalogue AS
SELECT
  product_id,
  product_name,
  category,
  avg_weight_kg
FROM sustainability_ds.products'

# monthly_metrics view
bq query --use_legacy_sql=false '
CREATE OR REPLACE VIEW sustainability_ds.monthly_metrics AS
SELECT
  FORMAT_DATE("%Y-%m", record_date) AS month,
  SUM(units_sold)                   AS total_units,
  SUM(energy_kwh * 0.82)            AS energy_co2,
  SUM(transport_km * 0.0525)        AS transport_co2,
  SUM(energy_kwh * 0.82 + transport_km * 0.0525) AS total_co2
FROM sustainability_ds.operations
GROUP BY month
ORDER BY month'

# total company footprint view
bq query --use_legacy_sql=false '
CREATE OR REPLACE VIEW sustainability_ds.company_emissions AS
WITH product AS (
  SELECT
    FORMAT_DATE("%Y-%m", record_date) AS month,
    SUM(energy_kwh * 0.82 + transport_km * 0.0525) AS co2
  FROM sustainability_ds.operations
  GROUP BY month
),
utility AS (
  SELECT
    FORMAT_DATE("%Y-%m", month) AS month,
    SUM(estimated_co2) AS co2
  FROM sustainability_ds.bill_emissions
  GROUP BY month
)
SELECT
  COALESCE(p.month, u.month)           AS month,
  IFNULL(p.co2, 0)                     AS product_co2,
  IFNULL(u.co2, 0)                     AS utility_co2,
  IFNULL(p.co2, 0) + IFNULL(u.co2, 0) AS total_co2
FROM product p
FULL OUTER JOIN utility u ON p.month = u.month
ORDER BY month'

# ═══════════════════════════════════════════
# 5. SEED PRODUCTS DATA
# ═══════════════════════════════════════════

bq query --use_legacy_sql=false '
INSERT INTO sustainability_ds.products
(product_id, product_name, category, avg_weight_kg) VALUES
("P001", "Cotton T-Shirt",   "Apparel",     0.25),
("P002", "Denim Jeans",      "Apparel",     0.60),
("P003", "Sports Shoes",     "Footwear",    0.90),
("P004", "Home Appliance",   "Electronics", 3.50),
("P005", "Accessories",      "Apparel",     0.20),
("P006", "Running Shorts",   "Apparel",     0.20),
("P007", "Wireless Earbuds", "Electronics", 0.15),
("P008", "Yoga Mat",         "Sports",      0.90),
("P009", "Backpack",         "Apparel",     0.80),
("P010", "Smart Watch",      "Electronics", 0.10),
("P011", "Water Bottle",     "Sports",      0.30),
("P012", "Desk Lamp",        "Electronics", 0.60),
("P013", "Notebook Set",     "Stationery",  0.40),
("P014", "Face Wash",        "Beauty",      0.25),
("P015", "Sunglasses",       "Apparel",     0.15)'

# ═══════════════════════════════════════════
# 6. SEED EMISSION FACTORS REFERENCE TABLE
# ═══════════════════════════════════════════

bq query --use_legacy_sql=false '
INSERT INTO sustainability_ds.emission_factors
(region, activity_type, factor, year, reference, created_at) VALUES
("India", "grid_electricity", 0.82,   2024, "CEA India 2024",        CURRENT_TIMESTAMP()),
("India", "renewable_energy", 0.05,   2024, "IPCC SRREN lifecycle",  CURRENT_TIMESTAMP()),
("India", "freight_truck",    0.0525, 2024, "GLEC Framework 2024",   CURRENT_TIMESTAMP()),
("India", "ev_transport",     0.021,  2024, "BEV India Grid 2024",   CURRENT_TIMESTAMP()),
("India", "fuel",             2.68,   2024, "IPCC AR6",              CURRENT_TIMESTAMP()),
("India", "courier",          0.18,   2024, "GHG Protocol 2023",     CURRENT_TIMESTAMP())'

# ═══════════════════════════════════════════
# 7. VERIFY EVERYTHING
# ═══════════════════════════════════════════

# List all tables and views
bq ls sustainability_ds

# Check row counts
bq query --use_legacy_sql=false '
SELECT "operations"     AS tbl, COUNT(*) AS rows FROM sustainability_ds.operations      UNION ALL
SELECT "products",               COUNT(*)         FROM sustainability_ds.products        UNION ALL
SELECT "utility_bills",          COUNT(*)         FROM sustainability_ds.utility_bills   UNION ALL
SELECT "upload_log",             COUNT(*)         FROM sustainability_ds.upload_log      UNION ALL
SELECT "app_settings",           COUNT(*)         FROM sustainability_ds.app_settings    UNION ALL
SELECT "simulation_scenarios",   COUNT(*)         FROM sustainability_ds.simulation_scenarios UNION ALL
SELECT "emission_factors",       COUNT(*)         FROM sustainability_ds.emission_factors'

# Verify product IDs match operations (should be > 0 if you have operations data)
bq query --use_legacy_sql=false '
SELECT COUNT(*) AS matching_rows
FROM sustainability_ds.operations o
JOIN sustainability_ds.products p ON o.product_id = p.product_id'
