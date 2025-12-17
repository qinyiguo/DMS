 (cd "$(git rev-parse --show-toplevel)" && git apply --3way <<'EOF' 
diff --git a/warehouse_schema.py b/warehouse_schema.py
index 47943556262e120934cc1f9ecd48d0bfcbad0d37..ec16451d7e645b90fd15f58ea0a2e85f6aa158a5 100644
--- a/warehouse_schema.py
+++ b/warehouse_schema.py
@@ -46,50 +46,75 @@ index 0000000000000000000000000000000000000000..546219ffac404e794ea7705941fd94ab
 +);
 +
 +CREATE TABLE IF NOT EXISTS fact_operations (
 +    id INTEGER PRIMARY KEY AUTOINCREMENT,
 +    factory_key INTEGER NOT NULL,
 +    period_key INTEGER NOT NULL,
 +    revenue REAL,
 +    cost REAL,
 +    output_qty REAL,
 +    downtime_hours REAL,
 +    FOREIGN KEY (factory_key) REFERENCES dim_factory(factory_key),
 +    FOREIGN KEY (period_key) REFERENCES dim_period(period_key)
 +);
 +
 +CREATE TABLE IF NOT EXISTS fact_kpi (
 +    id INTEGER PRIMARY KEY AUTOINCREMENT,
 +    employee_key INTEGER NOT NULL,
 +    period_key INTEGER NOT NULL,
 +    metric_code TEXT NOT NULL,
 +    value REAL,
 +    target REAL,
 +    FOREIGN KEY (employee_key) REFERENCES dim_employee(employee_key),
 +    FOREIGN KEY (period_key) REFERENCES dim_period(period_key)
 +);
 +
++-- KPI metric definitions for calculation engine
++CREATE TABLE IF NOT EXISTS kpi_metrics (
++    metric_code TEXT PRIMARY KEY,
++    scope TEXT NOT NULL CHECK (scope IN ('factory', 'employee')),
++    formula TEXT,
++    aggregation TEXT DEFAULT 'sum',
++    weight REAL,
++    target_source TEXT
++);
++
++-- Calculated KPI facts with batch tracking
++CREATE TABLE IF NOT EXISTS fact_kpi_calc (
++    id INTEGER PRIMARY KEY AUTOINCREMENT,
++    batch_id INTEGER NOT NULL,
++    period_key INTEGER NOT NULL,
++    scope TEXT NOT NULL CHECK (scope IN ('factory', 'employee')),
++    scope_id INTEGER NOT NULL,
++    metric_code TEXT NOT NULL,
++    value REAL,
++    target REAL,
++    weight REAL,
++    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
++    FOREIGN KEY (period_key) REFERENCES dim_period(period_key)
++);
++
 +-- Mapping tables for cleansing/alias handling
 +CREATE TABLE IF NOT EXISTS factory_code_alias (
 +    alias TEXT PRIMARY KEY,
 +    factory_code TEXT NOT NULL
 +);
 +
 +CREATE TABLE IF NOT EXISTS employee_id_alias (
 +    alias TEXT PRIMARY KEY,
 +    employee_id TEXT NOT NULL
 +);
 +"""
 +
 +
 +# -------------------------- SETUP HELPERS --------------------------
 +
 +def initialize_warehouse(connection: sqlite3.Connection) -> None:
 +    """Create all dimension/fact tables and alias mapping helpers."""
 +
 +    cursor = connection.cursor()
 +    cursor.executescript(WAREHOUSE_DDL)
 +    connection.commit()
 +
 +
 +# ------------------------- NORMALIZATION ----------------------------
 +
 
EOF
)
