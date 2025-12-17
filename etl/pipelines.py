 (cd "$(git rev-parse --show-toplevel)" && git apply --3way <<'EOF' 
diff --git a/etl/pipelines.py b/etl/pipelines.py
new file mode 100644
index 0000000000000000000000000000000000000000..372f44fbda6d762d72f9030864b7ff979c49916c
--- /dev/null
+++ b/etl/pipelines.py
@@ -0,0 +1,555 @@
+"""ETL batch runner for staging uploads.
+
+This module loads data from staging tables produced by the upload API,
+performs cleansing and data-quality checks, and writes results into the
+warehouse fact tables. Any quality issues discovered are recorded in the
+``dq_issues`` table for later review.
+"""
+
+from __future__ import annotations
+
+import json
+import os
+import sqlite3
+import time
+from datetime import datetime
+from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple
+
+from warehouse_schema import (
+    initialize_warehouse,
+    load_fact_kpi,
+    load_fact_operations,
+    normalize_employee_id,
+    normalize_factory_code,
+)
+
+DB_PATH = os.getenv("DB_PATH", "/data/excel_import.db")
+DEFAULT_ANOMALY_THRESHOLD = float(os.getenv("DQ_ANOMALY_THRESHOLD", "1000000"))
+
+
+def _get_connection() -> sqlite3.Connection:
+    conn = sqlite3.connect(DB_PATH)
+    conn.row_factory = sqlite3.Row
+    return conn
+
+
+def _ensure_column(cursor: sqlite3.Cursor, table: str, column: str, ddl: str) -> None:
+    columns = {row[1] for row in cursor.execute(f"PRAGMA table_info({table})").fetchall()}
+    if column not in columns:
+        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
+
+
+def _ensure_etl_tables(connection: sqlite3.Connection) -> None:
+    """Create ETL helper tables and add missing columns to batch metadata."""
+
+    cursor = connection.cursor()
+    cursor.execute(
+        """
+        CREATE TABLE IF NOT EXISTS dq_issues (
+            id INTEGER PRIMARY KEY AUTOINCREMENT,
+            batch_id INTEGER NOT NULL,
+            dataset TEXT NOT NULL,
+            row_number INTEGER,
+            issue_type TEXT NOT NULL,
+            issue_message TEXT,
+            context TEXT,
+            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
+        )
+        """
+    )
+
+    # Extend upload_batches with processing stats if needed.
+    _ensure_column(cursor, "upload_batches", "processed_rows", "INTEGER")
+    _ensure_column(cursor, "upload_batches", "dq_error_count", "INTEGER")
+    _ensure_column(cursor, "upload_batches", "processing_ms", "INTEGER")
+
+    connection.commit()
+
+
+def _parse_period(value: str) -> Tuple[int, int]:
+    """Parse an ISO-like date string into (year, month)."""
+
+    if not value:
+        raise ValueError("date is required for period parsing")
+    parsed = datetime.fromisoformat(str(value))
+    return parsed.year, parsed.month
+
+
+def _record_issue(
+    cursor: sqlite3.Cursor,
+    *,
+    batch_id: int,
+    dataset: str,
+    row_number: Optional[int],
+    issue_type: str,
+    issue_message: str,
+    context: Optional[Mapping[str, object]] = None,
+) -> None:
+    cursor.execute(
+        """
+        INSERT INTO dq_issues (batch_id, dataset, row_number, issue_type, issue_message, context)
+        VALUES (?, ?, ?, ?, ?, ?)
+        """,
+        (
+            batch_id,
+            dataset,
+            row_number,
+            issue_type,
+            issue_message,
+            json.dumps(context, ensure_ascii=False) if context else None,
+        ),
+    )
+
+
+def _validate_required_fields(
+    row: Mapping[str, object],
+    required: Iterable[str],
+    *,
+    cursor: sqlite3.Cursor,
+    batch_id: int,
+    dataset: str,
+    row_number: Optional[int],
+) -> List[str]:
+    missing = []
+    for field in required:
+        value = row.get(field)
+        if value is None or str(value).strip() == "":
+            missing.append(field)
+            _record_issue(
+                cursor,
+                batch_id=batch_id,
+                dataset=dataset,
+                row_number=row_number,
+                issue_type="missing_value",
+                issue_message=f"{field} is required",
+                context={"field": field},
+            )
+    return missing
+
+
+def _load_alias_maps(cursor: sqlite3.Cursor) -> Dict[str, Dict[str, str]]:
+    """Load alias mapping tables to reuse normalization rules."""
+
+    factory_alias = {
+        row[0]: row[1] for row in cursor.execute("SELECT alias, factory_code FROM factory_code_alias")
+    }
+    employee_alias = {
+        row[0]: row[1] for row in cursor.execute("SELECT alias, employee_id FROM employee_id_alias")
+    }
+    return {"factory": factory_alias, "employee": employee_alias}
+
+
+def _process_operations(
+    connection: sqlite3.Connection,
+    *,
+    batch_id: int,
+    dataset: str,
+    staging_rows: Sequence[sqlite3.Row],
+    anomaly_thresholds: Optional[Mapping[str, float]] = None,
+) -> Tuple[int, int]:
+    """Cleanse and load factory-level operational facts."""
+
+    cursor = connection.cursor()
+    alias_maps = _load_alias_maps(cursor)
+    seen_keys = set()
+    valid_records: List[MutableMapping[str, object]] = []
+    dq_count = 0
+    anomaly_thresholds = anomaly_thresholds or {}
+
+    for staging_row in staging_rows:
+        row_number = staging_row["row_number"]
+        try:
+            raw_data = json.loads(staging_row["data"])
+        except json.JSONDecodeError:
+            dq_count += 1
+            _record_issue(
+                cursor,
+                batch_id=batch_id,
+                dataset=dataset,
+                row_number=row_number,
+                issue_type="invalid_json",
+                issue_message="staging row contains invalid JSON",
+            )
+            continue
+
+        missing = _validate_required_fields(
+            raw_data,
+            required=["factory_code", "date"],
+            cursor=cursor,
+            batch_id=batch_id,
+            dataset=dataset,
+            row_number=row_number,
+        )
+        if missing:
+            dq_count += len(missing)
+            continue
+
+        try:
+            year, month = _parse_period(str(raw_data.get("date")))
+        except Exception as exc:  # noqa: BLE001
+            dq_count += 1
+            _record_issue(
+                cursor,
+                batch_id=batch_id,
+                dataset=dataset,
+                row_number=row_number,
+                issue_type="invalid_date",
+                issue_message=str(exc),
+                context={"value": raw_data.get("date")},
+            )
+            continue
+
+        factory_code = normalize_factory_code(
+            raw_data.get("factory_code"), alias_lookup=alias_maps.get("factory")
+        )
+        if not factory_code:
+            dq_count += 1
+            _record_issue(
+                cursor,
+                batch_id=batch_id,
+                dataset=dataset,
+                row_number=row_number,
+                issue_type="missing_value",
+                issue_message="factory_code is required",
+            )
+            continue
+
+        key = (factory_code, year, month)
+        if key in seen_keys:
+            dq_count += 1
+            _record_issue(
+                cursor,
+                batch_id=batch_id,
+                dataset=dataset,
+                row_number=row_number,
+                issue_type="duplicate_key",
+                issue_message="duplicate factory+period combination",
+                context={"factory_code": factory_code, "year": year, "month": month},
+            )
+            continue
+        seen_keys.add(key)
+
+        metrics: Dict[str, float] = {}
+        for field in ("revenue", "cost", "output_qty", "downtime_hours"):
+            if raw_data.get(field) is None:
+                continue
+            try:
+                metrics[field] = float(raw_data.get(field))
+            except (TypeError, ValueError):
+                dq_count += 1
+                _record_issue(
+                    cursor,
+                    batch_id=batch_id,
+                    dataset=dataset,
+                    row_number=row_number,
+                    issue_type="invalid_value",
+                    issue_message=f"{field} must be numeric",
+                    context={"field": field},
+                )
+
+        # Allow indicator/value shape for single-metric rows
+        if not metrics and raw_data.get("indicator") and raw_data.get("value") is not None:
+            indicator = str(raw_data.get("indicator")).strip().lower()
+            try:
+                metrics[indicator] = float(raw_data.get("value"))
+            except (TypeError, ValueError):
+                dq_count += 1
+                _record_issue(
+                    cursor,
+                    batch_id=batch_id,
+                    dataset=dataset,
+                    row_number=row_number,
+                    issue_type="invalid_value",
+                    issue_message="value must be numeric",
+                )
+                continue
+
+        if not metrics:
+            dq_count += 1
+            _record_issue(
+                cursor,
+                batch_id=batch_id,
+                dataset=dataset,
+                row_number=row_number,
+                issue_type="missing_value",
+                issue_message="no metrics to load",
+            )
+            continue
+
+        anomaly_found = False
+        for metric, value in metrics.items():
+            threshold = anomaly_thresholds.get(metric, DEFAULT_ANOMALY_THRESHOLD)
+            if abs(value) > threshold:
+                dq_count += 1
+                anomaly_found = True
+                _record_issue(
+                    cursor,
+                    batch_id=batch_id,
+                    dataset=dataset,
+                    row_number=row_number,
+                    issue_type="anomaly",
+                    issue_message=f"{metric} exceeds threshold {threshold}",
+                    context={"metric": metric, "value": value, "threshold": threshold},
+                )
+        if anomaly_found:
+            continue
+
+        valid_records.append(
+            {
+                "factory_code": factory_code,
+                "year": year,
+                "month": month,
+                "revenue": metrics.get("revenue"),
+                "cost": metrics.get("cost"),
+                "output_qty": metrics.get("output_qty"),
+                "downtime_hours": metrics.get("downtime_hours"),
+            }
+        )
+
+    if valid_records:
+        load_fact_operations(connection, valid_records)
+    return len(valid_records), dq_count
+
+
+def _process_kpi(
+    connection: sqlite3.Connection,
+    *,
+    batch_id: int,
+    dataset: str,
+    staging_rows: Sequence[sqlite3.Row],
+    anomaly_thresholds: Optional[Mapping[str, float]] = None,
+) -> Tuple[int, int]:
+    """Cleanse and load employee KPI facts."""
+
+    cursor = connection.cursor()
+    alias_maps = _load_alias_maps(cursor)
+    seen_keys = set()
+    valid_records: List[MutableMapping[str, object]] = []
+    dq_count = 0
+    anomaly_thresholds = {str(k).lower(): v for k, v in (anomaly_thresholds or {}).items()}
+
+    for staging_row in staging_rows:
+        row_number = staging_row["row_number"]
+        try:
+            raw_data = json.loads(staging_row["data"])
+        except json.JSONDecodeError:
+            dq_count += 1
+            _record_issue(
+                cursor,
+                batch_id=batch_id,
+                dataset=dataset,
+                row_number=row_number,
+                issue_type="invalid_json",
+                issue_message="staging row contains invalid JSON",
+            )
+            continue
+
+        missing = _validate_required_fields(
+            raw_data,
+            required=["employee_id", "indicator", "value", "date"],
+            cursor=cursor,
+            batch_id=batch_id,
+            dataset=dataset,
+            row_number=row_number,
+        )
+        if missing:
+            dq_count += len(missing)
+            continue
+
+        try:
+            year, month = _parse_period(str(raw_data.get("date")))
+        except Exception as exc:  # noqa: BLE001
+            dq_count += 1
+            _record_issue(
+                cursor,
+                batch_id=batch_id,
+                dataset=dataset,
+                row_number=row_number,
+                issue_type="invalid_date",
+                issue_message=str(exc),
+                context={"value": raw_data.get("date")},
+            )
+            continue
+
+        metric_code = str(raw_data.get("indicator")).strip()
+        employee_id = normalize_employee_id(
+            raw_data.get("employee_id"), alias_lookup=alias_maps.get("employee")
+        )
+        if not employee_id:
+            dq_count += 1
+            _record_issue(
+                cursor,
+                batch_id=batch_id,
+                dataset=dataset,
+                row_number=row_number,
+                issue_type="missing_value",
+                issue_message="employee_id is required",
+            )
+            continue
+
+        key = (employee_id, year, month, metric_code.lower())
+        if key in seen_keys:
+            dq_count += 1
+            _record_issue(
+                cursor,
+                batch_id=batch_id,
+                dataset=dataset,
+                row_number=row_number,
+                issue_type="duplicate_key",
+                issue_message="duplicate employee+period+metric combination",
+                context={"employee_id": employee_id, "year": year, "month": month, "metric": metric_code},
+            )
+            continue
+        seen_keys.add(key)
+
+        try:
+            value = float(raw_data.get("value"))
+        except (TypeError, ValueError):
+            dq_count += 1
+            _record_issue(
+                cursor,
+                batch_id=batch_id,
+                dataset=dataset,
+                row_number=row_number,
+                issue_type="invalid_value",
+                issue_message="value must be numeric",
+            )
+            continue
+
+        threshold = anomaly_thresholds.get(metric_code.lower(), DEFAULT_ANOMALY_THRESHOLD)
+        if abs(value) > threshold:
+            dq_count += 1
+            _record_issue(
+                cursor,
+                batch_id=batch_id,
+                dataset=dataset,
+                row_number=row_number,
+                issue_type="anomaly",
+                issue_message=f"{metric_code} exceeds threshold {threshold}",
+                context={"metric": metric_code, "value": value, "threshold": threshold},
+            )
+            continue
+
+        factory_code = None
+        if raw_data.get("factory_code") is not None:
+            factory_code = normalize_factory_code(
+                raw_data.get("factory_code"), alias_lookup=alias_maps.get("factory")
+            )
+
+        record: MutableMapping[str, object] = {
+            "employee_id": employee_id,
+            "factory_code": factory_code,
+            "metric_code": metric_code,
+            "value": value,
+            "target": raw_data.get("target"),
+            "year": year,
+            "month": month,
+        }
+        valid_records.append(record)
+
+    if valid_records:
+        load_fact_kpi(connection, valid_records)
+    return len(valid_records), dq_count
+
+
+def run_batch(
+    batch_id: int,
+    *,
+    anomaly_thresholds: Optional[Mapping[str, float]] = None,
+) -> Dict[str, object]:
+    """Run the ETL process for a given upload batch.
+
+    The function will:
+    - Read staging rows for the batch.
+    - Perform data-quality checks (duplicates, missing values, anomalies).
+    - Upsert related dimension tables and load fact tables.
+    - Record any data-quality issues in ``dq_issues``.
+    - Update batch status in ``upload_batches``.
+    """
+
+    start_time = time.time()
+    connection = _get_connection()
+    initialize_warehouse(connection)
+    _ensure_etl_tables(connection)
+    cursor = connection.cursor()
+
+    batch_row = cursor.execute(
+        "SELECT id, dataset FROM upload_batches WHERE id = ?", (batch_id,)
+    ).fetchone()
+    if not batch_row:
+        raise ValueError(f"batch {batch_id} not found")
+
+    dataset = batch_row["dataset"]
+    staging_table_map = {
+        "operations": "stg_operations",
+        "kpi": "stg_kpi_raw",
+    }
+    if dataset not in staging_table_map:
+        raise ValueError(f"unsupported dataset: {dataset}")
+
+    staging_rows = cursor.execute(
+        f"SELECT id, row_number, data FROM {staging_table_map[dataset]} WHERE batch_id = ?",
+        (batch_id,),
+    ).fetchall()
+
+    try:
+        if dataset == "operations":
+            loaded_count, dq_count = _process_operations(
+                connection,
+                batch_id=batch_id,
+                dataset=dataset,
+                staging_rows=staging_rows,
+                anomaly_thresholds=anomaly_thresholds,
+            )
+        else:
+            loaded_count, dq_count = _process_kpi(
+                connection,
+                batch_id=batch_id,
+                dataset=dataset,
+                staging_rows=staging_rows,
+                anomaly_thresholds=anomaly_thresholds,
+            )
+
+        processing_ms = int((time.time() - start_time) * 1000)
+        cursor.execute(
+            """
+            UPDATE upload_batches
+            SET status = 'completed',
+                processed_rows = ?,
+                dq_error_count = ?,
+                processing_ms = ?,
+                completed_at = CURRENT_TIMESTAMP,
+                message = ?
+            WHERE id = ?
+            """,
+            (
+                loaded_count,
+                dq_count,
+                processing_ms,
+                f"Loaded {loaded_count} rows with {dq_count} dq issues",
+                batch_id,
+            ),
+        )
+        connection.commit()
+        return {
+            "batch_id": batch_id,
+            "dataset": dataset,
+            "status": "completed",
+            "loaded_rows": loaded_count,
+            "dq_issues": dq_count,
+            "processing_ms": processing_ms,
+        }
+    except Exception as exc:  # noqa: BLE001 - ensure status updated
+        cursor.execute(
+            """
+            UPDATE upload_batches
+            SET status = 'failed', message = ?, completed_at = CURRENT_TIMESTAMP
+            WHERE id = ?
+            """,
+            (str(exc), batch_id),
+        )
+        connection.commit()
+        raise
+    finally:
+        cursor.close()
+        connection.close()
 
EOF
)
