 (cd "$(git rev-parse --show-toplevel)" && git apply --3way <<'EOF' 
diff --git a/kpi/engine.py b/kpi/engine.py
new file mode 100644
index 0000000000000000000000000000000000000000..0eda24117e8740b3567c1047a298e93c05f38838
--- /dev/null
+++ b/kpi/engine.py
@@ -0,0 +1,452 @@
+"""Config-driven KPI calculation engine."""
+from __future__ import annotations
+
+import ast
+import sqlite3
+from dataclasses import dataclass
+from typing import Callable, Dict, List, Mapping, Optional, Sequence, Tuple
+
+from warehouse_schema import get_or_create_period
+
+
+@dataclass
+class MetricDefinition:
+    """Definition for a KPI metric."""
+
+    metric_code: str
+    scope: str
+    formula: Optional[str] = None
+    aggregation: str = "sum"
+    weight: Optional[float] = None
+    target_source: Optional[str] = None
+
+
+_ALLOWED_AST_NODES = {
+    ast.Expression,
+    ast.BinOp,
+    ast.UnaryOp,
+    ast.Num,
+    ast.Load,
+    ast.Name,
+    ast.Add,
+    ast.Sub,
+    ast.Mult,
+    ast.Div,
+    ast.Pow,
+    ast.Mod,
+    ast.USub,
+    ast.UAdd,
+    ast.Call,
+    ast.Constant,
+    ast.FloorDiv,
+}
+
+
+class KpiEngine:
+    """Compute factory and employee KPIs from fact tables and configuration."""
+
+    def __init__(
+        self,
+        connection: sqlite3.Connection,
+        *,
+        function_map: Optional[Mapping[str, Callable[[Mapping[str, float]], float]]] = None,
+    ) -> None:
+        self.connection = connection
+        self.function_map: Dict[str, Callable[[Mapping[str, float]], float]] = dict(
+            function_map or {}
+        )
+        self._ensure_tables()
+
+    def _ensure_tables(self) -> None:
+        cursor = self.connection.cursor()
+        cursor.execute(
+            """
+            CREATE TABLE IF NOT EXISTS kpi_metrics (
+                metric_code TEXT PRIMARY KEY,
+                scope TEXT NOT NULL CHECK (scope IN ('factory', 'employee')),
+                formula TEXT,
+                aggregation TEXT DEFAULT 'sum',
+                weight REAL,
+                target_source TEXT
+            )
+            """
+        )
+        cursor.execute(
+            """
+            CREATE TABLE IF NOT EXISTS fact_kpi_calc (
+                id INTEGER PRIMARY KEY AUTOINCREMENT,
+                batch_id INTEGER NOT NULL,
+                period_key INTEGER NOT NULL,
+                scope TEXT NOT NULL CHECK (scope IN ('factory', 'employee')),
+                scope_id INTEGER NOT NULL,
+                metric_code TEXT NOT NULL,
+                value REAL,
+                target REAL,
+                weight REAL,
+                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
+            )
+            """
+        )
+        self.connection.commit()
+
+    def calculate(
+        self,
+        *,
+        batch_id: int,
+        period_keys: Optional[Sequence[int]] = None,
+    ) -> None:
+        """Calculate KPIs for the requested periods and persist results."""
+
+        metrics = self._load_metric_definitions()
+        period_index = self._load_period_index()
+        cursor = self.connection.cursor()
+        cursor.execute("DELETE FROM fact_kpi_calc WHERE batch_id = ?", (batch_id,))
+
+        factory_defs = [m for m in metrics if m.scope == "factory"]
+        employee_defs = [m for m in metrics if m.scope == "employee"]
+
+        factory_records = self._prepare_factory_records(period_keys)
+        employee_records = self._prepare_employee_records(period_keys)
+
+        results: List[Mapping[str, object]] = []
+        results.extend(self._calculate_for_scope(factory_records, factory_defs, "factory"))
+        results.extend(
+            self._calculate_for_scope(employee_records, employee_defs, "employee")
+        )
+
+        results.extend(
+            self._rollup_results(results, factory_defs, period_index, "factory", "quarter")
+        )
+        results.extend(
+            self._rollup_results(results, factory_defs, period_index, "factory", "year")
+        )
+        results.extend(
+            self._rollup_results(results, employee_defs, period_index, "employee", "quarter")
+        )
+        results.extend(
+            self._rollup_results(results, employee_defs, period_index, "employee", "year")
+        )
+
+        self._write_results(batch_id, results)
+
+    def _write_results(
+        self, batch_id: int, results: Sequence[Mapping[str, object]]
+    ) -> None:
+        cursor = self.connection.cursor()
+        cursor.executemany(
+            """
+            INSERT INTO fact_kpi_calc (
+                batch_id, period_key, scope, scope_id, metric_code, value, target, weight
+            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
+            """,
+            [
+                (
+                    batch_id,
+                    row["period_key"],
+                    row["scope"],
+                    row["scope_id"],
+                    row["metric_code"],
+                    row.get("value"),
+                    row.get("target"),
+                    row.get("weight"),
+                )
+                for row in results
+            ],
+        )
+        self.connection.commit()
+
+    def _load_period_index(self) -> Mapping[int, Mapping[str, int]]:
+        cursor = self.connection.cursor()
+        rows = cursor.execute(
+            "SELECT period_key, month, quarter, year FROM dim_period"
+        ).fetchall()
+        return {row[0]: {"month": row[1], "quarter": row[2], "year": row[3]} for row in rows}
+
+    def _load_metric_definitions(self) -> List[MetricDefinition]:
+        cursor = self.connection.cursor()
+        rows = cursor.execute(
+            """
+            SELECT metric_code, scope, formula, aggregation, weight, target_source
+            FROM kpi_metrics
+            """
+        ).fetchall()
+        return [
+            MetricDefinition(
+                metric_code=row[0],
+                scope=row[1],
+                formula=row[2],
+                aggregation=row[3] or "sum",
+                weight=row[4],
+                target_source=row[5],
+            )
+            for row in rows
+        ]
+
+    def _prepare_factory_records(
+        self, period_filter: Optional[Sequence[int]]
+    ) -> List[Mapping[str, object]]:
+        cursor = self.connection.cursor()
+        params: Tuple[object, ...] = ()
+        filter_sql = ""
+        if period_filter:
+            placeholders = ",".join("?" for _ in period_filter)
+            filter_sql = f"AND fo.period_key IN ({placeholders})"
+            params = tuple(period_filter)
+
+        rows = cursor.execute(
+            f"""
+            SELECT
+                fo.factory_key,
+                fo.period_key,
+                SUM(fo.revenue) AS revenue,
+                SUM(fo.cost) AS cost,
+                SUM(fo.output_qty) AS output_qty,
+                SUM(fo.downtime_hours) AS downtime_hours,
+                dp.month,
+                dp.quarter,
+                dp.year
+            FROM fact_operations fo
+            JOIN dim_period dp ON fo.period_key = dp.period_key
+            WHERE 1=1 {filter_sql}
+            GROUP BY fo.factory_key, fo.period_key, dp.month, dp.quarter, dp.year
+            """,
+            params,
+        ).fetchall()
+
+        records: List[Mapping[str, object]] = []
+        for row in rows:
+            base_context = {
+                "revenue": row[2],
+                "cost": row[3],
+                "output_qty": row[4],
+                "downtime_hours": row[5],
+            }
+            records.append(
+                {
+                    "scope_id": row[0],
+                    "period_key": row[1],
+                    "period": {"month": row[6], "quarter": row[7], "year": row[8]},
+                    "base": base_context,
+                    "targets": {},
+                }
+            )
+        return records
+
+    def _prepare_employee_records(
+        self, period_filter: Optional[Sequence[int]]
+    ) -> List[Mapping[str, object]]:
+        cursor = self.connection.cursor()
+        params: Tuple[object, ...] = ()
+        filter_sql = ""
+        if period_filter:
+            placeholders = ",".join("?" for _ in period_filter)
+            filter_sql = f"AND fk.period_key IN ({placeholders})"
+            params = tuple(period_filter)
+
+        rows = cursor.execute(
+            f"""
+            SELECT
+                fk.employee_key,
+                fk.period_key,
+                fk.metric_code,
+                SUM(fk.value) AS value,
+                AVG(fk.target) AS target,
+                dp.month,
+                dp.quarter,
+                dp.year
+            FROM fact_kpi fk
+            JOIN dim_period dp ON fk.period_key = dp.period_key
+            WHERE 1=1 {filter_sql}
+            GROUP BY fk.employee_key, fk.period_key, fk.metric_code, dp.month, dp.quarter, dp.year
+            """,
+            params,
+        ).fetchall()
+
+        grouped: Dict[Tuple[int, int], Dict[str, object]] = {}
+        for row in rows:
+            key = (row[0], row[1])
+            payload = grouped.setdefault(
+                key,
+                {
+                    "scope_id": row[0],
+                    "period_key": row[1],
+                    "period": {"month": row[5], "quarter": row[6], "year": row[7]},
+                    "base": {},
+                    "targets": {},
+                },
+            )
+            payload["base"][row[2]] = row[3]
+            if row[4] is not None:
+                payload["targets"][row[2]] = row[4]
+
+        return list(grouped.values())
+
+    def _calculate_for_scope(
+        self,
+        records: Sequence[Mapping[str, object]],
+        metric_defs: Sequence[MetricDefinition],
+        scope: str,
+    ) -> List[Mapping[str, object]]:
+        results: List[Mapping[str, object]] = []
+        for record in records:
+            computed = self._evaluate_metrics(metric_defs, record["base"])
+            for metric in metric_defs:
+                if metric.metric_code not in computed:
+                    continue
+                target = None
+                if metric.target_source == "fact_kpi":
+                    target = record["targets"].get(metric.metric_code)
+                results.append(
+                    {
+                        "scope": scope,
+                        "scope_id": record["scope_id"],
+                        "period_key": record["period_key"],
+                        "metric_code": metric.metric_code,
+                        "value": computed.get(metric.metric_code),
+                        "target": target,
+                        "weight": metric.weight,
+                    }
+                )
+        return results
+
+    def _evaluate_metrics(
+        self, metric_defs: Sequence[MetricDefinition], base_context: Mapping[str, float]
+    ) -> Mapping[str, Optional[float]]:
+        computed: Dict[str, Optional[float]] = {}
+        context = dict(base_context)
+
+        for _ in range(len(metric_defs) or 1):
+            progress = False
+            for metric in metric_defs:
+                if metric.metric_code in computed:
+                    continue
+                value = self._evaluate_metric(metric, context)
+                if value is None and not self._dependencies_ready(metric, context):
+                    continue
+                computed[metric.metric_code] = value
+                if value is not None:
+                    context[metric.metric_code] = value
+                progress = True
+            if not progress:
+                break
+        return computed
+
+    def _dependencies_ready(self, metric: MetricDefinition, context: Mapping[str, float]) -> bool:
+        if not metric.formula:
+            return True
+        expression = self._normalize_expression(metric.formula)
+        names = {node.id for node in ast.walk(ast.parse(expression)) if isinstance(node, ast.Name)}
+        return names.issubset(context.keys() | set(self.function_map.keys()))
+
+    def _evaluate_metric(
+        self, metric: MetricDefinition, context: Mapping[str, float]
+    ) -> Optional[float]:
+        if metric.formula is None:
+            return context.get(metric.metric_code)
+
+        if metric.formula in self.function_map:
+            try:
+                return float(self.function_map[metric.formula](context))
+            except Exception:  # noqa: BLE001
+                return None
+
+        expression = self._normalize_expression(metric.formula)
+        try:
+            tree = ast.parse(expression, mode="eval")
+            for node in ast.walk(tree):
+                if not isinstance(node, tuple(_ALLOWED_AST_NODES)):
+                    raise ValueError("Unsupported expression component")
+            code_obj = compile(tree, "<metric>", "eval")
+            return float(eval(code_obj, {}, context))  # noqa: S307
+        except Exception:  # noqa: BLE001
+            return None
+
+    def _normalize_expression(self, expression: str) -> str:
+        if "=" in expression:
+            _, rhs = expression.split("=", 1)
+            return rhs.strip()
+        return expression.strip()
+
+    def _rollup_results(
+        self,
+        monthly_results: Sequence[Mapping[str, object]],
+        metric_defs: Sequence[MetricDefinition],
+        period_index: Mapping[int, Mapping[str, int]],
+        scope: str,
+        level: str,
+    ) -> List[Mapping[str, object]]:
+        if level not in {"quarter", "year"}:
+            return []
+
+        metrics = {m.metric_code: m for m in metric_defs}
+        grouped: Dict[Tuple[int, str, int, Optional[int]], List[Tuple[int, float, Optional[float]]]] = {}
+
+        for row in monthly_results:
+            if row["scope"] != scope:
+                continue
+            period = period_index.get(row["period_key"])
+            if not period:
+                continue
+            key = (
+                row["scope_id"],
+                row["metric_code"],
+                period["year"],
+                period["quarter"] if level == "quarter" else None,
+            )
+            grouped.setdefault(key, []).append(
+                (period["month"], row.get("value"), row.get("target"))
+            )
+
+        aggregated: List[Mapping[str, object]] = []
+        for (scope_id, metric_code, year, quarter), values in grouped.items():
+            metric = metrics.get(metric_code)
+            if not metric:
+                continue
+            numeric_values = [v for _, v, _ in values if v is not None]
+            target_values = [t for _, _, t in values if t is not None]
+            value = self._aggregate_values(numeric_values, values, metric.aggregation)
+            target = self._aggregate_values(target_values, values, metric.aggregation)
+            period_key = self._resolve_rollup_period(year, quarter, level)
+            aggregated.append(
+                {
+                    "scope": scope,
+                    "scope_id": scope_id,
+                    "period_key": period_key,
+                    "metric_code": metric_code,
+                    "value": value,
+                    "target": target,
+                    "weight": metric.weight,
+                }
+            )
+        return aggregated
+
+    def _aggregate_values(
+        self,
+        numeric_values: Sequence[float],
+        values_with_month: Sequence[Tuple[int, Optional[float], Optional[float]]],
+        method: str,
+    ) -> Optional[float]:
+        if not numeric_values:
+            return None
+        method = (method or "sum").lower()
+        if method == "avg" or method == "mean":
+            return float(sum(numeric_values) / len(numeric_values))
+        if method == "max":
+            return float(max(numeric_values))
+        if method == "min":
+            return float(min(numeric_values))
+        if method == "latest":
+            latest = max(values_with_month, key=lambda item: item[0])
+            return float(latest[1]) if latest[1] is not None else None
+        return float(sum(numeric_values))
+
+    def _resolve_rollup_period(
+        self, year: int, quarter: Optional[int], level: str
+    ) -> int:
+        if level == "quarter":
+            month = (quarter or 1) * 3
+            return get_or_create_period(self.connection, month=month, year=year)
+        return get_or_create_period(self.connection, month=12, year=year)
+
+
+__all__ = ["KpiEngine", "MetricDefinition"]
 
EOF
)
