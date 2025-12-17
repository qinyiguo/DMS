 (cd "$(git rev-parse --show-toplevel)" && git apply --3way <<'EOF' 
diff --git a/validators.py b/validators.py
new file mode 100644
index 0000000000000000000000000000000000000000..01f17bf64e597bc65ea6eafaf02ca31ed55c951a
--- /dev/null
+++ b/validators.py
@@ -0,0 +1,107 @@
+from __future__ import annotations
+
+from datetime import datetime
+from typing import Any, Dict, Iterable, List, Optional, Set
+
+# 欄位名稱常數
+FACTORY_CODE = "factory_code"
+DATE = "date"
+EMPLOYEE_ID = "employee_id"
+INDICATOR = "indicator"
+VALUE = "value"
+
+ALLOWED_DATE_FORMATS = ["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"]
+
+
+def validate_required_columns(columns: Iterable[str], required: Iterable[str]) -> List[str]:
+    """檢查必要欄位是否存在。"""
+    column_set = {str(col).strip().lower() for col in columns}
+    required_set = {str(col).strip().lower() for col in required}
+    return sorted(required_set - column_set)
+
+
+def parse_date(value: Any) -> str:
+    """將日期欄位轉換為 ISO 格式字串，若格式不符則擲出 ValueError。"""
+    if value is None:
+        raise ValueError("date is required")
+
+    if isinstance(value, datetime):
+        return value.strftime("%Y-%m-%d")
+
+    text = str(value).strip()
+    for fmt in ALLOWED_DATE_FORMATS:
+        try:
+            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
+        except ValueError:
+            continue
+    raise ValueError("date format must be YYYY-MM-DD, YYYY/MM/DD, or YYYYMMDD")
+
+
+def validate_row(
+    row: Dict[str, Any],
+    *,
+    allowed_factories: Optional[Set[str]] = None,
+    allowed_indicators: Optional[Set[str]] = None,
+) -> List[Dict[str, str]]:
+    """驗證單筆資料，回傳錯誤列表。"""
+    errors: List[Dict[str, str]] = []
+
+    factory_code = row.get(FACTORY_CODE)
+    if not factory_code or str(factory_code).strip() == "":
+        errors.append({
+            "column": FACTORY_CODE,
+            "error_code": "missing_field",
+            "message": "factory_code is required",
+        })
+    elif allowed_factories and str(factory_code) not in allowed_factories:
+        errors.append({
+            "column": FACTORY_CODE,
+            "error_code": "invalid_value",
+            "message": f"factory_code must be one of: {', '.join(sorted(allowed_factories))}",
+        })
+
+    date_value = row.get(DATE)
+    try:
+        parsed_date = parse_date(date_value)
+        row[DATE] = parsed_date
+    except Exception as exc:  # noqa: BLE001 - 返回驗證錯誤
+        errors.append({
+            "column": DATE,
+            "error_code": "invalid_format",
+            "message": str(exc),
+        })
+
+    employee_id = row.get(EMPLOYEE_ID)
+    if not employee_id or str(employee_id).strip() == "":
+        errors.append({
+            "column": EMPLOYEE_ID,
+            "error_code": "missing_field",
+            "message": "employee_id is required",
+        })
+
+    indicator = row.get(INDICATOR)
+    if not indicator or str(indicator).strip() == "":
+        errors.append({
+            "column": INDICATOR,
+            "error_code": "missing_field",
+            "message": "indicator is required",
+        })
+    elif allowed_indicators and str(indicator) not in allowed_indicators:
+        errors.append({
+            "column": INDICATOR,
+            "error_code": "invalid_value",
+            "message": f"indicator must be one of: {', '.join(sorted(allowed_indicators))}",
+        })
+
+    value = row.get(VALUE)
+    try:
+        numeric_value = float(value)
+        row[VALUE] = numeric_value
+    except (TypeError, ValueError):
+        errors.append({
+            "column": VALUE,
+            "error_code": "invalid_type",
+            "message": "value must be numeric",
+        })
+
+    return errors
 
EOF
)
