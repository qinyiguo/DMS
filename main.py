 (cd "$(git rev-parse --show-toplevel)" && git apply --3way <<'EOF' 
diff --git a/main.py b/main.py
index a0c5fc5588005093b4444d56bcf11b24fe2d3918..fa99e6bfbd1db5afdbf91fcb06fac80f7e135b91 100644
--- a/main.py
+++ b/main.py
@@ -1,115 +1,518 @@
 from fastapi import FastAPI, UploadFile, File, HTTPException, Query
 from fastapi.middleware.cors import CORSMiddleware
 from fastapi.responses import HTMLResponse
 import pandas as pd
 import sqlite3
 import json
 import os
 from datetime import datetime
 import hashlib
 from pathlib import Path
+from io import BytesIO
+from typing import List
+
+import validators
 
 app = FastAPI(title="Excel Import API with SQLite")
 
 # CORS 設置
 app.add_middleware(
     CORSMiddleware,
     allow_origins=["*"],
     allow_credentials=True,
     allow_methods=["*"],
     allow_headers=["*"],
 )
 
 # SQLite 資料庫文件路徑
 DB_PATH = "/data/excel_import.db"
 os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
 
 def get_db_connection():
     """獲取資料庫連接"""
     conn = sqlite3.connect(DB_PATH)
     conn.row_factory = sqlite3.Row
     return conn
 
 def init_db():
     """初始化資料庫，建立表"""
     conn = get_db_connection()
     cursor = conn.cursor()
-    
+
     tables = [
         "provincial_operations",
         "parts_sales",
         "repair_income_details",
         "technician_performance",  # 新增 KPI 目標表
     ]
-    
+
     for table_name in tables:
         cursor.execute(f"""
             CREATE TABLE IF NOT EXISTS {table_name} (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 file_name TEXT,
                 row_number INTEGER,
                 data TEXT,
                 file_hash TEXT,
                 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                 updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
             )
         """)
-    
+
+    cursor.execute(
+        """
+        CREATE TABLE IF NOT EXISTS upload_batches (
+            id INTEGER PRIMARY KEY AUTOINCREMENT,
+            dataset TEXT,
+            status TEXT,
+            total_files INTEGER,
+            total_rows INTEGER,
+            valid_rows INTEGER,
+            invalid_rows INTEGER,
+            message TEXT,
+            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
+            completed_at TIMESTAMP
+        )
+        """
+    )
+
+    cursor.execute(
+        """
+        CREATE TABLE IF NOT EXISTS raw_files (
+            id INTEGER PRIMARY KEY AUTOINCREMENT,
+            batch_id INTEGER,
+            file_name TEXT,
+            file_hash TEXT,
+            rows_count INTEGER,
+            valid_rows INTEGER,
+            invalid_rows INTEGER,
+            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
+        )
+        """
+    )
+
+    cursor.execute(
+        """
+        CREATE TABLE IF NOT EXISTS validation_errors (
+            id INTEGER PRIMARY KEY AUTOINCREMENT,
+            batch_id INTEGER,
+            file_name TEXT,
+            row_number INTEGER,
+            column_name TEXT,
+            error_code TEXT,
+            error_message TEXT,
+            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
+        )
+        """
+    )
+
+    cursor.execute(
+        """
+        CREATE TABLE IF NOT EXISTS stg_operations (
+            id INTEGER PRIMARY KEY AUTOINCREMENT,
+            batch_id INTEGER,
+            file_name TEXT,
+            row_number INTEGER,
+            data TEXT,
+            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
+        )
+        """
+    )
+
+    cursor.execute(
+        """
+        CREATE TABLE IF NOT EXISTS stg_kpi_raw (
+            id INTEGER PRIMARY KEY AUTOINCREMENT,
+            batch_id INTEGER,
+            file_name TEXT,
+            row_number INTEGER,
+            data TEXT,
+            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
+        )
+        """
+    )
+
     conn.commit()
     cursor.close()
     conn.close()
 
 def calculate_file_hash(file_content):
     """計算文件的 hash 值"""
     return hashlib.md5(file_content).hexdigest()
 
 def check_file_exists(table_name: str, file_hash: str):
     """檢查文件是否已上傳過"""
     try:
         conn = get_db_connection()
         cursor = conn.cursor()
         
         cursor.execute(
             f"SELECT id, file_name, created_at FROM {table_name} WHERE file_hash = ? LIMIT 1",
             (file_hash,)
         )
         result = cursor.fetchone()
         
         cursor.close()
         conn.close()
         
         return dict(result) if result else None
     except:
         return None
 
+
+REQUIRED_UPLOAD_FIELDS = [
+    validators.FACTORY_CODE,
+    validators.DATE,
+    validators.EMPLOYEE_ID,
+    validators.INDICATOR,
+    validators.VALUE,
+]
+
+ALLOWED_FACTORY_CODES = {
+    code.strip() for code in os.getenv("ALLOWED_FACTORY_CODES", "").split(",") if code.strip()
+} or None
+DEFAULT_ALLOWED_INDICATORS = {"output", "quality", "safety", "kpi_score"}
+ALLOWED_INDICATORS = {
+    item.strip() for item in os.getenv("ALLOWED_INDICATORS", "").split(",") if item.strip()
+} or DEFAULT_ALLOWED_INDICATORS
+
+
+def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
+    """將欄位名稱轉為 snake_case 方便後續驗證。"""
+    df = df.copy()
+    df.columns = [str(col).strip().lower().replace(" ", "_") for col in df.columns]
+    return df
+
+
+def insert_validation_error(cursor, batch_id: int, file_name: str, row_number: int, error: dict):
+    cursor.execute(
+        """
+        INSERT INTO validation_errors (batch_id, file_name, row_number, column_name, error_code, error_message)
+        VALUES (?, ?, ?, ?, ?, ?)
+        """,
+        (
+            batch_id,
+            file_name,
+            row_number,
+            error.get("column"),
+            error.get("error_code"),
+            error.get("message"),
+        ),
+    )
+
+
+def create_upload_batch(cursor, dataset: str) -> int:
+    cursor.execute(
+        """
+        INSERT INTO upload_batches (dataset, status, total_files, total_rows, valid_rows, invalid_rows)
+        VALUES (?, 'processing', 0, 0, 0, 0)
+        """,
+        (dataset,),
+    )
+    return cursor.lastrowid
+
+
+def update_batch_summary(
+    cursor,
+    batch_id: int,
+    *,
+    status: str,
+    total_files: int,
+    total_rows: int,
+    valid_rows: int,
+    invalid_rows: int,
+    message: str = None,
+):
+    cursor.execute(
+        """
+        UPDATE upload_batches
+        SET status = ?,
+            total_files = ?,
+            total_rows = ?,
+            valid_rows = ?,
+            invalid_rows = ?,
+            message = ?,
+            completed_at = CURRENT_TIMESTAMP
+        WHERE id = ?
+        """,
+        (status, total_files, total_rows, valid_rows, invalid_rows, message, batch_id),
+    )
+
+
+async def process_uploads(files: List[UploadFile], dataset: str):
+    staging_table_map = {
+        "operations": "stg_operations",
+        "kpi": "stg_kpi_raw",
+    }
+    if dataset not in staging_table_map:
+        raise HTTPException(status_code=400, detail="Unsupported dataset")
+
+    conn = get_db_connection()
+    cursor = conn.cursor()
+
+    batch_id = create_upload_batch(cursor, dataset)
+    conn.commit()
+
+    total_rows = 0
+    valid_rows = 0
+    invalid_rows = 0
+    file_summaries = []
+    in_memory_errors = []
+
+    try:
+        for upload in files:
+            file_total_rows = 0
+            file_valid_rows = 0
+            file_invalid_rows = 0
+
+            file_content = await upload.read()
+            file_hash = calculate_file_hash(file_content)
+
+            try:
+                df = pd.read_excel(BytesIO(file_content), engine="openpyxl")
+            except Exception as exc:  # noqa: BLE001 - 需要回報檔案錯誤
+                insert_validation_error(
+                    cursor,
+                    batch_id,
+                    upload.filename,
+                    0,
+                    {
+                        "column": "__file__",
+                        "error_code": "invalid_file",
+                        "message": f"failed to read excel: {exc}",
+                    },
+                )
+                file_invalid_rows += 1
+                invalid_rows += 1
+                in_memory_errors.append(
+                    {
+                        "file": upload.filename,
+                        "row": 0,
+                        "column": "__file__",
+                        "error_code": "invalid_file",
+                        "message": str(exc),
+                    }
+                )
+                cursor.execute(
+                    """
+                    INSERT INTO raw_files (batch_id, file_name, file_hash, rows_count, valid_rows, invalid_rows)
+                    VALUES (?, ?, ?, ?, ?, ?)
+                    """,
+                    (
+                        batch_id,
+                        upload.filename,
+                        file_hash,
+                        0,
+                        0,
+                        file_invalid_rows,
+                    ),
+                )
+                file_summaries.append(
+                    {
+                        "file_name": upload.filename,
+                        "rows": 0,
+                        "valid_rows": 0,
+                        "invalid_rows": file_invalid_rows,
+                    }
+                )
+                continue
+
+            df = normalize_columns(df)
+            file_total_rows = len(df)
+            missing_columns = validators.validate_required_columns(df.columns, REQUIRED_UPLOAD_FIELDS)
+            if missing_columns:
+                for column in missing_columns:
+                    insert_validation_error(
+                        cursor,
+                        batch_id,
+                        upload.filename,
+                        0,
+                        {
+                            "column": column,
+                            "error_code": "missing_column",
+                            "message": "required column is missing",
+                        },
+                    )
+                    in_memory_errors.append(
+                        {
+                            "file": upload.filename,
+                            "row": 0,
+                            "column": column,
+                            "error_code": "missing_column",
+                            "message": "required column is missing",
+                        }
+                    )
+
+                file_invalid_rows += len(df)
+                invalid_rows += len(df)
+                total_rows += len(df)
+                cursor.execute(
+                    """
+                    INSERT INTO raw_files (batch_id, file_name, file_hash, rows_count, valid_rows, invalid_rows)
+                    VALUES (?, ?, ?, ?, ?, ?)
+                    """,
+                    (
+                        batch_id,
+                        upload.filename,
+                        file_hash,
+                        file_total_rows,
+                        0,
+                        file_invalid_rows,
+                    ),
+                )
+                file_summaries.append(
+                    {
+                        "file_name": upload.filename,
+                        "rows": file_total_rows,
+                        "valid_rows": 0,
+                        "invalid_rows": file_invalid_rows,
+                    }
+                )
+                continue
+
+            for index, row in df.iterrows():
+                row_number = index + 2  # 資料列從第二行開始
+                row_dict = row.where(pd.notna(row), None).to_dict()
+
+                errors = validators.validate_row(
+                    row_dict,
+                    allowed_factories=ALLOWED_FACTORY_CODES,
+                    allowed_indicators=ALLOWED_INDICATORS,
+                )
+
+                if errors:
+                    file_invalid_rows += 1
+                    invalid_rows += 1
+                    for error in errors:
+                        insert_validation_error(cursor, batch_id, upload.filename, row_number, error)
+                        in_memory_errors.append(
+                            {
+                                "file": upload.filename,
+                                "row": row_number,
+                                **error,
+                            }
+                        )
+                    continue
+
+                cursor.execute(
+                    f"""
+                    INSERT INTO {staging_table_map[dataset]} (batch_id, file_name, row_number, data)
+                    VALUES (?, ?, ?, ?)
+                    """,
+                    (
+                        batch_id,
+                        upload.filename,
+                        row_number,
+                        json.dumps(row_dict, ensure_ascii=False, default=str),
+                    ),
+                )
+                file_valid_rows += 1
+                valid_rows += 1
+
+            cursor.execute(
+                """
+                INSERT INTO raw_files (batch_id, file_name, file_hash, rows_count, valid_rows, invalid_rows)
+                VALUES (?, ?, ?, ?, ?, ?)
+                """,
+                (
+                    batch_id,
+                    upload.filename,
+                    file_hash,
+                    file_total_rows,
+                    file_valid_rows,
+                    file_invalid_rows,
+                ),
+            )
+
+            total_rows += file_total_rows
+            file_summaries.append(
+                {
+                    "file_name": upload.filename,
+                    "rows": file_total_rows,
+                    "valid_rows": file_valid_rows,
+                    "invalid_rows": file_invalid_rows,
+                }
+            )
+
+        update_batch_summary(
+            cursor,
+            batch_id,
+            status="completed" if invalid_rows == 0 else "completed_with_errors",
+            total_files=len(files),
+            total_rows=total_rows,
+            valid_rows=valid_rows,
+            invalid_rows=invalid_rows,
+        )
+        conn.commit()
+
+        return {
+            "batch_id": batch_id,
+            "status": "success" if invalid_rows == 0 else "partial_success",
+            "totals": {
+                "files": len(files),
+                "rows": total_rows,
+                "valid_rows": valid_rows,
+                "invalid_rows": invalid_rows,
+            },
+            "files": file_summaries,
+            "errors": in_memory_errors,
+        }
+    except Exception as exc:  # noqa: BLE001 - 需要確保批次狀態更新
+        update_batch_summary(
+            cursor,
+            batch_id,
+            status="failed",
+            total_files=len(files),
+            total_rows=total_rows,
+            valid_rows=valid_rows,
+            invalid_rows=invalid_rows,
+            message=str(exc),
+        )
+        conn.commit()
+        raise
+    finally:
+        cursor.close()
+        conn.close()
+
 # 初始化資料庫
 init_db()
 
 
 
 
+# ==================== 上傳模組 API ====================
+
+
+@app.post("/api/uploads")
+async def upload_files(
+    files: List[UploadFile] = File(..., description="Multiple Excel files"),
+    dataset: str = Query("operations", description="Target staging dataset: operations or kpi"),
+):
+    return await process_uploads(files, dataset)
+
+
 # ==================== 上傳 Excel 的 API ====================
 
 async def upload_excel(file: UploadFile, table_name: str, allow_duplicate: bool = False):
     """
     智能 Excel 上傳函數
     - 自動檢測重複資料
     - 重複的資料會被更新（覆蓋）
     - 新資料會被新增
     """
     try:
         # 讀取文件內容
         file_content = await file.read()
         file_hash = calculate_file_hash(file_content)
         
         # 讀取 Excel
         df = pd.read_excel(file_content, engine='openpyxl')
         
         # 連接資料庫
         conn = get_db_connection()
         cursor = conn.cursor()
         
         # 定義每個表的唯一性判斷欄位（關鍵欄位組合）
         unique_keys = {
             "parts_sales": ["日期", "銷售人員", "零件編號", "廠別"],  # 零件銷售
             "repair_income_details": ["日期", "技師", "工單號"],      # 維修收入
@@ -334,51 +737,51 @@ def get_single_row(table_name: str, id: int):
         if table_name not in valid_tables:
             raise HTTPException(status_code=400, detail="Invalid table name")
         
         conn = get_db_connection()
         cursor = conn.cursor()
         
         cursor.execute(
             f"SELECT * FROM {table_name} WHERE id = ?",
             (id,)
         )
         row = cursor.fetchone()
         
         cursor.close()
         conn.close()
         
         if not row:
             raise HTTPException(status_code=404, detail="Data not found")
         
         return {"status": "success", "data": dict(row)}
     
     except Exception as e:
         return {"status": "error", "message": str(e)}
 
 # ==================== KPI 分析查詢 API ====================
 
-from typing import Optional, List
+from typing import Optional
 from pydantic import BaseModel
 
 class KPIQueryRequest(BaseModel):
     """KPI 查詢請求模型"""
     year: Optional[int] = None
     month: Optional[int] = None
     factory: Optional[List[str]] = None
     salesperson: Optional[List[str]] = None
     part_category: Optional[List[str]] = None
     function_code: Optional[List[str]] = None
     show_fields: List[str] = ["quantity", "amount"]  # 預設顯示數量和金額
     group_by: str = "salesperson"  # 分組方式: salesperson, factory, part_category
 
 @app.post("/api/kpi/analysis")
 def analyze_kpi(query: KPIQueryRequest):
     """
     彈性 KPI 查詢分析
     可依廠別、銷售人員、零件類別、功能碼等條件篩選
     """
     try:
         conn = get_db_connection()
         cursor = conn.cursor()
         
         # 查詢零件銷售數據
         cursor.execute("SELECT id, data FROM parts_sales")
 
EOF
)
