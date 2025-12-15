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
    
    tables = [
        "provincial_operations",
        "parts_sales",
        "repair_income_details",
        "technician_performance"
    ]
    
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

# 初始化資料庫
init_db()

@app.get("/")
def read_root():
    return {"message": "Excel Import API is running"}

# ==================== 上傳 Excel 的 API ====================

@app.post("/upload/provincial-operations")
async def upload_provincial_operations(file: UploadFile = File(...), allow_duplicate: bool = Query(False)):
    """上傳全省營運數據"""
    return await upload_excel(file, "provincial_operations", allow_duplicate)

@app.post("/upload/parts-sales")
async def upload_parts_sales(file: UploadFile = File(...), allow_duplicate: bool = Query(False)):
    """上傳零件銷售資料"""
    return await upload_excel(file, "parts_sales", allow_duplicate)

@app.post("/upload/repair-income")
async def upload_repair_income(file: UploadFile = File(...), allow_duplicate: bool = Query(False)):
    """上傳維修收入明細"""
    return await upload_excel(file, "repair_income_details", allow_duplicate)

@app.post("/upload/technician-performance")
async def upload_technician_performance(file: UploadFile = File(...), allow_duplicate: bool = Query(False)):
    """上傳技師績效"""
    return await upload_excel(file, "technician_performance", allow_duplicate)

async def upload_excel(file: UploadFile, table_name: str, allow_duplicate: bool = False):
    """通用 Excel 上傳函數"""
    try:
        # 讀取文件內容
        file_content = await file.read()
        file_hash = calculate_file_hash(file_content)
        
        # 檢查文件是否已上傳
        existing_file = check_file_exists(table_name, file_hash)
        if existing_file and not allow_duplicate:
            return {
                "status": "warning",
                "message": f"此文件已於 {existing_file['created_at']} 上傳過",
                "table": table_name,
                "existing_file": existing_file['file_name'],
                "hint": "如要重新上傳，請添加參數 ?allow_duplicate=true"
            }
        
        # 讀取 Excel
        df = pd.read_excel(file_content, engine='openpyxl')
        
        # 連接資料庫
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 逐行匯入
        inserted_count = 0
        for index, row in df.iterrows():
            # 將 NaN 轉換為 None
            data_dict = row.where(pd.notna(row), None).to_dict()
            
            cursor.execute(
                f"INSERT INTO {table_name} (file_name, row_number, data, file_hash) VALUES (?, ?, ?, ?)",
                (file.filename, index + 1, json.dumps(data_dict, ensure_ascii=False, default=str), file_hash)
            )
            inserted_count += 1
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "status": "success",
            "message": f"成功匯入 {inserted_count} 筆數據",
            "table": table_name,
            "rows": inserted_count,
            "filename": file.filename,
            "file_hash": file_hash
        }
    
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "table": table_name
        }

# ==================== 查詢數據的 API ====================

@app.get("/data/{table_name}")
def get_data(table_name: str, limit: int = 100, offset: int = 0, file_name: str = None):
    """查詢表中的所有數據"""
    try:
        # 驗證表名（防止 SQL 注入）
        valid_tables = ["provincial_operations", "parts_sales", "repair_income_details", "technician_performance"]
        if table_name not in valid_tables:
            raise HTTPException(status_code=400, detail="Invalid table name")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 構建查詢條件
        where_clause = ""
        params = []
        if file_name:
            where_clause = "WHERE file_name = ?"
            params.append(file_name)
        
        # 查詢總數
        cursor.execute(f"SELECT COUNT(*) as total FROM {table_name} {where_clause}", params)
        total = cursor.fetchone()["total"]
        
        # 查詢數據
        cursor.execute(
            f"SELECT id, file_name, row_number, data, created_at FROM {table_name} {where_clause} ORDER BY created_at DESC LIMIT ? OFFSET ?",
            params + [limit, offset]
        )
        rows = [dict(row) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return {
            "status": "success",
            "table": table_name,
            "total": total,
            "limit": limit,
            "offset": offset,
            "file_name_filter": file_name,
            "data": rows
        }
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/data/{table_name}/{id}")
def get_single_row(table_name: str, id: int):
    """查詢單筆數據"""
    try:
        valid_tables = ["provincial_operations", "parts_sales", "repair_income_details", "technician_performance"]
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

# ==================== 修改數據的 API（管理者） ====================

@app.put("/data/{table_name}/{id}")
def update_data(table_name: str, id: int, updated_data: dict):
    """修改單筆數據（管理者功能）"""
    try:
        valid_tables = ["provincial_operations", "parts_sales", "repair_income_details", "technician_performance"]
        if table_name not in valid_tables:
            raise HTTPException(status_code=400, detail="Invalid table name")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 更新 data 欄位
        cursor.execute(
            f"UPDATE {table_name} SET data = ?, updated_at = ? WHERE id = ?",
            (json.dumps(updated_data, ensure_ascii=False, default=str), datetime.now(), id)
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "status": "success",
            "message": "數據已更新",
            "table": table_name,
            "id": id
        }
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ==================== 統計數據 ====================

@app.get("/stats")
def get_stats():
    """獲取所有表的統計信息"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        tables = ["provincial_operations", "parts_sales", "repair_income_details", "technician_performance"]
        stats = {}
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            count = cursor.fetchone()["count"]
            
            # 查詢不同的文件數
            cursor.execute(f"SELECT COUNT(DISTINCT file_name) as file_count FROM {table}")
            file_count = cursor.fetchone()["file_count"]
            
            stats[table] = {
                "total_rows": count,
                "total_files": file_count
            }
        
        cursor.close()
        conn.close()
        
        return {"status": "success", "stats": stats}
    
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ==================== 前端頁面 ====================

@app.get("/ui", response_class=HTMLResponse)
def get_frontend():
    """前端管理界面"""
    return """
    <!DOCTYPE html>
    <html lang="zh-TW">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Excel 數據管理系統</title>
        <style>
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                padding: 20px;
            }
            
            .container {
                max-width: 1200px;
                margin: 0 auto;
            }
            
            header {
                text-align: center;
                color: white;
                margin-bottom: 40px;
            }
            
            header h1 {
                font-size: 2.5em;
                margin-bottom: 10px;
            }
            
            header p {
                font-size: 1.1em;
                opacity: 0.9;
            }
            
            .tabs {
                display: flex;
                gap: 10px;
                margin-bottom: 20px;
                flex-wrap: wrap;
            }
            
            .tab-button {
                padding: 12px 24px;
                border: none;
                background: white;
                color: #667eea;
                font-size: 1em;
                font-weight: bold;
                border-radius: 8px;
                cursor: pointer;
                transition: all 0.3s;
            }
            
            .tab-button.active {
                background: #667eea;
                color: white;
                box-shadow: 0 4px 15px rgba(0,0,0,0.2);
            }
            
            .tab-button:hover {
                transform: translateY(-2px);
                box-shadow: 0 4px 15px rgba(0,0,0,0.2);
            }
            
            .tab-content {
                display: none;
                background: white;
                border-radius: 12px;
                padding: 30px;
                box-shadow: 0 10px 40px rgba(0,0,0,0.2);
            }
            
            .tab-content.active {
                display: block;
            }
            
            .upload-section {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 20px;
                margin-bottom: 30px;
            }
            
            .upload-card {
                border: 2px dashed #667eea;
                border-radius: 8px;
                padding: 20px;
                text-align: center;
                cursor: pointer;
                transition: all 0.3s;
            }
            
            .upload-card:hover {
                background: #f0f4ff;
                border-color: #764ba2;
            }
            
            .upload-card h3 {
                color: #667eea;
                margin-bottom: 10px;
            }
            
            .upload-card p {
                color: #666;
                font-size: 0.9em;
                margin-bottom: 15px;
            }
            
            .upload-card input[type="file"] {
                display: none;
            }
            
            .upload-btn {
                background: #667eea;
                color: white;
                padding: 10px 20px;
                border: none;
                border-radius: 6px;
                cursor: pointer;
                font-weight: bold;
                transition: all 0.3s;
            }
            
            .upload-btn:hover {
                background: #764ba2;
            }
            
            .upload-progress {
                margin-top: 10px;
                display: none;
            }
            
            .progress-bar {
                width: 100%;
                height: 6px;
                background: #eee;
                border-radius: 3px;
                overflow: hidden;
            }
            
            .progress-fill {
                height: 100%;
                background: #667eea;
                width: 0%;
                transition: width 0.3s;
            }
            
            .message {
                padding: 12px;
                border-radius: 6px;
                margin-top: 10px;
                font-size: 0.9em;
            }
            
            .message.success {
                background: #d4edda;
                color: #155724;
                border: 1px solid #c3e6cb;
            }
            
            .message.error {
                background: #f8d7da;
                color: #721c24;
                border: 1px solid #f5c6cb;
            }
            
            .stats {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 15px;
                margin-bottom: 30px;
            }
            
            .stat-card {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 20px;
                border-radius: 8px;
                text-align: center;
            }
            
            .stat-card h4 {
                font-size: 0.9em;
                opacity: 0.9;
                margin-bottom: 10px;
            }
            
            .stat-card .number {
                font-size: 2em;
                font-weight: bold;
            }
            
            .data-table {
                width: 100%;
                border-collapse: collapse;
                margin-top: 20px;
            }
            
            .data-table thead {
                background: #f8f9fa;
            }
            
            .data-table th {
                padding: 12px;
                text-align: left;
                font-weight: bold;
                color: #667eea;
                border-bottom: 2px solid #667eea;
            }
            
            .data-table td {
                padding: 12px;
                border-bottom: 1px solid #eee;
            }
            
            .data-table tr:hover {
                background: #f8f9fa;
            }
            
            .table-controls {
                display: flex;
                gap: 10px;
                margin-bottom: 20px;
                flex-wrap: wrap;
            }
            
            .search-box {
                flex: 1;
                min-width: 200px;
                padding: 10px;
                border: 1px solid #ddd;
                border-radius: 6px;
                font-size: 1em;
            }
            
            .btn {
                padding: 10px 20px;
                border: none;
                border-radius: 6px;
                cursor: pointer;
                font-weight: bold;
                transition: all 0.3s;
            }
            
            .btn-primary {
                background: #667eea;
                color: white;
            }
            
            .btn-primary:hover {
                background: #764ba2;
            }
            
            .btn-small {
                padding: 6px 12px;
                font-size: 0.9em;
            }
            
            .loading {
                text-align: center;
                padding: 20px;
                color: #667eea;
            }
            
            .spinner {
                border: 4px solid #f3f3f3;
                border-top: 4px solid #667eea;
                border-radius: 50%;
                width: 40px;
                height: 40px;
                animation: spin 1s linear infinite;
                margin: 0 auto 10px;
            }
            
            @keyframes spin {
                0% { transform: rotate(0deg); }
                100% { transform: rotate(360deg); }
            }
            
            .modal {
                display: none;
                position: fixed;
                z-index: 1000;
                left: 0;
                top: 0;
                width: 100%;
                height: 100%;
                background-color: rgba(0,0,0,0.5);
            }
            
            .modal.active {
                display: flex;
                align-items: center;
                justify-content: center;
            }
            
            .modal-content {
                background-color: white;
                padding: 30px;
                border-radius: 12px;
                max-width: 600px;
                width: 90%;
                max-height: 80vh;
                overflow-y: auto;
            }
            
            .modal-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 20px;
            }
            
            .modal-header h2 {
                color: #667eea;
            }
            
            .close-btn {
                background: none;
                border: none;
                font-size: 1.5em;
                cursor: pointer;
                color: #666;
            }
            
            .form-group {
                margin-bottom: 15px;
            }
            
            .form-group label {
                display: block;
                margin-bottom: 5px;
                color: #333;
                font-weight: bold;
            }
            
            .form-group input,
            .form-group textarea {
                width: 100%;
                padding: 10px;
                border: 1px solid #ddd;
                border-radius: 6px;
                font-size: 1em;
            }
            
            .form-group textarea {
                resize: vertical;
                min-height: 100px;
            }
            
            .modal-footer {
                display: flex;
                gap: 10px;
                justify-content: flex-end;
                margin-top: 20px;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>📊 Excel 數據管理系統</h1>
                <p>輕鬆上傳、查詢和管理你的數據</p>
            </header>
            
            <div class="tabs">
                <button class="tab-button active" onclick="switchTab('upload')">📤 上傳數據</button>
                <button class="tab-button" onclick="switchTab('data')">📋 數據明細</button>
                <button class="tab-button" onclick="switchTab('stats')">📈 統計信息</button>
            </div>
            
            <!-- 上傳頁面 -->
            <div id="upload" class="tab-content active">
                <h2>上傳 Excel 文件</h2>
                <p style="color: #666; margin-bottom: 20px;">選擇對應的表格上傳你的 Excel 文件</p>
                
                <div class="upload-section">
                    <div class="upload-card">
                        <h3>🏢 全省營運數據</h3>
                        <p>provincial_operations</p>
                        <button class="upload-btn" onclick="document.getElementById('file-provincial').click()">選擇文件</button>
                        <input type="file" id="file-provincial" accept=".xlsx,.xls" onchange="uploadFile(this, 'provincial-operations')">
                        <div class="upload-progress" id="progress-provincial">
                            <div class="progress-bar">
                                <div class="progress-fill"></div>
                            </div>
                        </div>
                        <div id="message-provincial"></div>
                    </div>
                    
                    <div class="upload-card">
                        <h3>🔧 零件銷售資料</h3>
                        <p>parts_sales</p>
                        <button class="upload-btn" onclick="document.getElementById('file-parts').click()">選擇文件</button>
                        <input type="file" id="file-parts" accept=".xlsx,.xls" onchange="uploadFile(this, 'parts-sales')">
                        <div class="upload-progress" id="progress-parts">
                            <div class="progress-bar">
                                <div class="progress-fill"></div>
                            </div>
                        </div>
                        <div id="message-parts"></div>
                    </div>
                    
                    <div class="upload-card">
                        <h3>💰 維修收入明細</h3>
                        <p>repair_income_details</p>
                        <button class="upload-btn" onclick="document.getElementById('file-repair').click()">選擇文件</button>
                        <input type="file" id="file-repair" accept=".xlsx,.xls" onchange="uploadFile(this, 'repair-income')">
                        <div class="upload-progress" id="progress-repair">
                            <div class="progress-bar">
                                <div class="progress-fill"></div>
                            </div>
                        </div>
                        <div id="message-repair"></div>
                    </div>
                    
                    <div class="upload-card">
                        <h3>👨‍💼 技師績效</h3>
                        <p>technician_performance</p>
                        <button class="upload-btn" onclick="document.getElementById('file-technician').click()">選擇文件</button>
                        <input type="file" id="file-technician" accept=".xlsx,.xls" onchange="uploadFile(this, 'technician-performance')">
                        <div class="upload-progress" id="progress-technician">
                            <div class="progress-bar">
                                <div class="progress-fill"></div>
                            </div>
                        </div>
                        <div id="message-technician"></div>
                    </div>
                </div>
            </div>
            
            <!-- 數據明細頁面 -->
            <div id="data" class="tab-content">
                <h2>數據明細</h2>
                
                <div class="table-controls">
                    <select id="table-select" onchange="loadTableData()" style="padding: 10px; border: 1px solid #ddd; border-radius: 6px; font-size: 1em;">
                        <option value="provincial_operations">全省營運數據</option>
                        <option value="parts_sales">零件銷售資料</option>
                        <option value="repair_income_details">維修收入明細</option>
                        <option value="technician_performance">技師績效</option>
                    </select>
                    <input type="text" id="search-box" class="search-box" placeholder="搜尋文件名..." onkeyup="loadTableData()">
                    <button class="btn btn-primary" onclick="loadTableData()">🔄 刷新</button>
                </div>
                
                <div id="data-container">
                    <div class="loading">
                        <div class="spinner"></div>
                        <p>加載中...</p>
                    </div>
                </div>
            </div>
            
            <!-- 統計信息頁面 -->
            <div id="stats" class="tab-content">
                <h2>統計信息</h2>
                <div id="stats-container">
                    <div class="loading">
                        <div class="spinner"></div>
                        <p>加載中...</p>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- 詳細信息模態框 -->
        <div id="detailModal" class="modal">
            <div class="modal-content">
                <div class="modal-header">
                    <h2>數據詳情</h2>
                    <button class="close-btn" onclick="closeModal()">×</button>
                </div>
                <div id="modal-body"></div>
                <div class="modal-footer">
                    <button class="btn btn-primary" onclick="closeModal()">關閉</button>
                </div>
            </div>
        </div>
        
        <script>
            function switchTab(tabName) {
                const tabs = document.querySelectorAll('.tab-content');
                tabs.forEach(tab => tab.classList.remove('active'));
                
                const buttons = document.querySelectorAll('.tab-button');
                buttons.forEach(btn => btn.classList.remove('active'));
                
                document.getElementById(tabName).classList.add('active');
                event.target.classList.add('active');
                
                if (tabName === 'data') {
                    loadTableData();
                } else if (tabName === 'stats') {
                    loadStats();
                }
            }
            
            async function uploadFile(input, endpoint) {
                if (!input.files[0]) return;
                
                const file = input.files[0];
                const formData = new FormData();
                formData.append('file', file);
                
                const prefix = endpoint.split('-')[0];
                const progressDiv = document.getElementById(`progress-${prefix}`);
                const messageDiv = document.getElementById(`message-${prefix}`);
                
                progressDiv.style.display = 'block';
                messageDiv.innerHTML = '';
                
                try {
                    const response = await fetch(`/upload/${endpoint}`, {
                        method: 'POST',
                        body: formData
                    });
                    
                    const data = await response.json();
                    
                    if (data.status === 'success') {
                        messageDiv.innerHTML = `<div class="message success">✓ ${data.message}</div>`;
                        input.value = '';
                    } else if (data.status === 'warning') {
                        messageDiv.innerHTML = `<div class="message success">⚠️ ${data.message}</div>`;
                    } else {
                        messageDiv.innerHTML = `<div class="message error">✗ ${data.message || '上傳失敗'}</div>`;
                    }
                } catch (error) {
                    console.error('Upload error:', error);
                    messageDiv.innerHTML = `<div class="message error">✗ 上傳失敗: ${error.message}</div>`;
                }
                
                progressDiv.style.display = 'none';
            }
            
            async function loadTableData() {
                const tableName = document.getElementById('table-select').value;
                const searchTerm = document.getElementById('search-box').value;
                const container = document.getElementById('data-container');
                
                container.innerHTML = '<div class="loading"><div class="spinner"></div><p>加載中...</p></div>';
                
                try {
                    let url = `/data/${tableName}?limit=100`;
                    if (searchTerm) {
                        url += `&file_name=${encodeURIComponent(searchTerm)}`;
                    }
                    
                    const response = await fetch(url);
                    const data = await response.json();
                    
                    if (data.status === 'success' && data.data.length > 0) {
                        let html = `<p style="color: #666; margin-bottom: 15px;">共 ${data.total} 筆數據</p>`;
                        html += '<table class="data-table"><thead><tr>';
                        html += '<th>ID</th><th>文件名</th><th>行號</th><th>上傳時間</th><th>操作</th>';
                        html += '</tr></thead><tbody>';
                        
                        data.data.forEach(row => {
                            const date = new Date(row.created_at).toLocaleString('zh-TW');
                            html += `<tr>
                                <td>${row.id}</td>
                                <td>${row.file_name}</td>
                                <td>${row.row_number}</td>
                                <td>${date}</td>
                                <td><button class="btn btn-small btn-primary" onclick="showDetail('${tableName}', ${row.id})">查看</button></td>
                            </tr>`;
                        });
                        
                        html += '</tbody></table>';
                        container.innerHTML = html;
                    } else {
                        container.innerHTML = '<p style="text-align: center; color: #999; padding: 40px;">暫無數據</p>';
                    }
                } catch (error) {
                    container.innerHTML = `<p style="color: red;">加載失敗: ${error.message}</p>`;
                }
            }
            
            async function showDetail(tableName, id) {
                try {
                    const response = await fetch(`/data/${tableName}/${id}`);
                    const data = await response.json();
                    
                    if (data.status === 'success') {
                        const row = data.data;
                        const rowData = JSON.parse(row.data);
                        
                        let html = '<div class="form-group">';
                        html += `<label>ID</label><input type="text" value="${row.id}" readonly>`;
                        html += '</div>';
                        
                        html += '<div class="form-group">';
                        html += `<label>文件名</label><input type="text" value="${row.file_name}" readonly>`;
                        html += '</div>';
                        
                        html += '<div class="form-group">';
                        html += `<label>行號</label><input type="text" value="${row.row_number}" readonly>`;
                        html += '</div>';
                        
                        html += '<div class="form-group">';
                        html += `<label>上傳時間</label><input type="text" value="${new Date(row.created_at).toLocaleString('zh-TW')}" readonly>`;
                        html += '</div>';
                        
                        html += '<div class="form-group">';
                        html += `<label>數據內容</label><textarea readonly>${JSON.stringify(rowData, null, 2)}</textarea>`;
                        html += '</div>';
                        
                        document.getElementById('modal-body').innerHTML = html;
                        document.getElementById('detailModal').classList.add('active');
                    }
                } catch (error) {
                    alert('加載詳情失敗: ' + error.message);
                }
            }
            
            function closeModal() {
                document.getElementById('detailModal').classList.remove('active');
            }
            
            async function loadStats() {
                const container = document.getElementById('stats-container');
                
                try {
                    const response = await fetch('/stats');
                    const data = await response.json();
                    
                    if (data.status === 'success') {
                        let html = '<div class="stats">';
                        
                        const tables = {
                            'provincial_operations': '全省營運數據',
                            'parts_sales': '零件銷售資料',
                            'repair_income_details': '維修收入明細',
                            'technician_performance': '技師績效'
                        };
                        
                        for (const [key, label] of Object.entries(tables)) {
                            const stat = data.stats[key];
                            html += `<div class="stat-card">
                                <h4>${label}</h4>
                                <div class="number">${stat.total_rows}</div>
                                <p style="font-size: 0.9em; margin-top: 5px;">筆數據 (${stat.total_files} 個文件)</p>
                            </div>`;
                        }
                        
                        html += '</div>';
                        container.innerHTML = html;
                    }
                } catch (error) {
                    container.innerHTML = `<p style="color: red;">加載失敗: ${error.message}</p>`;
                }
            }
            
            window.onload = function() {
                loadStats();
            };
        </script>
    </body>
    </html>
    """

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
