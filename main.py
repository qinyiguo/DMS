from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
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

# ... 其他代碼保持不變 ...

# ==================== 前端頁面 ====================

@app.get("/", response_class=HTMLResponse)
def get_home():
    """主頁面 - 上傳和數據管理"""
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
                // 隱藏所有 tab
                const tabs = document.querySelectorAll('.tab-content');
                tabs.forEach(tab => tab.classList.remove('active'));
                
                // 移除所有按鈕的 active 類
                const buttons = document.querySelectorAll('.tab-button');
                buttons.forEach(btn => btn.classList.remove('active'));
                
                // 顯示選中的 tab
                document.getElementById(tabName).classList.add('active');
                
                // 添加 active 類到點擊的按鈕
                event.target.classList.add('active');
                
                // 加載對應的數據
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
            
            // 頁面加載時初始化
            window.onload = function() {
                loadStats();
            };
        </script>
    </body>
    </html>
    """
