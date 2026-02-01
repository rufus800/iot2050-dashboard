# IoT2050 Water Plant Dashboard

A real-time monitoring dashboard for the ASEJIRE BF water plant system built with Dash, Flask, and SIEMENS S7 PLC integration.

## Features

- **Real-time PLC Monitoring**: Live pump pressure and speed data from SIEMENS S7 PLCs
- **Beautiful Trend View Charts**: Dual-axis visualization with professional styling
  - Pressure monitoring (0-10 BAR) in cyan
  - Motor speed monitoring (0-50 HZ) in gold
- **Historical Data Reports**: Query and analyze historical data with trend analysis
- **PDF Export**: Generate professional PDF reports with historical data
- **Database Logging**: Automatic 1-hour batch logging with immediate TRIP event logging
- **404 Error Handling**: Custom error pages for invalid routes
- **Dark Theme UI**: Professional dark-themed interface optimized for industrial environments

## Installation

### Prerequisites
- Python 3.14+
- Virtual environment (recommended)
- SIEMENS S7 PLC access (optional - dashboard runs without PLC connection)

### Setup

1. **Clone the repository**
```bash
cd c:\Users\ZENBOOK\iot2050-dashboard
```

2. **Create virtual environment** (if not already created)
```bash
python -m venv .venv
```

3. **Activate virtual environment**
```bash
.venv\Scripts\activate
```

4. **Install dependencies**
```bash
pip install -r requirements.txt
```

## Running the Dashboard

### Option 1: Using the Batch Script (Recommended for Windows)
Double-click: `run_dashboard.bat`

### Option 2: Using PowerShell
```powershell
.\run_dashboard.ps1
```

### Option 3: Using Virtual Environment Python (Command Line)
```bash
.venv\Scripts\python.exe dashboard_app.py
```

### ⚠️ Do NOT use system Python
```bash
# ❌ WRONG - This will fail with "ModuleNotFoundError: No module named 'reportlab'"
python dashboard_app.py
python.exe dashboard_app.py

# ✅ CORRECT - Use virtual environment Python
.venv\Scripts\python.exe dashboard_app.py
```

The dashboard will start on:
- **Local**: http://127.0.0.1:8051
- **Network**: http://192.168.100.180:8051 (or your local IP)

## Configuration

Edit `config.json` to customize:
- PLC connection settings (IP, rack, slot)
- Pump definitions and sensor offsets
- Chiller configurations
- Poll intervals

## Project Structure

```
.
├── dashboard_app.py          # Main application (production-ready)
├── config.json               # PLC and sensor configuration
├── logs.db                   # SQLite database (auto-created)
├── requirements.txt          # Python dependencies
└── .venv/                    # Virtual environment
```

## Database Schema

### pump_logs
- `ts`: Timestamp
- `pump_id`: Pump identifier
- `pressure`: Pressure reading (BAR)
- `speed`: Motor speed (HZ)
- `ready`, `running`, `trip`: Status flags

### pump_events
- `ts`: Timestamp
- `pump_id`: Pump identifier
- `event`: Event type (e.g., TRIP)
- `pressure`: Pressure at event
- `speed`: Speed at event

## Pages

- **Home** (`/`): Dashboard overview with pump gauges
- **Pump Details** (`/pump/{pump_id}`): Individual pump monitoring
- **Chillers** (`/chillers`): Chiller system monitoring
- **Reports** (`/reports`): Historical data analysis and PDF export

## Technical Stack

- **Backend**: Flask, Dash
- **Frontend**: Plotly, Bootstrap, React
- **Database**: SQLite3
- **PLC Communication**: python-snap7 (S7 protocol)
- **PDF Generation**: ReportLab
- **Data Processing**: pandas

## Notes

- Dashboard runs without PLC connection (test mode)
- All requests return HTTP 200 OK
- Database auto-creates on first run
- Batch logging occurs every 1 hour
- TRIP events logged immediately
- Connection pooling for optimized queries

## Status

✅ Production Ready
- Code validated and optimized
- All features implemented
- Clean codebase with no debug code
