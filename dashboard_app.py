import io
import json
import logging
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional
import dash
import dash_daq as daq
import pandas as pd
import plotly.graph_objects as go
import snap7
from dash import dash_table, dcc as dcc2, html, Input, Output, State
import dash_bootstrap_components as dbc
from dash_iconify import DashIconify
from snap7.util import get_real, get_bool
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors

# ---------------------
# Load config.json
# ---------------------
logging.basicConfig(level=logging.INFO, stream=sys.stdout, format='%(asctime)s - %(levelname)s - %(message)s')

CONFIG_FILE = Path("config.json")
if not CONFIG_FILE.exists():
    raise SystemExit("config.json not found. Create it next to this script (see sample).")

with open(CONFIG_FILE, "r") as f:
    cfg: Dict[str, Any] = json.load(f)

# deduplicated pump keys (preserve order)
pump_keys = list(dict.fromkeys(cfg.get("pumps", {}).keys()))

PLC = cfg.get("plc", {})
POLL_INTERVAL = int(cfg.get("poll_interval_seconds", 2))

DB_FILE = Path("logs.db")

# ---------------------
# Database Connection Pool & Caching
# ---------------------
class DBConnection:
    """Database connection manager with caching."""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def query(self, sql: str, params: list = None) -> pd.DataFrame:
        """Execute query and return DataFrame."""
        try:
            with sqlite3.connect(str(DB_FILE), timeout=5) as conn:
                return pd.read_sql_query(sql, conn, params=params or [])
        except Exception as e:
            logging.error(f"Database query error: {e}")
            return pd.DataFrame()

db_conn = DBConnection()

# ---------------------
# PDF Generation Helper
# ---------------------
def generate_pdf_report(df: pd.DataFrame, df_events: pd.DataFrame, pump: str, start_date: str, end_date: str) -> bytes:
    """Generate PDF report from pump logs data."""
    buffer = io.BytesIO()
    
    try:
        doc = SimpleDocTemplate(buffer, pagesize=A4, leftMargin=0.5*inch, rightMargin=0.5*inch)
        elements = []
        styles = getSampleStyleSheet()
        
        # Title
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#ff3333'),
            spaceAfter=30,
            alignment=1
        )
        elements.append(Paragraph("ASEJIRE BF WATER PLANT SYSTEM", title_style))
        elements.append(Paragraph("Historical Data Report", styles['Heading2']))
        elements.append(Spacer(1, 0.3*inch))
        
        # Report Info
        info_data = [
            ["Report Information"],
            ["Pump", pump.upper() if pump != "all" else "ALL PUMPS"],
            ["Date Range", f"{start_date} to {end_date}"],
            ["Generated", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")],
        ]
        info_table = Table(info_data, colWidths=[2*inch, 4*inch])
        info_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#880b0b')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 14),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
        ]))
        elements.append(info_table)
        elements.append(Spacer(1, 0.2*inch))
        
        # Pump Logs Data
        if not df.empty:
            elements.append(Paragraph("Pump Logs Data", styles['Heading3']))
            
            # Prepare table data
            log_data = [["Timestamp", "Pump ID", "Pressure (Bar)", "Speed (Hz)", "Ready", "Running", "Trip"]]
            for _, row in df.iterrows():
                log_data.append([
                    str(row['ts'])[:19],
                    str(row['pump_id']),
                    f"{row['pressure']:.2f}" if pd.notna(row['pressure']) else "N/A",
                    f"{row['speed']:.2f}" if pd.notna(row['speed']) else "N/A",
                    "✓" if row['ready'] == 1 else "✗",
                    "✓" if row['running'] == 1 else "✗",
                    "✓" if row['trip'] == 1 else "✗",
                ])
            
            log_table = Table(log_data, colWidths=[1.2*inch, 1*inch, 1.1*inch, 1*inch, 0.7*inch, 0.8*inch, 0.7*inch])
            log_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#ff3333')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 9),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.lightgrey),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 8),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey]),
            ]))
            elements.append(log_table)
            elements.append(Spacer(1, 0.2*inch))
        
        # Pump Events Data
        if not df_events.empty:
            elements.append(PageBreak())
            elements.append(Paragraph("Pump Events (Trips & Alarms)", styles['Heading3']))
            
            # Prepare events table data
            events_data = [["Timestamp", "Pump ID", "Event", "Pressure (Bar)", "Speed (Hz)"]]
            for _, row in df_events.iterrows():
                events_data.append([
                    str(row['ts'])[:19],
                    str(row['pump_id']),
                    str(row['event']),
                    f"{row['pressure']:.2f}" if pd.notna(row['pressure']) else "N/A",
                    f"{row['speed']:.2f}" if pd.notna(row['speed']) else "N/A",
                ])
            
            events_table = Table(events_data, colWidths=[1.5*inch, 1*inch, 1.5*inch, 1.2*inch, 1.2*inch])
            events_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#ff0000')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 9),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.lightyellow),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 8),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightyellow]),
            ]))
            elements.append(events_table)
        
        doc.build(elements)
        buffer.seek(0)
        return buffer.getvalue()
    except Exception as e:
        logging.error(f"Error generating PDF: {e}")
        return None
state: Dict[str, Any] = {
    "home": {"kwh": "--", "level": "--", "temp": "--", "ts": "--", "alarm": False},
    "pumps": {},
    "chillers": {}
}

# Initialize pump placeholders
for pkey in pump_keys:
    info = cfg["pumps"][pkey]
    state["pumps"][pkey] = {
        "label": info.get("label", pkey),
        "ready": False,
        "running": False,
        "trip": False,
        "pressure": 0.0,
        "speed": 0.0,
        "ts": "--",
    }

# Initialize chillers placeholders
for ckey, info in cfg.get("chillers", {}).items():
    state["chillers"][ckey] = {"ready": False, "running": False, "trip": False, "ts": "--"}

# PLC connection status tracking
plc_status = {"connected": False, "last_error": None}

# ---------------------
# Database helpers
# ---------------------
def create_db() -> None:
    """Create DB and indexes if not present."""
    with sqlite3.connect(str(DB_FILE)) as conn:
        cur = conn.cursor()
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS pump_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            pump_id TEXT NOT NULL,
            pressure REAL,
            speed REAL,
            ready INTEGER,
            running INTEGER,
            trip INTEGER
        )"""
        )
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS pump_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            pump_id TEXT NOT NULL,
            event TEXT NOT NULL,
            pressure REAL,
            speed REAL
        )"""
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pump_logs_ts ON pump_logs(ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pump_logs_pid ON pump_logs(pump_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pump_events_ts ON pump_events(ts)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pump_events_pid ON pump_events(pump_id)")
        conn.commit()

def log_pump_batch(batch_data: list) -> None:
    """Batch insert multiple pump log records for efficiency."""
    if not batch_data:
        return
    try:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.connect(str(DB_FILE)) as conn:
            cur = conn.cursor()
            for pump_id, pdata in batch_data:
                cur.execute(
                    "INSERT INTO pump_logs (ts, pump_id, pressure, speed, ready, running, trip) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        ts,
                        pump_id,
                        pdata.get("pressure"),
                        pdata.get("speed"),
                        int(bool(pdata.get("ready"))),
                        int(bool(pdata.get("running"))),
                        int(bool(pdata.get("trip"))),
                    ),
                )
            conn.commit()
            logging.info(f"Batch logged {len(batch_data)} pump records to database")
    except Exception as e:
        logging.error(f"Failed to batch log pump data: {e}")

def log_pump_data(pump_id: str, pdata: Dict[str, Any]) -> None:
    """Insert a pump_samples row."""
    try:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.connect(str(DB_FILE)) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO pump_logs (ts, pump_id, pressure, speed, ready, running, trip) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    ts,
                    pump_id,
                    pdata.get("pressure"),
                    pdata.get("speed"),
                    int(bool(pdata.get("ready"))),
                    int(bool(pdata.get("running"))),
                    int(bool(pdata.get("trip"))),
                ),
            )
            conn.commit()
    except Exception as e:
        logging.error(f"Failed to log pump data for {pump_id}: {e}")

def log_pump_event(pump_id: str, event: str, pressure: Optional[float], speed: Optional[float]) -> None:
    """Insert a pump event (e.g. TRIP)."""
    try:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.connect(str(DB_FILE)) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO pump_events (ts, pump_id, event, pressure, speed) VALUES (?, ?, ?, ?, ?)",
                (ts, pump_id, event, pressure, speed),
            )
            conn.commit()
    except Exception as e:
        logging.error(f"Failed to log pump event for {pump_id}: {e}")

create_db()

# track previous pump states for state change detection
_prev_pump_state: Dict[str, Dict[str, Any]] = {}
for pkey in pump_keys:
    _prev_pump_state[pkey] = {"ready": False, "running": False, "trip": False, "pressure": None, "speed": None}

# ---------------------
# PLC read helpers
# ---------------------
def _safe_int(v: Any) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return None

def read_real_from_db(client: snap7.client.Client, dbnum: Any, byte_index: Any) -> Optional[float]:
    """Read REAL (4 bytes) from DB; return None on failure or missing params."""
    db = _safe_int(dbnum)
    bx = _safe_int(byte_index)
    if db is None or bx is None:
        return None
    try:
        data = client.db_read(db, bx, 4)
        return float(get_real(data, 0))
    except Exception:
        return None

def read_bool_from_db(client: snap7.client.Client, dbnum: Any, byte_index: Any, bit_index: Any) -> Optional[bool]:
    """Read a single BIT and return bool or None."""
    db = _safe_int(dbnum)
    bx = _safe_int(byte_index)
    bit = _safe_int(bit_index)
    if db is None or bx is None or bit is None:
        return None
    try:
        data = client.db_read(db, bx, 1)
        return bool(get_bool(data, 0, bit))
    except Exception:
        return None

# ---------------------
# PLC worker thread
# ---------------------
def plc_worker() -> None:
    """PLC worker thread for reading data from PLC."""
    client = snap7.client.Client()
    connected = False
    connection_error_count = 0
    MAX_CONNECTION_RETRIES = 5
    
    while True:
        try:
            if not connected:
                try:
                    logging.info(f"Attempting PLC connection to {PLC['host']}:{PLC.get('port', 102)}")
                    client.connect(PLC["host"], PLC.get("rack", 0), PLC.get("slot", 1))
                    connected = client.get_connected()
                    if connected:
                        logging.info("PLC connected successfully")
                        connection_error_count = 0
                    else:
                        connection_error_count += 1
                        if connection_error_count >= MAX_CONNECTION_RETRIES:
                            logging.warning(f"Connection failed {MAX_CONNECTION_RETRIES} times, retrying...")
                            connection_error_count = 0
                except Exception as e:
                    connected = False
                    connection_error_count += 1
                    logging.debug(f"PLC connection attempt failed: {e}")
                
                if not connected:
                    time.sleep(POLL_INTERVAL)
                    continue

            # HOME tags
            home_cfg = cfg.get("home", {})
            if home_cfg:
                try:
                    kwh_tag = home_cfg.get("ALL_PUMPS_KWH")
                    if kwh_tag:
                        val = read_real_from_db(client, kwh_tag.get("db"), kwh_tag.get("offset"))
                        if val is not None:
                            state["home"]["kwh"] = f"{val:.2f}"
                    
                    lvl_tag = home_cfg.get("TANK_WATER_LEVEL")
                    if lvl_tag:
                        val = read_real_from_db(client, lvl_tag.get("db"), lvl_tag.get("offset"))
                        if val is not None:
                            state["home"]["level"] = f"{val:.2f}"
                    
                    tmp_tag = home_cfg.get("TANK_TEMPERATURE")
                    if tmp_tag:
                        val = read_real_from_db(client, tmp_tag.get("db"), tmp_tag.get("offset"))
                        if val is not None:
                            state["home"]["temp"] = f"{val:.1f}"
                    
                    alarm_tag = home_cfg.get("ALARM")
                    if alarm_tag:
                        val = read_bool_from_db(client, alarm_tag.get("db"), alarm_tag.get("byte"), alarm_tag.get("bit"))
                        if val is not None:
                            state["home"]["alarm"] = val
                    
                    state["home"]["ts"] = time.strftime("%d/%m/%Y %H:%M:%S")
                except Exception as e:
                    logging.error(f"Error reading HOME tags: {e}")

            # Pumps
            try:
                current_time = time.time()
                
                for pkey, pinfo in cfg.get("pumps", {}).items():
                    dbnum = pinfo.get("db")
                    ready = read_bool_from_db(client, dbnum, pinfo.get("ready", {}).get("byte"), pinfo.get("ready", {}).get("bit"))
                    running = read_bool_from_db(client, dbnum, pinfo.get("running", {}).get("byte"), pinfo.get("running", {}).get("bit"))
                    trip = read_bool_from_db(client, dbnum, pinfo.get("trip", {}).get("byte"), pinfo.get("trip", {}).get("bit"))
                    pressure = None
                    speed = None
                    
                    if "pressure" in pinfo:
                        pressure = read_real_from_db(client, dbnum, pinfo.get("pressure", {}).get("offset"))
                    if "speed" in pinfo:
                        speed = read_real_from_db(client, dbnum, pinfo.get("speed", {}).get("offset"))

                    st = state["pumps"].get(pkey, {})
                    if ready is not None:
                        st["ready"] = bool(ready)
                    if running is not None:
                        st["running"] = bool(running)
                    if trip is not None:
                        st["trip"] = bool(trip)
                    if pressure is not None:
                        st["pressure"] = round(float(pressure), 2)
                    if speed is not None:
                        st["speed"] = round(float(speed), 2)
                    st["ts"] = time.strftime("%d/%m/%Y %H:%M:%S")
                    state["pumps"][pkey] = st

                    # Track previous state for change detection
                    prev_st = _prev_pump_state.get(pkey, {})
                    
                    # Log immediately on state changes (READY, RUNNING, TRIP)
                    if bool(st.get("ready", False)) != bool(prev_st.get("ready", False)):
                        log_pump_event(pkey, f"READY={st.get('ready')}", st.get("pressure"), st.get("speed"))
                        logging.info(f"READY changed on {pkey}: {st.get('ready')}")
                    
                    if bool(st.get("running", False)) != bool(prev_st.get("running", False)):
                        log_pump_event(pkey, f"RUNNING={st.get('running')}", st.get("pressure"), st.get("speed"))
                        logging.info(f"RUNNING changed on {pkey}: {st.get('running')}")
                    
                    if bool(st.get("trip", False)) != bool(prev_st.get("trip", False)):
                        log_pump_event(pkey, f"TRIP={st.get('trip')}", st.get("pressure"), st.get("speed"))
                        logging.warning(f"TRIP changed on {pkey}: {st.get('trip')}")
                    
                    # Log pump data immediately (every poll)
                    log_pump_data(pkey, st)
                    
                    # Update previous state
                    _prev_pump_state[pkey] = st.copy()
                    
            except Exception as e:
                logging.error(f"Error reading pump data: {e}")

            # Chillers
            try:
                for ckey, cinfo in cfg.get("chillers", {}).items():
                    dbnum = cinfo.get("db")
                    ready = read_bool_from_db(client, dbnum, cinfo.get("ready", {}).get("byte"), cinfo.get("ready", {}).get("bit"))
                    running = read_bool_from_db(client, dbnum, cinfo.get("running", {}).get("byte"), cinfo.get("running", {}).get("bit"))
                    trip = read_bool_from_db(client, dbnum, cinfo.get("trip", {}).get("byte"), cinfo.get("trip", {}).get("bit"))
                    
                    ch = state["chillers"].get(ckey, {})
                    if ready is not None:
                        ch["ready"] = bool(ready)
                    if running is not None:
                        ch["running"] = bool(running)
                    if trip is not None:
                        ch["trip"] = bool(trip)
                    ch["ts"] = time.strftime("%d/%m/%Y %H:%M:%S")
                    state["chillers"][ckey] = ch
            except Exception as e:
                logging.error(f"Error reading chiller data: {e}")

        except Exception as e:
            logging.error(f"Unexpected error in PLC worker: {e}")
            try:
                client.disconnect()
            except Exception:
                pass
            connected = False
            state["home"]["ts"] = "PLC read error"
        
        time.sleep(POLL_INTERVAL)


# Start PLC worker thread
threading.Thread(target=plc_worker, daemon=True).start()

# ---------------------
# Shared Styles & Constants
# ---------------------
CARD_STYLE = {"background": "#0b0b0b", "border": "1px solid rgba(255,0,0,0.08)", "borderRadius": "12px", "padding": "18px"}
CONTAINER_STYLE = {"padding": "24px", "maxWidth": "1200px", "margin": "12px auto"}

# ---------------------
# Error Pages
# ---------------------
def render_404():
    """Render 404 error page when site is down or page not found."""
    return html.Div(
        style=CONTAINER_STYLE,
        children=[
            html.Div(
                style={"textAlign": "center", "padding": "60px 20px"},
                children=[
                    html.H1("404", style={"color": "#ff3333", "fontSize": "72px", "margin": "0"}),
                    html.H2("Page Not Found", style={"color": "#ff7777", "margin": "20px 0"}),
                    html.P("The page you're looking for doesn't exist or the system is unavailable.", style={"color": "#aaa", "fontSize": "16px"}),
                    dbc.Button("Back to Home", href="/", color="danger", size="lg", style={"marginTop": "20px"}),
                ]
            )
        ]
    )

# ---------------------
# Build Dash app (UI preserved)
# ---------------------
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=False)
app.index_string = """
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>ASEJIRE BF WATER PLANT SYSTEM</title>
        {%favicon%}
        {%css%}
        <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
        <meta http-equiv="Pragma" content="no-cache">
        <meta http-equiv="Expires" content="0">
        <style>
            body { background-color: #111111; margin: 0; font-family: 'Segoe UI', sans-serif; }
            .card { transition: all 0.3s ease; }
            .card:hover { transform: translateY(-5px); box-shadow: 0 12px 36px 0 rgba(0,0,0,0.45) !important; }
            .dark-theme-control { background-color: #1e1e1e !important; color: #ffffff !important; }
            .dark-theme-control text { fill: #ffffff !important; }
            @keyframes flash {
                0% { background-color: red; }
                50% { background-color: #111111; }
                100% { background-color: red; }
            }
            .alarm-indicator {
                width: 50px;
                height: 50px;
                border-radius: 50%;
                animation: flash 1s infinite;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
"""
server = app.server

# Add Flask headers to prevent Cloudflare caching issues
@server.after_request
def set_cache_headers(response):
    """Prevent Cloudflare caching of dynamic content."""
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    return response

# --- UI helper functions (unchanged behavior) ---
def status_dot(condition: bool, label: str, color: str = "#888888"):
    return html.Div(
        [
            html.Div(
                style={
                    "width": "12px",
                    "height": "12px",
                    "borderRadius": "50%",
                    "backgroundColor": color if condition else "#333333",
                    "display": "inline-block",
                    "marginRight": "8px",
                    "boxShadow": f"0 0 8px {color}" if condition else "none",
                }
            ),
            html.Span(label, style={"color": "#ffffff"}),
        ],
        style={"display": "flex", "alignItems": "center"},
    )

def pump_card(pkey: str, pdata: Dict[str, Any]):
    """Generate pump card with real-time updates."""
    status_color = "#FF0000" if pdata["trip"] else "#00FF00" if pdata["running"] else "#FFD700" if pdata["ready"] else "#333333"
    pinfo = cfg["pumps"].get(pkey, {})
    
    return dbc.Card(
        [
            dbc.CardHeader(
                [
                    html.Div(
                        [
                            status_dot(pdata["ready"], "READY", pinfo.get("ready", {}).get("color", "#888")),
                            status_dot(pdata["running"], "RUNNING", pinfo.get("running", {}).get("color", "#888")),
                            status_dot(pdata["trip"], "TRIP", pinfo.get("trip", {}).get("color", "#888")),
                        ],
                        style={"display": "flex", "gap": "12px", "justifyContent": "center", "marginTop": "6px"},
                    ),
                ],
                style={"background": "transparent", "border": "none"},
            ),
            dbc.CardBody(
                [
                    dbc.Row(
                        [
                            dbc.Col(
                                html.Div(
                                    [
                                        html.H5("System Pressure", style={"color": "#f7caca", "textAlign": "center", "marginBottom": "10px", "fontSize": "1.2rem", "fontWeight": "500"}),
                                        daq.Gauge(
                                            id={"type": "pressure-gauge", "index": pkey},
                                            color={
                                                "gradient": True,
                                                "ranges": {"#00ff00": [0, 6], "#ffeb3b": [6, 8], "#ff0000": [8, 10]},
                                            },
                                            value=pdata.get("pressure", 0),
                                            min=0,
                                            max=10,
                                            showCurrentValue=True,
                                            units="BAR",
                                            size=200,
                                            style={"margin": "0 auto"},
                                            label={"label": "Pressure", "style": {"color": "#ffffff"}},
                                            className="dark-theme-control",
                                        ),
                                        html.Div([html.Span("Current: ", style={"color": "#888"}), html.Span(f"{pdata.get('pressure', 0):.1f} BAR", style={"color": "#fff", "fontSize": "1.2rem", "marginLeft": "5px"})], style={"textAlign": "center", "marginTop": "10px"}),
                                    ],
                                    style={"background": "linear-gradient(145deg, #1a1a1a, #2a2a2a)", "borderRadius": "15px", "padding": "20px", "boxShadow": "0 8px 32px 0 rgba(0,0,0,0.37)", "border": "1px solid rgba(255,255,255,0.1)"},
                                ),
                                md=6,
                            ),
                            dbc.Col(
                                html.Div(
                                    [
                                        html.H5("Motor Speed", style={"color": "#f7caca", "textAlign": "center", "marginBottom": "10px", "fontSize": "1.2rem", "fontWeight": "500"}),
                                        daq.Tank(
                                            id={"type": "speed-tank", "index": pkey},
                                            value=pdata.get("speed", 0),
                                            min=0,
                                            max=50,
                                            style={"margin": "0 auto"},
                                            showCurrentValue=True,
                                            units="Hz",
                                            height=200,
                                            color=status_color,
                                            className="dark-theme-control",
                                        ),
                                        html.Div([html.Span("Current: ", style={"color": "#888"}), html.Span(f"{pdata.get('speed', 0):.1f} Hz", style={"color": "#fff", "fontSize": "1.2rem", "marginLeft": "5px"})], style={"textAlign": "center", "marginTop": "10px"}),
                                        html.Div(
                                            [
                                                daq.LEDDisplay(id={"type": "speed-display", "index": pkey}, value=str(pdata.get("speed", 0)), color="#00ff00", backgroundColor="#1e1e1e", size=24, style={"display": "inline-block"}),
                                                html.Span("Hz", style={"color": "#fff", "marginLeft": "10px", "fontSize": "1.2rem"}),
                                            ],
                                            style={"textAlign": "center", "marginTop": "15px", "padding": "10px", "background": "rgba(0,0,0,0.3)", "borderRadius": "8px"},
                                        ),
                                    ],
                                    style={"background": "linear-gradient(145deg, #1a1a1a, #2a2a2a)", "borderRadius": "15px", "padding": "20px", "boxShadow": "0 8px 32px 0 rgba(0,0,0,0.37)", "border": "1px solid rgba(255,255,255,0.1)"},
                                ),
                                md=6,
                            ),
                        ]
                    ),
                    html.Div(f"Last Updated: {pdata.get('ts','--')}", style={"color": "#999", "marginTop": "20px", "textAlign": "center", "fontSize": "12px"}),
                ],
                style={"background": "linear-gradient(180deg, #111111, #1a1a2a)", "border": "1px solid rgba(255,0,0,0.1)", "borderRadius": "12px", "padding": "20px"},
            ),
        ],
        style={"background": "transparent", "border": "1px solid rgba(255,255,255,0.05)", "marginBottom": "20px"},
    )

# ---------------------
# Layout definitions (preserved)
# ---------------------
app.layout = dbc.Container(
    fluid=True,
    # Add interval component to trigger updates
    children=[
        dcc2.Interval(id="interval-refresh", interval=POLL_INTERVAL * 1000, n_intervals=0),
        dcc2.Store(id="plc-state-store", data={}),
        dcc2.Location(id="url", refresh=False),
        dbc.NavbarSimple(
            children=[
                dbc.NavItem(dbc.NavLink("Home", href="/")),
                dbc.DropdownMenu(children=[dbc.DropdownMenuItem(cfg["pumps"][k].get("label", k).upper(), href=f"/pump/{k}") for k in pump_keys], nav=True, in_navbar=True, label="Pumps"),
                dbc.NavItem(dbc.NavLink("Chillers", href="/chillers")),
                dbc.NavItem(dbc.NavLink("Reports", href="/reports")),
            ],
            brand="ASEJIRE BF WATER PLANT SYSTEM",
            color="#2a0b0b",
            dark=True,
            style={"background": "#880b0b", "boxShadow": "0 2px 6px rgba(0,0,0,0.5)"},
        ),
        html.Div(id="page-content", children=[]),
    ],
    style={"background": "#070707", "minHeight": "100vh", "padding": "0"},
)

def make_stat_card(title: str, value: str, unit: str = "", card_id: str = ""):
    """Create a reusable stat card component."""
    return dbc.Col(
        dbc.Card(
            dbc.CardBody([
                html.H5(title, style={"color": "#ff7777"}),
                html.H2(id=card_id, children=f"{value} {unit}".strip(), style={"color": "#fff", "textShadow": "0 0 12px #ff0000"})
            ]),
            style=CARD_STYLE
        ),
        md=3,
    )

def render_home():
    """Render home page with live updates."""
    home = state["home"]
    cards = dbc.Row(
        [
            make_stat_card("Total Energy", home.get('kwh', '--'), "kWh", "kwh-stat"),
            make_stat_card("Tank Level", home.get('level', '--'), "Ltr", "level-stat"),
            make_stat_card("Temperature", home.get('temp', '--'), "°C", "temp-stat"),
            dbc.Col(
                dbc.Card(
                    dbc.CardBody([
                        html.H5("Alarm", style={"color": "#ff7777"}),
                        html.Div(id="alarm-stat", children=html.Div(className="alarm-indicator") if home.get("alarm") else html.Div("Inactive", style={"color":"#fff"}))
                    ]),
                    style=CARD_STYLE
                ),
                md=3,
            ),
        ],
        style={"padding": "20px", "maxWidth": "1200px", "margin": "24px auto"},
    )
    return html.Div([
        html.Div(
            style=CONTAINER_STYLE,
            children=[
                html.H3("System Overview", style={"color": "#ff3333", "textAlign": "center", "textShadow": "0 0 10px #ff0000"}),
                cards,
                html.Div(id="home-timestamp", children=f"Last updated: {home.get('ts','--')}", style={"color": "#aaa", "textAlign": "center", "marginTop": "6px"})
            ]
        )
    ])

def render_pump(pkey: str):
    """Render individual pump page."""
    pdata = state["pumps"].get(pkey, {})
    if not pdata:
        return html.Div(f"Pump {pkey} not configured", style={"color": "#fff"})
    return html.Div([
        html.Div(
            style=CONTAINER_STYLE,
            children=[
                html.H3(pdata.get("label", pkey).upper(), style={"color": "#ff3333", "textAlign": "center"}),
                dbc.Row([dbc.Col(html.Div(pump_card(pkey, pdata)), md=8)], justify="center"),
                html.Div(f"Last update: {pdata.get('ts','--')}", style={"textAlign": "center", "color": "#888", "marginTop": "10px"})
            ]
        )
    ])

def render_chillers():
    """Render chillers overview page."""
    chcards = []
    for ckey, cinfo in state["chillers"].items():
        cfg_cinfo = cfg.get("chillers", {}).get(ckey, {})
        chcards.append(
            dbc.Col(
                dbc.Card(
                    dbc.CardBody([
                        html.H5(ckey.upper(), style={"color": "#ff3333"}),
                        html.Div([
                            status_dot(cinfo.get("ready", False), "READY", cfg_cinfo.get("ready", {}).get("color", "#888")),
                            status_dot(cinfo.get("running", False), "RUNNING", cfg_cinfo.get("running", {}).get("color", "#888")),
                            status_dot(cinfo.get("trip", False), "TRIP", cfg_cinfo.get("trip", {}).get("color", "#888")),
                        ], style={"display": "flex", "justifyContent": "space-between"}),
                        html.Div(f"Last: {cinfo.get('ts','--')}", style={"color": "#999"}),
                    ]),
                    style=CARD_STYLE
                ),
                md=4,
            )
        )
    return html.Div([
        html.Div(
            style=CONTAINER_STYLE,
            children=[
                html.H3("Chillers Overview", style={"color": "#ff3333", "textAlign": "center"}),
                dbc.Row(chcards, justify="center")
            ]
        )
    ])

def render_reports():
    """Render historical reports page."""
    pump_options = [{"label": cfg["pumps"][k].get("label", k).upper(), "value": k} for k in pump_keys]
    pump_options.insert(0, {"label": "All Pumps", "value": "all"})
    today = datetime.now(timezone.utc).date()
    
    return html.Div(
        style=CONTAINER_STYLE,
        children=[
            html.H3("Historical Reports", style={"color": "#ff3333", "textAlign": "center"}),
            dbc.Card(
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([html.Label("Pump", style={"color": "#ccc"}), dcc2.Dropdown(id="report-pump", options=pump_options, value="all")], md=3),
                        dbc.Col([html.Label("Date range", style={"color": "#ccc"}), dcc2.DatePickerRange(id="report-range", start_date=(today - timedelta(days=1)).isoformat(), end_date=today.isoformat(), display_format="YYYY-MM-DD")], md=4),
                        dbc.Col([html.Label("Quick", style={"color": "#ccc"}), dbc.Button("Yesterday", id="btn-yesterday", color="secondary", style={"marginRight": "8px"}), dbc.Button("Last 7 days", id="btn-7d", color="secondary")], md=3),
                        dbc.Col([dbc.Button("Query", id="btn-query", color="primary"), dbc.Button("Download PDF", id="btn-download", color="success", style={"marginLeft": "8px"}), dcc2.Download(id="download-data")], md=2)
                    ]),
                    html.Hr(),
                    html.Div(id="report-results")
                ]),
                style={"background": "#0b0b0b", "border": "1px solid rgba(255,0,0,0.06)", "borderRadius": "12px", "padding": "12px"}
            )
        ]
    )

# Router callback
@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def display_page(pathname: Optional[str]):
    """Route to appropriate page or show 404 error."""
    try:
        if pathname is None or pathname in ("/", "/home"):
            return render_home()
        if pathname.startswith("/pump/"):
            pkey = pathname.split("/pump/")[-1]
            if pkey in state["pumps"]:
                return render_pump(pkey)
            return render_404()
        if pathname.startswith("/chillers"):
            return render_chillers()
        if pathname.startswith("/reports"):
            return render_reports()
    except Exception as e:
        logging.error(f"Error rendering page {pathname}: {e}")
    return render_404()

# Real-time update callback: Push PLC state to Store (fires every POLL_INTERVAL)
@app.callback(
    Output("plc-state-store", "data"),
    [Input("interval-refresh", "n_intervals")],
    prevent_initial_call=False
)
def update_store(n):
    """Update Store with latest PLC state - triggers immediately every poll cycle."""
    return {
        "home": state["home"],
        "pumps": state["pumps"],
        "chillers": state["chillers"]
    }

# Real-time update callback for home page (responds to Store changes instantly)
@app.callback(
    [Output("kwh-stat", "children"), Output("level-stat", "children"), Output("temp-stat", "children"), Output("alarm-stat", "children"), Output("home-timestamp", "children")],
    [Input("plc-state-store", "data")],
    prevent_initial_call=False
)
def update_home_stats(store_data):
    """Update home page statistics instantly from Store."""
    if not store_data or "home" not in store_data:
        return ("--", "--", "--", html.Div("Offline", style={"color":"#fff"}), "--")
    home = store_data["home"]
    return (
        f"{home.get('kwh', '--')} kWh",
        f"{home.get('level', '--')} Ltr",
        f"{home.get('temp', '--')} °C",
        html.Div(className="alarm-indicator") if home.get("alarm") else html.Div("Inactive", style={"color":"#fff"}),
        f"Last updated: {home.get('ts','--')}"
    )

# Real-time update callback for pump cards using pattern-matching (responds instantly)
@app.callback(
    [Output({"type": "pressure-gauge", "index": dash.ALL}, "value"), 
     Output({"type": "speed-tank", "index": dash.ALL}, "value"), 
     Output({"type": "speed-display", "index": dash.ALL}, "value")],
    [Input("plc-state-store", "data")],
    prevent_initial_call=False
)
def update_pump_cards(store_data):
    """Update all pump gauges instantly from Store."""
    if not store_data or "pumps" not in store_data:
        return [], [], []
    
    # Get the actual output IDs that matched
    ctx = dash.callback_context
    if not ctx.outputs_list:
        return [], [], []
    
    # Extract the pump indices that are actually in the layout
    matched_indices = [output["id"]["index"] for output in ctx.outputs_list[0]]
    
    pumps = store_data["pumps"]
    pressures = []
    speeds = []
    speed_displays = []
    
    # Only return values for pumps that are actually present in the layout
    for pkey in matched_indices:
        pdata = pumps.get(pkey, {})
        pressures.append(pdata.get("pressure", 0))
        speeds.append(pdata.get("speed", 0))
        speed_displays.append(str(pdata.get("speed", 0)))
    
    return pressures, speeds, speed_displays



# ---------------------
# Callbacks for reports (query + download)
# ---------------------
@app.callback(
    Output("report-results", "children"),
    [Input("btn-query", "n_clicks"), Input("btn-yesterday", "n_clicks"), Input("btn-7d", "n_clicks")],
    [State("report-pump", "value"), State("report-range", "start_date"), State("report-range", "end_date")],
    prevent_initial_call=True
)
def query_reports(n_query, n_yest, n_7d, pump, start_date, end_date):
    """Query historical data from database."""
    ctx = dash.callback_context
    if not ctx.triggered:
        return html.Div("Use the controls above to query historical data.", style={"color": "#aaa", "padding": "12px"})
    
    try:
        btn_id = ctx.triggered[0]["prop_id"].split(".")[0]
        today = datetime.now(timezone.utc).date()
        
        if btn_id == "btn-yesterday":
            start_date = (today - timedelta(days=1)).isoformat()
            end_date = start_date
        elif btn_id == "btn-7d":
            start_date = (today - timedelta(days=7)).isoformat()
            end_date = today.isoformat()

        sd = datetime.fromisoformat(start_date)
        ed = datetime.fromisoformat(end_date) + timedelta(days=1)
        
        # Query logs
        sql = "SELECT ts, pump_id, pressure, speed, ready, running, trip FROM pump_logs WHERE ts >= ? AND ts < ?"
        params = [sd.strftime("%Y-%m-%d %H:%M:%S"), ed.strftime("%Y-%m-%d %H:%M:%S")]
        if pump and pump != "all":
            sql += " AND pump_id = ?"
            params.append(pump)
        sql += " ORDER BY ts DESC LIMIT 500"
        df = db_conn.query(sql, params)

        # Query events
        esql = "SELECT ts, pump_id, event, pressure, speed FROM pump_events WHERE ts >= ? AND ts < ?"
        eparams = [sd.strftime("%Y-%m-%d %H:%M:%S"), ed.strftime("%Y-%m-%d %H:%M:%S")]
        if pump and pump != "all":
            esql += " AND pump_id = ?"
            eparams.append(pump)
        esql += " ORDER BY ts DESC LIMIT 500"
        df_events = db_conn.query(esql, eparams)
        
        if df.empty and df_events.empty:
            return html.Div("No data for selected range.", style={"color": "#aaa", "padding": "12px"})

        # Sort data for trend chart
        if not df.empty:
            df["ts_parsed"] = pd.to_datetime(df["ts"])
            df_sorted = df.sort_values("ts_parsed", ascending=True)
        else:
            df_sorted = pd.DataFrame()
        
        # Create data table
        table = dash_table.DataTable(
            id="report-table",
            columns=[{"name": c, "id": c} for c in (df.columns if not df.empty else [])],
            data=df.to_dict("records") if not df.empty else [],
            page_size=25,
            style_table={"overflowX": "auto", "maxHeight": "400px", "overflowY": "auto"},
            style_header={"background": "#111", "color": "#fff", "fontWeight": "bold"},
            style_cell={"background": "#0b0b0b", "color": "#ddd", "textAlign": "left", "padding": "8px"},
            style_data_conditional=[
                {"if": {"row_index": "odd"}, "backgroundColor": "#1a1a1a"},
                {"if": {"column_id": "trip"}, "color": "#ff6666"},
            ]
        )

        # Create trend chart
        fig = go.Figure()
        
        if not df_sorted.empty and "pressure" in df_sorted.columns:
            fig.add_trace(go.Scatter(
                x=df_sorted["ts_parsed"],
                y=df_sorted["pressure"],
                name="Pressure (BAR)",
                mode="lines+markers",
                line=dict(color="#00FFEF", width=3),
                marker=dict(size=6, symbol="circle"),
                fill="tozeroy",
                fillcolor="rgba(0, 255, 239, 0.1)",
                hovertemplate="<b>Pressure</b><br>Time: %{x}<br>Pressure: %{y:.2f} BAR<extra></extra>",
                yaxis="y"
            ))
        
        if not df_sorted.empty and "speed" in df_sorted.columns:
            fig.add_trace(go.Scatter(
                x=df_sorted["ts_parsed"],
                y=df_sorted["speed"],
                name="Speed (HZ)",
                mode="lines+markers",
                line=dict(color="#FFD700", width=3),
                marker=dict(size=6, symbol="diamond"),
                fill="tozeroy",
                fillcolor="rgba(255, 215, 0, 0.1)",
                hovertemplate="<b>Speed</b><br>Time: %{x}<br>Speed: %{y:.2f} HZ<extra></extra>",
                yaxis="y2"
            ))
        
        fig.update_layout(
            title=dict(
                text="<b>System Performance Trend View</b>",
                font=dict(size=20, color="#ff3333")
            ),
            plot_bgcolor="#0b0b0b",
            paper_bgcolor="#111111",
            font=dict(color="#ddd", size=11),
            height=600,
            hovermode="x unified",
            xaxis=dict(
                title="<b>Timeline</b>",
                gridcolor="#333333",
                showgrid=True,
                zeroline=False,
                showline=True,
                linewidth=2,
                linecolor="#444444"
            ),
            yaxis=dict(
                title=dict(text="<b>Pressure (BAR)</b>", font=dict(color="#00FFEF", size=14)),
                tickfont=dict(color="#00FFEF"),
                gridcolor="#333333",
                showgrid=True,
                zeroline=False,
                range=[0, 10],
                showline=True,
                linewidth=2,
                linecolor="#00FFEF"
            ),
            yaxis2=dict(
                title=dict(text="<b>Speed (HZ)</b>", font=dict(color="#FFD700", size=14)),
                tickfont=dict(color="#FFD700"),
                overlaying="y",
                side="right",
                range=[0, 50],
                showline=True,
                linewidth=2,
                linecolor="#FFD700",
                zeroline=False
            ),
            legend=dict(
                bgcolor="rgba(0, 0, 0, 0.5)",
                bordercolor="#444444",
                borderwidth=1,
                x=0.01,
                y=0.99,
                font=dict(size=12)
            ),
            margin=dict(l=80, r=80, t=100, b=80)
        )

        header = html.Div([
            html.Div(f"📊 Logs: {len(df)} | Events: {len(df_events)}", style={"color": "#aaa", "display": "inline-block", "marginRight": "12px", "fontSize": "14px", "fontWeight": "bold"}),
            html.Div(f"Pump: {pump}", style={"color": "#aaa", "display": "inline-block", "marginRight": "12px"}),
            html.Div(f"From {start_date} to {end_date}", style={"color": "#aaa", "display": "inline-block"})
        ], style={"padding": "8px"})

        parts = [header, dcc2.Graph(figure=fig, config={"displayModeBar": True, "responsive": True})]
        if not df.empty:
            parts.append(html.Hr())
            parts.append(html.H5("Pump Logs", style={"color": "#fff"}))
            parts.append(table)
        
        if not df_events.empty:
            # Enhance event data with event type parsing
            df_events['event_type'] = df_events['event'].str.extract(r'(TRIP|READY|RUNNING)')[0].fillna('OTHER')
            df_events['event_status'] = df_events['event'].str.extract(r'=(True|False)')[0].fillna('N/A')
            
            # Reorder columns for better display
            display_cols = ['ts', 'pump_id', 'event_type', 'event_status', 'pressure', 'speed']
            df_events_display = df_events[display_cols].rename(columns={
                'ts': 'Timestamp',
                'pump_id': 'Pump ID',
                'event_type': 'Event Type',
                'event_status': 'Status',
                'pressure': 'Pressure (Bar)',
                'speed': 'Speed (Hz)'
            })
            
            events_table = dash_table.DataTable(
                id="events-table",
                columns=[{"name": c, "id": c} for c in df_events_display.columns],
                data=df_events_display.to_dict("records"),
                page_size=10,
                style_table={"overflowX": "auto"},
                style_header={"background": "#111", "color": "#fff", "fontWeight": "bold"},
                style_cell={"background": "#0b0b0b", "color": "#f88", "textAlign": "left", "padding": "8px"},
                style_data_conditional=[
                    {"if": {"column_id": "Event Type"}, "color": "#ff6666", "fontWeight": "bold"},
                    {"if": {"column_id": "Status", "filter_query": '{Status} = True'}, "backgroundColor": "#1a4d1a", "color": "#00ff00"},
                    {"if": {"column_id": "Status", "filter_query": '{Status} = False'}, "backgroundColor": "#4d1a1a", "color": "#ff6666"},
                ]
            )
            parts.extend([html.Hr(), html.H5("Event Log (TRIP/READY/RUNNING)", style={"color": "#ff7777"}), events_table])
        
        return html.Div(parts)
    
    except Exception as e:
        logging.error(f"Error in query_reports: {e}")
        return html.Div(f"Error: {str(e)}", style={"color": "#ff7777", "padding": "12px"})

@app.callback(
    Output("download-data", "data"),
    [Input("btn-download", "n_clicks")],
    [State("report-pump", "value"), State("report-range", "start_date"), State("report-range", "end_date")],
    prevent_initial_call=True
)
def download_pdf(n_clicks, pump, start_date, end_date):
    """Download historical data as PDF report."""
    if not n_clicks:
        return dash.no_update
    
    try:
        logging.info(f"PDF download started for pump={pump}, dates={start_date} to {end_date}")
        
        sd = datetime.fromisoformat(start_date)
        ed = datetime.fromisoformat(end_date) + timedelta(days=1)
        
        # Query pump logs
        sql = "SELECT ts, pump_id, pressure, speed, ready, running, trip FROM pump_logs WHERE ts >= ? AND ts < ?"
        params = [sd.strftime("%Y-%m-%d %H:%M:%S"), ed.strftime("%Y-%m-%d %H:%M:%S")]
        if pump and pump != "all":
            sql += " AND pump_id = ?"
            params.append(pump)
        sql += " ORDER BY ts DESC LIMIT 500"
        df = db_conn.query(sql, params)
        logging.info(f"Found {len(df)} pump logs")
        
        # Query pump events
        esql = "SELECT ts, pump_id, event, pressure, speed FROM pump_events WHERE ts >= ? AND ts < ?"
        eparams = [sd.strftime("%Y-%m-%d %H:%M:%S"), ed.strftime("%Y-%m-%d %H:%M:%S")]
        if pump and pump != "all":
            esql += " AND pump_id = ?"
            eparams.append(pump)
        esql += " ORDER BY ts DESC LIMIT 500"
        df_events = db_conn.query(esql, eparams)
        logging.info(f"Found {len(df_events)} pump events")
        
        if df.empty and df_events.empty:
            logging.warning("No data found for PDF generation")
            return dash.no_update
        
        # Generate PDF
        pdf_bytes = generate_pdf_report(df, df_events, pump, start_date, end_date)
        
        if pdf_bytes is None or len(pdf_bytes) == 0:
            logging.error("PDF generation returned empty result")
            return dash.no_update
        
        filename = f"pump_logs_{pump}_{start_date.replace('/', '-')}_to_{end_date.replace('/', '-')}.pdf"
        logging.info(f"PDF generated successfully: {filename} ({len(pdf_bytes)} bytes)")
        
        return dcc2.send_bytes(pdf_bytes, filename)
    
    except Exception as e:
        logging.error(f"Error in download_pdf: {e}", exc_info=True)
        return dash.no_update

# run server
if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8052, debug=False, threaded=True, use_reloader=False)
