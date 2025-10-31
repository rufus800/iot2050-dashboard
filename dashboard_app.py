import io
import json
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import dash
import dash_daq as daq
import pandas as pd
import plotly.graph_objects as go
import snap7
from dash import dash_table, dcc as dcc2, html, Input, Output
import dash_bootstrap_components as dbc
from dash_iconify import DashIconify
from snap7.util import get_real, get_bool

# ---------------------
# Load config.json
# ---------------------
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
# Shared state
# ---------------------
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

def log_pump_data(pump_id: str, pdata: Dict[str, Any]) -> None:
    """Insert a pump_samples row."""
    try:
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
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
    except Exception:
        # keep PLC loop robust; ignore DB errors
        pass

def log_pump_event(pump_id: str, event: str, pressure: Optional[float], speed: Optional[float]) -> None:
    """Insert a pump event (e.g. TRIP)."""
    try:
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.connect(str(DB_FILE)) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO pump_events (ts, pump_id, event, pressure, speed) VALUES (?, ?, ?, ?, ?)",
                (ts, pump_id, event, pressure, speed),
            )
            conn.commit()
    except Exception:
        pass

create_db()

# track previous trip states for rising-edge event logging
_prev_trip_state: Dict[str, bool] = {pkey: False for pkey in pump_keys}

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
    client = snap7.client.Client()
    connected = False
    while True:
        try:
            if not connected:
                try:
                    client.connect(PLC["host"], PLC.get("rack", 0), PLC.get("slot", 1))
                    connected = client.get_connected()
                except Exception:
                    connected = False
                    # wait and retry
                    time.sleep(POLL_INTERVAL)
                    continue

            # HOME tags
            home_cfg = cfg.get("home", {})
            if home_cfg:
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

            # Pumps
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

                # log measurement row
                log_pump_data(pkey, st)

                # detect trip rising edge
                prev = _prev_trip_state.get(pkey, False)
                cur_trip = bool(st.get("trip", False))
                if cur_trip and not prev:
                    log_pump_event(pkey, "TRIP", st.get("pressure"), st.get("speed"))
                _prev_trip_state[pkey] = cur_trip

            # Chillers
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

        except Exception:
            # on any error reset connection and continue loop
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
# Build Dash app (UI preserved)
# ---------------------
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.index_string = """
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>ASEJIRE BF WATER PLANT SYSTEM</title>
        {%favicon%}
        {%css%}
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
    status_color = "#FF0000" if pdata["trip"] else "#00FF00" if pdata["running"] else "#FFD700" if pdata["ready"] else "#333333"
    # Removed duplicate header (H4). Keep only status indicators in the card header.
    return dbc.Card(
        [
            dbc.CardHeader(
                [
                    html.Div(
                        [
                            status_dot(pdata["ready"], "READY", cfg["pumps"][pkey]["ready"]["color"]),
                            status_dot(pdata["running"], "RUNNING", cfg["pumps"][pkey]["running"]["color"]),
                            status_dot(pdata["trip"], "TRIP", cfg["pumps"][pkey]["trip"]["color"]),
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
                                            id=f"pressure-gauge-{pkey}",
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
                                            id=f"speed-tank-{pkey}",
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
                                                daq.LEDDisplay(id=f"speed-display-{pkey}", value=str(pdata.get("speed", 0)), color="#00ff00", backgroundColor="#1e1e1e", size=24, style={"display": "inline-block"}),
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
    children=[
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

# Home page layout generation
def render_home():
    home = state["home"]
    cards = dbc.Row(
        [
            dbc.Col(
                dbc.Card(dbc.CardBody([html.H5("Total Energy", style={"color": "#ff7777"}), html.H2(f"{home.get('kwh','--')} kWh", style={"color": "#fff", "textShadow": "0 0 12px #ff0000"})]), style={"background": "#0b0b0b", "border": "1px solid rgba(255,0,0,0.08)", "borderRadius": "12px", "padding": "18px"}),
                md=3,
            ),
            dbc.Col(
                dbc.Card(dbc.CardBody([html.H5("Tank Level", style={"color": "#ff7777"}), html.H2(f"{home.get('level','--')} Ltr", style={"color": "#fff", "textShadow": "0 0 12px #ff0000"})]), style={"background": "#0b0b0b", "border": "1px solid rgba(255,0,0,0.08)", "borderRadius": "12px", "padding": "18px"}),
                md=3,
            ),
            dbc.Col(
                dbc.Card(dbc.CardBody([html.H5("Temperature", style={"color": "#ff7777"}), html.H2(f"{home.get('temp','--')} °C", style={"color": "#fff", "textShadow": "0 0 12px #ff0000"})]), style={"background": "#0b0b0b", "border": "1px solid rgba(255,0,0,0.08)", "borderRadius": "12px", "padding": "18px"}),
                md=3,
            ),
            dbc.Col(
                dbc.Card(dbc.CardBody([html.H5("Alarm", style={"color": "#ff7777"}), html.Div(className="alarm-indicator") if home.get("alarm") else html.Div("Inactive", style={"color":"#fff"})]), style={"background": "#0b0b0b", "border": "1px solid rgba(255,0,0,0.08)", "borderRadius": "12px", "padding": "18px"}),
                md=3,
            ),
        ],
        style={"padding": "20px", "maxWidth": "1200px", "margin": "24px auto"},
    )
    return html.Div([html.Div(style={"padding": "24px", "maxWidth": "1200px", "margin": "12px auto"}, children=[html.H3("System Overview", style={"color": "#ff3333", "textAlign": "center", "textShadow": "0 0 10px #ff0000"}), cards, html.Div(f"Last updated: {home.get('ts','--')}", style={"color": "#aaa", "textAlign": "center", "marginTop": "6px"})])])

def render_pump(pkey: str):
    pdata = state["pumps"].get(pkey, {})
    if not pdata:
        return html.Div(f"Pump {pkey} not configured", style={"color": "#fff"})
    return html.Div([html.Div(style={"padding": "24px", "maxWidth": "1200px", "margin": "12px auto"}, children=[html.H3(pdata.get("label", pkey).upper(), style={"color": "#ff3333", "textAlign": "center"}), dbc.Row([dbc.Col(html.Div(pump_card(pkey, pdata)), md=8)], justify="center"), html.Div(style={"textAlign": "center", "color": "#888", "marginTop": "10px"}, children=[f"Last update: {pdata.get('ts','--')}"])])])

def render_chillers():
    chcards = []
    for ckey, cinfo in state["chillers"].items():
        cfg_cinfo = cfg.get("chillers", {}).get(ckey, {})
        chcards.append(
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.H5(ckey.upper(), style={"color": "#ff3333"}),
                            html.Div(
                                [
                                    status_dot(cinfo.get("ready", False), "READY", cfg_cinfo.get("ready", {}).get("color")),
                                    status_dot(cinfo.get("running", False), "RUNNING", cfg_cinfo.get("running", {}).get("color")),
                                    status_dot(cinfo.get("trip", False), "TRIP", cfg_cinfo.get("trip", {}).get("color")),
                                ],
                                style={"display": "flex", "justifyContent": "space-between"},
                            ),
                            html.Div(f"Last: {cinfo.get('ts','--')}", style={"color": "#999"}),
                        ]
                    ),
                    style={"background": "#0b0b0b", "border": "1px solid rgba(255,0,0,0.08)", "borderRadius": "12px", "padding": "18px"},
                ),
                md=4,
            )
        )
    return html.Div([html.Div(style={"padding": "24px", "maxWidth": "1200px", "margin": "12px auto"}, children=[html.H3("Chillers Overview", style={"color": "#ff3333", "textAlign": "center"}), dbc.Row(chcards, justify="center")])])

def render_reports():
    pump_options = [{"label": cfg["pumps"][k].get("label", k).upper(), "value": k} for k in pump_keys]
    pump_options.insert(0, {"label": "All Pumps", "value": "all"})
    today = datetime.utcnow().date()
    return html.Div(style={"padding": "24px", "maxWidth": "1200px", "margin": "12px auto"}, children=[html.H3("Historical Reports", style={"color": "#ff3333", "textAlign": "center"}), dbc.Card(dbc.CardBody([dbc.Row([dbc.Col([html.Label("Pump", style={"color": "#ccc"}), dcc2.Dropdown(id="report-pump", options=pump_options, value="pump1")], md=3), dbc.Col([html.Label("Date range", style={"color": "#ccc"}), dcc2.DatePickerRange(id="report-range", start_date=(today - timedelta(days=1)).isoformat(), end_date=today.isoformat(), display_format="YYYY-MM-DD")], md=4), dbc.Col([html.Label("Quick", style={"color": "#ccc"}), dbc.Button("Yesterday", id="btn-yesterday", color="secondary", style={"marginRight": "8px"}), dbc.Button("Last 7 days", id="btn-7d", color="secondary")], md=3), dbc.Col([dbc.Button("Query", id="btn-query", color="primary"), dbc.Button("Download CSV", id="btn-download", color="success", style={"marginLeft": "8px"}), dcc2.Download(id="download-data")], md=2)]), html.Hr(), html.Div(id="report-results")]), style={"background": "#0b0b0b", "border": "1px solid rgba(255,0,0,0.06)", "borderRadius": "12px", "padding": "12px"})])

# Router callback
@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def display_page(pathname: Optional[str]):
    if pathname is None or pathname in ("/", "/home"):
        return render_home()
    if pathname.startswith("/pump/"):
        pkey = pathname.split("/pump/")[-1]
        return render_pump(pkey)
    if pathname.startswith("/chillers"):
        return render_chillers()
    if pathname.startswith("/reports"):
        return render_reports()
    return html.Div("Page not found", style={"color": "#fff"})

# client-side interval refresh
app.layout.children.append(dcc2.Interval(id="interval-refresh", interval=POLL_INTERVAL * 1000, n_intervals=0))

# ---------------------
# Reports callbacks (query + download) (behavior preserved)
# ---------------------
@app.callback(
    Output("report-results", "children"),
    [Input("btn-query", "n_clicks"), Input("btn-yesterday", "n_clicks"), Input("btn-7d", "n_clicks")],
    [dash.dependencies.State("report-pump", "value"), dash.dependencies.State("report-range", "start_date"), dash.dependencies.State("report-range", "end_date")],
)
def query_reports(n_query, n_yest, n_7d, pump, start_date, end_date):
    ctx = dash.callback_context
    if not ctx.triggered:
        return html.Div("Use the controls above to query historical data.", style={"color": "#aaa", "padding": "12px"})
    btn_id = ctx.triggered[0]["prop_id"].split(".")[0]
    today = datetime.utcnow().date()
    if btn_id == "btn-yesterday":
        start_date = (today - timedelta(days=1)).isoformat()
        end_date = start_date
    elif btn_id == "btn-7d":
        start_date = (today - timedelta(days=7)).isoformat()
        end_date = today.isoformat()

    try:
        sd = datetime.fromisoformat(start_date)
        ed = datetime.fromisoformat(end_date) + timedelta(days=1)
    except Exception:
        return html.Div("Invalid date range", style={"color": "#ff7777"})

    conn = sqlite3.connect(str(DB_FILE))
    sql = "SELECT ts, pump_id, pressure, speed, ready, running, trip FROM pump_logs WHERE ts >= ? AND ts < ?"
    params = [sd.strftime("%Y-%m-%d %H:%M:%S"), ed.strftime("%Y-%m-%d %H:%M:%S")]
    if pump and pump != "all":
        sql += " AND pump_id = ?"
        params.append(pump)
    df = pd.read_sql_query(sql, conn, params=params)

    esql = "SELECT ts, pump_id, event, pressure, speed FROM pump_events WHERE ts >= ? AND ts < ?"
    eparams = [sd.strftime("%Y-%m-%d %H:%M:%S"), ed.strftime("%Y-%m-%d %H:%M:%S")]
    if pump and pump != "all":
        esql += " AND pump_id = ?"
        eparams.append(pump)
    df_events = pd.read_sql_query(esql, conn, params=eparams)

    conn.close()
    if df.empty and df_events.empty:
        return html.Div("No data for selected range.", style={"color": "#aaa", "padding": "12px"})

    table = dash_table.DataTable(id="report-table", columns=[{"name": c, "id": c} for c in df.columns] if not df.empty else [], data=df.to_dict("records") if not df.empty else [], page_size=25, style_table={"overflowX": "auto"}, style_header={"background": "#111", "color": "#fff"}, style_cell={"background": "#0b0b0b", "color": "#ddd", "textAlign": "left"})

    df["ts_parsed"] = pd.to_datetime(df["ts"]) if not df.empty else pd.Series(dtype="datetime64[ns]")
    fig = go.Figure()
    if not df.empty and "pressure" in df.columns:
        fig.add_trace(go.Scatter(x=df["ts_parsed"], y=df["pressure"], mode="lines+markers", name="Pressure (bar)", line={"color": "#00FFEF"}))
    if not df.empty and "speed" in df.columns:
        fig.add_trace(go.Scatter(x=df["ts_parsed"], y=df["speed"], mode="lines+markers", name="Speed (Hz)", yaxis="y2", line={"color": "#FFD700"}))
    fig.update_layout(plot_bgcolor="#0b0b0b", paper_bgcolor="#0b0b0b", font={"color": "#ddd"}, xaxis_title="Time", yaxis_title="Pressure (bar)", yaxis2={"overlaying": "y", "side": "right", "title": "Speed (Hz)"}, legend={"bgcolor": "rgba(0,0,0,0)"})

    header = html.Div([html.Div(f"Rows: {len(df)}", style={"color": "#aaa", "display": "inline-block", "marginRight": "12px"}), html.Div(f"Events: {len(df_events)}", style={"color": "#aaa", "display": "inline-block", "marginRight": "12px"}), html.Div(f"Pump: {pump}", style={"color": "#aaa", "display": "inline-block", "marginRight": "12px"}), html.Div(f"From {start_date} to {end_date}", style={"color": "#aaa", "display": "inline-block"})], style={"padding": "8px"})

    events_table = None
    if not df_events.empty:
        events_table = dash_table.DataTable(id="events-table", columns=[{"name": c, "id": c} for c in df_events.columns], data=df_events.to_dict("records"), page_size=10, style_table={"overflowX": "auto"}, style_header={"background": "#111", "color": "#fff"}, style_cell={"background": "#0b0b0b", "color": "#f88", "textAlign": "left"})

    parts = [header, dcc2.Graph(figure=fig, config={"displayModeBar": True})]
    if not df.empty:
        parts.append(table)
    if events_table is not None:
        parts.extend([html.Hr(), html.H5("Logged Events (Trips)", style={"color": "#ff7777"}), events_table])
    return html.Div(parts)

@app.callback(Output("download-data", "data"), [Input("btn-download", "n_clicks")], [dash.dependencies.State("report-pump", "value"), dash.dependencies.State("report-range", "start_date"), dash.dependencies.State("report-range", "end_date")])
def download_csv(n_clicks, pump, start_date, end_date):
    if not n_clicks:
        return dash.no_update
    try:
        sd = datetime.fromisoformat(start_date)
        ed = datetime.fromisoformat(end_date) + timedelta(days=1)
    except Exception:
        return dash.no_update
    conn = sqlite3.connect(str(DB_FILE))
    sql = "SELECT ts, pump_id, pressure, speed, ready, running, trip FROM pump_logs WHERE ts >= ? AND ts < ?"
    params = [sd.strftime("%Y-%m-%d %H:%M:%S"), ed.strftime("%Y-%m-%d %H:%M:%S")]
    if pump and pump != "all":
        sql += " AND pump_id = ?"
        params.append(pump)
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    if df.empty:
        return dash.no_update
    return dcc2.send_data_frame(df.to_csv, f"pump_logs_{pump}_{start_date}_to_{end_date}.csv", index=False)

# run server
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8051, debug=False)
