
import os
import json
import time
import uuid
import queue
import logging
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta

import pandas as pd
import streamlit as st
import altair as alt
import paho.mqtt.client as mqtt

# =========================================
# Projeto: monitoramento_energia (Dashboard)
# =========================================

ROOT_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT_DIR / "config" / "config.json"
LOG_DIR = ROOT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "dashboard.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

CURRENT_SCHEMA_VERSION = 4

DEFAULT_CFG = {
    "MQTT_HOST": "localhost",
    "MQTT_PORT": 1883,
    "MQTT_USER": "",
    "MQTT_PASS": "",
    "DB_PATH": "data/monitoramento.db",
    "DEFAULT_LUGAR": "casa",
    "DEFAULT_MEDICAO": "energia",
    "TOPIC_ACK": ["monitoramento_energia/+/+/ack", "+/+/+/+/ack"],
    "TOPIC_CMD_TEMPLATE": "monitoramento_energia/{ambiente}/{device_id}/cmd",
    "PUBLISH_COMPAT_BOTH": True
}


# =========================
# Helpers gerais
# =========================
def _as_list(x):
    return x if isinstance(x, list) else [x]


def _resolve_path(p: str) -> str:
    pp = Path(p)
    return str(pp if pp.is_absolute() else (ROOT_DIR / pp).resolve())


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def parse_dt_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def humanize_last_seen(last_seen_utc: str) -> str:
    if not last_seen_utc:
        return "sem registro"
    try:
        dt = datetime.fromisoformat(str(last_seen_utc).replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = now - dt.astimezone(timezone.utc)
        secs = int(delta.total_seconds())
        if secs < 0:
            return "agora"
        if secs < 60:
            return f"há {secs} s"
        mins = secs // 60
        if mins < 60:
            return f"há {mins} min"
        hours = mins // 60
        if hours < 24:
            return f"há {hours} h"
        days = hours // 24
        return f"há {days} dia(s)"
    except Exception:
        return str(last_seen_utc)


def status_badge_html(status: str, last_seen_utc: str) -> str:
    st_norm = (status or "offline").strip().lower()
    if st_norm == "online":
        bg = "#d1fae5"
        fg = "#065f46"
        label = "ONLINE"
        dot = "🟢"
    elif st_norm == "offline":
        bg = "#fee2e2"
        fg = "#991b1b"
        label = "OFFLINE"
        dot = "🔴"
    else:
        bg = "#fef3c7"
        fg = "#92400e"
        label = st_norm.upper()
        dot = "🟡"

    last_seen_h = humanize_last_seen(last_seen_utc)

    return f"""
    <div style="display:flex; gap:12px; align-items:center; flex-wrap:wrap; margin:6px 0 14px 0;">
        <span style="
            background:{bg};
            color:{fg};
            padding:6px 12px;
            border-radius:999px;
            font-weight:700;
            font-size:0.95rem;
            display:inline-block;">
            {dot} {label}
        </span>
        <span style="color:#475569; font-size:0.95rem;">
            Último contato: <strong>{last_seen_h}</strong>
        </span>
    </div>
    """


def metric_text(value, fmt: str, suffix: str = "") -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{format(value, fmt)}{suffix}"


# =========================
# Config
# =========================
def load_config():
    if CONFIG_PATH.exists():
        try:
            data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            cfg = {**DEFAULT_CFG, **data}
        except Exception as e:
            logging.warning(f"Falha lendo config/config.json. Usando default. Erro: {e}")
            cfg = DEFAULT_CFG.copy()
    else:
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        CONFIG_PATH.write_text(json.dumps(DEFAULT_CFG, indent=2, ensure_ascii=False), encoding="utf-8")
        cfg = DEFAULT_CFG.copy()

    cfg["MQTT_HOST"] = os.getenv("MQTT_HOST", str(cfg["MQTT_HOST"]))
    cfg["MQTT_PORT"] = int(os.getenv("MQTT_PORT", str(cfg["MQTT_PORT"])))
    cfg["MQTT_USER"] = os.getenv("MQTT_USER", str(cfg.get("MQTT_USER", "")))
    cfg["MQTT_PASS"] = os.getenv("MQTT_PASS", str(cfg.get("MQTT_PASS", "")))
    cfg["DB_PATH"] = _resolve_path(os.getenv("ENERGIA_DB", str(cfg["DB_PATH"])))
    return cfg


# =========================
# DB + Migrações
# =========================
def init_db(db_path):
    conn = sqlite3.connect(db_path, check_same_thread=False)
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys=ON;")
    cur.execute("PRAGMA journal_mode=WAL;")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_meta(
            id INTEGER PRIMARY KEY CHECK (id=1),
            version INTEGER NOT NULL
        )
    """)
    conn.commit()

    row = cur.execute("SELECT version FROM schema_meta WHERE id=1").fetchone()
    if row is None:
        cur.execute("INSERT INTO schema_meta(id, version) VALUES (1, 0)")
        conn.commit()

    migrate_db(conn, cur)
    return conn, cur


def _get_schema_version(cur):
    row = cur.execute("SELECT version FROM schema_meta WHERE id=1").fetchone()
    return int(row[0]) if row else 0


def _set_schema_version(conn, cur, v):
    cur.execute("UPDATE schema_meta SET version=? WHERE id=1", (int(v),))
    conn.commit()


def _add_column_if_missing(conn, cur, table, col, coltype, default_sql=None):
    cols = [r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()]
    if col in cols:
        return
    if default_sql is None:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
    else:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype} DEFAULT {default_sql}")
    conn.commit()


def migrate_db(conn, cur):
    version = _get_schema_version(cur)

    if version < 1:
        cur.executescript("""
        CREATE TABLE IF NOT EXISTS environment (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          lugar TEXT NOT NULL,
          ambiente TEXT NOT NULL,
          created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE UNIQUE INDEX IF NOT EXISTS ux_environment ON environment(lugar, ambiente);

        CREATE TABLE IF NOT EXISTS device (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          lugar TEXT NOT NULL,
          ambiente TEXT NOT NULL,
          medicao TEXT NOT NULL,
          dispositivo TEXT NOT NULL,
          name TEXT,
          status TEXT DEFAULT 'offline',
          last_seen_utc TEXT,
          created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE UNIQUE INDEX IF NOT EXISTS ux_device ON device(lugar, ambiente, medicao, dispositivo);

        CREATE TABLE IF NOT EXISTS reading (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          device_id INTEGER NOT NULL,
          ts_utc TEXT NOT NULL,
          vrms REAL,
          irms REAL,
          p REAL,
          pf REAL,
          created_at TEXT DEFAULT (datetime('now')),
          FOREIGN KEY(device_id) REFERENCES device(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS ix_reading_device_ts ON reading(device_id, ts_utc);

        CREATE TABLE IF NOT EXISTS relay_channel (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          device_id INTEGER NOT NULL,
          channel_no INTEGER NOT NULL,
          label TEXT,
          last_state TEXT,
          updated_at TEXT,
          FOREIGN KEY(device_id) REFERENCES device(id) ON DELETE CASCADE
        );
        CREATE UNIQUE INDEX IF NOT EXISTS ux_relay_channel ON relay_channel(device_id, channel_no);

        CREATE TABLE IF NOT EXISTS rule (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          device_id INTEGER NOT NULL,
          metric TEXT NOT NULL,
          threshold REAL NOT NULL,
          operator TEXT NOT NULL,
          action TEXT NOT NULL,
          alert_text TEXT,
          enabled INTEGER NOT NULL DEFAULT 1,
          cooldown_s INTEGER DEFAULT 0,
          last_trigger_utc TEXT,
          created_at TEXT DEFAULT (datetime('now')),
          FOREIGN KEY(device_id) REFERENCES device(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS ix_rule_device_enabled ON rule(device_id, enabled);

        CREATE TABLE IF NOT EXISTS event_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          device_id INTEGER,
          ts_utc TEXT NOT NULL,
          level TEXT NOT NULL,
          code TEXT,
          msg TEXT,
          FOREIGN KEY(device_id) REFERENCES device(id) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS ix_event_ts ON event_log(ts_utc);
        """)
        _set_schema_version(conn, cur, 1)
        version = 1

    if version < 2:
        _set_schema_version(conn, cur, 2)
        version = 2

    if version < 3:
        _add_column_if_missing(conn, cur, "reading", "e", "REAL")
        _add_column_if_missing(conn, cur, "reading", "f", "REAL")
        _add_column_if_missing(conn, cur, "reading", "seq", "INTEGER")
        _set_schema_version(conn, cur, 3)
        version = 3

    if version < 4:
        _add_column_if_missing(conn, cur, "rule", "edge_only", "INTEGER", "1")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rule_state (
              rule_id INTEGER PRIMARY KEY,
              is_active INTEGER NOT NULL DEFAULT 0,
              initialized INTEGER NOT NULL DEFAULT 0,
              last_eval_utc TEXT,
              last_change_utc TEXT,
              FOREIGN KEY(rule_id) REFERENCES rule(id) ON DELETE CASCADE
            )
        """)
        conn.commit()
        _set_schema_version(conn, cur, 4)
        version = 4


# =========================
# MQTT (somente cmd + ack)
# =========================
def parse_topic(topic: str, default_lugar="casa", default_medicao="energia"):
    parts = topic.split("/")
    if len(parts) == 4 and parts[0] == "monitoramento_energia":
        _, ambiente, device_id, fluxo = parts
        return (default_lugar, ambiente, default_medicao, device_id, fluxo)
    if len(parts) == 5:
        return tuple(parts)
    return None


def init_mqtt_for_ack(cfg, incoming_ack: queue.Queue):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    if cfg.get("MQTT_USER"):
        client.username_pw_set(cfg.get("MQTT_USER"), cfg.get("MQTT_PASS", ""))

    def on_connect(client, userdata, flags, rc, properties=None):
        logging.info(f"MQTT conectado: {rc}")
        for t in _as_list(cfg.get("TOPIC_ACK", [])):
            client.subscribe(t, qos=1)

    def on_message(client, userdata, msg):
        parsed = parse_topic(msg.topic, cfg.get("DEFAULT_LUGAR", "casa"), cfg.get("DEFAULT_MEDICAO", "energia"))
        if not parsed:
            return
        lugar, ambiente, medicao, dispositivo, fluxo = parsed
        if fluxo != "ack":
            return
        try:
            data = json.loads(msg.payload.decode("utf-8", errors="ignore").strip())
            incoming_ack.put(((lugar, ambiente, medicao, dispositivo), data))
        except Exception:
            pass

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(cfg["MQTT_HOST"], cfg["MQTT_PORT"], 60)
    client.loop_start()
    return client


def publish_cmd(cfg, mqtt_client, lugar, ambiente, medicao, dispositivo, channel, state):
    req_id = str(uuid.uuid4())
    payload = {
        "ts": int(time.time() * 1000),
        "type": "relay",
        "channel": int(channel),
        "state": str(state).upper(),
        "req_id": req_id
    }

    topic_new = cfg["TOPIC_CMD_TEMPLATE"].format(ambiente=ambiente, device_id=dispositivo)
    mqtt_client.publish(topic_new, json.dumps(payload), qos=1, retain=False)

    if cfg.get("PUBLISH_COMPAT_BOTH", True):
        topic_legacy = f"{lugar}/{ambiente}/{medicao}/{dispositivo}/cmd"
        if topic_legacy != topic_new:
            mqtt_client.publish(topic_legacy, json.dumps(payload), qos=1, retain=False)

    return req_id


def wait_ack(incoming_ack, key_tuple, req_id, timeout=5.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            lk, ak = incoming_ack.get(timeout=0.2)
            if lk == key_tuple and ak.get("req_id") == req_id:
                ok = bool(ak.get("ok", False))
                details = ak.get("details") or ak.get("msg") or ak.get("code") or ""
                return ok, details
        except queue.Empty:
            pass
    return False, "ACK timeout"


# =========================
# Queries
# =========================
def list_devices_df(conn):
    return pd.read_sql_query("""
        SELECT id, lugar, ambiente, medicao, dispositivo, name, status, last_seen_utc
        FROM device ORDER BY lugar, ambiente, dispositivo
    """, conn)


def list_rules_df(conn):
    q = """
    SELECT r.id, r.device_id, d.lugar||'/'||d.ambiente||'/'||d.medicao||'/'||d.dispositivo AS device_key,
           r.metric, r.threshold, r.operator, r.action, r.alert_text, r.enabled,
           r.cooldown_s, r.last_trigger_utc, r.edge_only, r.created_at
    FROM rule r
    JOIN device d ON d.id = r.device_id
    ORDER BY r.created_at DESC
    """
    df = pd.read_sql_query(q, conn)
    if not df.empty:
        df["modo_disparo"] = df["edge_only"].apply(lambda x: "Transição" if int(x) == 1 else "Contínuo")
        df = df[["id", "device_id", "device_key", "metric", "threshold", "operator", "action",
                 "alert_text", "enabled", "cooldown_s", "modo_disparo", "last_trigger_utc", "created_at"]]
    return df


def get_readings_df(conn, dev_id: int, start_iso: str, end_iso: str) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT ts_utc, vrms, irms, p, pf, e, f
        FROM reading
        WHERE device_id=? AND ts_utc BETWEEN ? AND ?
        ORDER BY ts_utc
        """,
        conn,
        params=(dev_id, start_iso, end_iso)
    )


def get_events_df(conn, device_id=None, start_iso=None, end_iso=None, limit=5000) -> pd.DataFrame:
    q = """
    SELECT
        e.ts_utc,
        e.level,
        e.code,
        e.msg,
        e.device_id,
        CASE
            WHEN d.id IS NOT NULL THEN d.lugar||'/'||d.ambiente||'/'||d.medicao||'/'||d.dispositivo
            ELSE '(sem dispositivo)'
        END AS device_key
    FROM event_log e
    LEFT JOIN device d ON d.id = e.device_id
    WHERE 1=1
    """
    params = []

    if device_id is not None:
        q += " AND e.device_id=?"
        params.append(int(device_id))

    if start_iso is not None:
        q += " AND e.ts_utc >= ?"
        params.append(start_iso)

    if end_iso is not None:
        q += " AND e.ts_utc <= ?"
        params.append(end_iso)

    q += " ORDER BY e.ts_utc DESC LIMIT ?"
    params.append(int(limit))

    return pd.read_sql_query(q, conn, params=params)


def insert_rule(conn, device_id, metric, threshold, operator, action, alert_text, enabled, cooldown_s, edge_only):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO rule(device_id, metric, threshold, operator, action, alert_text, enabled, cooldown_s, edge_only, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
    """, (device_id, metric, float(threshold), operator, action, alert_text or "",
          1 if enabled else 0, int(cooldown_s or 0), 1 if edge_only else 0))
    conn.commit()


def delete_rule(conn, rule_id):
    cur = conn.cursor()
    cur.execute("DELETE FROM rule WHERE id=?", (rule_id,))
    conn.commit()


def set_rule_enabled(conn, rule_id, enabled: bool):
    cur = conn.cursor()
    cur.execute("UPDATE rule SET enabled=? WHERE id=?", (1 if enabled else 0, rule_id))
    conn.commit()


def delete_device_and_history(conn, device_id: int):
    cur = conn.cursor()
    cur.execute("DELETE FROM event_log WHERE device_id=?", (int(device_id),))
    cur.execute("DELETE FROM device WHERE id=?", (int(device_id),))
    conn.commit()


def _relay_last_state(conn, dev_id, ch):
    df = pd.read_sql_query(
        "SELECT last_state, label FROM relay_channel WHERE device_id=? AND channel_no=?",
        conn, params=(dev_id, ch)
    )
    if df.empty:
        return None, f"Relé {ch}"
    return df["last_state"].iloc[0], (df["label"].iloc[0] or f"Relé {ch}")


def _set_relay_state_db(conn, dev_id, ch, state):
    cur = conn.cursor()
    cur.execute("""INSERT OR IGNORE INTO relay_channel(device_id, channel_no, last_state, label)
                   VALUES (?, ?, NULL, ?)""", (dev_id, ch, f"Relé {ch}"))
    cur.execute("""UPDATE relay_channel SET last_state=?, updated_at=? WHERE device_id=? AND channel_no=?""",
                (state, datetime.now(timezone.utc).isoformat(), dev_id, ch))
    conn.commit()


def _save_relay_label(conn, dev_id, ch, label):
    cur = conn.cursor()
    cur.execute("""INSERT OR IGNORE INTO relay_channel(device_id, channel_no, last_state, label)
                   VALUES (?, ?, NULL, ?)""",
                (dev_id, ch, label))
    cur.execute("""UPDATE relay_channel SET label=? WHERE device_id=? AND channel_no=?""",
                (label, dev_id, ch))
    conn.commit()


# =========================
# Preparação de dados para gráficos
# =========================
def prepare_plot_df(dfr: pd.DataFrame) -> pd.DataFrame:
    if dfr.empty:
        return dfr

    dfp = dfr.copy()
    dfp["ts_dt"] = parse_dt_utc(dfp["ts_utc"])
    dfp = dfp.dropna(subset=["ts_dt"]).sort_values("ts_dt").reset_index(drop=True)

    if dfp.empty:
        return dfp

    diffs = dfp["ts_dt"].diff().dropna()
    if diffs.empty:
        gap_threshold = pd.Timedelta(seconds=30)
    else:
        med = diffs.median()
        gap_threshold = max(med * 3, pd.Timedelta(seconds=30))

    breaks = dfp["ts_dt"].diff() > gap_threshold
    dfp["segment"] = breaks.fillna(False).cumsum().astype(str)
    dfp["ts_plot"] = dfp["ts_dt"].dt.tz_localize(None)
    return dfp


def prepare_events_plot_df(dfe: pd.DataFrame) -> pd.DataFrame:
    if dfe.empty:
        return dfe
    dfe = dfe.copy()
    dfe["ts_dt"] = parse_dt_utc(dfe["ts_utc"])
    dfe = dfe.dropna(subset=["ts_dt"]).sort_values("ts_dt").reset_index(drop=True)
    dfe["ts_plot"] = dfe["ts_dt"].dt.tz_localize(None)
    return dfe


def build_time_chart(dfr: pd.DataFrame, ycol: str, title: str, events_df: pd.DataFrame | None = None) -> alt.Chart:
    if dfr.empty or ycol not in dfr.columns or dfr[ycol].dropna().empty:
        empty = pd.DataFrame({"ts_plot": [], ycol: [], "segment": []})
        return alt.Chart(empty).mark_line().properties(height=260, title=title)

    dfp = prepare_plot_df(dfr)
    if dfp.empty or dfp[ycol].dropna().empty:
        empty = pd.DataFrame({"ts_plot": [], ycol: [], "segment": []})
        return alt.Chart(empty).mark_line().properties(height=260, title=title)

    tmin = dfp["ts_plot"].min()
    tmax = dfp["ts_plot"].max()
    span = tmax - tmin if pd.notna(tmin) and pd.notna(tmax) else timedelta(days=1)
    xfmt = "%H:%M:%S" if span <= timedelta(days=1) else "%d/%m %H:%M"

    zoom_x = alt.selection_interval(bind="scales", encodings=["x"])

    line = (
        alt.Chart(dfp)
        .mark_line()
        .encode(
            x=alt.X(
                "ts_plot:T",
                title="Data/hora",
                axis=alt.Axis(format=xfmt, labelAngle=-20, tickCount=8)
            ),
            y=alt.Y(f"{ycol}:Q", title=title),
            detail="segment:N",
            tooltip=[
                alt.Tooltip("ts_plot:T", title="Data/hora", format="%d/%m %H:%M:%S"),
                alt.Tooltip(f"{ycol}:Q", title=title, format=".4f")
            ]
        )
        .properties(height=260, title=title)
        .add_params(zoom_x)
    )

    if events_df is not None and not events_df.empty:
        ev = prepare_events_plot_df(events_df)
        rules = (
            alt.Chart(ev)
            .mark_rule(opacity=0.35)
            .encode(
                x=alt.X("ts_plot:T"),
                tooltip=[
                    alt.Tooltip("ts_plot:T", title="Evento", format="%d/%m %H:%M:%S"),
                    alt.Tooltip("level:N", title="Nível"),
                    alt.Tooltip("code:N", title="Código"),
                    alt.Tooltip("msg:N", title="Mensagem")
                ]
            )
        )
        return alt.layer(line, rules).resolve_scale(y="shared")

    return line


def render_period_summary_cards(dfr: pd.DataFrame):
    if dfr.empty:
        return

    e_valid = dfr["e"].dropna() if "e" in dfr.columns else pd.Series(dtype=float)
    energy_delta = None
    if len(e_valid) >= 2:
        energy_delta = float(e_valid.iloc[-1] - e_valid.iloc[0])

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Leituras", f"{len(dfr)}")
    c2.metric("Potência média", metric_text(dfr["p"].mean(), ".2f", " W"))
    c3.metric("Potência máxima", metric_text(dfr["p"].max(), ".2f", " W"))
    c4.metric("Tensão média", metric_text(dfr["vrms"].mean(), ".2f", " V"))
    c5.metric("Corrente média", metric_text(dfr["irms"].mean(), ".3f", " A"))
    c6.metric("Energia no período", metric_text(energy_delta, ".4f", " kWh"))


# =========================
# UI - Dashboard
# =========================
def render_dashboard(conn):
    st.header("📊 Monitoramento em tempo real")

    df_dev = list_devices_df(conn)
    if df_dev.empty:
        st.info("Nenhum dispositivo cadastrado ainda. O ingestor cadastra automaticamente quando recebe dados.")
        return

    df_dev["key"] = df_dev.apply(
        lambda r: f"{r['lugar']}/{r['ambiente']}/{r['medicao']}/{r['dispositivo']}",
        axis=1
    )

    dev_idx = st.selectbox(
        "Dispositivo",
        df_dev.index,
        format_func=lambda i: df_dev.loc[i, "key"],
        key="dash_dev_sel"
    )
    row = df_dev.loc[dev_idx]
    dev_id = int(row["id"])

    st.markdown(status_badge_html(row["status"], row["last_seen_utc"]), unsafe_allow_html=True)

    colf1, colf2, colf3 = st.columns([1, 1, 1])
    with colf1:
        dt_start = st.date_input(
            "Data inicial",
            value=(datetime.now().date() - timedelta(days=1)),
            key="dash_dt_start"
        )
    with colf2:
        dt_end = st.date_input(
            "Data final",
            value=datetime.now().date(),
            key="dash_dt_end"
        )
    with colf3:
        show_events = st.toggle(
            "Sobrepor eventos nos gráficos",
            value=False,
            key="dash_show_events",
            help="Mostra marcadores verticais de eventos no período selecionado."
        )

    start_iso = datetime.combine(dt_start, datetime.min.time()).replace(tzinfo=timezone.utc).isoformat()
    end_iso = datetime.combine(dt_end, datetime.max.time()).replace(tzinfo=timezone.utc).isoformat()

    dfr = get_readings_df(conn, dev_id, start_iso, end_iso)

    if dfr.empty:
        st.warning("Sem dados no período selecionado.")
        return

    last = dfr.iloc[-1]
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Tensão", metric_text(last.get("vrms"), ".2f", " V"))
    c2.metric("Corrente", metric_text(last.get("irms"), ".3f", " A"))
    c3.metric("Potência", metric_text(last.get("p"), ".2f", " W"))
    c4.metric("FP", metric_text(last.get("pf"), ".3f"))
    c5.metric("Energia", metric_text(last.get("e"), ".4f", " kWh"))
    c6.metric("Frequência", metric_text(last.get("f"), ".2f", " Hz"))

    st.markdown("#### Resumo do período selecionado")
    render_period_summary_cards(dfr)

    dfe = get_events_df(conn, device_id=dev_id, start_iso=start_iso, end_iso=end_iso, limit=2000) if show_events else pd.DataFrame()

    cexp1, cexp2 = st.columns([1, 1])
    with cexp1:
        st.download_button(
            "⬇️ Exportar leituras (CSV)",
            data=df_to_csv_bytes(dfr),
            file_name=f"leituras_{row['dispositivo']}_{dt_start}_{dt_end}.csv",
            mime="text/csv",
            key="download_readings_csv"
        )
    with cexp2:
        st.caption("Dica: use o mouse no gráfico para ampliar/navegar no eixo X.")

    st.altair_chart(build_time_chart(dfr, "p", "Potência (W)", dfe), use_container_width=True)
    st.altair_chart(build_time_chart(dfr, "vrms", "Tensão (V)", dfe), use_container_width=True)
    st.altair_chart(build_time_chart(dfr, "irms", "Corrente (A)", dfe), use_container_width=True)
    st.altair_chart(build_time_chart(dfr, "pf", "Fator de potência", dfe), use_container_width=True)

    if dfr["e"].dropna().any():
        st.altair_chart(build_time_chart(dfr, "e", "Energia acumulada (kWh)", dfe), use_container_width=True)

    if dfr["f"].dropna().any():
        st.altair_chart(build_time_chart(dfr, "f", "Frequência (Hz)", dfe), use_container_width=True)


# =========================
# UI - Dispositivos e Controle
# =========================
def render_led_controls(conn, cfg, mqtt_client, incoming_ack, row):
    st.markdown("### Controle rápido (1 relé)")
    dev_id = int(row["id"])
    lugar, ambiente, medicao, dispositivo = row["lugar"], row["ambiente"], row["medicao"], row["dispositivo"]

    st.markdown("""
    <style>
      div.stButton > button {
        font-size: 38px;
        height: 80px;
        width: 80px;
        border-radius: 50%;
        line-height: 40px;
        text-align: center;
      }
    </style>
    """, unsafe_allow_html=True)

    ch = 1
    last_state, label = _relay_last_state(conn, dev_id, ch)
    is_on = (last_state == "ON")
    led = "🟢" if is_on else "⚫"

    if st.button(led, key=f"led_btn_{dev_id}_{ch}", help="Clique para alternar o estado do relé"):
        try:
            next_state = "OFF" if is_on else "ON"
            req_id = publish_cmd(cfg, mqtt_client, lugar, ambiente, medicao, dispositivo, ch, next_state)
            ok, details = wait_ack(incoming_ack, (lugar, ambiente, medicao, dispositivo), req_id, timeout=5.0)
            if ok:
                _set_relay_state_db(conn, dev_id, ch, next_state)
                st.success(f"ACK OK: {details}")
            else:
                st.error(f"Falha/timeout: {details}")
        except Exception as e:
            st.error(f"Erro ao alternar relé: {e}")

    state_text = "Ligado" if is_on else "Desligado"
    st.caption(f"{label} | Estado atual: **{state_text}**")

    with st.expander("Editar legenda do relé"):
        new_label = st.text_input("Legenda", value=label, key=f"lbl_{dev_id}_{ch}")
        if new_label != label:
            _save_relay_label(conn, dev_id, ch, new_label)
            st.success("Legenda atualizada.")


def render_dispositivos(conn, cfg, mqtt_client, incoming_ack):
    st.header("🔌 Dispositivos e controle")

    df = list_devices_df(conn)
    if df.empty:
        st.info("Nenhum dispositivo cadastrado ainda. O ingestor cadastra automaticamente quando recebe dados.")
        return

    df["key"] = df.apply(
        lambda r: f"{r['lugar']}/{r['ambiente']}/{r['medicao']}/{r['dispositivo']}",
        axis=1
    )

    idx = st.selectbox("Dispositivo", df.index, format_func=lambda i: df.loc[i, "key"], key="dev_sel")
    row = df.loc[idx]
    row = row.copy()
    row["key"] = f"{row['lugar']}/{row['ambiente']}/{row['medicao']}/{row['dispositivo']}"

    st.markdown(status_badge_html(row["status"], row["last_seen_utc"]), unsafe_allow_html=True)

    c1, c2 = st.columns([2, 3])
    with c1:
        st.write("**Identificação:**", row["key"])
        st.write("**Nome:**", row["name"] or "(sem nome)")
        st.write("**Último visto:**", humanize_last_seen(row["last_seen_utc"]))

        with st.expander("🗑️ Remover dispositivo e histórico"):
            st.warning("Esta ação remove o dispositivo selecionado, leituras, regras, estados de relé e eventos associados.")
            confirm_delete = st.checkbox(
                "Confirmo que quero apagar este dispositivo e todo o histórico relacionado.",
                key=f"confirm_delete_dev_{int(row['id'])}"
            )
            if st.button("Apagar dispositivo", key=f"delete_dev_btn_{int(row['id'])}", type="primary", disabled=not confirm_delete):
                delete_device_and_history(conn, int(row["id"]))
                st.success("Dispositivo e histórico removidos.")
                st.rerun()
    with c2:
        render_led_controls(conn, cfg, mqtt_client, incoming_ack, row)


# =========================
# UI - Regras
# =========================
_METRIC_LABEL = {
    "p": "Potência (W)",
    "vrms": "Tensão (V)",
    "irms": "Corrente (A)",
    "pf": "Fator de potência",
    "e": "Energia (kWh)",
    "f": "Frequência (Hz)"
}
_OPERATORS = ["≥ (>=)", "> (>)", "≤ (<=)", "< (<)", "==", "!="]
_OP_MAP = {"≥ (>=)": ">=", "> (>)": ">", "≤ (<=)": "<=", "< (<)": "<", "==": "==", "!=": "!="}
_ACTION_LABEL = {"ALERT": "Alerta", "RELAY1_ON": "Acionar Relé", "RELAY1_OFF": "Desligar Relé"}


def render_regras(conn):
    st.header("⚙️ Regras de automação")

    df_dev = list_devices_df(conn)
    if df_dev.empty:
        st.info("Cadastre/ligue um dispositivo para criar regras.")
        return

    df_dev["key"] = df_dev.apply(
        lambda r: f"{r['lugar']}/{r['ambiente']}/{r['medicao']}/{r['dispositivo']}",
        axis=1
    )

    dev_idx = st.selectbox("Dispositivo", df_dev.index, format_func=lambda i: df_dev.loc[i, "key"], key="reg_dev_sel")
    device_id = int(df_dev.loc[dev_idx, "id"])

    metric_h = st.selectbox("Métrica", list(_METRIC_LABEL.values()), index=0, key="reg_metric")
    metric_key = [k for k, v in _METRIC_LABEL.items() if v == metric_h][0]

    op_h = st.selectbox("Operador", _OPERATORS, index=0, key="reg_operator")
    operator = _OP_MAP[op_h]

    threshold = st.number_input("Valor de limiar", value=0.0, step=0.1, key="reg_threshold")

    action_h = st.selectbox("Ação", list(_ACTION_LABEL.values()), index=0, key="reg_action")
    action_key = [k for k, v in _ACTION_LABEL.items() if v == action_h][0]

    alert_text = ""
    if action_key == "ALERT":
        alert_text = st.text_input("Texto do alerta", value="Alerta de condição atingida.", key="reg_alert_text")

    enabled = st.checkbox("Habilitada", value=True, key="reg_enabled")
    cooldown_s = st.number_input("Cooldown (s)", min_value=0, value=0, step=1, key="reg_cooldown")
    edge_only = st.checkbox(
        "Disparar apenas na transição da condição",
        value=True,
        key="reg_edge_only",
        help="Quando ativado, a regra dispara somente ao entrar na condição. Para disparar novamente, a condição precisa deixar de ser verdadeira e voltar a ser satisfeita."
    )

    if st.button("Salvar regra", key="reg_save"):
        insert_rule(conn, device_id, metric_key, threshold, operator, action_key, alert_text, enabled, cooldown_s, edge_only)
        st.success("Regra cadastrada. O ingestor aplicará a lógica automaticamente.")

    st.markdown("---")
    st.subheader("Regras cadastradas")

    df_rules = list_rules_df(conn)
    if df_rules.empty:
        st.info("Sem regras cadastradas.")
        return

    st.dataframe(df_rules, use_container_width=True, height=420)
    rid = st.number_input("ID da regra", min_value=1, value=int(df_rules["id"].iloc[0]), step=1, key="reg_id_sel")

    colb1, colb2 = st.columns(2)
    with colb1:
        if st.button("Excluir regra", key="reg_del"):
            delete_rule(conn, int(rid))
            st.success("Regra excluída.")
    with colb2:
        if st.button("Alternar habilitação", key="reg_toggle"):
            row = df_rules[df_rules["id"] == int(rid)]
            if not row.empty:
                new_en = 0 if int(row["enabled"].iloc[0]) == 1 else 1
                set_rule_enabled(conn, int(rid), bool(new_en))
                st.success(f"Habilitada = {bool(new_en)}")


# =========================
# UI - Eventos
# =========================
def render_eventos(conn):
    st.header("🧾 Eventos do sistema")

    df_dev = list_devices_df(conn)

    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        device_options = {"Todos os dispositivos": None}
        if not df_dev.empty:
            df_dev = df_dev.copy()
            df_dev["key"] = df_dev.apply(
                lambda r: f"{r['lugar']}/{r['ambiente']}/{r['medicao']}/{r['dispositivo']}",
                axis=1
            )
            for _, row in df_dev.iterrows():
                device_options[row["key"]] = int(row["id"])

        selected_dev_label = st.selectbox("Dispositivo", list(device_options.keys()), index=0, key="evt_dev_sel")
        selected_dev_id = device_options[selected_dev_label]

    with col2:
        dt_start = st.date_input(
            "Data inicial",
            value=(datetime.now().date() - timedelta(days=7)),
            key="evt_dt_start"
        )

    with col3:
        dt_end = st.date_input(
            "Data final",
            value=datetime.now().date(),
            key="evt_dt_end"
        )

    start_iso = datetime.combine(dt_start, datetime.min.time()).replace(tzinfo=timezone.utc).isoformat()
    end_iso = datetime.combine(dt_end, datetime.max.time()).replace(tzinfo=timezone.utc).isoformat()

    df = get_events_df(conn, device_id=selected_dev_id, start_iso=start_iso, end_iso=end_iso, limit=2000)

    if df.empty:
        st.info("Sem eventos para o filtro selecionado.")
        return

    st.caption(f"Eventos encontrados: {len(df)}")

    colb1, colb2 = st.columns([1, 1])
    with colb1:
        st.download_button(
            "⬇️ Exportar eventos (CSV)",
            data=df_to_csv_bytes(df),
            file_name=f"eventos_{dt_start}_{dt_end}.csv",
            mime="text/csv",
            key="download_events_csv"
        )
    with colb2:
        st.caption("Tabela ampliada para facilitar leitura e auditoria dos eventos.")

    st.dataframe(df, use_container_width=True, height=720)


# =========================
# UI - Configurações
# =========================
def render_config(cfg):
    st.header("🔧 Configurações")
    st.caption("Arquivo: config/config.json. Reinicie o Streamlit para aplicar integralmente.")

    with st.form("cfg_form"):
        mqtt_host = st.text_input("MQTT Host", cfg["MQTT_HOST"])
        mqtt_port = st.number_input("MQTT Port", min_value=1, max_value=65535, value=int(cfg["MQTT_PORT"]))
        mqtt_user = st.text_input("MQTT User", cfg.get("MQTT_USER", ""))
        mqtt_pass = st.text_input("MQTT Pass", cfg.get("MQTT_PASS", ""), type="password")

        submitted = st.form_submit_button("Salvar configuração")
        if submitted:
            new_cfg = dict(cfg)
            new_cfg["MQTT_HOST"] = mqtt_host
            new_cfg["MQTT_PORT"] = int(mqtt_port)
            new_cfg["MQTT_USER"] = mqtt_user
            new_cfg["MQTT_PASS"] = mqtt_pass
            CONFIG_PATH.write_text(json.dumps(new_cfg, indent=2, ensure_ascii=False), encoding="utf-8")
            st.success("Configuração salva. Reinicie o Streamlit para aplicar.")


# =========================
# Main
# =========================
def main():
    st.set_page_config(page_title="Monitoramento de Energia", layout="wide")

    cfg = load_config()

    if "state" not in st.session_state:
        st.session_state["state"] = {}
    S = st.session_state["state"]

    if "incoming_ack" not in S:
        S["incoming_ack"] = queue.Queue()

    if "conn" not in S:
        S["conn"], S["cur"] = init_db(cfg["DB_PATH"])

    if "mqtt_client" not in S:
        S["mqtt_client"] = init_mqtt_for_ack(cfg, S["incoming_ack"])

    conn = S["conn"]
    mqtt_client = S["mqtt_client"]
    incoming_ack = S["incoming_ack"]

    with st.sidebar:
        st.markdown("## Monitoramento de Energia")
        st.markdown("---")
        page = st.radio(
            "Navegação",
            [
                "📊 Monitoramento",
                "🔌 Dispositivos e controle",
                "⚙️ Regras de automação",
                "🧾 Eventos do sistema",
                "🔧 Configurações"
            ],
            index=0
        )
        st.markdown("---")
        st.success("MQTT (cmd/ack): conectado" if mqtt_client else "MQTT: desconectado")
        st.caption("⚠️ O ingestor deve estar rodando para coletar medições e aplicar regras.")

    if page == "📊 Monitoramento":
        render_dashboard(conn)
    elif page == "🔌 Dispositivos e controle":
        render_dispositivos(conn, cfg, mqtt_client, incoming_ack)
    elif page == "⚙️ Regras de automação":
        render_regras(conn)
    elif page == "🧾 Eventos do sistema":
        render_eventos(conn)
    elif page == "🔧 Configurações":
        render_config(cfg)


if __name__ == "__main__":
    main()
