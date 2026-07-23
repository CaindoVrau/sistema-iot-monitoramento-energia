import os
import json
import time
import uuid
import queue
import threading
import logging
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta

import paho.mqtt.client as mqtt

STALE_SECONDS = 90
OFFLINE_SCAN_PERIOD_S = 15

ROOT_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT_DIR / "config" / "config.json"
LOG_DIR = ROOT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "server.log", encoding="utf-8"),
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

    "TOPIC_STATUS":  ["monitoramento_energia/+/+/status", "+/+/+/+/status"],
    "TOPIC_ACK":     ["monitoramento_energia/+/+/ack", "+/+/+/+/ack"],
    "TOPIC_MEDICAO": ["monitoramento_energia/+/+/medicao", "+/+/+/+/medicao"],

    "TOPIC_CMD_TEMPLATE": "monitoramento_energia/{ambiente}/{device_id}/cmd",
    "PUBLISH_COMPAT_BOTH": True
}

def _as_list(x):
    return x if isinstance(x, list) else [x]

def _resolve_path(p: str) -> str:
    pp = Path(p)
    return str(pp if pp.is_absolute() else (ROOT_DIR / pp).resolve())

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

    # env override
    cfg["MQTT_HOST"] = os.getenv("MQTT_HOST", str(cfg["MQTT_HOST"]))
    cfg["MQTT_PORT"] = int(os.getenv("MQTT_PORT", str(cfg["MQTT_PORT"])))
    cfg["MQTT_USER"] = os.getenv("MQTT_USER", str(cfg.get("MQTT_USER", "")))
    cfg["MQTT_PASS"] = os.getenv("MQTT_PASS", str(cfg.get("MQTT_PASS", "")))
    cfg["DB_PATH"] = _resolve_path(os.getenv("ENERGIA_DB", str(cfg["DB_PATH"])))
    return cfg

# =========================
# DB + MIGRAÇÕES
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

    # v1: tabelas base
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
          metric TEXT NOT NULL,           -- 'p','vrms','irms','pf','e','f'
          threshold REAL NOT NULL,
          operator TEXT NOT NULL,         -- '>=','>','<=','<','==','!='
          action TEXT NOT NULL,           -- 'ALERT','RELAY1_ON','RELAY1_OFF'
          alert_text TEXT,
          enabled INTEGER NOT NULL DEFAULT 1,
          cooldown_s INTEGER DEFAULT 0,
          last_trigger_utc TEXT,
          edge_only INTEGER NOT NULL DEFAULT 1,
          created_at TEXT DEFAULT (datetime('now')),
          FOREIGN KEY(device_id) REFERENCES device(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS ix_rule_device_enabled ON rule(device_id, enabled);

        CREATE TABLE IF NOT EXISTS rule_state (
          rule_id INTEGER PRIMARY KEY,
          is_active INTEGER NOT NULL DEFAULT 0,
          initialized INTEGER NOT NULL DEFAULT 0,
          last_eval_utc TEXT,
          last_change_utc TEXT,
          FOREIGN KEY(rule_id) REFERENCES rule(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS event_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          device_id INTEGER,
          ts_utc TEXT NOT NULL,
          level TEXT NOT NULL,   -- INFO/WARN/ERROR/ALERT
          code TEXT,
          msg TEXT,
          FOREIGN KEY(device_id) REFERENCES device(id) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS ix_event_ts ON event_log(ts_utc);
        """)
        _set_schema_version(conn, cur, 1)
        version = 1

    # v2: reservado (compat)
    if version < 2:
        _set_schema_version(conn, cur, 2)
        version = 2

    # v3: adiciona energia e frequência e seq
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

def ensure_environment_exists(conn, cur, lugar, ambiente):
    cur.execute("INSERT OR IGNORE INTO environment(lugar, ambiente) VALUES (?,?)", (lugar, ambiente))
    conn.commit()

def register_device(conn, cur, lugar, ambiente, medicao, dispositivo, name=None):
    ensure_environment_exists(conn, cur, lugar, ambiente)
    cur.execute("""
        INSERT OR IGNORE INTO device(lugar, ambiente, medicao, dispositivo, name, status, last_seen_utc)
        VALUES (?, ?, ?, ?, ?, 'offline', NULL)
    """, (lugar, ambiente, medicao, dispositivo, name))
    conn.commit()

def get_device_id(cur, lugar, ambiente, medicao, dispositivo):
    row = cur.execute("""
        SELECT id FROM device WHERE lugar=? AND ambiente=? AND medicao=? AND dispositivo=?
    """, (lugar, ambiente, medicao, dispositivo)).fetchone()
    return int(row[0]) if row else None

def get_device_tuple_by_id(cur, device_id):
    row = cur.execute("""
        SELECT lugar, ambiente, medicao, dispositivo FROM device WHERE id=?
    """, (device_id,)).fetchone()
    return tuple(row) if row else (None, None, None, None)

def set_device_status_by_tuple(conn, cur, lugar, ambiente, medicao, dispositivo, status):
    if get_device_id(cur, lugar, ambiente, medicao, dispositivo) is None:
        register_device(conn, cur, lugar, ambiente, medicao, dispositivo, name=None)

    dev_id = get_device_id(cur, lugar, ambiente, medicao, dispositivo)
    if dev_id:
        cur.execute("""UPDATE device SET status=?, last_seen_utc=? WHERE id=?""",
                    (status, datetime.now(timezone.utc).isoformat(), dev_id))
        conn.commit()

def insert_reading_by_tuple(conn, cur, lugar, ambiente, medicao, dispositivo,
                            ts_utc, vrms, irms, p, pf, e, f, seq):
    if get_device_id(cur, lugar, ambiente, medicao, dispositivo) is None:
        register_device(conn, cur, lugar, ambiente, medicao, dispositivo, name=None)

    dev_id = get_device_id(cur, lugar, ambiente, medicao, dispositivo)
    if not dev_id:
        return

    cur.execute("""INSERT INTO reading(device_id, ts_utc, vrms, irms, p, pf, e, f, seq)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (dev_id, ts_utc, vrms, irms, p, pf, e, f, seq))
    cur.execute("""UPDATE device SET status='online', last_seen_utc=? WHERE id=?""",
                (datetime.now(timezone.utc).isoformat(), dev_id))
    conn.commit()

def _log_event(conn, device_id, level, code, msg):
    cur = conn.cursor()
    cur.execute("""INSERT INTO event_log(device_id, ts_utc, level, code, msg)
                   VALUES (?, ?, ?, ?, ?)""",
                (device_id, datetime.now(timezone.utc).isoformat(), level, code, msg))
    conn.commit()

# =========================
# MQTT + PARSE
# =========================
def parse_topic(topic: str, default_lugar="casa", default_medicao="energia"):
    # MVP: monitoramento_energia/{ambiente}/{device_id}/{fluxo}
    parts = topic.split("/")
    if len(parts) == 4 and parts[0] == "monitoramento_energia":
        _, ambiente, device_id, fluxo = parts
        return (default_lugar, ambiente, default_medicao, device_id, fluxo)
    # Legado: {lugar}/{ambiente}/{medicao}/{dispositivo}/{fluxo}
    if len(parts) == 5:
        return tuple(parts)
    return None

def _ts_to_iso(ts):
    # aceita epoch_ms, epoch_s, ISO ou ausente
    if ts is None:
        return datetime.now(timezone.utc).isoformat()
    try:
        if isinstance(ts, (int, float)):
            # heurística: ms se for grande
            if ts > 1e12:
                return datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).isoformat()
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        if isinstance(ts, str):
            # se já for ISO, tenta parse
            try:
                return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
            except Exception:
                # se for string numérica
                if ts.isdigit():
                    n = int(ts)
                    return _ts_to_iso(n)
    except Exception:
        pass
    return datetime.now(timezone.utc).isoformat()

# =========================
# REGRAS (EXECUÇÃO NO INGESTOR)
# =========================
def _cmp(value, op, thr):
    try:
        v = float(value) if value is not None else None
        t = float(thr)
    except Exception:
        return False
    if v is None:
        return False
    if op == ">=": return v >= t
    if op == ">":  return v >  t
    if op == "<=": return v <= t
    if op == "<":  return v <  t
    if op == "==": return v == t
    if op == "!=": return v != t
    return False

def _relay_action_parse(action):
    # MVP: 1 relé
    if action == "RELAY1_ON":  return (1, "ON")
    if action == "RELAY1_OFF": return (1, "OFF")
    return None

def get_rule_state(cur, rule_id):
    row = cur.execute("""
        SELECT is_active, initialized, last_eval_utc, last_change_utc
        FROM rule_state
        WHERE rule_id=?
    """, (rule_id,)).fetchone()

    if row is None:
        cur.execute("""
            INSERT OR IGNORE INTO rule_state(rule_id, is_active, initialized, last_eval_utc, last_change_utc)
            VALUES (?, 0, 0, NULL, NULL)
        """, (rule_id,))
        return {
            "is_active": 0,
            "initialized": 0,
            "last_eval_utc": None,
            "last_change_utc": None
        }

    return {
        "is_active": int(row[0]),
        "initialized": int(row[1]),
        "last_eval_utc": row[2],
        "last_change_utc": row[3]
    }

def save_rule_state(conn, cur, rule_id, is_active, initialized, now_iso, changed=False):
    cur.execute("""
        INSERT OR IGNORE INTO rule_state(rule_id, is_active, initialized, last_eval_utc, last_change_utc)
        VALUES (?, 0, 0, NULL, NULL)
    """, (rule_id,))
    if changed:
        cur.execute("""
            UPDATE rule_state
            SET is_active=?, initialized=?, last_eval_utc=?, last_change_utc=?
            WHERE rule_id=?
        """, (int(is_active), int(initialized), now_iso, now_iso, rule_id))
    else:
        cur.execute("""
            UPDATE rule_state
            SET is_active=?, initialized=?, last_eval_utc=?
            WHERE rule_id=?
        """, (int(is_active), int(initialized), now_iso, rule_id))
    conn.commit()

def update_rule_last_trigger(conn, rule_id):
    cur = conn.cursor()
    cur.execute("UPDATE rule SET last_trigger_utc=datetime('now') WHERE id=?", (rule_id,))
    conn.commit()

def enforce_rules_on_reading(conn, cur, device_id, reading, cmd_queue):
    rules = cur.execute("""
        SELECT id, metric, threshold, operator, action, alert_text, cooldown_s, last_trigger_utc, edge_only
        FROM rule WHERE device_id=? AND enabled=1
    """, (device_id,)).fetchall()
    if not rules:
        return

    lugar, ambiente, medicao, dispositivo = get_device_tuple_by_id(cur, device_id)
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    for rid, metric, thr, op, action, alert_text, cooldown_s, last_trig, edge_only in rules:
        val = reading.get(metric)
        condition_now = _cmp(val, op, thr)

        if int(edge_only or 0) == 1:
            state = get_rule_state(cur, rid)

            # Inicializa o estado sem disparar na primeira leitura
            if not state["initialized"]:
                save_rule_state(conn, cur, rid, condition_now, 1, now_iso, changed=True)
                continue

            was_active = bool(state["is_active"])

            # Sem transição: não dispara novamente
            if was_active == bool(condition_now):
                save_rule_state(conn, cur, rid, was_active, 1, now_iso, changed=False)
                continue

            # Atualiza o estado e só dispara quando ENTRA na condição
            save_rule_state(conn, cur, rid, condition_now, 1, now_iso, changed=True)
            if not condition_now:
                continue
        else:
            if not condition_now:
                continue

        if last_trig:
            try:
                last_dt = datetime.fromisoformat(last_trig)
                if (now - last_dt).total_seconds() < int(cooldown_s or 0):
                    continue
            except Exception:
                pass

        if action == "ALERT":
            _log_event(conn, device_id, "ALERT", f"RULE#{rid}",
                       f"{metric}={val} {op} {thr} | {alert_text or 'condição atingida'}")
            update_rule_last_trigger(conn, rid)
            continue

        ch_state = _relay_action_parse(action)
        if ch_state is None:
            continue

        ch, next_state = ch_state
        update_rule_last_trigger(conn, rid)
        _log_event(conn, device_id, "INFO", f"RULE#{rid}", f"Enfileirando comando: relé {ch} -> {next_state}")
        cmd_queue.put({
            "rule_id": rid,
            "device_id": device_id,
            "lugar": lugar,
            "ambiente": ambiente,
            "medicao": medicao,
            "dispositivo": dispositivo,
            "channel": ch,
            "state": next_state
        })

def publish_cmd(mqtt_client, cfg, lugar, ambiente, medicao, dispositivo, channel, state):
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

def rule_cmd_worker(cmd_queue, mqtt_client, incoming_ack, conn, cur, db_lock, cfg):
    while True:
        task = cmd_queue.get()
        if task is None:
            continue

        device_id = task["device_id"]
        lugar = task["lugar"]
        ambiente = task["ambiente"]
        medicao = task["medicao"]
        dispositivo = task["dispositivo"]
        rid = task.get("rule_id")
        ch = int(task["channel"])
        state = task["state"]

        try:
            if mqtt_client is None:
                with db_lock:
                    _log_event(conn, device_id, "ERROR", f"RULE#{rid}", "MQTT desconectado (sem comando)")
                continue

            req_id = publish_cmd(mqtt_client, cfg, lugar, ambiente, medicao, dispositivo, ch, state)
            ok, details = wait_ack(incoming_ack, (lugar, ambiente, medicao, dispositivo), req_id, timeout=4.0)

            with db_lock:
                if ok:
                    _log_event(conn, device_id, "INFO", f"RULE#{rid}", f"Relé {ch} -> {state} (ACK OK: {details})")
                else:
                    _log_event(conn, device_id, "WARN", f"RULE#{rid}", f"Falha ao acionar relé: {details}")

        except Exception as e:
            with db_lock:
                _log_event(conn, device_id, "ERROR", f"RULE#{rid}", f"Exceção no worker: {e}")

# =========================
# OFFLINE POR STALE
# =========================
def refresh_offline_status(conn):
    cur = conn.cursor()
    rows = cur.execute("SELECT id, last_seen_utc, status FROM device").fetchall()
    now = datetime.now(timezone.utc)
    changed = 0
    for dev_id, last_seen, status in rows:
        if not last_seen:
            continue
        try:
            seen_dt = datetime.fromisoformat(last_seen)
        except Exception:
            continue
        if (now - seen_dt) > timedelta(seconds=STALE_SECONDS):
            if status != "offline":
                cur.execute("UPDATE device SET status='offline' WHERE id=?", (dev_id,))
                changed += 1
    if changed:
        conn.commit()

def offline_monitor_loop(conn, stop_event):
    while not stop_event.is_set():
        try:
            refresh_offline_status(conn)
        except Exception:
            pass
        stop_event.wait(OFFLINE_SCAN_PERIOD_S)

# =========================
# MQTT CALLBACK (INGESTÃO)
# =========================
def make_on_message(conn, cur, incoming_ack, cmd_queue, db_lock, cfg):
    default_lugar = cfg.get("DEFAULT_LUGAR", "casa")
    default_medicao = cfg.get("DEFAULT_MEDICAO", "energia")

    def on_message(client, userdata, msg):
        parsed = parse_topic(msg.topic, default_lugar, default_medicao)
        if not parsed:
            return

        lugar, ambiente, medicao, dispositivo, fluxo = parsed
        payload = msg.payload.decode("utf-8", errors="ignore").strip()

        try:
            if fluxo == "status":
                # aceita status string ou JSON {online: true/false}
                status = payload
                try:
                    data = json.loads(payload)
                    if isinstance(data, dict) and "online" in data:
                        status = "online" if bool(data["online"]) else "offline"
                except Exception:
                    pass

                with db_lock:
                    set_device_status_by_tuple(conn, cur, lugar, ambiente, medicao, dispositivo, status)

            elif fluxo == "ack":
                try:
                    data = json.loads(payload)
                except Exception:
                    return
                with db_lock:
                    dev_id = get_device_id(cur, lugar, ambiente, medicao, dispositivo)
                    if dev_id:
                        incoming_ack.put(((lugar, ambiente, medicao, dispositivo), data))
                        _log_event(conn, dev_id, "INFO", "ACK", f"{data}")

            elif fluxo == "medicao":
                try:
                    data = json.loads(payload)
                except Exception:
                    return

                ts_iso = _ts_to_iso(data.get("ts"))
                vrms = float(data["vrms"]) if data.get("vrms") is not None else None
                irms = float(data["irms"]) if data.get("irms") is not None else None
                p    = float(data["p"]) if data.get("p") is not None else None
                pf   = float(data["pf"]) if data.get("pf") is not None else None
                e    = float(data["e"]) if data.get("e") is not None else None
                f    = float(data["f"]) if data.get("f") is not None else None
                seq  = int(data["seq"]) if data.get("seq") is not None else None

                with db_lock:
                    insert_reading_by_tuple(conn, cur, lugar, ambiente, medicao, dispositivo,
                                           ts_iso, vrms, irms, p, pf, e, f, seq)

                    dev_id = get_device_id(cur, lugar, ambiente, medicao, dispositivo)
                    if dev_id:
                        enforce_rules_on_reading(conn, cur, dev_id,
                                                 {"p": p, "vrms": vrms, "irms": irms, "pf": pf, "e": e, "f": f},
                                                 cmd_queue)

        except Exception as e:
            logging.exception("Erro no callback MQTT")
            with db_lock:
                dev_id = get_device_id(cur, lugar, ambiente, medicao, dispositivo)
                if dev_id:
                    _log_event(conn, dev_id, "ERROR", "MQTT", f"{e} :: {payload}")

    return on_message

def init_mqtt(cfg, on_message_cb):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    if cfg.get("MQTT_USER"):
        client.username_pw_set(cfg.get("MQTT_USER"), cfg.get("MQTT_PASS", ""))

    def on_connect(client, userdata, flags, rc, properties=None):
        logging.info(f"MQTT conectado: {rc}")
        for t in _as_list(cfg.get("TOPIC_STATUS", [])):
            client.subscribe(t, qos=1)
        for t in _as_list(cfg.get("TOPIC_ACK", [])):
            client.subscribe(t, qos=1)
        for t in _as_list(cfg.get("TOPIC_MEDICAO", [])):
            client.subscribe(t, qos=1)

    client.on_connect = on_connect
    client.on_message = on_message_cb

    client.connect(cfg["MQTT_HOST"], cfg["MQTT_PORT"], 60)
    client.loop_start()
    return client

def main():
    cfg = load_config()
    logging.info(f"DB: {cfg['DB_PATH']}")
    logging.info(f"MQTT: {cfg['MQTT_HOST']}:{cfg['MQTT_PORT']}")

    conn, cur = init_db(cfg["DB_PATH"])

    incoming_ack = queue.Queue()
    cmd_queue = queue.Queue()
    db_lock = threading.Lock()

    on_msg = make_on_message(conn, cur, incoming_ack, cmd_queue, db_lock, cfg)
    mqtt_client = init_mqtt(cfg, on_msg)

    # Worker regras
    worker = threading.Thread(
        target=rule_cmd_worker,
        args=(cmd_queue, mqtt_client, incoming_ack, conn, cur, db_lock, cfg),
        daemon=True
    )
    worker.start()

    # Offline monitor
    stop_event = threading.Event()
    offline_thr = threading.Thread(target=offline_monitor_loop, args=(conn, stop_event), daemon=True)
    offline_thr.start()

    logging.info("Ingestor ativo. Pressione Ctrl+C para encerrar.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Encerrando...")
    finally:
        stop_event.set()
        try:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
