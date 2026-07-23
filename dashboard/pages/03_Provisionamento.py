import json
import time
import streamlit as st
import serial
import serial.tools.list_ports

st.set_page_config(page_title="Provisionamento", layout="wide")
st.title("🔌 Provisionamento via USB (CFG:<json>)")

ports = [p.device for p in serial.tools.list_ports.comports()]
port = st.selectbox("Porta COM", [""] + ports)

col1, col2 = st.columns(2)
with col1:
    ssid = st.text_input("Wi-Fi SSID")
    psk = st.text_input("Wi-Fi Senha", type="password")
    ambiente = st.text_input("Ambiente (novo)", value="oficina")
    device_id = st.text_input("Device ID", value="esp32_002")
with col2:
    mqtt_host = st.text_input("MQTT Host (IP do PC)", value="192.168.0.20")
    mqtt_port = st.number_input("MQTT Port", value=1883, step=1)
    mqtt_user = st.text_input("MQTT User (opcional)", value="")
    mqtt_pass = st.text_input("MQTT Pass (opcional)", value="", type="password")

period_ms = st.number_input("Período telemetria (ms)", value=5000, step=500)
relay_pin = st.number_input("GPIO do Relé", value=26, step=1)

cfg = {
    "wifi": {"ssid": ssid, "psk": psk},
    "mqtt": {"host": mqtt_host, "port": int(mqtt_port), "user": mqtt_user, "pass": mqtt_pass},
    "device": {"ambiente": ambiente, "dispositivo": device_id},
    "telemetry_period_ms": int(period_ms),
    "relay_pin": int(relay_pin),
}

st.code("CFG:" + json.dumps(cfg), language="json")

if st.button("Enviar CFG"):
    if not port:
        st.error("Selecione a porta COM.")
    elif not ssid or not mqtt_host or not ambiente or not device_id:
        st.error("Preencha SSID, MQTT Host, Ambiente e Device ID.")
    else:
        try:
            with serial.Serial(port, 115200, timeout=3) as ser:
                time.sleep(0.2)
                line = "CFG:" + json.dumps(cfg) + "\n"
                ser.write(line.encode("utf-8"))
                ser.flush()
                resp = ser.read(256).decode("utf-8", errors="ignore").strip()
            if "ACK:OK" in resp:
                st.success(f"OK! ESP aceitou CFG. Resposta: {resp}")
            else:
                st.warning(f"Resposta: {resp or '(vazio)'}")
        except Exception as e:
            st.error(f"Erro serial: {e}")