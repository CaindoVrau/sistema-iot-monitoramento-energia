#include <WiFi.h>
#include <Preferences.h>
#include <ArduinoJson.h>   // v7
#include <PubSubClient.h>

#define USE_PZEM 1                // 1 = ler PZEM004T v3 | 0 = mock
#if USE_PZEM
  #include <PZEM004Tv30.h>
  // Ajuste conforme sua fiação:
  #define PZEM_RX 16  // ESP32 RX2
  #define PZEM_TX 17  // ESP32 TX2
  HardwareSerial PZEMSerial(2);   // UART2
  PZEM004Tv30 pzem(PZEMSerial, PZEM_RX, PZEM_TX);
#endif

// ---------- Pinos ----------
static const int PIN_RELAY = 26;  // D26
static const int LED_WIFI  = 22;  // D22 (status Wi-Fi)
static const int LED_MQTT  = 23;  // D23 (status MQTT)

// ---------- Config persistente ----------
Preferences prefs;

struct AppCfg {
  String ssid, psk;
  String mqttHost; uint16_t mqttPort;
  String mqttUser, mqttPass;
  String lugar, ambiente, medicao, dispositivo;
  uint32_t telemetry_ms;
};
AppCfg cfg;

// ---------- Estados ----------
enum class ConnState { DISCONNECTED, CONNECTING, CONNECTED };
volatile ConnState wifiState = ConnState::DISCONNECTED;
volatile ConnState mqttState = ConnState::DISCONNECTED;

// ---------- MQTT ----------
WiFiClient net;
PubSubClient mqtt(net);

uint32_t wifiNextTryMs = 0;
uint32_t mqttNextTryMs = 0;

// ---------- Telemetria ----------
uint32_t lastTelem = 0;
uint32_t telemSeq = 0;

// ---------- Helpers de tópico ----------
String topicBase() {
  return cfg.lugar + "/" + cfg.ambiente + "/" + cfg.medicao + "/" + cfg.dispositivo;
}
String topicStatus() { return topicBase() + "/status"; }
String topicAck()    { return topicBase() + "/ack"; }
String topicCmd()    { return topicBase() + "/cmd"; }
String topicMed()    { return topicBase() + "/medicao"; }

// ---------- Persistência ----------
void saveCfgToNVS(const AppCfg& c) {
  prefs.begin("app", false);
  prefs.putString("ssid", c.ssid);
  prefs.putString("psk", c.psk);
  prefs.putString("mqttHost", c.mqttHost);
  prefs.putUShort("mqttPort", c.mqttPort);
  prefs.putString("mqttUser", c.mqttUser);
  prefs.putString("mqttPass", c.mqttPass);
  prefs.putString("lugar", c.lugar);
  prefs.putString("ambiente", c.ambiente);
  prefs.putString("medicao", c.medicao);
  prefs.putString("dispositivo", c.dispositivo);
  prefs.putULong("telemetry", c.telemetry_ms);
  prefs.end();
}

void loadCfgFromNVS(AppCfg& c) {
  prefs.begin("app", true);
  c.ssid         = prefs.getString("ssid", "");
  c.psk          = prefs.getString("psk", "");
  c.mqttHost     = prefs.getString("mqttHost", "");
  c.mqttPort     = prefs.getUShort("mqttPort", 1883);
  c.mqttUser     = prefs.getString("mqttUser", "");
  c.mqttPass     = prefs.getString("mqttPass", "");
  c.lugar        = prefs.getString("lugar", "casa");
  c.ambiente     = prefs.getString("ambiente", "quarto");
  c.medicao      = prefs.getString("medicao", "energia");
  c.dispositivo  = prefs.getString("dispositivo", "esp-001");
  c.telemetry_ms = prefs.getULong("telemetry", 5000);
  prefs.end();
}

// ---------- LED pattern único ----------
bool ledPattern(ConnState st, uint32_t now_ms) {
  if (st == ConnState::DISCONNECTED) return false;
  if (st == ConnState::CONNECTING)  return ((now_ms / 100) % 2) == 0; // 100ms on/off
  uint32_t t = now_ms % 5000; // CONNECTED: 2 flashes/5s
  return (t < 100) || (t >= 250 && t < 350);
}

void driveStatusLed(int pin, ConnState st, uint32_t now_ms) {
  digitalWrite(pin, ledPattern(st, now_ms) ? HIGH : LOW);
}

// ---------- Wi-Fi ----------
void startWifiIfIdle() {
  if (cfg.ssid.isEmpty()) return;
  if (wifiState == ConnState::CONNECTING || wifiState == ConnState::CONNECTED) return;

  WiFi.mode(WIFI_STA);
  WiFi.persistent(false);
  WiFi.setAutoReconnect(false);
  WiFi.disconnect(true, true);
  delay(30);

  wifiState = ConnState::CONNECTING;
  WiFi.begin(cfg.ssid.c_str(), cfg.psk.c_str());
}

void onWifiEvent(WiFiEvent_t event) {
  switch (event) {
    case ARDUINO_EVENT_WIFI_STA_GOT_IP:
      wifiState = ConnState::CONNECTED;
      if (mqttState == ConnState::DISCONNECTED) mqttNextTryMs = millis();
      break;
    case ARDUINO_EVENT_WIFI_STA_DISCONNECTED:
      wifiState = ConnState::DISCONNECTED;
      mqttState = ConnState::DISCONNECTED;
      wifiNextTryMs = millis() + 3000;
      break;
    default:
      break;
  }
}

// ---------- Relé ----------
void relaySet(bool on) {
  digitalWrite(PIN_RELAY, on ? HIGH : LOW);
}

// ---------- MQTT: ACK ----------
void publishAck(const String& reqId, bool ok, const String& details) {
  JsonDocument doc;
  doc["req_id"] = reqId;
  doc["ok"] = ok;
  doc["details"] = details;

  char buf[256];
  size_t n = serializeJson(doc, buf, sizeof(buf));
  mqtt.publish(
    topicAck().c_str(),
    reinterpret_cast<const uint8_t*>(buf),
    static_cast<unsigned int>(n),
    false
  );
}

// ---------- MQTT: comandos ----------
void onMqttMessage(char* topic, byte* payload, unsigned int len) {
  String t = String(topic);
  if (!t.endsWith("/cmd")) return;

  JsonDocument doc;
  DeserializationError err = deserializeJson(doc, payload, len);
  if (err) {
    publishAck("no-reqid", false, "invalid json");
    return;
  }

  String type = doc["type"] | "";
  int channel = doc["channel"] | 0;
  String state = doc["state"] | "";
  String reqId = doc["req_id"] | "";
  state.toUpperCase();
  type.toLowerCase();

  if (type != "relay" || reqId.length() == 0) {
    publishAck(reqId.length() ? reqId : String("no-reqid"), false, "invalid command/type");
    return;
  }

  if (channel == 1) {
    relaySet(state == "ON");
    publishAck(reqId, true, String("Relay 1 -> ") + state);
  } else {
    publishAck(reqId, false, "channel not available");
  }
}

// ---------- MQTT connect ----------
String mqttClientId() {
  String id = "esp32-" + cfg.dispositivo + "-" + String((uint32_t)ESP.getEfuseMac(), HEX);
  id.replace(" ", "_");
  return id;
}

void mqttSubscribeCmd() {
  mqtt.subscribe(topicCmd().c_str());
}

void mqttTryConnect() {
  if (cfg.mqttHost.length() == 0) {
    mqttState = ConnState::DISCONNECTED;
    return;
  }

  mqtt.setServer(cfg.mqttHost.c_str(), cfg.mqttPort);
  mqtt.setCallback(onMqttMessage);

  mqttState = ConnState::CONNECTING;

  String willTopic = topicStatus();
  const char* willMsg = "offline";
  bool ok = false;

  if (cfg.mqttUser.length()) {
    ok = mqtt.connect(
      mqttClientId().c_str(),
      cfg.mqttUser.c_str(), cfg.mqttPass.c_str(),
      willTopic.c_str(), 1, true, willMsg
    );
  } else {
    ok = mqtt.connect(
      mqttClientId().c_str(),
      willTopic.c_str(), 1, true, willMsg
    );
  }

  if (ok) {
    mqttState = ConnState::CONNECTED;
    mqtt.publish(topicStatus().c_str(), "online", true); // retained
    mqttSubscribeCmd();
  } else {
    mqtt.disconnect();
    mqttState = ConnState::DISCONNECTED;
    mqttNextTryMs = millis() + 5000;
  }
}

// ---------- Telemetria ----------
void publishTelemetry(float p, float vrms, float irms, float pf, float e, float f) {
  JsonDocument doc;

  doc["seq"] = telemSeq++;

  if (!isnan(p))    doc["p"]    = p;
  if (!isnan(vrms)) doc["vrms"] = vrms;
  if (!isnan(irms)) doc["irms"] = irms;
  if (!isnan(pf))   doc["pf"]   = pf;
  if (!isnan(e))    doc["e"]    = e;
  if (!isnan(f))    doc["f"]    = f;

  char buf[320];
  size_t n = serializeJson(doc, buf, sizeof(buf));
  mqtt.publish(
    topicMed().c_str(),
    reinterpret_cast<const uint8_t*>(buf),
    static_cast<unsigned int>(n),
    false
  );
}

#if USE_PZEM
bool readPZEM(float &p, float &vrms, float &irms, float &pf, float &e, float &freq) {
  float v  = pzem.voltage();
  float i  = pzem.current();
  float w  = pzem.power();
  float fp = pzem.pf();
  float en = pzem.energy();
  float fr = pzem.frequency();

  bool ok = isfinite(v) || isfinite(i) || isfinite(w) || isfinite(fp) || isfinite(en) || isfinite(fr);

  vrms = isfinite(v)  ? v  : NAN;
  irms = isfinite(i)  ? i  : NAN;
  p    = isfinite(w)  ? w  : NAN;
  pf   = isfinite(fp) ? fp : NAN;
  e    = isfinite(en) ? en : NAN;
  freq = isfinite(fr) ? fr : NAN;

  return ok;
}
#else
bool readMock(float &p, float &vrms, float &irms, float &pf, float &e, float &freq) {
  static float t = 0.0f;
  static float energy_kwh = 0.0f;

  t += 0.2f;
  vrms = 127.0f + 5.0f * sin(t);
  irms = 0.8f + 0.2f * sin(0.5f * t + 1.0f);
  pf   = 0.85f + 0.1f * sin(0.3f * t);
  p    = vrms * irms * pf;
  freq = 60.0f;

  // Integração simples de potência em kWh para modo mock
  energy_kwh += (p * (cfg.telemetry_ms / 1000.0f)) / 3600000.0f;
  e = energy_kwh;
  return true;
}
#endif

void doTelemetryIfDue(uint32_t now) {
  if (!mqtt.connected()) return;
  if (cfg.telemetry_ms < 500) return;
  if (now - lastTelem < cfg.telemetry_ms) return;

  lastTelem = now;

  float p = NAN, v = NAN, i = NAN, pf = NAN, e = NAN, f = NAN;

#if USE_PZEM
  bool ok = readPZEM(p, v, i, pf, e, f);
#else
  bool ok = readMock(p, v, i, pf, e, f);
#endif

  if (ok) {
    publishTelemetry(p, v, i, pf, e, f);
  }
}

// ---------- Provisionamento Serial ----------
void applyCfgFromJson(const String& jsonStr) {
  JsonDocument doc;
  if (deserializeJson(doc, jsonStr)) {
    return;
  }

  cfg.ssid         = doc["wifi"]["ssid"]          | cfg.ssid;
  cfg.psk          = doc["wifi"]["psk"]           | cfg.psk;
  cfg.mqttHost     = doc["mqtt"]["host"]          | cfg.mqttHost;
  cfg.mqttPort     = doc["mqtt"]["port"]          | cfg.mqttPort;
  cfg.mqttUser     = doc["mqtt"]["user"]          | cfg.mqttUser;
  cfg.mqttPass     = doc["mqtt"]["pass"]          | cfg.mqttPass;
  cfg.lugar        = doc["device"]["lugar"]       | cfg.lugar;
  cfg.ambiente     = doc["device"]["ambiente"]    | cfg.ambiente;
  cfg.medicao      = doc["device"]["medicao"]     | cfg.medicao;
  cfg.dispositivo  = doc["device"]["dispositivo"] | cfg.dispositivo;
  cfg.telemetry_ms = doc["telemetry_period_ms"]   | cfg.telemetry_ms;

  saveCfgToNVS(cfg);

  mqtt.disconnect();
  mqttState = ConnState::DISCONNECTED;
  WiFi.disconnect(true, true);
  wifiState = ConnState::DISCONNECTED;

  uint32_t now = millis();
  wifiNextTryMs = now + 100;
  mqttNextTryMs = now + 500;

  Serial.println("ACK:OK");
}

void pollSerialProvisioning() {
  while (Serial.available()) {
    String line = Serial.readStringUntil('\n');
    line.trim();
    if (line.startsWith("CFG:")) {
      String json = line.substring(4);
      json.trim();
      applyCfgFromJson(json);
    }
  }
}

// ---------- Setup/Loop ----------
void setup() {
  Serial.begin(115200);

  pinMode(PIN_RELAY, OUTPUT);
  pinMode(LED_WIFI, OUTPUT);
  pinMode(LED_MQTT, OUTPUT);

  relaySet(false);
  digitalWrite(LED_WIFI, LOW);
  digitalWrite(LED_MQTT, LOW);

  loadCfgFromNVS(cfg);

#if USE_PZEM
  PZEMSerial.begin(9600, SERIAL_8N1, PZEM_RX, PZEM_TX);
#endif

  WiFi.onEvent(onWifiEvent);
  wifiNextTryMs = millis() + 100;
  mqttNextTryMs = millis() + 1000;
}

void loop() {
  uint32_t now = millis();

  // 1) Provisionamento USB
  pollSerialProvisioning();

  // 2) Wi-Fi backoff
  if (now >= wifiNextTryMs && wifiState == ConnState::DISCONNECTED && cfg.ssid.length() > 0) {
    startWifiIfIdle();
  }

  // 3) MQTT tentativa periódica
  if (now >= mqttNextTryMs) {
    if (!mqtt.connected()) mqttTryConnect();
  }

  if (mqtt.connected()) mqtt.loop();

  // 4) LEDs
  driveStatusLed(LED_WIFI, wifiState, now);
  driveStatusLed(LED_MQTT, mqttState, now);

  // 5) Telemetria
  doTelemetryIfDue(now);
}
