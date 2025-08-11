import os, ssl, json, time, queue, threading, statistics
from datetime import datetime, timezone
from pathlib import Path

import requests
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
print("BOOT TRAITEMENT", flush=True)

# --------- Config & chemins ---------
ROOT = Path(__file__).resolve().parents[1]            # .../POC_Peed
ENV_PATH = ROOT / "config" / ".env"
load_dotenv(ENV_PATH)

MQTT_HOST  = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT  = int(os.getenv("MQTT_PORT", "8883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "usine/capteurs/machine1")

# paths ABSOLUS pour éviter toute ambiguïté
CA_FILE     = Path(os.getenv("CA_FILE", str(ROOT / "config" / "ca.crt")))
CLIENT_CERT = Path(os.getenv("CLIENT_CERT", str(ROOT / "config" / "client.crt")))
CLIENT_KEY  = Path(os.getenv("CLIENT_KEY", str(ROOT / "config" / "client.key")))

# si l'env contient des chemins relatifs (../config/...), on les résout par rapport à ROOT
if not CA_FILE.is_absolute():     CA_FILE     = (ROOT / CA_FILE).resolve()
if not CLIENT_CERT.is_absolute(): CLIENT_CERT = (ROOT / CLIENT_CERT).resolve()
if not CLIENT_KEY.is_absolute():  CLIENT_KEY  = (ROOT / CLIENT_KEY).resolve()

Z_THRESHOLD  = float(os.getenv("Z_THRESHOLD", "3.0"))
WINDOW_SIZE  = int(os.getenv("WINDOW_SIZE", "30"))
SAP_API_URL  = os.getenv("SAP_API_URL", "http://127.0.0.1:5000/sap/api/notifications")
SAP_API_TOKEN= os.getenv("SAP_API_TOKEN", "secret-token-demo")

BASE_DIR = Path(__file__).parent
LOG_FILE = BASE_DIR / "log.txt"
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

print(f"[CONFIG] host={MQTT_HOST} port={MQTT_PORT} topic={MQTT_TOPIC}", flush=True)
print(f"[CONFIG] ca={CA_FILE}", flush=True)
print(f"[CONFIG] cert={CLIENT_CERT}", flush=True)
print(f"[CONFIG] key={CLIENT_KEY}", flush=True)

# --------- État ---------
values_window: list[float] = []
msg_queue: "queue.Queue[dict]" = queue.Queue()

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def log(line: str) -> None:
    out = f"{now_iso()} {line}"
    print(out, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(out + "\n")

def compute_z(value: float):
    if len(values_window) < 5:
        return 0.0, None, None
    mean = statistics.fmean(values_window)
    stdev = statistics.pstdev(values_window) or 1e-9
    z = (value - mean) / stdev
    return z, mean, stdev

def post_sap_notification(device_id: str, temperature: float, timestamp: str):
    payload = {
        "machine": device_id,
        "anomalie": "Température moteur élevée",
        "valeur": temperature,
        "unite": "°C",
        "timestamp": timestamp,
    }
    headers = {"Content-Type": "application/json", "X-API-TOKEN": SAP_API_TOKEN}
    try:
        r = requests.post(SAP_API_URL, headers=headers, json=payload, timeout=5)
        if r.status_code == 201:
            notif_id = r.json().get("notif_id")
            log(f"[SAP] Notification créée -> {notif_id}")
        else:
            log(f"[SAP][ERREUR] status={r.status_code} body={r.text}")
    except Exception as e:
        log(f"[SAP][EXCEPTION] {e}")

# --------- MQTT callbacks ---------
def on_connect(client: mqtt.Client, userdata, flags, reason_code, properties=None):
    log(f"[MQTT] connect rc={reason_code}")
    if reason_code == 0:
        client.subscribe(MQTT_TOPIC, qos=1)
        log(f"[MQTT] SUBSCRIBE {MQTT_TOPIC}")
    else:
        log(f"[MQTT][ERREUR] Connexion échouée: code={reason_code}")

def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
    try:
        data = json.loads(msg.payload.decode("utf-8"))
        msg_queue.put(data)
    except Exception as e:
        log(f"[PARSER][ERREUR] {e}")

def build_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(client_id="traitement-ia", protocol=mqtt.MQTTv311)
    client.enable_logger()  # logs paho -> stdout
    client.tls_set(
        ca_certs=str(CA_FILE),
        certfile=str(CLIENT_CERT),
        keyfile=str(CLIENT_KEY),
        tls_version=ssl.PROTOCOL_TLS_CLIENT,
    )
    client.tls_insecure_set(False)
    client.on_connect = on_connect
    client.on_message = on_message
    return client

# --------- Worker de traitement ---------
def worker():
    while True:
        data = msg_queue.get()
        try:
            device_id = data.get("device_id", "unknown")
            ts        = data.get("timestamp")
            temp      = float(data.get("temperature"))

            values_window.append(temp)
            if len(values_window) > WINDOW_SIZE:
                values_window.pop(0)

            z, mean, stdev = compute_z(temp)
            if mean is None:
                log(f"[VAL] {device_id} temp={temp:.2f}°C (warming up)")
                continue

            if abs(z) >= Z_THRESHOLD or temp >= (mean + 3 * stdev):
                log(f"[ALERTE] {device_id} temp={temp:.2f}°C z={z:.2f} "
                    f"(mean={mean:.2f}, std={stdev:.2f}) -> Envoi SAP")
                post_sap_notification(device_id, temp, ts)
            else:
                log(f"[OK] {device_id} temp={temp:.2f}°C z={z:.2f} "
                    f"(mean={mean:.2f}, std={stdev:.2f})")
        except Exception as e:
            log(f"[TRAITEMENT][ERREUR] {e}")
        finally:
            msg_queue.task_done()

def main():
    # thread de traitement
    threading.Thread(target=worker, daemon=True).start()

    # connexion MQTT
    client = build_mqtt_client()
    try:
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    except Exception as e:
        log(f"[MQTT][EXCEPTION] échec connect(): {e}")
        time.sleep(3)
        return

    client.loop_forever()

if __name__ == "__main__":
    main()
