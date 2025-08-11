import os, ssl, json, time, random
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

# --------- Config & chemins ---------
ROOT = Path(__file__).resolve().parents[1]   # .../POC_Peed
ENV_PATH = ROOT / "config" / ".env"
load_dotenv(ENV_PATH)

MQTT_HOST  = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT  = int(os.getenv("MQTT_PORT", "8883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "usine/capteurs/machine1")

CA_FILE     = Path(os.getenv("CA_FILE", str(ROOT / "config" / "ca.crt")))
CLIENT_CERT = Path(os.getenv("CLIENT_CERT", str(ROOT / "config" / "client.crt")))
CLIENT_KEY  = Path(os.getenv("CLIENT_KEY", str(ROOT / "config" / "client.key")))
if not CA_FILE.is_absolute():     CA_FILE     = (ROOT / CA_FILE).resolve()
if not CLIENT_CERT.is_absolute(): CLIENT_CERT = (ROOT / CLIENT_CERT).resolve()
if not CLIENT_KEY.is_absolute():  CLIENT_KEY  = (ROOT / CLIENT_KEY).resolve()

EMIT_PERIOD = int(os.getenv("EMIT_PERIOD", "5"))
DEVICE_ID   = os.getenv("DEVICE_ID", "Machine_01")

# génération
NOMINAL_MEAN     = 67.0
NOMINAL_STD      = 2.5
ANOMALY_EVERY_N  = 20   # approx toutes 20 mesures
ANOMALY_DELTA    = 15.0 # +15°C

print(f"[CONFIG] host={MQTT_HOST} port={MQTT_PORT} topic={MQTT_TOPIC}", flush=True)

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def build_client() -> mqtt.Client:
    client = mqtt.Client(client_id=f"capteur-{DEVICE_ID}", protocol=mqtt.MQTTv311)
    client.enable_logger()
    client.tls_set(
        ca_certs=str(CA_FILE),
        certfile=str(CLIENT_CERT),
        keyfile=str(CLIENT_KEY),
        tls_version=ssl.PROTOCOL_TLS_CLIENT,
    )
    client.tls_insecure_set(False)
    return client

def main():
    client = build_client()
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()

    counter = 0
    print(f"[CAPTEUR] Démarré. Publication sur {MQTT_TOPIC} toutes les {EMIT_PERIOD}s.", flush=True)

    try:
        while True:
            counter += 1
            temp = random.gauss(NOMINAL_MEAN, NOMINAL_STD)
            if counter % ANOMALY_EVERY_N == 0:
                temp += ANOMALY_DELTA
                print(f"[CAPTEUR] >> ANOMALIE simulée: {temp:.2f}°C", flush=True)

            payload = {
                "device_id": DEVICE_ID,
                "timestamp": now_iso(),
                "temperature": round(temp, 2),
            }
            client.publish(MQTT_TOPIC, json.dumps(payload), qos=1)
            print(f"[CAPTEUR] Publié: {payload}", flush=True)
            time.sleep(EMIT_PERIOD)
    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()
        print("[CAPTEUR] Arrêt.", flush=True)

if __name__ == "__main__":
    main()
