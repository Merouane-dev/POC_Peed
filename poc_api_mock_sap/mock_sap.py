from flask import Flask, request, jsonify
from datetime import datetime

app = Flask(__name__)
API_TOKEN = "secret-token-demo"  # charge le depuis .env si tu préfères

@app.post("/sap/api/notifications")
def create_notification():
    # Auth simple par token
    token = request.headers.get("X-API-TOKEN", "")
    if token != API_TOKEN:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    notif_id = f"NOTIF{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
    print(f"[SAP-MOCK] Notification reçue -> ID={notif_id} | payload={data}")
    return jsonify({"notif_id": notif_id, "status": "enregistree"}), 201

if __name__ == "__main__":
    # port 5000 local
    app.run(host="127.0.0.1", port=5000, debug=True)
