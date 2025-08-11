Prereqs: Python 3.11, Mosquitto 2.x, OpenSSL (pour tes certs), Windows 10+.

Install:


py -3.11 -m venv .venv
.venv\Scripts\pip install -U pip
.venv\Scripts\pip install paho-mqtt python-dotenv requests Flask
Config: mets un .env.example (tu l’as déjà dans l’annexe, parfait).

Run: les 4 commandes “Reproductibilité” ci‑dessus.

Sécurité: bref paragraphe sur TLS/mTLS + token API.

