import json
import os
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from kafka import KafkaConsumer
import psycopg2

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "fintech-kafka-kafka-bootstrap.fintech-messaging.svc.cluster.local:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "fintech-transactions")
DB_HOST = os.environ.get("DB_HOST", "fintech-db-rw.fintech-databases.svc.cluster.local")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "fintech_prod")
DB_USER = os.environ.get("DB_USER", "fintech_admin")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "StrongPassword123")
HEALTH_PORT = int(os.environ.get("HEALTH_PORT", "8081"))

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/healthz':
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.end_headers()

def run_health_server():
    httpd = HTTPServer(('0.0.0.0', HEALTH_PORT), HealthHandler)
    httpd.serve_forever()

threading.Thread(target=run_health_server, daemon=True).start()
print(f"[CONSUMER] Health check probe server active on port {HEALTH_PORT}", flush=True)

def get_db_connection():
    while True:
        try:
            print(f"[CONSUMER] Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}...", flush=True)
            c = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                connect_timeout=5
            )
            c.autocommit = True
            print("[CONSUMER] Successfully connected to PostgreSQL!", flush=True)
            return c
        except Exception as e:
            print(f"[CONSUMER] Waiting for DB: {e}", flush=True)
            time.sleep(2)

conn = get_db_connection()

consumer = None
while not consumer:
    try:
        print(f"[CONSUMER] Connecting to Kafka at {KAFKA_BOOTSTRAP}...", flush=True)
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="fintech-tx-workers",
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print(f"[CONSUMER] Subscribed to topic: {TOPIC}!", flush=True)
    except Exception as e:
        print(f"[CONSUMER] Waiting for Kafka: {e}", flush=True)
        time.sleep(2)

print("[CONSUMER] Worker is running and waiting for transactions...", flush=True)

for message in consumer:
    tx = message.value
    try:
        if conn.closed:
            conn = get_db_connection()

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO transactions
                (transaction_id, from_account, to_account, amount, currency, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING;
            """, (
                tx.get("transaction_id"),
                tx.get("from_account"),
                tx.get("to_account"),
                tx.get("amount"),
                tx.get("currency", "USD"),
                tx.get("status", "PROCESSED")
            ))
        print(f"[CONSUMER SUCCESS] Processed Tx: {tx.get('transaction_id')} | Amount: {tx.get('amount')}", flush=True)
    except Exception as err:
        print(f"[CONSUMER ERROR] Failed to process {tx}: {err}", flush=True)
        try:
            conn.close()
        except Exception:
            pass
        conn = get_db_connection()