import json
import os
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "fintech-kafka-kafka-bootstrap.fintech-messaging.svc.cluster.local:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "fintech-transactions")
PORT = int(os.environ.get("PORT", "8080"))

print(f"[PRODUCER] Connecting to Kafka at {KAFKA_BOOTSTRAP}...", flush=True)
producer = None
while not producer:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=5000,
            acks='all'
        )
        print("[PRODUCER] Successfully connected to Kafka cluster!", flush=True)
    except Exception as e:
        print(f"[PRODUCER] Waiting for Kafka: {e}", flush=True)
        time.sleep(2)

class TransactionHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/healthz':
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == '/transfer':
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length == 0:
                self.send_response(400)
                self.end_headers()
                return

            post_data = self.rfile.read(content_length)
            try:
                payload = json.loads(post_data.decode('utf-8'))
                
                # Отправка транзакции в топик Kafka с подтверждением
                future = producer.send(TOPIC, payload)
                future.get(timeout=5)

                self.send_response(202)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                response = {"status": "QUEUED", "message": "Transaction sent to Kafka"}
                self.wfile.write(json.dumps(response).encode('utf-8'))
                print(f"[PRODUCER SUCCESS] Queued Tx: {payload.get('transaction_id')}", flush=True)
            except Exception as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(str(e).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

if __name__ == "__main__":
    server = HTTPServer(('0.0.0.0', PORT), TransactionHandler)
    print(f"[PRODUCER] Production HTTP API started on port {PORT}", flush=True)
    server.serve_forever()