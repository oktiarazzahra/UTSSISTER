# UTS Sister - Pub/Sub Log Aggregator

README ini berisi instruksi clone, build, run, asumsi, dan endpoint API.

## Clone Repository

```bash
git clone https://github.com/oktiarazzahra/UTSSISTER.git
cd UTSSISTER
```

## Instruksi Run Singkat (Docker)

```bash
# Build
docker build -t uts-aggregator .

# Run
docker run -p 8080:8080 uts-aggregator
```

Setelah container jalan, API tersedia di:

```text
http://localhost:8080
```

## Menjalankan Dengan Docker Compose (Opsional)

```bash
docker compose up --build
```

## Asumsi

- Docker sudah ter-install dan berjalan.
- Port 8080 pada host tidak dipakai proses lain.
- Service aggregator menyimpan data dedup/event secara persisten menggunakan SQLite.
- Event yang dikirim ke endpoint publish wajib sesuai schema (topic, event_id, timestamp, source, payload).

## Endpoint API

- GET / : info service dan daftar endpoint.
- POST /publish : menerima 1 event atau list event, lalu memasukkan ke queue.
- GET /events : mengambil daftar event yang sudah diproses.
- GET /events?topic=<nama_topic> : filter event berdasarkan topic.
- GET /stats : statistik received, unique_processed, duplicate_dropped, topics, uptime.
- GET /health : health check service.
- GET /docs : dokumentasi interaktif Swagger.

## Contoh Request Publish

```bash
curl -X POST "http://localhost:8080/publish" \
	-H "Content-Type: application/json" \
	-d '{
		"topic": "logs",
		"event_id": "evt-001",
		"timestamp": "2024-01-01T00:00:00Z",
		"source": "service-a",
		"payload": {
			"message": "hello"
		}
	}'
```

## Struktur Project

```text
UTSSISTER/
|-- src/
|   |-- __init__.py
|   |-- main.py
|   |-- models.py
|   |-- dedup_store.py
|   `-- consumer.py
|-- tests/
|   `-- test_aggregator.py
|-- data/
|-- Dockerfile
|-- docker-compose.yml
|-- requirements.txt
`-- README.md
```
