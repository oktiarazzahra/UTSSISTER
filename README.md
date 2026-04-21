# Struktur Folder Project

Dokumen ini hanya menjelaskan struktur folder project secara singkat.

## Struktur

```text
UTSSISTER11231076/
|-- src/
|   |-- __init__.py
|   |-- main.py
|   |-- models.py
|   |-- dedup_store.py
|   `-- consumer.py
|-- tests/
|   `-- test_aggregator.py
|-- Dockerfile
|-- docker-compose.yml
|-- requirements.txt
`-- README.md
```

## Penjelasan Singkat Folder

- src: kode utama aplikasi aggregator (API, consumer, model event, dan dedup store).
- tests: unit test untuk validasi endpoint, deduplication, persistensi, dan health check.
- Dockerfile: konfigurasi build image aplikasi.
- docker-compose.yml: konfigurasi service aggregator dan publisher untuk menjalankan via Docker Compose.
- requirements.txt: daftar dependency Python.
- README.md: dokumen ringkas struktur project.
