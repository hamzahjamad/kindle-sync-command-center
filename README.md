# Komga Library Pull

Small FastAPI service that pulls your **Komga** catalog into this container: a JSON snapshot (libraries, series, books) and, by default, each matching book file on disk.

## What it does

- `POST /komga/library/refresh` — fetch from Komga, write `KOMGA_LIBRARY_DB_PATH`, optionally download binaries to `KOMGA_BINARIES_DIR`.
- `GET /status` — progress flag, configured paths, live catalog counts during a run, last completed pull time, `binary_pull_progress` while downloading, then final `binary_pull` totals from the snapshot.
- `GET /komga/library` — same summary as embedded in `/status`.
- `GET /komga/library/snapshot` — download the JSON snapshot file.
- `GET /logs` — last 100 lines (in-memory ring buffer plus optional `APP_LOG_FILE` tail; see Compose `APP_LOG_FILE`).
- Dashboard at `/` (Tailwind + Font Awesome).

## Configuration

Copy `.env.example` to `.env` and set at least `KOMGA_BASE_URL`, `KOMGA_USERNAME`, and `KOMGA_PASSWORD`. See `.env.example` for optional filters and paths.

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
```

Open `http://localhost:8080` and use **Download from Komga**.

## Docker

```bash
docker build -t komga-library-pull .
docker run -d --name komga-library-pull -p 8080:8080 --env-file .env \
  -e KOMGA_LIBRARY_DB_PATH=/data/komga/komga_library_snapshot.json \
  -e KOMGA_BINARIES_DIR=/data/komga/komga_library_files \
  -v komga_data:/data/komga \
  komga-library-pull
```

Or from the repo root:

```bash
docker compose up -d --build
```

Compose mounts a named volume at `/data/komga` and sets the snapshot and binaries paths. The `komga-library-cron` service calls `POST /komga/library/refresh` on the schedule in `KOMGA_LIBRARY_CRON_SCHEDULE` (default every 6 hours).

## Scheduled pull (cron sidecar)

Set in `.env`:

```env
KOMGA_LIBRARY_CRON_SCHEDULE=0 */6 * * *
```

Then `docker compose up -d --build`. Check the cron container with `docker logs -f komga-library-cron`.
