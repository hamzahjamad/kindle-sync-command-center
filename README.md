# Kindle Sync Command Center

Python + FastAPI command center for syncing books to a Kindle (KOReader over SSH) using `rsync`.

## Features

- `POST /sync`: starts a sync job immediately and returns `{"status":"started"}`.
- `GET /status`: returns last sync state, run time, remaining storage, and live in-progress flag.
- `GET /logs`: returns last 50 lines from `sync.log`.
- Safety checks before sync:
  - Ping Kindle to detect offline/sleeping device.
  - SSH `df -k` storage guard (blocks sync if remaining storage is below threshold).
- Dashboard UI with Tailwind CSS:
  - Sync button
  - Status panel
  - Storage bar
  - Recent logs

## Project structure

```text
kindle-sync-command-center/
  app/main.py
  static/index.html
  Dockerfile
  requirements.txt
  .env.example
```

## Environment variables

Copy `.env.example` to `.env` and update values.

- `KINDLE_IP`: Kindle LAN IP
- `KINDLE_USER`: SSH user (`root` is common for KOReader setups)
- `SOURCE_DIR`: local folder to sync from
- `DEST_DIR`: Kindle destination path (usually `/mnt/us/documents`)
- `MIN_STORAGE_THRESHOLD`: minimum free GB required before sync
- `SSH_KEY_PATH`: private key path used by container/app
- `SSH_TIMEOUT_SECONDS`: timeout for `ssh`/`df`
- `PING_TIMEOUT_SECONDS`: timeout for `ping`

## Run locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
```

Open: `http://localhost:8080`

## Docker deployment (Proxmox/Docker)

Build image:

```bash
docker build -t kindle-sync-cc .
```

Run container (example):

```bash
docker run -d \
  --name kindle-sync-cc \
  --restart unless-stopped \
  -p 8080:8080 \
  --env-file .env \
  -v /path/to/local/books:/books/library:ro \
  -v /path/to/ssh-keys:/app/keys:ro \
  kindle-sync-cc
```

## KOReader SSH authorized_keys setup (Kindle)

1. **Generate key on your server/container host**
   ```bash
   ssh-keygen -t ed25519 -f ./kindle_ed25519 -C "kindle-sync"
   ```

2. **Enable SSH on KOReader**
   - In KOReader, install/enable SSH plugin (or use your jailbroken SSH service setup).
   - Ensure Kindle is reachable over Wi-Fi.

3. **Copy public key to Kindle**
   - SSH once with password (if available) or use your existing access method:
     ```bash
     ssh root@<KINDLE_IP>
     ```
   - On Kindle:
     ```bash
     mkdir -p /mnt/us/.ssh
     chmod 700 /mnt/us/.ssh
     ```
   - Append the public key (`kindle_ed25519.pub`) to:
     `/mnt/us/.ssh/authorized_keys`
   - Set permissions:
     ```bash
     chmod 600 /mnt/us/.ssh/authorized_keys
     ```

4. **Test key-based login**
   ```bash
   ssh -i ./kindle_ed25519 root@<KINDLE_IP> "echo ok"
   ```

5. **Mount key into container**
   - Put private key in mounted path, e.g. `/app/keys/kindle_ed25519`.
   - Set `SSH_KEY_PATH=/app/keys/kindle_ed25519` in `.env`.

## Nightly cron trigger via curl

Run sync every night at 2:30 AM:

```cron
30 2 * * * curl -s -X POST http://127.0.0.1:8080/sync >/dev/null 2>&1
```

If API runs on another host/IP, replace `127.0.0.1:8080`.

## Notes on edge cases

- **Kindle sleeping/offline**: ping failure returns `Offline` status.
- **SSH timeout**: handled with configured timeout and marked as `Fail`.
- **Malformed filenames**: `rsync --protect-args` is enabled to avoid shell expansion issues.
