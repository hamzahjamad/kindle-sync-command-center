import json
import logging
import os
import re
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse

load_dotenv()

APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parent
LOG_FILE = PROJECT_ROOT / "sync.log"
STATUS_FILE = PROJECT_ROOT / "status.json"
DASHBOARD_FILE = PROJECT_ROOT / "static" / "index.html"

KINDLE_IP = os.getenv("KINDLE_IP", "")
KINDLE_USER = os.getenv("KINDLE_USER", "root")
SOURCE_DIR = os.getenv("SOURCE_DIR", "/data/books")
DEST_DIR = os.getenv("DEST_DIR", "/mnt/us/documents")
MIN_STORAGE_THRESHOLD_GB = float(os.getenv("MIN_STORAGE_THRESHOLD", "1"))
SSH_KEY_PATH = os.getenv("SSH_KEY_PATH", "")
SSH_TIMEOUT_SECONDS = int(os.getenv("SSH_TIMEOUT_SECONDS", "12"))
PING_TIMEOUT_SECONDS = int(os.getenv("PING_TIMEOUT_SECONDS", "2"))

app = FastAPI(title="Kindle Sync Command Center")
sync_lock = threading.Lock()
sync_in_progress = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger("kindle-sync")


def _load_status() -> dict[str, Any]:
    if not STATUS_FILE.exists():
        return {
            "status": "Never Run",
            "last_run": None,
            "remaining_storage_gb": None,
            "message": "No sync has been run yet.",
        }
    try:
        return json.loads(STATUS_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {
            "status": "Fail",
            "last_run": datetime.now(timezone.utc).isoformat(),
            "remaining_storage_gb": None,
            "message": "Status file was corrupted.",
        }


def _save_status(status: dict[str, Any]) -> None:
    STATUS_FILE.write_text(json.dumps(status, indent=2), encoding="utf-8")


def _ssh_base_command() -> list[str]:
    command = [
        "ssh",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "ServerAliveInterval=10",
        "-o",
        "ServerAliveCountMax=2",
    ]
    if SSH_KEY_PATH:
        command.extend(["-i", SSH_KEY_PATH])
    command.append(f"{KINDLE_USER}@{KINDLE_IP}")
    return command


def _ping_kindle() -> bool:
    if not KINDLE_IP:
        return False
    result = subprocess.run(
        ["ping", "-c", "1", "-W", str(PING_TIMEOUT_SECONDS), KINDLE_IP],
        capture_output=True,
        text=True,
        timeout=PING_TIMEOUT_SECONDS + 2,
        check=False,
    )
    return result.returncode == 0


def _get_storage_available_gb() -> float:
    ssh_cmd = _ssh_base_command() + [f"df -k \"{DEST_DIR}\""]
    result = subprocess.run(
        ssh_cmd,
        capture_output=True,
        text=True,
        timeout=SSH_TIMEOUT_SECONDS,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"SSH df failed: {result.stderr.strip()}")

    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if len(lines) < 2:
        raise RuntimeError("Unexpected df output; cannot parse storage.")

    data_line = lines[-1]
    parts = re.split(r"\s+", data_line)
    if len(parts) < 4:
        raise RuntimeError("Malformed df output; cannot parse available column.")

    try:
        available_kb = int(parts[3])
    except ValueError as exc:
        raise RuntimeError("Available storage is not an integer.") from exc

    return round(available_kb / (1024 * 1024), 2)


def _run_rsync() -> tuple[int, str, str]:
    rsync_target = f"{KINDLE_USER}@{KINDLE_IP}:{DEST_DIR.rstrip('/')}/"
    rsync_cmd = [
        "rsync",
        "-avz",
        "--delete",
        "--protect-args",
        "--partial",
        "--timeout=30",
    ]
    if SSH_KEY_PATH:
        rsync_cmd.extend(["-e", f"ssh -i {SSH_KEY_PATH} -o ConnectTimeout=10"])
    rsync_cmd.extend([f"{SOURCE_DIR.rstrip('/')}/", rsync_target])

    result = subprocess.run(
        rsync_cmd,
        capture_output=True,
        text=True,
        timeout=60 * 20,
        check=False,
    )
    return result.returncode, result.stdout, result.stderr


def _run_sync_job() -> None:
    global sync_in_progress
    now = datetime.now(timezone.utc).isoformat()
    status: dict[str, Any] = {
        "status": "Started",
        "last_run": now,
        "remaining_storage_gb": None,
        "message": "Sync started.",
    }
    _save_status(status)

    try:
        if not _ping_kindle():
            status.update(
                {
                    "status": "Offline",
                    "message": "Device Offline: Kindle is not reachable by ping.",
                }
            )
            logger.error(status["message"])
            return

        remaining_storage_gb = _get_storage_available_gb()
        status["remaining_storage_gb"] = remaining_storage_gb

        if remaining_storage_gb < MIN_STORAGE_THRESHOLD_GB:
            status.update(
                {
                    "status": "Fail",
                    "message": (
                        f"Storage guard blocked sync. Available {remaining_storage_gb} GB "
                        f"< threshold {MIN_STORAGE_THRESHOLD_GB} GB."
                    ),
                }
            )
            logger.error(status["message"])
            return

        code, stdout, stderr = _run_rsync()
        if stdout:
            logger.info("rsync stdout:\n%s", stdout)
        if stderr:
            logger.error("rsync stderr:\n%s", stderr)

        if code == 0:
            status.update({"status": "Success", "message": "Sync completed successfully."})
        else:
            status.update({"status": "Fail", "message": f"rsync failed with exit code {code}."})
    except subprocess.TimeoutExpired as exc:
        status.update({"status": "Fail", "message": f"Timeout while syncing: {exc}"})
        logger.exception("Timeout error during sync")
    except Exception as exc:  # noqa: BLE001
        status.update({"status": "Fail", "message": f"Unhandled error: {exc}"})
        logger.exception("Unhandled sync error")
    finally:
        _save_status(status)
        sync_in_progress = False
        sync_lock.release()


@app.get("/")
def dashboard() -> FileResponse:
    return FileResponse(DASHBOARD_FILE)


@app.post("/sync")
def trigger_sync() -> JSONResponse:
    global sync_in_progress
    if not KINDLE_IP:
        raise HTTPException(status_code=500, detail="KINDLE_IP is not configured.")

    if not sync_lock.acquire(blocking=False):
        return JSONResponse({"status": "already_running"})

    sync_in_progress = True
    thread = threading.Thread(target=_run_sync_job, daemon=True)
    thread.start()
    return JSONResponse({"status": "started"})


@app.get("/status")
def get_status() -> dict[str, Any]:
    status = _load_status()
    status["in_progress"] = sync_in_progress
    return status


@app.get("/logs")
def get_logs() -> dict[str, list[str]]:
    if not LOG_FILE.exists():
        return {"lines": []}
    lines = LOG_FILE.read_text(encoding="utf-8", errors="ignore").splitlines()
    return {"lines": lines[-50:]}
