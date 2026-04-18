import collections
import email.message
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from base64 import b64encode
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse

load_dotenv()

APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parent


def _default_app_log_file() -> Path:
    raw = os.getenv("APP_LOG_FILE", "").strip()
    if raw:
        return Path(raw)
    return PROJECT_ROOT / "sync.log"


LOG_FILE = _default_app_log_file()
DASHBOARD_FILE = PROJECT_ROOT / "static" / "index.html"


class InMemoryLogHandler(logging.Handler):
    """Ring buffer for ``GET /logs``; survives uvicorn replacing root handlers at startup."""

    def __init__(self, maxlen: int = 500) -> None:
        super().__init__()
        self._buf: collections.deque[str] = collections.deque(maxlen=maxlen)
        self._lock = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            with self._lock:
                self._buf.append(msg)
        except Exception:
            self.handleError(record)

    def tail(self, n: int) -> list[str]:
        with self._lock:
            return list(self._buf)[-n:]


_mem_log_handler: InMemoryLogHandler | None = None


def _install_app_logging_handlers() -> None:
    """Attach file + in-memory handlers after the ASGI server is up (avoids lost FileHandler)."""
    global _mem_log_handler
    if _mem_log_handler is not None:
        return
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    has_console = any(
        isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler) for h in root.handlers
    )
    if not has_console:
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        root.addHandler(sh)
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)
    except OSError as exc:
        print(f"APP_LOG_FILE not writable ({LOG_FILE}): {exc}", flush=True)
    _mem_log_handler = InMemoryLogHandler(500)
    _mem_log_handler.setFormatter(fmt)
    root.addHandler(_mem_log_handler)


KOMGA_SOURCE_DIR = os.getenv("KOMGA_SOURCE_DIR", "")
KOMGA_BASE_URL = os.getenv("KOMGA_BASE_URL", "")
KOMGA_USERNAME = os.getenv("KOMGA_USERNAME", "")
KOMGA_PASSWORD = os.getenv("KOMGA_PASSWORD", "")
KOMGA_LIBRARY_IDS = [x.strip() for x in os.getenv("KOMGA_LIBRARY_IDS", "").split(",") if x.strip()]
KOMGA_ALLOWED_EXTENSIONS = [
    x.strip().lower().lstrip(".") for x in os.getenv("KOMGA_ALLOWED_EXTENSIONS", "epub,pdf,cbz").split(",") if x.strip()
]
KOMGA_DB_PAGE_SIZE = max(50, min(1000, int(os.getenv("KOMGA_DB_PAGE_SIZE", "200"))))
KOMGA_HTTP_TIMEOUT_SECONDS = max(10, int(os.getenv("KOMGA_HTTP_TIMEOUT_SECONDS", "120")))
KOMGA_LIBRARY_REFRESH_ON_START = os.getenv("KOMGA_LIBRARY_REFRESH_ON_START", "false").strip().lower() in (
    "1",
    "true",
    "yes",
)
KOMGA_DOWNLOAD_BINARIES = os.getenv("KOMGA_DOWNLOAD_BINARIES", "true").strip().lower() in ("1", "true", "yes")
KOMGA_BINARIES_SKIP_UNCHANGED = os.getenv("KOMGA_BINARIES_SKIP_UNCHANGED", "true").strip().lower() in (
    "1",
    "true",
    "yes",
)
KOMGA_FILE_DOWNLOAD_TIMEOUT = max(60, int(os.getenv("KOMGA_FILE_DOWNLOAD_TIMEOUT", "600")))

KINDLE_IP = os.getenv("KINDLE_IP", "").strip()
KINDLE_USER = os.getenv("KINDLE_USER", "root")
DEST_DIR = os.getenv("DEST_DIR", "/mnt/us/documents")
SSH_KEY_PATH = os.getenv("SSH_KEY_PATH", "").strip()
SSH_TIMEOUT_SECONDS = int(os.getenv("SSH_TIMEOUT_SECONDS", "12"))
# Kindle SSH is often dropbear with an empty root password when no key is configured.
if "KINDLE_PASSWORD" in os.environ:
    SSH_PASSWORD: str | None = os.environ["KINDLE_PASSWORD"]
elif SSH_KEY_PATH:
    SSH_PASSWORD = None
elif KINDLE_IP:
    SSH_PASSWORD = ""
else:
    SSH_PASSWORD = None
KINDLE_STATS_INTERVAL_SECONDS = max(5, int(os.getenv("KINDLE_STATS_INTERVAL_SECONDS", "30")))
_ASKPASS_SCRIPT_PATH: str | None = None


def _default_komga_library_db_path() -> Path:
    raw = os.getenv("KOMGA_LIBRARY_DB_PATH", "").strip()
    if raw:
        return Path(raw)
    if KOMGA_SOURCE_DIR.strip():
        return Path(KOMGA_SOURCE_DIR).resolve() / "komga_library_snapshot.json"
    return Path("/data/komga/komga_library_snapshot.json")


KOMGA_LIBRARY_DB_PATH = _default_komga_library_db_path()


def _default_komga_binaries_dir() -> Path:
    raw = os.getenv("KOMGA_BINARIES_DIR", "").strip()
    if raw:
        return Path(raw)
    if KOMGA_SOURCE_DIR.strip():
        return Path(KOMGA_SOURCE_DIR).resolve() / "komga_library_files"
    return Path("/data/komga/komga_library_files")


KOMGA_BINARIES_DIR = _default_komga_binaries_dir()
KOMGA_BINARIES_HUMAN_DIRS = os.getenv("KOMGA_BINARIES_HUMAN_DIRS", "true").strip().lower() in (
    "1",
    "true",
    "yes",
)
# Rename ``media.<ext>`` to ``00042 - Chapter Title.<ext>`` for KOReader / sideload file sorting.
KOMGA_KOREADER_FILENAMES = os.getenv("KOMGA_KOREADER_FILENAMES", "true").strip().lower() in (
    "1",
    "true",
    "yes",
)
# Slug-only dirs (``Manga/Bleach/Chapter_1/``) with ``.komga.json`` sidecars holding Komga ids (no ``__id`` in names).
KOMGA_BINARIES_PLAIN_SLUGS = os.getenv("KOMGA_BINARIES_PLAIN_SLUGS", "true").strip().lower() in (
    "1",
    "true",
    "yes",
)
KOMGA_DISK_META_NAME = ".komga.json"

app = FastAPI(title="Komga Library Pull")
komga_db_lock = threading.Lock()
komga_refresh_lock = threading.Lock()
komga_library_refresh_in_progress = False
komga_library_db_summary: dict[str, Any] = {
    "last_fetched_at": None,
    "last_error": None,
    "db_path": str(KOMGA_LIBRARY_DB_PATH),
    "binaries_path": str(KOMGA_BINARIES_DIR),
    "counts": {"libraries": 0, "series": 0, "books": 0},
    "binary_pull": {
        "enabled": KOMGA_DOWNLOAD_BINARIES,
        "last_completed_at": None,
        "downloaded": None,
        "skipped": None,
        "failed": None,
    },
    "binary_pull_progress": None,
    "on_disk_books_with_media": None,
}

kindle_stats_lock = threading.Lock()
kindle_stats: dict[str, Any] = {
    "status": "Unknown",
    "last_checked": None,
    "reachable": False,
    "reachability_method": "none",
    "storage_path": DEST_DIR,
    "storage_total_gb": None,
    "storage_used_gb": None,
    "user_content_used_gb": None,
    "partition_used_gb": None,
    "storage_available_gb": None,
    "storage_use_percent": None,
    "message": "Kindle stats poller not started (set KINDLE_IP).",
}

logger = logging.getLogger("komga-pull")
logger.setLevel(logging.INFO)


def _ssh_base_command() -> list[str]:
    use_password_auth = SSH_PASSWORD is not None
    command = [
        "ssh",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "ServerAliveInterval=10",
        "-o",
        "ServerAliveCountMax=2",
        "-o",
        "BatchMode=no" if use_password_auth else "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
    ]
    if use_password_auth:
        command.extend(
            [
                "-o",
                "PasswordAuthentication=yes",
                "-o",
                "PubkeyAuthentication=no",
                "-o",
                "KbdInteractiveAuthentication=no",
                "-o",
                "PreferredAuthentications=password",
                "-o",
                "NumberOfPasswordPrompts=1",
            ]
        )
    else:
        command.extend(
            [
                "-o",
                "PasswordAuthentication=no",
                "-o",
                "PreferredAuthentications=publickey",
                "-o",
                "IdentitiesOnly=yes",
            ]
        )
    if SSH_KEY_PATH and not use_password_auth:
        command.extend(["-i", SSH_KEY_PATH])
    command.append(f"{KINDLE_USER}@{KINDLE_IP}")
    return command


def _prepare_askpass_script() -> str:
    global _ASKPASS_SCRIPT_PATH
    if _ASKPASS_SCRIPT_PATH:
        return _ASKPASS_SCRIPT_PATH
    askpass_file = tempfile.NamedTemporaryFile(prefix="kindle_askpass_", delete=False, mode="w", encoding="utf-8")
    askpass_file.write("#!/bin/sh\n")
    askpass_file.write('printf "%s" "$KINDLE_PASSWORD"\n')
    askpass_file.close()
    os.chmod(askpass_file.name, 0o700)
    _ASKPASS_SCRIPT_PATH = askpass_file.name
    return _ASKPASS_SCRIPT_PATH


def _ssh_subprocess_env() -> dict[str, str] | None:
    if SSH_PASSWORD is None:
        return None
    env = os.environ.copy()
    env["KINDLE_PASSWORD"] = SSH_PASSWORD
    env["SSH_ASKPASS"] = _prepare_askpass_script()
    env["SSH_ASKPASS_REQUIRE"] = "force"
    env.setdefault("DISPLAY", "dummy:0")
    return env


def _ssh_probe_kindle() -> bool:
    if not KINDLE_IP:
        return False
    result = subprocess.run(
        _ssh_base_command() + ["true"],
        capture_output=True,
        text=True,
        env=_ssh_subprocess_env(),
        timeout=SSH_TIMEOUT_SECONDS,
        check=False,
    )
    return result.returncode == 0


def _is_kindle_reachable() -> tuple[bool, str]:
    if _ssh_probe_kindle():
        return True, "ssh"
    return False, "none"


def _collect_kindle_stats() -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    if not KINDLE_IP:
        return {
            "status": "Not configured",
            "last_checked": now,
            "reachable": False,
            "reachability_method": "none",
            "storage_path": DEST_DIR,
            "storage_total_gb": None,
            "storage_used_gb": None,
            "user_content_used_gb": None,
            "partition_used_gb": None,
            "storage_available_gb": None,
            "storage_use_percent": None,
            "message": "Set KINDLE_IP for storage stats (SSH uses an empty password by default when SSH_KEY_PATH is unset; set KINDLE_PASSWORD or SSH_KEY_PATH to override).",
        }

    reachable, method = _is_kindle_reachable()
    base: dict[str, Any] = {
        "status": "Offline",
        "last_checked": now,
        "reachable": reachable,
        "reachability_method": method,
        "storage_path": DEST_DIR,
        "storage_total_gb": None,
        "storage_used_gb": None,
        "user_content_used_gb": None,
        "partition_used_gb": None,
        "storage_available_gb": None,
        "storage_use_percent": None,
        "message": "Kindle is not reachable via SSH.",
    }
    if not reachable:
        return base

    ssh_cmd = _ssh_base_command() + [f'df -k "{DEST_DIR}"']
    result = subprocess.run(
        ssh_cmd,
        capture_output=True,
        text=True,
        env=_ssh_subprocess_env(),
        timeout=SSH_TIMEOUT_SECONDS,
        check=False,
    )
    if result.returncode != 0:
        base["status"] = "Error"
        base["message"] = f"SSH df failed: {result.stderr.strip()}"
        return base

    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if len(lines) < 2:
        base["status"] = "Error"
        base["message"] = "Unexpected df output; cannot parse Kindle storage stats."
        return base

    parts = re.split(r"\s+", lines[-1])
    if len(parts) < 5:
        base["status"] = "Error"
        base["message"] = "Malformed df output; expected total/used/available/percent columns."
        return base

    try:
        total_kb = int(parts[1])
        partition_used_kb = int(parts[2])
        available_kb = int(parts[3])
    except ValueError:
        base["status"] = "Error"
        base["message"] = "Unable to convert Kindle storage totals."
        return base

    du_cmd = _ssh_base_command() + [f'du -sk "{DEST_DIR}"']
    du_result = subprocess.run(
        du_cmd,
        capture_output=True,
        text=True,
        env=_ssh_subprocess_env(),
        timeout=SSH_TIMEOUT_SECONDS,
        check=False,
    )
    user_used_kb: int | None = None
    if du_result.returncode == 0:
        du_lines = [line.strip() for line in du_result.stdout.splitlines() if line.strip()]
        if du_lines:
            du_parts = re.split(r"\s+", du_lines[0], maxsplit=1)
            if du_parts:
                try:
                    user_used_kb = int(du_parts[0])
                except ValueError:
                    user_used_kb = None

    if user_used_kb is None:
        base["status"] = "Error"
        base["message"] = "Unable to read user storage usage from destination path (du)."
        return base

    user_use_percent = round((user_used_kb / total_kb) * 100) if total_kb > 0 else 0

    base.update(
        {
            "status": "Online",
            "storage_total_gb": round(total_kb / (1024 * 1024), 2),
            "storage_used_gb": round(user_used_kb / (1024 * 1024), 2),
            "user_content_used_gb": round(user_used_kb / (1024 * 1024), 2),
            "partition_used_gb": round(partition_used_kb / (1024 * 1024), 2),
            "storage_available_gb": round(available_kb / (1024 * 1024), 2),
            "storage_use_percent": user_use_percent,
            "message": "Kindle stats refreshed (user content = du on DEST_DIR; partition = df used).",
        }
    )
    return base


def _kindle_stats_poller() -> None:
    while True:
        try:
            snapshot = _collect_kindle_stats()
        except Exception as exc:  # noqa: BLE001
            snapshot = {
                "status": "Error",
                "last_checked": datetime.now(timezone.utc).isoformat(),
                "reachable": False,
                "reachability_method": "none",
                "storage_path": DEST_DIR,
                "storage_total_gb": None,
                "storage_used_gb": None,
                "user_content_used_gb": None,
                "partition_used_gb": None,
                "storage_available_gb": None,
                "storage_use_percent": None,
                "message": f"Stats poll failed: {exc}",
            }
            logger.exception("Unhandled Kindle stats polling error")
        with kindle_stats_lock:
            kindle_stats.update(snapshot)
        time.sleep(KINDLE_STATS_INTERVAL_SECONDS)


def _komga_headers() -> dict[str, str]:
    auth = b64encode(f"{KOMGA_USERNAME}:{KOMGA_PASSWORD}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {auth}", "Accept": "application/json"}


def _komga_download_book_file_url(book_id: str) -> str:
    encoded = urllib.parse.quote(str(book_id), safe="")
    return f"{KOMGA_BASE_URL.rstrip('/')}/api/v1/books/{encoded}/file"


def _komga_download_book_file_request(book_id: str) -> urllib.request.Request:
    return urllib.request.Request(
        _komga_download_book_file_url(book_id),
        headers={
            "Authorization": _komga_headers()["Authorization"],
            "Accept": "application/octet-stream",
        },
        method="GET",
    )


def _komga_request_json(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    query = f"?{urllib.parse.urlencode(params)}" if params else ""
    req = urllib.request.Request(f"{KOMGA_BASE_URL.rstrip('/')}{path}{query}", headers=_komga_headers())
    with urllib.request.urlopen(req, timeout=float(KOMGA_HTTP_TIMEOUT_SECONDS)) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def _komga_fetch_paginated(path: str, extra_params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    page = 0
    items: list[dict[str, Any]] = []
    while True:
        params: dict[str, Any] = {"page": page, "size": KOMGA_DB_PAGE_SIZE}
        if extra_params:
            params.update(extra_params)
        data = _komga_request_json(path, params)
        if isinstance(data, list):
            items.extend([x for x in data if isinstance(x, dict)])
            break
        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected Komga response type for {path}: {type(data).__name__}")

        batch = data.get("content", [])
        if not isinstance(batch, list) or not batch:
            break
        items.extend([x for x in batch if isinstance(x, dict)])
        if data.get("last", False):
            break
        page += 1
    return items


def _komga_filter_by_libraries(items: list[dict[str, Any]], library_id_key: str) -> list[dict[str, Any]]:
    if not KOMGA_LIBRARY_IDS:
        return items
    allowed = set(KOMGA_LIBRARY_IDS)
    out: list[dict[str, Any]] = []
    for item in items:
        lid = item.get(library_id_key)
        if lid is not None and str(lid) in allowed:
            out.append(item)
    return out


_WIN_RESERVED = frozenset(
    {"CON", "PRN", "AUX", "NUL"} | {f"COM{i}" for i in range(1, 10)} | {f"LPT{i}" for i in range(1, 10)}
)


def _komga_fs_slug(text: str, max_len: int) -> str:
    """Filesystem-safe single path segment (no slashes); ASCII-ish, underscores."""
    s = str(text or "").strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_.-]+", "", s)
    s = re.sub(r"_+", "_", s).strip("._-")
    if not s:
        return ""
    up = s.upper()
    if up in _WIN_RESERVED or up in {x + "." for x in _WIN_RESERVED}:
        s = f"komga_{s}"
    if len(s) > max_len:
        s = s[:max_len].rstrip("._-")
    return s or ""


def _komga_disk_dir_segment(label: str, komga_id: str) -> str:
    """``ReadableName__<canonical_komga_id>`` — id suffix keeps paths unique and machine-parseable."""
    kid = str(komga_id or "").strip()
    if not kid:
        kid = "unknown"
    tail = f"__{kid}"
    # Typical single-component limit 255 bytes; UTF-8 slug may be shorter in chars.
    max_slug = max(16, 200 - len(tail.encode("utf-8")))
    base = _komga_fs_slug(label, max_slug)
    if not base:
        base = "item"
    return f"{base}{tail}"


def _komga_disk_parse_dir_component(name: str) -> str:
    """Return canonical Komga id from a disk directory (``Title__id`` or legacy ``id`` only)."""
    if "__" in name:
        left, right = name.rsplit("__", 1)
        if left.strip() and right.strip() and not any(c in right for c in "/\\"):
            return right.strip()
    return name


def _komga_read_sidecar(dir_path: Path) -> dict[str, Any] | None:
    f = dir_path / KOMGA_DISK_META_NAME
    if not f.is_file():
        return None
    try:
        raw = json.loads(f.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return raw if isinstance(raw, dict) else None


def _komga_write_sidecar_merge(dir_path: Path, payload: dict[str, Any]) -> None:
    """Write ``dir_path/.komga.json`` merging with any existing dict."""
    dir_path.mkdir(parents=True, exist_ok=True)
    cur = _komga_read_sidecar(dir_path) or {}
    cur.update({k: v for k, v in payload.items() if v is not None})
    tmp = dir_path / (KOMGA_DISK_META_NAME + ".tmp")
    tmp.write_text(json.dumps(cur, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(dir_path / KOMGA_DISK_META_NAME)


def _komga_write_book_disk_meta(book_dir: Path, library_id: str, series_id: str, book_id: str) -> None:
    _komga_write_sidecar_merge(
        book_dir,
        {"libraryId": str(library_id), "seriesId": str(series_id), "bookId": str(book_id)},
    )


def _komga_disk_resolve_book_ids(
    book_dir: Path, lib_dir: Path, series_dir: Path
) -> tuple[str, str, str]:
    """Resolve ids from book ``.komga.json``, then library/series sidecars, then ``Title__id`` path segments."""
    sc = _komga_read_sidecar(book_dir)
    if sc and sc.get("bookId") is not None:
        return (
            str(sc.get("libraryId") or _komga_disk_parse_dir_component(lib_dir.name)),
            str(sc.get("seriesId") or _komga_disk_parse_dir_component(series_dir.name)),
            str(sc["bookId"]),
        )
    lsc = _komga_read_sidecar(lib_dir)
    ssc = _komga_read_sidecar(series_dir)
    library_id = (
        str(lsc["libraryId"])
        if lsc and lsc.get("libraryId") is not None
        else _komga_disk_parse_dir_component(lib_dir.name)
    )
    series_id = (
        str(ssc["seriesId"])
        if ssc and ssc.get("seriesId") is not None
        else _komga_disk_parse_dir_component(series_dir.name)
    )
    book_id = _komga_disk_parse_dir_component(book_dir.name)
    return (library_id, series_id, book_id)


def _komga_alloc_slug_dir(
    parent: Path,
    base_slug: str,
    *,
    id_key: str,
    entity_id: str,
    extra: dict[str, str] | None = None,
    write_meta: bool = True,
) -> Path:
    """
    Pick ``parent/{base_slug}`` or ``parent/{base_slug}_N`` where the dir is free or the sidecar's
    ``id_key`` matches ``entity_id``. When ``write_meta`` is false, a missing directory is not
    created up-front (used by migrate to compute a destination path only).
    """
    if not base_slug:
        base_slug = "item"
    for n in range(10000):
        name = base_slug if n == 0 else f"{base_slug}_{n}"
        cand = parent / name
        if not cand.is_dir():
            if write_meta:
                payload = {id_key: str(entity_id)}
                if extra:
                    payload.update(extra)
                _komga_write_sidecar_merge(cand, payload)
            return cand
        sc = _komga_read_sidecar(cand)
        if sc and str(sc.get(id_key) or "") == str(entity_id):
            return cand
    raise RuntimeError(f"Could not allocate directory under {parent}")


def _komga_alloc_book_dir(ser_dir: Path, book_slug: str, book_id: str) -> Path:
    if not book_slug:
        book_slug = "book"
    for n in range(10000):
        name = book_slug if n == 0 else f"{book_slug}_{n}"
        cand = ser_dir / name
        if not cand.is_dir():
            return cand
        sc = _komga_read_sidecar(cand)
        if sc and str(sc.get("bookId") or "") == str(book_id):
            return cand
    raise RuntimeError(f"Could not allocate book directory under {ser_dir}")


def _build_library_label_map(libraries: list[dict[str, Any]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for lib in libraries:
        if not isinstance(lib, dict) or lib.get("id") is None:
            continue
        lid = str(lib["id"])
        lmeta = lib.get("metadata") if isinstance(lib.get("metadata"), dict) else {}
        out[lid] = str(lmeta.get("title") or lib.get("name") or lid)
    return out


def _build_series_label_map(series_all: list[dict[str, Any]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for s in series_all:
        if not isinstance(s, dict) or s.get("id") is None:
            continue
        sid = str(s["id"])
        smeta = s.get("metadata") if isinstance(s.get("metadata"), dict) else {}
        out[sid] = str(smeta.get("title") or s.get("name") or sid)
    return out


def _book_size_bytes(book: dict[str, Any]) -> int | None:
    for key in ("sizeBytes", "size"):
        v = book.get(key)
        if v is not None:
            try:
                return int(v)
            except (TypeError, ValueError):
                pass
    media = book.get("media") if isinstance(book.get("media"), dict) else {}
    for key in ("size", "fileSize"):
        v = media.get(key)
        if v is not None:
            try:
                return int(v)
            except (TypeError, ValueError):
                pass
    return None


def _book_binary_dir(
    book: dict[str, Any],
    root: Path,
    *,
    library_labels: dict[str, str] | None = None,
    series_labels: dict[str, str] | None = None,
    force_human_layout: bool = False,
    write_slug_meta: bool = True,
) -> Path:
    book_id = str(book.get("id") or "")
    library_id = str(book.get("libraryId") or "unknown")
    series_id = str(book.get("seriesId") if book.get("seriesId") is not None else "no-series")
    use_human = (
        (KOMGA_BINARIES_HUMAN_DIRS or force_human_layout)
        and library_labels
        and series_labels
    )
    if not use_human:
        return root / library_id / series_id / book_id
    lib_label = library_labels.get(library_id) or library_id
    ser_label = series_labels.get(series_id) or series_id
    book_label = _book_title_from_record(book, book_id) if book_id else "book"
    if use_human and KOMGA_BINARIES_PLAIN_SLUGS:
        lib_slug = _komga_fs_slug(lib_label, 120) or "library"
        ser_slug = _komga_fs_slug(ser_label, 120) or "series"
        book_slug = _komga_fs_slug(book_label, 120) or "book"
        lib_path = _komga_alloc_slug_dir(
            root, lib_slug, id_key="libraryId", entity_id=library_id, write_meta=write_slug_meta
        )
        ser_path = _komga_alloc_slug_dir(
            lib_path,
            ser_slug,
            id_key="seriesId",
            entity_id=series_id,
            extra={"libraryId": library_id},
            write_meta=write_slug_meta,
        )
        return _komga_alloc_book_dir(ser_path, book_slug, book_id)
    return root / _komga_disk_dir_segment(lib_label, library_id) / _komga_disk_dir_segment(
        ser_label, series_id
    ) / _komga_disk_dir_segment(book_label, book_id)


# Komga ≥1.2x ``MediaDto`` often has no ``extension``; infer from name, MIME, or download headers.
_KNOWN_BOOK_EXTS = frozenset("cbz cbr zip rar pdf epub 7z".split())
_MEDIA_TYPE_TO_EXT: dict[str, str] = {
    "application/pdf": "pdf",
    "application/epub+zip": "epub",
    "application/x-epub+zip": "epub",
    "application/vnd.comicbook+zip": "cbz",
    "application/x-cbz": "cbz",
    "application/x-rar-compressed": "cbr",
    "application/vnd.rar": "cbr",
    "application/x-cbr": "cbr",
    "application/x-7z-compressed": "7z",
}


def _first_media_file_in_book_dir(book_dir: Path) -> Path | None:
    """Prefer legacy ``media.*``; else the single known-format book file in the folder."""
    if not book_dir.is_dir():
        return None
    for p in book_dir.iterdir():
        if not p.is_file():
            continue
        if p.name in (KOMGA_DISK_META_NAME, KOMGA_DISK_META_NAME + ".tmp"):
            continue
        if p.name.startswith("media.") and not p.name.endswith(".part"):
            return p
    alt: list[Path] = []
    for p in book_dir.iterdir():
        if not p.is_file() or p.name.endswith(".part"):
            continue
        if p.name in (KOMGA_DISK_META_NAME, KOMGA_DISK_META_NAME + ".tmp"):
            continue
        suf = p.suffix.lower().lstrip(".")
        if suf in _KNOWN_BOOK_EXTS:
            alt.append(p)
    if len(alt) == 1:
        return alt[0]
    if len(alt) > 1:
        alt.sort(key=lambda x: x.name.lower())
        return alt[0]
    return None


def _koreader_sort_stem(book: dict[str, Any]) -> str:
    """Zero-padded sort key for file ordering in KOReader / plain file managers."""
    meta = book.get("metadata") if isinstance(book.get("metadata"), dict) else {}
    raw = meta.get("numberSort")
    if raw is None:
        raw = meta.get("number")
    try:
        if raw is None or (isinstance(raw, str) and not str(raw).strip()):
            raise ValueError
        n = float(raw)
    except (TypeError, ValueError):
        return "99999"
    if abs(n - round(n)) < 1e-9:
        return f"{int(round(n)):05d}"
    s = f"{n:08.2f}"
    return s.replace(".", "_")


def _koreader_filename_for_book(book: dict[str, Any], ext: str) -> str:
    ext = str(ext or "bin").lower().lstrip(".")
    sort_stem = _koreader_sort_stem(book)
    title = _book_title_from_record(book, str(book.get("id") or ""))
    title_slug = _komga_fs_slug(title, 100)
    if not title_slug:
        title_slug = "book"
    base = f"{sort_stem} - {title_slug}"
    max_base = 200 - len(ext) - 1
    if len(base) > max_base:
        base = base[:max_base].rstrip("._- ")
    if not base:
        base = sort_stem or "book"
    return f"{base}.{ext}"


def _komga_finalize_koreader_media_filename(book_dir: Path, book: dict[str, Any], ext: str) -> None:
    """Rename ``media.<ext>`` to a sortable KOReader-friendly filename; remove stray duplicates."""
    if not KOMGA_KOREADER_FILENAMES:
        return
    src = book_dir / f"media.{ext}"
    if not src.is_file():
        return
    dest = book_dir / _koreader_filename_for_book(book, ext)
    if dest.resolve() == src.resolve():
        return
    if dest.exists():
        try:
            dest.unlink()
        except OSError:
            pass
    for p in list(book_dir.iterdir()):
        if not p.is_file() or p.name.endswith(".part") or p == src:
            continue
        suf = p.suffix.lower().lstrip(".")
        if suf in _KNOWN_BOOK_EXTS:
            try:
                p.unlink()
            except OSError:
                pass
    try:
        src.rename(dest)
    except OSError as exc:
        logger.warning("KOReader-style rename failed in %s: %s", book_dir, exc)


def _count_book_dirs_with_media(root: Path) -> int:
    """Count ``library/series/book_id`` folders that contain at least one ``media.*`` file."""
    n = 0
    if not root.is_dir():
        return 0
    try:
        for lib_dir in root.iterdir():
            if not lib_dir.is_dir():
                continue
            for series_dir in lib_dir.iterdir():
                if not series_dir.is_dir():
                    continue
                for book_dir in series_dir.iterdir():
                    if not book_dir.is_dir():
                        continue
                    if _first_media_file_in_book_dir(book_dir) is not None:
                        n += 1
    except OSError as exc:
        logger.warning("Could not scan binaries dir for on-disk count %s: %s", root, exc)
    return n


def _extension_from_any_filename(filename: str) -> str | None:
    suf = Path(filename).suffix.lower().lstrip(".")
    return suf if suf in _KNOWN_BOOK_EXTS else None


def _parse_content_disposition_filename(cd: str | None) -> str | None:
    if not cd:
        return None
    m = email.message.Message()
    m["Content-Disposition"] = cd
    fn = m.get_param("filename", failobj=None, header="content-disposition")
    if isinstance(fn, tuple):
        fn = fn[-1] if fn else None
    return str(fn) if fn else None


def _extension_from_zip_mimetype(book: dict[str, Any]) -> str:
    """Zip archives with pages are almost always CBZ in Komga; plain zip otherwise."""
    media = book.get("media") if isinstance(book.get("media"), dict) else {}
    pages = media.get("pagesCount")
    try:
        pc = int(pages) if pages is not None else 0
    except (TypeError, ValueError):
        pc = 0
    return "cbz" if pc > 0 else "zip"


def _extension_from_mimetype(mime_raw: str, book: dict[str, Any]) -> str | None:
    mt = mime_raw.split(";")[0].strip().lower()
    if mt in ("application/zip", "application/x-zip-compressed"):
        return _extension_from_zip_mimetype(book)
    if mt == "application/octet-stream":
        return None
    return _MEDIA_TYPE_TO_EXT.get(mt)


def _extension_hints_from_book(book: dict[str, Any]) -> str | None:
    media = book.get("media") if isinstance(book.get("media"), dict) else {}
    ext = str(media.get("extension", "")).lower().lstrip(".")
    if ext:
        return ext
    name = book.get("name")
    if isinstance(name, str):
        fe = _extension_from_any_filename(name)
        if fe:
            return fe
    meta = book.get("metadata") if isinstance(book.get("metadata"), dict) else {}
    for key in ("title",):
        v = meta.get(key)
        if isinstance(v, str):
            fe = _extension_from_any_filename(v)
            if fe:
                return fe
    mt = str(media.get("mediaType", "")).strip()
    if mt:
        got = _extension_from_mimetype(mt, book)
        if got:
            return got
    return None


def _extension_from_download_response(response: Any, book: dict[str, Any]) -> str:
    """Pick file extension: Content-Disposition, then Content-Type, then book JSON."""
    cd = response.headers.get("Content-Disposition")
    fn = _parse_content_disposition_filename(cd)
    if fn:
        ext = _extension_from_any_filename(fn)
        if ext:
            return ext
    ct = response.headers.get("Content-Type")
    if ct:
        ext = _extension_from_mimetype(ct, book)
        if ext:
            return ext
    return _extension_hints_from_book(book) or "bin"


def _komga_download_book_to_book_dir(book_id: str, book_dir: Path, book: dict[str, Any]) -> str:
    """Stream download to ``book_dir/media.{ext}``; return extension used (no dot)."""
    req = _komga_download_book_file_request(book_id)
    book_dir.mkdir(parents=True, exist_ok=True)
    tmp = book_dir / "media.download.part"
    try:
        with urllib.request.urlopen(req, timeout=float(KOMGA_FILE_DOWNLOAD_TIMEOUT)) as response:
            ext = _extension_from_download_response(response, book)
            final = book_dir / f"media.{ext}"
            with tmp.open("wb") as out:
                shutil.copyfileobj(response, out, length=256 * 1024)
            if final.exists():
                final.unlink()
            tmp.replace(final)
            for p in book_dir.iterdir():
                if not p.is_file():
                    continue
                if p.name.startswith("media.") and not p.name.endswith(".part") and p != final:
                    try:
                        p.unlink()
                    except OSError:
                        pass
            _komga_finalize_koreader_media_filename(book_dir, book, ext)
            if KOMGA_BINARIES_PLAIN_SLUGS and KOMGA_BINARIES_HUMAN_DIRS:
                lid = str(book.get("libraryId") or "unknown")
                sid = str(book.get("seriesId") if book.get("seriesId") is not None else "no-series")
                bid = str(book.get("id") or book_id or "")
                if bid:
                    _komga_write_sidecar_merge(book_dir.parent.parent, {"libraryId": lid})
                    _komga_write_sidecar_merge(book_dir.parent, {"libraryId": lid, "seriesId": sid})
                    _komga_write_book_disk_meta(book_dir, lid, sid, bid)
            return ext
    except Exception:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        raise


_BINARY_PULL_PROGRESS_EVERY = 40


def _pull_komga_binaries_for_catalog(
    books: list[dict[str, Any]],
    root: Path,
    *,
    libraries: list[dict[str, Any]] | None = None,
    series_all: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    library_labels = _build_library_label_map(libraries or [])
    series_labels = _build_series_label_map(series_all or [])
    downloaded = 0
    skipped = 0
    failed = 0
    total = len(books)
    for i, book in enumerate(books):
        try:
            book_id = book.get("id")
            if not book_id:
                skipped += 1
                continue

            ext_hint = _extension_hints_from_book(book)
            if KOMGA_ALLOWED_EXTENSIONS and ext_hint and ext_hint not in KOMGA_ALLOWED_EXTENSIONS:
                skipped += 1
                continue

            book_dir = _book_binary_dir(
                book,
                root,
                library_labels=library_labels,
                series_labels=series_labels,
            )
            existing = _first_media_file_in_book_dir(book_dir)
            expected_size = _book_size_bytes(book)

            try:
                if (
                    KOMGA_BINARIES_SKIP_UNCHANGED
                    and expected_size is not None
                    and existing is not None
                    and existing.is_file()
                    and existing.stat().st_size == expected_size
                ):
                    sc = _komga_read_sidecar(book_dir) if KOMGA_BINARIES_PLAIN_SLUGS else None
                    if sc and str(sc.get("bookId") or "") != str(book_id):
                        pass
                    else:
                        skipped += 1
                        continue

                if existing is not None and existing.is_file() and expected_size is None:
                    skipped += 1
                    continue

                ext = _komga_download_book_to_book_dir(str(book_id), book_dir, book)
                if KOMGA_ALLOWED_EXTENSIONS and ext not in KOMGA_ALLOWED_EXTENSIONS:
                    wrong = _first_media_file_in_book_dir(book_dir)
                    if wrong is not None:
                        wrong.unlink(missing_ok=True)
                    skipped += 1
                    continue
                downloaded += 1
            except urllib.error.HTTPError as exc:
                failed += 1
                logger.warning("Komga binary HTTP %s for book %s: %s", exc.code, book_id, exc.reason)
            except Exception as exc:  # noqa: BLE001
                failed += 1
                logger.warning("Komga binary download failed for book %s: %s", book_id, exc)
        finally:
            if i % _BINARY_PULL_PROGRESS_EVERY == 0 or i >= total - 1:
                with komga_db_lock:
                    komga_library_db_summary["binary_pull_progress"] = {
                        "phase": "binaries",
                        "processed": i + 1,
                        "total": total,
                        "downloaded": downloaded,
                        "skipped": skipped,
                        "failed": failed,
                    }
    return {
        "enabled": True,
        "root": str(root.resolve()),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "downloaded": downloaded,
        "skipped": skipped,
        "failed": failed,
    }


def _hydrate_komga_library_summary_from_path(path: Path) -> None:
    global komga_library_db_summary
    if not path.exists():
        return
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        libs = raw.get("libraries") or []
        series = raw.get("series") or []
        books = raw.get("books") or []
        bp = raw.get("binary_pull") if isinstance(raw.get("binary_pull"), dict) else {}
        with komga_db_lock:
            komga_library_db_summary.update(
                {
                    "last_fetched_at": raw.get("fetched_at"),
                    "last_error": None,
                    "db_path": str(path),
                    "binaries_path": str(KOMGA_BINARIES_DIR.resolve()),
                    "counts": {
                        "libraries": len(libs) if isinstance(libs, list) else 0,
                        "series": len(series) if isinstance(series, list) else 0,
                        "books": len(books) if isinstance(books, list) else 0,
                    },
                    "binary_pull": {
                        "enabled": bp.get("enabled", KOMGA_DOWNLOAD_BINARIES),
                        "last_completed_at": bp.get("completed_at"),
                        "downloaded": bp.get("downloaded"),
                        "skipped": bp.get("skipped"),
                        "failed": bp.get("failed"),
                    },
                    "binary_pull_progress": None,
                    "on_disk_books_with_media": raw.get("on_disk_books_with_media"),
                }
            )
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not read Komga library snapshot at %s: %s", path, exc)


def _write_komga_library_snapshot(payload: dict[str, Any]) -> None:
    path = KOMGA_LIBRARY_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    tmp_path.write_text(text, encoding="utf-8")
    tmp_path.replace(path)
    _hydrate_komga_library_summary_from_path(path)


def _publish_catalog_counts(
    libraries: list[dict[str, Any]],
    series_all: list[dict[str, Any]],
    books_all: list[dict[str, Any]],
) -> None:
    with komga_db_lock:
        komga_library_db_summary["counts"] = {
            "libraries": len(libraries),
            "series": len(series_all),
            "books": len(books_all),
        }


def _run_komga_library_refresh_job() -> None:
    global komga_library_refresh_in_progress
    try:
        with komga_db_lock:
            komga_library_db_summary["binary_pull_progress"] = {"phase": "catalog"}
            komga_library_db_summary["last_error"] = None
        logger.info("Komga refresh started (fetching catalog from API).")

        libraries = _komga_fetch_paginated("/api/v1/libraries")
        libraries = _komga_filter_by_libraries(libraries, "id")

        try:
            series_all = _komga_fetch_paginated("/api/v1/series")
        except urllib.error.HTTPError as exc:
            logger.warning("Komga /api/v1/series unavailable (%s); continuing without series.", exc.code)
            series_all = []
        series_all = _komga_filter_by_libraries(series_all, "libraryId")

        books_all = _komga_fetch_paginated("/api/v1/books")
        books_all = _komga_filter_by_libraries(books_all, "libraryId")

        _publish_catalog_counts(libraries, series_all, books_all)
        logger.info(
            "Komga catalog fetched: %s libraries, %s series, %s books.",
            len(libraries),
            len(series_all),
            len(books_all),
        )

        now = datetime.now(timezone.utc).isoformat()
        if KOMGA_DOWNLOAD_BINARIES:
            with komga_db_lock:
                komga_library_db_summary["binary_pull_progress"] = {
                    "phase": "binaries",
                    "processed": 0,
                    "total": len(books_all),
                    "downloaded": 0,
                    "skipped": 0,
                    "failed": 0,
                }
            logger.info("Komga binary download phase starting (%s books).", len(books_all))
            binary_pull = _pull_komga_binaries_for_catalog(
                books_all,
                KOMGA_BINARIES_DIR,
                libraries=libraries,
                series_all=series_all,
            )
        else:
            binary_pull = {
                "enabled": False,
                "root": str(KOMGA_BINARIES_DIR.resolve()),
                "completed_at": now,
                "downloaded": 0,
                "skipped": 0,
                "failed": 0,
            }

        on_disk = _count_book_dirs_with_media(KOMGA_BINARIES_DIR)
        payload = {
            "format_version": 1,
            "fetched_at": now,
            "komga_base_url": KOMGA_BASE_URL.rstrip("/"),
            "library_id_filter": list(KOMGA_LIBRARY_IDS) if KOMGA_LIBRARY_IDS else None,
            "libraries": libraries,
            "series": series_all,
            "books": books_all,
            "binary_pull": binary_pull,
            "on_disk_books_with_media": on_disk,
        }
        _write_komga_library_snapshot(payload)
        with komga_db_lock:
            komga_library_db_summary["last_error"] = None
        logger.info(
            "Komga refresh completed: %s libraries, %s series, %s books; binary_pull=%s; on_disk_books=%s",
            len(libraries),
            len(series_all),
            len(books_all),
            {
                "enabled": binary_pull.get("enabled"),
                "downloaded": binary_pull.get("downloaded"),
                "skipped": binary_pull.get("skipped"),
                "failed": binary_pull.get("failed"),
            },
            on_disk,
        )
    except urllib.error.HTTPError as exc:
        err = f"Komga API HTTP error: {exc.code}"
        with komga_db_lock:
            komga_library_db_summary["last_error"] = err
        logger.exception("Komga library refresh HTTP error")
    except urllib.error.URLError as exc:
        err = f"Komga API connectivity error: {exc.reason}"
        with komga_db_lock:
            komga_library_db_summary["last_error"] = err
        logger.exception("Komga library refresh URL error")
    except Exception as exc:  # noqa: BLE001
        err = f"Komga library refresh failed: {exc}"
        with komga_db_lock:
            komga_library_db_summary["last_error"] = err
        logger.exception("Komga library refresh error")
    finally:
        with komga_db_lock:
            komga_library_db_summary["binary_pull_progress"] = None
        komga_library_refresh_in_progress = False
        komga_refresh_lock.release()


def _spawn_komga_refresh_thread_if_possible() -> bool:
    global komga_library_refresh_in_progress
    if not komga_refresh_lock.acquire(blocking=False):
        return False
    komga_library_refresh_in_progress = True
    threading.Thread(target=_run_komga_library_refresh_job, daemon=True).start()
    return True


def _start_komga_library_refresh() -> JSONResponse:
    if not KOMGA_BASE_URL or not KOMGA_USERNAME or not KOMGA_PASSWORD:
        raise HTTPException(
            status_code=500,
            detail="KOMGA_BASE_URL, KOMGA_USERNAME, and KOMGA_PASSWORD must be configured.",
        )
    if not _spawn_komga_refresh_thread_if_possible():
        return JSONResponse({"status": "already_running"})
    return JSONResponse(
        {
            "status": "started",
            "db_path": str(KOMGA_LIBRARY_DB_PATH),
            "binaries_path": str(KOMGA_BINARIES_DIR),
            "download_binaries": KOMGA_DOWNLOAD_BINARIES,
        }
    )


def _snapshot_series_and_library_maps() -> tuple[dict[str, dict[str, str]], dict[str, str]]:
    """Map series_id -> {name, libraryId}; library_id -> display name."""
    series_by_id: dict[str, dict[str, str]] = {}
    library_name_by_id: dict[str, str] = {}
    if not KOMGA_LIBRARY_DB_PATH.exists():
        return series_by_id, library_name_by_id
    try:
        raw = json.loads(KOMGA_LIBRARY_DB_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return series_by_id, library_name_by_id
    for lib in raw.get("libraries") or []:
        if not isinstance(lib, dict) or lib.get("id") is None:
            continue
        lid = str(lib["id"])
        lmeta = lib.get("metadata") if isinstance(lib.get("metadata"), dict) else {}
        library_name_by_id[lid] = str(lmeta.get("title") or lib.get("name") or lid)
    for s in raw.get("series") or []:
        if not isinstance(s, dict) or s.get("id") is None:
            continue
        sid = str(s["id"])
        smeta = s.get("metadata") if isinstance(s.get("metadata"), dict) else {}
        series_by_id[sid] = {
            "name": str(smeta.get("title") or s.get("name") or sid),
            "libraryId": str(s.get("libraryId") or ""),
        }
    return series_by_id, library_name_by_id


def _catalog_book_counts_by_library_series() -> dict[tuple[str, str], int]:
    """Count catalog books per (libraryId, seriesId) from the snapshot (library filter applied)."""
    counts: dict[tuple[str, str], int] = {}
    if not KOMGA_LIBRARY_DB_PATH.exists():
        return counts
    try:
        raw = json.loads(KOMGA_LIBRARY_DB_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return counts
    books = raw.get("books") or []
    books = _komga_filter_by_libraries([b for b in books if isinstance(b, dict)], "libraryId")
    for b in books:
        sid = b.get("seriesId")
        lid = b.get("libraryId")
        if sid is None or lid is None:
            continue
        key = (str(lid), str(sid))
        counts[key] = counts.get(key, 0) + 1
    return counts


def _snapshot_books_by_id() -> dict[str, dict[str, Any]]:
    """Book id -> book object from the on-disk catalog snapshot (for titles when listing files)."""
    out: dict[str, dict[str, Any]] = {}
    if not KOMGA_LIBRARY_DB_PATH.exists():
        return out
    try:
        raw = json.loads(KOMGA_LIBRARY_DB_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return out
    for b in raw.get("books") or []:
        if isinstance(b, dict) and b.get("id") is not None:
            out[str(b["id"])] = b
    return out


def _book_title_from_record(book: dict[str, Any], fallback_id: str) -> str:
    meta = book.get("metadata") if isinstance(book.get("metadata"), dict) else {}
    return str(meta.get("title") or book.get("name") or book.get("id") or fallback_id)


def _book_sort_key(book: dict[str, Any]) -> tuple[float, str]:
    meta = book.get("metadata") if isinstance(book.get("metadata"), dict) else {}
    ns = meta.get("numberSort")
    if ns is None:
        ns = meta.get("number")
    try:
        if ns is None or (isinstance(ns, str) and not ns.strip()):
            n = float("inf")
        else:
            n = float(ns)
    except (TypeError, ValueError):
        n = float("inf")
    title = _book_title_from_record(book, str(book.get("id") or "")).lower()
    return (n, title)


def _collect_downloaded_books_by_series(root: Path) -> dict[str, Any]:
    """
    Walk ``library_id / series_id / book_id`` under ``root``; include folders that contain a ``media.*`` file.
    Series names prefer the on-disk JSON snapshot when present.
    """
    series_by_id, library_name_by_id = _snapshot_series_and_library_maps()
    books_by_id = _snapshot_books_by_id()
    catalog_counts = _catalog_book_counts_by_library_series()
    if not root.is_dir():
        return {
            "binaries_path": str(root.resolve()),
            "total_books": 0,
            "total_series": 0,
            "series": [],
            "message": "Binaries directory does not exist yet.",
        }

    buckets: dict[tuple[str, str], list[dict[str, Any]]] = {}

    try:
        for lib_dir in sorted(root.iterdir(), key=lambda p: p.name.lower()):
            if not lib_dir.is_dir():
                continue
            for series_dir in sorted(lib_dir.iterdir(), key=lambda p: p.name.lower()):
                if not series_dir.is_dir():
                    continue
                for book_dir in sorted(series_dir.iterdir(), key=lambda p: p.name.lower()):
                    if not book_dir.is_dir():
                        continue
                    media_path = _first_media_file_in_book_dir(book_dir)
                    if media_path is None:
                        continue
                    library_id, series_id, book_id = _komga_disk_resolve_book_ids(
                        book_dir, lib_dir, series_dir
                    )
                    book = books_by_id.get(book_id, {})
                    sort_book = book if book else {"id": book_id, "name": book_id, "metadata": {}}
                    ext = media_path.suffix.lower().lstrip(".") or None
                    try:
                        size_b = media_path.stat().st_size
                    except OSError:
                        size_b = None
                    rel = media_path.relative_to(root).as_posix()
                    key = (library_id, series_id)
                    buckets.setdefault(key, []).append(
                        {
                            "id": book_id,
                            "title": _book_title_from_record(book, book_id),
                            "extension": ext,
                            "size_bytes": size_b,
                            "relative_path": rel,
                            "_sort": sort_book,
                        }
                    )
    except OSError as exc:
        logger.warning("Could not scan binaries dir %s: %s", root, exc)
        return {
            "binaries_path": str(root.resolve()),
            "total_books": 0,
            "total_series": 0,
            "series": [],
            "message": f"Could not read binaries directory: {exc}",
        }

    series_rows: list[dict[str, Any]] = []
    for (library_id, series_id), books_raw in buckets.items():
        books_raw.sort(key=lambda b: _book_sort_key(b["_sort"]))
        sm = series_by_id.get(series_id)
        series_name = sm["name"] if sm else f"Series {series_id}"
        lib_name = library_name_by_id.get(library_id)
        clean_books = [
            {
                "id": b["id"],
                "title": b["title"],
                "extension": b["extension"],
                "size_bytes": b["size_bytes"],
                "relative_path": b["relative_path"],
            }
            for b in books_raw
        ]
        on_disk_n = len(clean_books)
        cat_n = catalog_counts.get((library_id, series_id), 0)
        download_pct: float | None
        if cat_n > 0:
            download_pct = round(100.0 * on_disk_n / cat_n, 1)
        else:
            download_pct = None
        series_rows.append(
            {
                "id": series_id,
                "name": series_name,
                "libraryId": library_id,
                "libraryName": lib_name,
                "books": clean_books,
                "catalog_book_count": cat_n,
                "on_disk_book_count": on_disk_n,
                "download_percent": download_pct,
            }
        )

    series_rows.sort(
        key=lambda s: (
            (s.get("libraryName") or "").lower(),
            s["name"].lower(),
        )
    )

    total_books = sum(len(s["books"]) for s in series_rows)
    catalog_total = sum(catalog_counts.values())
    overall_pct: float | None = (
        round(100.0 * total_books / catalog_total, 1) if catalog_total > 0 else None
    )
    out: dict[str, Any] = {
        "binaries_path": str(root.resolve()),
        "total_books": total_books,
        "total_series": len(series_rows),
        "catalog_book_total": catalog_total,
        "overall_download_percent": overall_pct,
        "series": series_rows,
    }
    return out


def _prune_empty_dirs_under(root: Path) -> int:
    """Remove empty directories under ``root`` (deepest first); never removes ``root``."""
    removed = 0
    if not root.is_dir():
        return 0
    try:
        all_dirs = sorted(
            (p for p in root.rglob("*") if p.is_dir()),
            key=lambda p: len(p.parts),
            reverse=True,
        )
    except OSError:
        return 0
    root_res = root.resolve()
    for p in all_dirs:
        if p.resolve() == root_res:
            continue
        try:
            next(p.iterdir())
        except StopIteration:
            try:
                p.rmdir()
                removed += 1
            except OSError:
                pass
        except OSError:
            pass
    return removed


def migrate_komga_binaries_to_human_layout(
    *,
    root: Path | None = None,
    snapshot_path: Path | None = None,
    dry_run: bool = True,
) -> dict[str, Any]:
    """
    Move each ``library/series/book`` folder that contains ``media.*`` into the human-readable
    ``Title__id`` layout derived from the catalog snapshot (e.g. from legacy ``id/id/id`` paths).

    Safe to re-run: skips when source already equals the computed destination, or when the
    destination exists and is non-empty.
    """
    root = (root or KOMGA_BINARIES_DIR).resolve()
    snapshot_path = snapshot_path or KOMGA_LIBRARY_DB_PATH
    errors: list[str] = []
    result: dict[str, Any] = {
        "dry_run": dry_run,
        "binaries_path": str(root),
        "snapshot_path": str(snapshot_path),
        "moved": 0,
        "planned_moves": 0,
        "skipped_already": 0,
        "skipped_conflict": 0,
        "errors": errors,
    }

    if not snapshot_path.exists():
        errors.append(f"Snapshot missing: {snapshot_path}")
        return result
    try:
        raw = json.loads(snapshot_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(f"Could not read snapshot: {exc}")
        return result

    libraries = [x for x in (raw.get("libraries") or []) if isinstance(x, dict)]
    series_all = [x for x in (raw.get("series") or []) if isinstance(x, dict)]
    books_raw = [x for x in (raw.get("books") or []) if isinstance(x, dict)]
    library_labels = _build_library_label_map(libraries)
    series_labels = _build_series_label_map(series_all)
    books_by_id: dict[str, dict[str, Any]] = {}
    for b in books_raw:
        bid = b.get("id")
        if bid is not None:
            books_by_id[str(bid)] = b

    if not root.is_dir():
        errors.append(f"Binaries root is not a directory: {root}")
        return result

    candidates: list[tuple[Path, str, str, str]] = []
    try:
        for lib_dir in sorted(root.iterdir(), key=lambda p: p.name.lower()):
            if not lib_dir.is_dir():
                continue
            for series_dir in sorted(lib_dir.iterdir(), key=lambda p: p.name.lower()):
                if not series_dir.is_dir():
                    continue
                for book_dir in sorted(series_dir.iterdir(), key=lambda p: p.name.lower()):
                    if not book_dir.is_dir():
                        continue
                    if _first_media_file_in_book_dir(book_dir) is None:
                        continue
                    try:
                        book_dir.resolve().relative_to(root)
                    except ValueError:
                        continue
                    library_id, series_id, book_id = _komga_disk_resolve_book_ids(
                        book_dir, lib_dir, series_dir
                    )
                    candidates.append((book_dir, library_id, series_id, book_id))
    except OSError as exc:
        errors.append(f"Scan failed: {exc}")
        return result

    for book_dir, library_id, series_id, book_id in candidates:
        book = books_by_id.get(
            book_id,
            {
                "id": book_id,
                "libraryId": library_id,
                "seriesId": series_id,
                "name": book_id,
                "metadata": {},
            },
        )
        target = _book_binary_dir(
            book,
            root,
            library_labels=library_labels,
            series_labels=series_labels,
            force_human_layout=True,
            write_slug_meta=False,
        )
        try:
            src = book_dir.resolve()
            dst = target.resolve()
        except OSError as exc:
            errors.append(f"{book_dir}: {exc}")
            continue
        if src == dst:
            result["skipped_already"] += 1
            continue
        if dst.exists():
            try:
                has_children = any(dst.iterdir())
            except OSError as exc:
                errors.append(f"{dst}: {exc}")
                result["skipped_conflict"] += 1
                continue
            if has_children:
                logger.warning(
                    "migrate_komga_binaries: destination non-empty, skip %s -> %s",
                    src,
                    dst,
                )
                result["skipped_conflict"] += 1
                continue
            try:
                dst.rmdir()
            except OSError as exc:
                errors.append(f"rmdir {dst}: {exc}")
                result["skipped_conflict"] += 1
                continue
        if dry_run:
            result["planned_moves"] += 1
            continue
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(dst))
            if KOMGA_BINARIES_PLAIN_SLUGS:
                _komga_write_sidecar_merge(dst.parent.parent, {"libraryId": library_id})
                _komga_write_sidecar_merge(dst.parent, {"libraryId": library_id, "seriesId": series_id})
                _komga_write_book_disk_meta(dst, library_id, series_id, book_id)
            result["moved"] += 1
            logger.info("migrate_komga_binaries: moved %s -> %s", src, dst)
        except OSError as exc:
            errors.append(f"move {src} -> {dst}: {exc}")

    if not dry_run and result["moved"] > 0:
        result["empty_dirs_removed"] = _prune_empty_dirs_under(root)
    else:
        result["empty_dirs_removed"] = 0

    return result


def rename_komga_binaries_for_koreader(
    *,
    root: Path | None = None,
    snapshot_path: Path | None = None,
    dry_run: bool = True,
) -> dict[str, Any]:
    """
    Rename each ``media.<ext>`` under the binaries tree to ``00042 - Title.<ext>`` for KOReader-style sorting.

    Skips folders that already use a KOReader-style name or have no legacy ``media.*`` file.
    """
    root = (root or KOMGA_BINARIES_DIR).resolve()
    snapshot_path = snapshot_path or KOMGA_LIBRARY_DB_PATH
    errors: list[str] = []
    result: dict[str, Any] = {
        "dry_run": dry_run,
        "binaries_path": str(root),
        "snapshot_path": str(snapshot_path),
        "renamed": 0,
        "planned_renames": 0,
        "skipped_no_media": 0,
        "skipped_already_named": 0,
        "errors": errors,
    }
    if not KOMGA_KOREADER_FILENAMES:
        errors.append("KOMGA_KOREADER_FILENAMES is disabled; enable it to rename files.")
        return result
    if not snapshot_path.exists():
        errors.append(f"Snapshot missing: {snapshot_path}")
        return result
    try:
        raw = json.loads(snapshot_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(f"Could not read snapshot: {exc}")
        return result
    books_raw = [x for x in (raw.get("books") or []) if isinstance(x, dict)]
    books_by_id: dict[str, dict[str, Any]] = {}
    for b in books_raw:
        bid = b.get("id")
        if bid is not None:
            books_by_id[str(bid)] = b
    if not root.is_dir():
        errors.append(f"Binaries root is not a directory: {root}")
        return result
    try:
        for lib_dir in sorted(root.iterdir(), key=lambda p: p.name.lower()):
            if not lib_dir.is_dir():
                continue
            for series_dir in sorted(lib_dir.iterdir(), key=lambda p: p.name.lower()):
                if not series_dir.is_dir():
                    continue
                for book_dir in sorted(series_dir.iterdir(), key=lambda p: p.name.lower()):
                    if not book_dir.is_dir():
                        continue
                    med = _first_media_file_in_book_dir(book_dir)
                    if med is None:
                        result["skipped_no_media"] += 1
                        continue
                    if not med.name.startswith("media."):
                        result["skipped_already_named"] += 1
                        continue
                    ext = med.suffix.lower().lstrip(".")
                    if ext not in _KNOWN_BOOK_EXTS:
                        continue
                    library_id, series_id, book_id = _komga_disk_resolve_book_ids(
                        book_dir, lib_dir, series_dir
                    )
                    book = books_by_id.get(
                        book_id,
                        {
                            "id": book_id,
                            "libraryId": library_id,
                            "seriesId": series_id,
                            "metadata": {},
                            "name": book_id,
                        },
                    )
                    dest_name = _koreader_filename_for_book(book, ext)
                    if med.name == dest_name:
                        result["skipped_already_named"] += 1
                        continue
                    if dry_run:
                        result["planned_renames"] += 1
                    else:
                        _komga_finalize_koreader_media_filename(book_dir, book, ext)
                        result["renamed"] += 1
    except OSError as exc:
        errors.append(f"Walk failed: {exc}")
    return result


@app.on_event("startup")
def _startup_background_workers() -> None:
    _install_app_logging_handlers()
    logger.info("Application startup complete (log file: %s).", LOG_FILE)
    _hydrate_komga_library_summary_from_path(KOMGA_LIBRARY_DB_PATH)
    with komga_db_lock:
        if komga_library_db_summary.get("on_disk_books_with_media") is None:
            komga_library_db_summary["on_disk_books_with_media"] = _count_book_dirs_with_media(KOMGA_BINARIES_DIR)
    if KOMGA_LIBRARY_REFRESH_ON_START:
        if not (KOMGA_BASE_URL and KOMGA_USERNAME and KOMGA_PASSWORD):
            logger.warning("KOMGA_LIBRARY_REFRESH_ON_START is enabled but Komga credentials are incomplete.")
        elif not _spawn_komga_refresh_thread_if_possible():
            logger.warning("Komga library refresh on start skipped: a refresh is already running.")

    if KINDLE_IP:
        threading.Thread(target=_kindle_stats_poller, daemon=True).start()
    else:
        with kindle_stats_lock:
            kindle_stats["message"] = "Set KINDLE_IP to enable background Kindle storage polling."


@app.get("/")
def dashboard() -> FileResponse:
    return FileResponse(DASHBOARD_FILE)


@app.get("/status")
def get_status() -> dict[str, Any]:
    with komga_db_lock:
        komga = {
            "in_progress": komga_library_refresh_in_progress,
            **komga_library_db_summary,
        }
    with kindle_stats_lock:
        ks = dict(kindle_stats)
    return {
        "in_progress": komga_library_refresh_in_progress,
        "komga_library": komga,
        "kindle_stats": ks,
        "message": "Download books from Komga into this container via POST /komga/library/refresh.",
    }


@app.get("/kindle-stats")
def get_kindle_stats() -> dict[str, Any]:
    with kindle_stats_lock:
        return dict(kindle_stats)


@app.post("/komga/library/refresh")
def trigger_komga_library_refresh() -> JSONResponse:
    """Fetch libraries, series, and books from Komga; write JSON snapshot; optionally download each book file."""
    return _start_komga_library_refresh()


@app.get("/komga/library")
def get_komga_library_catalog() -> dict[str, Any]:
    with komga_db_lock:
        return {
            "in_progress": komga_library_refresh_in_progress,
            **komga_library_db_summary,
        }


@app.get("/komga/library/books-by-series")
def list_downloaded_books_by_series() -> dict[str, Any]:
    """On-disk books under ``KOMGA_BINARIES_DIR`` (``media.*`` present), grouped by series."""
    return _collect_downloaded_books_by_series(KOMGA_BINARIES_DIR)


@app.get("/komga/library/snapshot")
def get_komga_library_snapshot_file() -> FileResponse:
    if not KOMGA_LIBRARY_DB_PATH.exists():
        raise HTTPException(
            status_code=404,
            detail="No Komga snapshot on disk yet. POST /komga/library/refresh first.",
        )
    return FileResponse(
        KOMGA_LIBRARY_DB_PATH,
        media_type="application/json",
        filename="komga_library_snapshot.json",
    )


@app.get("/logs")
def get_logs() -> dict[str, list[str]]:
    if _mem_log_handler is None:
        _install_app_logging_handlers()
    if _mem_log_handler is not None:
        lines = _mem_log_handler.tail(100)
        if lines:
            return {"lines": lines}
    if LOG_FILE.exists():
        try:
            lines = LOG_FILE.read_text(encoding="utf-8", errors="ignore").splitlines()
            return {"lines": lines[-100:]}
        except OSError:
            pass
    return {"lines": []}
