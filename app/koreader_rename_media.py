"""Rename ``media.<ext>`` files to KOReader-friendly ``00042 - Title.<ext>`` (see ``rename_komga_binaries_for_koreader``)."""

from __future__ import annotations

import argparse
import json
import sys

try:
    from dotenv import load_dotenv

    load_dotenv()
except ModuleNotFoundError:
    pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Apply renames (default is dry-run).",
    )
    args = parser.parse_args()

    from app.main import rename_komga_binaries_for_koreader

    out = rename_komga_binaries_for_koreader(dry_run=not args.yes)
    print(json.dumps(out, indent=2))
    if out.get("errors"):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
