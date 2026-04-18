"""Reorganize on-disk Komga binaries into ``Title__id`` folders (see ``migrate_komga_binaries_to_human_layout``)."""

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
        help="Actually move directories (default is dry-run: plan only).",
    )
    args = parser.parse_args()

    from app.main import migrate_komga_binaries_to_human_layout

    out = migrate_komga_binaries_to_human_layout(dry_run=not args.yes)
    print(json.dumps(out, indent=2))
    if out.get("errors"):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
