#!/usr/bin/env python3
"""Upload ci_stages.jsonl produced by CI jobs into analytics/ci_pr_check_stages."""

from __future__ import annotations

import argparse
import os
import sys

from ci_pr_check_stages import (
    github_env_defaults,
    resolve_table_path,
    rows_from_jsonl,
    upsert_rows,
)
from ydb_wrapper import YDBWrapper


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload CI stage JSONL to YDB")
    parser.add_argument("--file", required=True, help="Path to ci_stages.jsonl")
    parser.add_argument(
        "--table-path",
        default=None,
        help="Override table path (default: analytics/ci_pr_check_stages)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    path = args.file
    if not path or not os.path.exists(path):
        print(f"No CI stages file at {path!r}, skipping")
        return 0

    try:
        with open(path, "r", encoding="utf-8") as handle:
            rows = rows_from_jsonl(handle, defaults=github_env_defaults())
        if not rows:
            print(f"No valid stage rows in {path}")
            return 0

        with YDBWrapper() as wrapper:
            if not wrapper.check_credentials():
                print("Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping")
                return 0
            table_path = args.table_path or resolve_table_path(wrapper)
            uploaded = upsert_rows(wrapper, table_path, rows)
            print(f"Uploaded {uploaded} CI stage rows to {table_path}")
        return 0
    except Exception as exc:  # noqa: BLE001 — telemetry must not fail CI
        print(f"Warning: Failed to upload CI stages to YDB: {exc}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
