#!/usr/bin/env python3
"""Fetch OLAP per-query daily series via YDBWrapper scan query.

Uses streaming scan (same stack as `.github/scripts/analytics/ydb_wrapper.py`) —
no ydb CLI ~1000-row cap.

Auth:
  export CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=/path/to/sa-key.json
  # or pass --sa-key-file

Example:
  ./.venv/bin/python fetch_daily.py --since 2026-06-08 --output out/raw_test_daily.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent
# olap → perfomance_tests_status → tools → ydb/ → <repo>
REPO_ROOT = ROOT.parents[3]
ANALYTICS = REPO_ROOT / ".github" / "scripts" / "analytics"
SQL_PATH = ROOT / "queries" / "fetch_olap_test_daily.sql"


def _setup_path() -> None:
    # Prefer analytics ydb_wrapper; never put repo root on path (shadows pip `ydb` SDK).
    ap = str(ANALYTICS)
    if ap not in sys.path:
        sys.path.insert(0, ap)


def _jsonable(v):
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if hasattr(v, "as_py"):
        return _jsonable(v.as_py())
    return str(v)


def row_to_obj(row) -> dict:
    d = dict(row)
    return {k: _jsonable(v) for k, v in d.items()}


def build_sql(since: str) -> str:
    sql = SQL_PATH.read_text()
    if "{{SINCE}}" not in sql:
        raise SystemExit(f"{SQL_PATH} missing {{SINCE}} placeholder")
    sql = sql.replace("{{SINCE}}", f"{since}T00:00:00Z")
    # ORDER BY is useless for dump and expensive on scan
    lines = [ln for ln in sql.splitlines() if not ln.strip().upper().startswith("ORDER BY")]
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--since", default="2026-06-08", help="UTC day lower bound (YYYY-MM-DD)")
    ap.add_argument("--output", "-o", default=str(ROOT / "out" / "raw_test_daily.json"))
    ap.add_argument(
        "--sa-key-file",
        default=os.environ.get("CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS")
        or os.environ.get("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"),
        help="Service account JSON key (sets CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS)",
    )
    ap.add_argument(
        "--config",
        default=str(REPO_ROOT / ".github" / "config" / "ydb_qa_config.json"),
        help="ydb_qa_config.json path",
    )
    args = ap.parse_args()

    if not args.sa_key_file:
        raise SystemExit(
            "Need --sa-key-file or env CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"
        )
    key = Path(args.sa_key_file).expanduser().resolve()
    if not key.is_file():
        raise SystemExit(f"SA key not found: {key}")
    os.environ["CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = str(key)
    os.environ["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"] = str(key)

    _setup_path()
    from ydb_wrapper import YDBWrapper  # noqa: E402

    sql = build_sql(args.since)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    print(f"scan fetch since={args.since} → {out}", flush=True)
    with YDBWrapper(
        config_path=args.config,
        enable_statistics=False,
        script_name="olap/fetch_daily.py",
        silent=False,
        use_local_config=True,
    ) as ydb_w:
        if not ydb_w.check_credentials():
            raise SystemExit("YDB credentials check failed")
        rows = ydb_w.execute_scan_query(sql, query_name="fetch_olap_test_daily")

    n = 0
    with out.open("w") as f:
        for row in rows:
            f.write(json.dumps(row_to_obj(row), ensure_ascii=False, separators=(",", ":")) + "\n")
            n += 1
    print(f"wrote {n} rows → {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
