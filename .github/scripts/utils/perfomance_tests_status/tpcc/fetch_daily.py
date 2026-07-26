#!/usr/bin/env python3
"""Fetch TPC-C runs via common.ydb_client (YDBWrapper scan).

Auth:
  export CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=/path/to/sa-key.json
  # or: eval "$(python3 ../duty_agent/dutyctl.py init-token --shell)"
  # or pass --sa-key-file

Example:
  python3 fetch_daily.py -o out/raw.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PTS = ROOT.parent
if str(PTS) not in sys.path:
    sys.path.insert(0, str(PTS))

from common.ydb_client import (  # noqa: E402
    DEFAULT_CONFIG,
    YdbClientError,
    ensure_sa_credentials,
    scan_query,
    to_result_sets,
)

SQL_PATH = ROOT / "queries" / "fetch_tpcc.sql"
DEFAULT_OUT = ROOT / "out" / "raw.json"
DEFAULT_WINDOW_DAYS = 30


def build_sql(since_iso: str) -> str:
    sql = SQL_PATH.read_text()
    if "{{SINCE}}" not in sql:
        raise SystemExit(f"{SQL_PATH} missing {{SINCE}} placeholder")
    sql = sql.replace("{{SINCE}}", since_iso)
    lines = [ln for ln in sql.splitlines() if not ln.strip().upper().startswith("ORDER BY")]
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--since",
        default=None,
        help=f"UTC day lower bound YYYY-MM-DD (default: last {DEFAULT_WINDOW_DAYS} days)",
    )
    ap.add_argument(
        "--days",
        type=int,
        default=DEFAULT_WINDOW_DAYS,
        help=f"Lookback days when --since omitted (default: {DEFAULT_WINDOW_DAYS})",
    )
    ap.add_argument(
        "--output",
        "-o",
        default=str(DEFAULT_OUT),
        help="Output result_sets JSON (default: out/raw.json)",
    )
    ap.add_argument(
        "--sa-key-file",
        default=None,
        help="Service account JSON key",
    )
    ap.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG),
        help="ydb_qa_config.json path",
    )
    args = ap.parse_args()

    try:
        ensure_sa_credentials(args.sa_key_file)
    except YdbClientError as e:
        raise SystemExit(str(e)) from e

    since_day = args.since or (
        datetime.now(timezone.utc).date() - timedelta(days=max(1, args.days))
    ).isoformat()
    since_iso = f"{since_day}T00:00:00Z"
    sql = build_sql(since_iso)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    print(f"scan fetch tpcc since={since_iso} → {out}", flush=True)
    try:
        rows = scan_query(
            sql,
            query_name="fetch_tpcc",
            script_name="tpcc/fetch_daily.py",
            sa_key_file=args.sa_key_file,
            config=args.config,
            use_local_config=True,
        )
    except YdbClientError as e:
        raise SystemExit(str(e)) from e

    payload = to_result_sets(rows)
    cols = payload["result_sets"][0]["columns"]
    payload["result_sets"][0]["columns"] = [{"name": c} for c in cols]
    out.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"wrote {len(rows)} rows (result_sets JSON) → {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
