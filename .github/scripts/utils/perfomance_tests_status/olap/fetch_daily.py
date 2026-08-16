#!/usr/bin/env python3
"""Fetch OLAP suite / per-query series via common.ydb_client (YDBWrapper scan).

Default: one row per (suite, test, run) with datetime — no day averaging.
Legacy `--mode daily` still available (day buckets).

Auth:
  export CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=/path/to/sa-key.json
  # or: eval "$(python3 ../duty_agent/dutyctl.py init-token --shell)"
  # or pass --sa-key-file

Example:
  python3 fetch_daily.py --mode suites -o out/raw.json
  python3 fetch_daily.py --output out/raw_test_runs.json
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

from common.report_config import cfg_int, load_report_config  # noqa: E402
from common.ydb_client import (  # noqa: E402
    DEFAULT_CONFIG,
    YdbClientError,
    ensure_sa_credentials,
    scan_query,
    to_result_sets,
)

SQL_BY_MODE = {
    "runs": ROOT / "queries" / "fetch_olap_test_runs.sql",
    "daily": ROOT / "queries" / "fetch_olap_test_daily.sql",
    "suites": ROOT / "queries" / "fetch_olap_suites.sql",
    "tests": ROOT / "queries" / "fetch_olap_test_issues.sql",
}
DEFAULT_OUT = {
    "runs": ROOT / "out" / "raw_test_runs.json",
    "daily": ROOT / "out" / "raw_test_daily.json",
    "suites": ROOT / "out" / "raw.json",
    "tests": ROOT / "out" / "raw_tests.json",
}
_CFG = load_report_config(ROOT)
DEFAULT_WINDOW_DAYS = cfg_int(_CFG, "window_days", 30)


def build_sql(since: str, mode: str) -> str:
    sql_path = SQL_BY_MODE[mode]
    sql = sql_path.read_text()
    if "{{SINCE}}" not in sql:
        raise SystemExit(f"{sql_path} missing {{SINCE}} placeholder")
    sql = sql.replace("{{SINCE}}", f"{since}T00:00:00Z")
    # ORDER BY is useless for dump and expensive on scan
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
        "--mode",
        choices=sorted(SQL_BY_MODE),
        default="runs",
        help="runs = one point per launch (default); daily = day buckets (legacy)",
    )
    ap.add_argument(
        "--output",
        "-o",
        default=None,
        help="Output path (default depends on --mode)",
    )
    ap.add_argument(
        "--sa-key-file",
        default=None,
        help="Service account JSON key (sets CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS)",
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

    since = args.since or (
        datetime.now(timezone.utc).date() - timedelta(days=max(1, args.days))
    ).isoformat()
    sql = build_sql(since, args.mode)
    out = Path(args.output or DEFAULT_OUT[args.mode])
    out.parent.mkdir(parents=True, exist_ok=True)

    qname = f"fetch_olap_{args.mode}"
    print(f"scan fetch mode={args.mode} since={since} → {out}", flush=True)
    try:
        rows = scan_query(
            sql,
            query_name=qname,
            script_name="olap/fetch_daily.py",
            sa_key_file=args.sa_key_file,
            config=args.config,
            use_local_config=True,
        )
    except YdbClientError as e:
        raise SystemExit(str(e)) from e

    # suites/tests → result_sets JSON for generate.load_rows; runs/daily → jsonl
    if args.mode in ("suites", "tests"):
        payload = to_result_sets(rows)
        # generate.load_rows accepts columns as strings or {name:…}
        cols = payload["result_sets"][0]["columns"]
        payload["result_sets"][0]["columns"] = [{"name": c} for c in cols]
        out.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        print(f"wrote {len(rows)} rows (result_sets JSON) → {out}", flush=True)
    else:
        with out.open("w") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
        print(f"wrote {len(rows)} rows (jsonl) → {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
