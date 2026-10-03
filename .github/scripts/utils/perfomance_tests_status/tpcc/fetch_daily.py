#!/usr/bin/env python3
"""Fetch TPC-C runs + Allure report URLs via common.ydb_client (YDBWrapper scan).

Writes:
  out/raw.json       — perfomance/tpcc metrics
  out/reports.json   — tests_results Info.report_url for TpccW* suites

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

from common.report_config import cfg_int, load_report_config  # noqa: E402
from common.ydb_client import (  # noqa: E402
    DEFAULT_CONFIG,
    YdbClientError,
    ensure_sa_credentials,
    scan_query,
    to_result_sets,
)

SQL_PATH = ROOT / "queries" / "fetch_tpcc.sql"
REPORTS_SQL_PATH = ROOT / "queries" / "fetch_tpcc_reports.sql"
DEFAULT_OUT = ROOT / "out" / "raw.json"
DEFAULT_WINDOW_DAYS = cfg_int(load_report_config(ROOT), "window_days", 60)


def build_sql(sql_path: Path, since_iso: str) -> str:
    sql = sql_path.read_text()
    if "{{SINCE}}" not in sql:
        raise SystemExit(f"{sql_path} missing {{SINCE}} placeholder")
    sql = sql.replace("{{SINCE}}", since_iso)
    lines = [ln for ln in sql.splitlines() if not ln.strip().upper().startswith("ORDER BY")]
    return "\n".join(lines) + "\n"


def _write_result_sets(path: Path, rows: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = to_result_sets(rows)
    cols = payload["result_sets"][0]["columns"]
    payload["result_sets"][0]["columns"] = [{"name": c} for c in cols]
    path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


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
        help="Output metrics result_sets JSON (default: out/raw.json)",
    )
    ap.add_argument(
        "--reports-output",
        default=None,
        help="Allure URLs JSON (default: <output-dir>/reports.json)",
    )
    ap.add_argument(
        "--skip-reports",
        action="store_true",
        help="Fetch metrics only (no tests_results Allure URLs)",
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
    out = Path(args.output)
    reports_out = Path(args.reports_output) if args.reports_output else out.parent / "reports.json"

    print(f"scan fetch tpcc since={since_iso} → {out}", flush=True)
    try:
        rows = scan_query(
            build_sql(SQL_PATH, since_iso),
            query_name="fetch_tpcc",
            script_name="tpcc/fetch_daily.py",
            sa_key_file=args.sa_key_file,
            config=args.config,
            use_local_config=True,
        )
    except YdbClientError as e:
        raise SystemExit(str(e)) from e

    _write_result_sets(out, rows)
    print(f"wrote {len(rows)} rows (result_sets JSON) → {out}", flush=True)

    if not args.skip_reports:
        print(f"scan fetch tpcc reports since={since_iso} → {reports_out}", flush=True)
        try:
            report_rows = scan_query(
                build_sql(REPORTS_SQL_PATH, since_iso),
                query_name="fetch_tpcc_reports",
                script_name="tpcc/fetch_daily.py",
                sa_key_file=args.sa_key_file,
                config=args.config,
                use_local_config=True,
            )
        except YdbClientError as e:
            raise SystemExit(str(e)) from e
        _write_result_sets(reports_out, report_rows)
        print(f"wrote {len(report_rows)} report rows → {reports_out}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
