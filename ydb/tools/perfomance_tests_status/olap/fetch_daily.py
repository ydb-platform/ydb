#!/usr/bin/env python3
"""Fetch OLAP per-query run series via YDBWrapper scan query.

Default: one row per (suite, test, run) with datetime — no day averaging.
Legacy `--mode daily` still available (day buckets).

Uses streaming scan (same stack as `.github/scripts/analytics/ydb_wrapper.py`) —
no ydb CLI ~1000-row cap.

Auth:
  export CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=/path/to/sa-key.json
  # or pass --sa-key-file

Example:
  ./.venv/bin/python fetch_daily.py --output out/raw_test_runs.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent
# olap → perfomance_tests_status → tools → ydb/ → <repo>
REPO_ROOT = ROOT.parents[3]
ANALYTICS = REPO_ROOT / ".github" / "scripts" / "analytics"
SQL_BY_MODE = {
    "runs": ROOT / "queries" / "fetch_olap_test_runs.sql",
    "daily": ROOT / "queries" / "fetch_olap_test_daily.sql",
}
DEFAULT_WINDOW_DAYS = 30


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
        help="Output jsonl (default: out/raw_test_runs.json or out/raw_test_daily.json)",
    )
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

    since = args.since or (
        datetime.now(timezone.utc).date() - timedelta(days=max(1, args.days))
    ).isoformat()
    sql = build_sql(since, args.mode)
    out = Path(
        args.output
        or (
            ROOT / "out" / ("raw_test_runs.json" if args.mode == "runs" else "raw_test_daily.json")
        )
    )
    out.parent.mkdir(parents=True, exist_ok=True)

    qname = f"fetch_olap_test_{args.mode}"
    print(f"scan fetch mode={args.mode} since={since} → {out}", flush=True)
    with YDBWrapper(
        config_path=args.config,
        enable_statistics=False,
        script_name="olap/fetch_daily.py",
        silent=False,
        use_local_config=True,
    ) as ydb_w:
        if not ydb_w.check_credentials():
            raise SystemExit("YDB credentials check failed")
        rows = ydb_w.execute_scan_query(sql, query_name=qname)

    n = 0
    with out.open("w") as f:
        for row in rows:
            f.write(json.dumps(row_to_obj(row), ensure_ascii=False, separators=(",", ":")) + "\n")
            n += 1
    print(f"wrote {n} rows → {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
