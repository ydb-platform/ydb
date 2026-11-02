#!/usr/bin/env python3
"""Fetch OLAP + TPC-C data from YDB and generate Now HTML reports (no LLM).

Auth:
  export CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=/path/to/sa-key.json

Example:
  python3 run_reports.py --publish-dir /tmp/perf_reports
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parents[2]  # ydb/tools/perfomance_tests_status → repo
ANALYTICS = REPO_ROOT / ".github" / "scripts" / "analytics"
OLAP = ROOT / "olap"
TPCC = ROOT / "tpcc"

OLAP_SUITE_DAYS = 30
OLAP_RUNS_DAYS = 30
TPCC_DAYS = 30


def _setup_path() -> None:
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
    return {k: _jsonable(v) for k, v in dict(row).items()}


def since_iso(days: int) -> str:
    d = datetime.now(timezone.utc).date() - timedelta(days=max(1, days))
    return f"{d.isoformat()}T00:00:00Z"


def load_sql(path: Path, since: str) -> str:
    sql = path.read_text()
    if "{{SINCE}}" not in sql:
        raise SystemExit(f"{path} missing {{SINCE}} placeholder")
    sql = sql.replace("{{SINCE}}", since)
    lines = [ln for ln in sql.splitlines() if not ln.strip().upper().startswith("ORDER BY")]
    return "\n".join(lines) + "\n"


def fetch_rows(ydb_w, sql: str, query_name: str) -> list[dict]:
    rows = ydb_w.execute_scan_query(sql, query_name=query_name)
    return [row_to_obj(r) for r in rows]


def write_json_list(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, ensure_ascii=False, separators=(",", ":")))
    print(f"wrote {len(rows)} rows → {path}", flush=True)


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    print(f"wrote {len(rows)} rows → {path}", flush=True)


def run_generate(cmd: list[str], cwd: Path) -> None:
    print("+", " ".join(cmd), flush=True)
    subprocess.check_call(cmd, cwd=str(cwd))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--publish-dir",
        type=Path,
        default=ROOT / "out" / "publish",
        help="Directory for final HTML reports",
    )
    ap.add_argument(
        "--work-dir",
        type=Path,
        default=None,
        help="Scratch dir for raw dumps (default: <publish-dir>/_raw)",
    )
    ap.add_argument(
        "--sa-key-file",
        default=os.environ.get("CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS")
        or os.environ.get("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"),
        help="Service account JSON key",
    )
    ap.add_argument(
        "--config",
        default=str(REPO_ROOT / ".github" / "config" / "ydb_qa_config.json"),
        help="ydb_qa_config.json path",
    )
    ap.add_argument("--skip-fetch", action="store_true", help="Reuse dumps in work-dir")
    ap.add_argument("--olap-only", action="store_true")
    ap.add_argument("--tpcc-only", action="store_true")
    args = ap.parse_args()

    do_olap = not args.tpcc_only
    do_tpcc = not args.olap_only
    publish = args.publish_dir.resolve()
    work = (args.work_dir or (publish / "_raw")).resolve()
    publish.mkdir(parents=True, exist_ok=True)
    work.mkdir(parents=True, exist_ok=True)

    olap_suites = work / "olap_suites.json"
    olap_runs = work / "olap_test_runs.jsonl"
    tpcc_raw = work / "tpcc.json"

    if not args.skip_fetch:
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

        with YDBWrapper(
            config_path=args.config,
            enable_statistics=False,
            script_name="perfomance_tests_status/run_reports.py",
            silent=False,
            # None: prefer env YDB_QA_CONFIG (CI), else local config file
            use_local_config=None,
        ) as ydb_w:
            if not ydb_w.check_credentials():
                raise SystemExit("YDB credentials check failed")

            if do_olap:
                sql = load_sql(
                    OLAP / "queries" / "fetch_olap_suites.sql", since_iso(OLAP_SUITE_DAYS)
                )
                write_json_list(
                    olap_suites, fetch_rows(ydb_w, sql, "fetch_olap_suites")
                )
                sql = load_sql(
                    OLAP / "queries" / "fetch_olap_test_runs.sql", since_iso(OLAP_RUNS_DAYS)
                )
                write_jsonl(olap_runs, fetch_rows(ydb_w, sql, "fetch_olap_test_runs"))

            if do_tpcc:
                sql = load_sql(TPCC / "queries" / "fetch_tpcc.sql", since_iso(TPCC_DAYS))
                write_json_list(tpcc_raw, fetch_rows(ydb_w, sql, "fetch_tpcc"))

    if do_olap:
        if not olap_suites.is_file():
            raise SystemExit(f"missing {olap_suites}")
        out_html = publish / "olap-report.html"
        cmd = [
            sys.executable,
            str(OLAP / "generate.py"),
            "--input",
            str(olap_suites),
            "--output",
            str(out_html),
        ]
        if olap_runs.is_file():
            cmd.extend(["--tests-daily-input", str(olap_runs)])
        run_generate(cmd, OLAP)
        print(f"OLAP report → {out_html}", flush=True)

    if do_tpcc:
        if not tpcc_raw.is_file():
            raise SystemExit(f"missing {tpcc_raw}")
        out_html = publish / "tpcc-report.html"
        run_generate(
            [
                sys.executable,
                str(TPCC / "generate.py"),
                "--input",
                str(tpcc_raw),
                "--output",
                str(out_html),
            ],
            TPCC,
        )
        print(f"TPC-C report → {out_html}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
