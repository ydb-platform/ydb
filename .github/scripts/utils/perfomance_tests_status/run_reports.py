#!/usr/bin/env python3
"""Fetch OLAP + TPC-C data from YDB and generate Now HTML reports (no LLM).

Auth:
  export CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS=/path/to/sa-key.json
  # or: eval "$(python3 duty_agent/dutyctl.py init-token --shell)"

Example:
  python3 run_reports.py --publish-dir /tmp/perf_reports
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.report_config import cfg_int, load_report_config  # noqa: E402
from common.ydb_client import (  # noqa: E402
    DEFAULT_CONFIG,
    YdbClientError,
    ensure_sa_credentials,
    open_wrapper,
    row_to_obj,
)

OLAP = ROOT / "olap"
TPCC = ROOT / "tpcc"

_OLAP_CFG = load_report_config(OLAP)
_TPCC_CFG = load_report_config(TPCC)
OLAP_SUITE_DAYS = cfg_int(_OLAP_CFG, "suite_window_days", cfg_int(_OLAP_CFG, "window_days", 30))
OLAP_RUNS_DAYS = cfg_int(_OLAP_CFG, "runs_window_days", cfg_int(_OLAP_CFG, "window_days", 30))
TPCC_DAYS = cfg_int(_TPCC_CFG, "window_days", 60)


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
        default=None,
        help="Service account JSON key",
    )
    ap.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG),
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
    tpcc_reports = work / "tpcc_reports.json"
    status: dict = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "olap": {"ok": False, "error": None, "path": None},
        "tpcc": {"ok": False, "error": None, "path": None},
    }

    if not args.skip_fetch:
        try:
            ensure_sa_credentials(args.sa_key_file)
        except YdbClientError as e:
            raise SystemExit(str(e)) from e

        try:
            with open_wrapper(
                script_name="perfomance_tests_status/run_reports.py",
                config=args.config,
                # None: prefer env YDB_QA_CONFIG (CI), else local config file
                use_local_config=None,
            ) as ydb_w:
                if do_olap:
                    try:
                        sql = load_sql(
                            OLAP / "queries" / "fetch_olap_suites.sql",
                            since_iso(OLAP_SUITE_DAYS),
                        )
                        write_json_list(
                            olap_suites, fetch_rows(ydb_w, sql, "fetch_olap_suites")
                        )
                        sql = load_sql(
                            OLAP / "queries" / "fetch_olap_test_runs.sql",
                            since_iso(OLAP_RUNS_DAYS),
                        )
                        write_jsonl(
                            olap_runs, fetch_rows(ydb_w, sql, "fetch_olap_test_runs")
                        )
                    except Exception as e:
                        status["olap"]["error"] = f"fetch: {e}"
                        print(f"OLAP fetch FAILED: {e}", flush=True)
                        traceback.print_exc()

                if do_tpcc:
                    try:
                        sql = load_sql(
                            TPCC / "queries" / "fetch_tpcc.sql", since_iso(TPCC_DAYS)
                        )
                        write_json_list(tpcc_raw, fetch_rows(ydb_w, sql, "fetch_tpcc"))
                        # Allure URLs live in tests_results (mart has none).
                        # Soft-fail: metrics report still publishes without reports.
                        try:
                            sql = load_sql(
                                TPCC / "queries" / "fetch_tpcc_reports.sql",
                                since_iso(TPCC_DAYS),
                            )
                            write_json_list(
                                tpcc_reports,
                                fetch_rows(ydb_w, sql, "fetch_tpcc_reports"),
                            )
                        except Exception as e:
                            print(f"TPC-C reports fetch FAILED (continue): {e}", flush=True)
                            traceback.print_exc()
                    except Exception as e:
                        status["tpcc"]["error"] = f"fetch: {e}"
                        print(f"TPC-C fetch FAILED: {e}", flush=True)
                        traceback.print_exc()
        except YdbClientError as e:
            raise SystemExit(str(e)) from e

    if do_olap and not status["olap"]["error"]:
        try:
            if not olap_suites.is_file():
                raise RuntimeError(f"missing {olap_suites}")
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
            status["olap"]["ok"] = True
            status["olap"]["path"] = str(out_html)
            print(f"OLAP report → {out_html}", flush=True)
        except Exception as e:
            status["olap"]["error"] = f"generate: {e}"
            print(f"OLAP generate FAILED: {e}", flush=True)
            traceback.print_exc()

    if do_tpcc and not status["tpcc"]["error"]:
        try:
            if not tpcc_raw.is_file():
                raise RuntimeError(f"missing {tpcc_raw}")
            out_html = publish / "tpcc-report.html"
            cmd = [
                sys.executable,
                str(TPCC / "generate.py"),
                "--input",
                str(tpcc_raw),
                "--output",
                str(out_html),
            ]
            if tpcc_reports.is_file():
                cmd.extend(["--reports-input", str(tpcc_reports)])
            run_generate(cmd, TPCC)
            status["tpcc"]["ok"] = True
            status["tpcc"]["path"] = str(out_html)
            print(f"TPC-C report → {out_html}", flush=True)
        except Exception as e:
            status["tpcc"]["error"] = f"generate: {e}"
            print(f"TPC-C generate FAILED: {e}", flush=True)
            traceback.print_exc()

    status_path = publish / "status.json"
    status_path.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n")
    print(f"wrote {status_path}", flush=True)

    any_ok = (not do_olap or status["olap"]["ok"]) and (
        not do_tpcc or status["tpcc"]["ok"]
    )
    # Partial success still exits 0 so CI can upload what worked; fail only if nothing.
    if do_olap and do_tpcc:
        if status["olap"]["ok"] or status["tpcc"]["ok"]:
            if not (status["olap"]["ok"] and status["tpcc"]["ok"]):
                print("PARTIAL: one report failed — uploading successes", flush=True)
            return 0
        return 1
    return 0 if any_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
