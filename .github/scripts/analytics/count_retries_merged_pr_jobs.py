#!/usr/bin/env python3
"""
Count how many of our merged-PR last jobs were retries (run_attempt > 1) via GitHub API.

By default runs a YDB query to get (pr_number, job_id) for merged PRs (one last job per PR).
Alternatively: --csv FILE or --job-ids ID [ID ...].

Uses same retry logic as alert_queued_jobs.is_retry_job: run_attempt > 1.
Requires: GITHUB_TOKEN env (read repo actions); for YDB default: config in .github/config/ydb_qa_config.json.
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import requests

# Allow importing ydb_wrapper when run from repo root
_script_dir = os.path.dirname(os.path.abspath(__file__))
if _script_dir not in sys.path:
    sys.path.insert(0, _script_dir)

from ydb_wrapper import YDBWrapper

GITHUB_API_BASE = "https://api.github.com/repos/ydb-platform/ydb/actions/runs"
GITHUB_API_TIMEOUT_SEC = 30
DEFAULT_REPO = "ydb-platform/ydb"

# YDB: merged PR, one last job per (pr_number, branch) by max(last_run_timestamp)
MERGED_PR_LAST_JOBS_QUERY = """
SELECT
    pr_number AS pr_number,
    branch AS branch,
    MAX_BY(job_id, last_run_timestamp) AS job_id
FROM
    `test_results/analytics/pr_blocked_by_failed_tests_rich_with_pr_and_mute`
WHERE
    pr_merged = 1
    AND last_run_timestamp > CurrentUtcDate() - 15 * Interval("P1D")
    AND job_id IS NOT NULL
    AND branch IS NOT NULL
    AND pr_number IS NOT NULL
GROUP BY
    pr_number,
    branch
"""


def is_retry_job(run: Dict[str, Any]) -> bool:
    """True if this run is a retry (Re-run in GitHub Actions). Same as alert_queued_jobs.is_retry_job."""
    return run.get("run_attempt", 1) > 1


def fetch_run(run_id: str, token: str) -> Tuple[Dict[str, Any] | None, str]:
    """GET a single workflow run. Returns (run_dict, error)."""
    url = f"{GITHUB_API_BASE}/{run_id}"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    try:
        r = requests.get(url, headers=headers, timeout=GITHUB_API_TIMEOUT_SEC)
        if r.status_code == 200:
            # GitHub sometimes returns HTML error page instead of JSON (e.g. "We had issues producing...")
            ct = (r.headers.get("Content-Type") or "").lower()
            text = (r.text or "").strip()
            if "text/html" in ct or text.lower().startswith("<!doctype") or "we had issues producing" in text.lower():
                return None, "GitHub returned HTML (server error), try again later"
            try:
                data = r.json()
            except ValueError:
                return None, "GitHub returned non-JSON (server error), try again later"
            if not isinstance(data, dict) or "id" not in data:
                return None, "GitHub returned invalid run object, try again later"
            return data, ""
        if r.status_code == 404:
            return None, "run not found"
        if r.status_code >= 500:
            try:
                msg = r.json().get("message", r.text[:200])
            except Exception:
                msg = r.text[:200] if r.text else "server error"
            return None, f"HTTP {r.status_code}: {msg} (try again later)"
        try:
            msg = r.json().get("message", r.text)
        except Exception:
            msg = r.text
        return None, f"HTTP {r.status_code}: {msg}"
    except requests.exceptions.Timeout:
        return None, "timeout (try again later)"
    except requests.exceptions.RequestException as e:
        return None, str(e)


def load_jobs_from_csv(path: str) -> List[Tuple[str, str, str]]:
    """Load (pr_number, branch, job_id) from CSV. Expects pr_number, job_id; optional branch."""
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "job_id" not in reader.fieldnames:
            raise SystemExit(f"CSV must have columns pr_number and job_id. Got: {reader.fieldnames}")
        for row in reader:
            pr = (row.get("pr_number") or "").strip()
            job = (row.get("job_id") or "").strip()
            branch = (row.get("branch") or "").strip()
            if pr and job:
                rows.append((pr, branch, job))
    return rows


def _cell_str(val: Any) -> str:
    """Convert YDB cell (int, bytes, str) to plain string for display."""
    if val is None:
        return ""
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    return str(val)


def load_jobs_from_ydb(lookback_days: int = 15) -> List[Tuple[str, str, str]]:
    """Load (pr_number, branch, job_id) from YDB: merged PRs, one last job per (pr_number, branch)."""
    query = MERGED_PR_LAST_JOBS_QUERY
    if lookback_days != 15:
        query = query.replace("15 * Interval", f"{lookback_days} * Interval")
    with YDBWrapper(silent=True) as w:
        if not w.check_credentials():
            raise RuntimeError("YDB credentials check failed (config or env)")
        results = w.execute_scan_query(query, query_name="merged_pr_last_jobs")
    return [
        (_cell_str(r["pr_number"]), _cell_str(r["branch"]), _cell_str(r["job_id"]))
        for r in results
    ]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Count retries among merged-PR last jobs using GitHub API (run_attempt > 1)."
    )
    parser.add_argument(
        "--csv",
        metavar="FILE",
        help="CSV with columns pr_number, job_id (e.g. export from DataLens mart)",
    )
    parser.add_argument(
        "--job-ids",
        nargs="+",
        metavar="ID",
        help="Space-separated workflow run IDs (instead of CSV)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Seconds to sleep between API requests (default 0.5)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print each retry with pr_number and run URL",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=15,
        metavar="N",
        help="YDB query: last N days for merged PR (default 15)",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=2,
        metavar="N",
        help="Retry each API request up to N times on transient errors (default 2)",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=5.0,
        metavar="SEC",
        help="Seconds to wait before retry (default 5)",
    )
    parser.add_argument(
        "--output",
        "-o",
        metavar="FILE",
        default=None,
        help="Save table to CSV (default: merged_pr_retries_<timestamp>.csv in cwd)",
    )
    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="Do not save CSV file",
    )
    args = parser.parse_args()

    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        print("Error: GITHUB_TOKEN environment variable is not set", file=sys.stderr)
        return 1

    if args.csv:
        try:
            jobs = load_jobs_from_csv(args.csv)
        except FileNotFoundError:
            print(f"Error: file not found: {args.csv}", file=sys.stderr)
            return 1
        except Exception as e:
            print(f"Error reading CSV: {e}", file=sys.stderr)
            return 1
        if not jobs:
            print("No rows with pr_number and job_id in CSV.")
            return 0
    elif args.job_ids:
        jobs = [("", "", jid) for jid in args.job_ids if jid.strip()]
        if not jobs:
            print("No valid job IDs provided.")
            return 0
    else:
        # Default: run YDB query for merged PR last jobs
        try:
            print("Fetching merged PR last jobs from YDB...")
            jobs = load_jobs_from_ydb(lookback_days=args.lookback_days)
        except Exception as e:
            print(f"Error querying YDB: {e}", file=sys.stderr)
            return 1
        if not jobs:
            print("No merged PR last jobs in YDB for the given lookback.")
            return 0
        print(f"Got {len(jobs)} (pr_number, job_id) from YDB.")

    total = len(jobs)
    table_rows: List[Tuple[str, str, str, int]] = []  # (branch, pr_number, job_url, run_attempt)
    errors = 0
    progress_step = 50

    def is_transient_error(err: str) -> bool:
        e = err.lower()
        return "try again" in e or "timeout" in e or "server error" in e or "connection" in e

    for i, (pr_number, branch, job_id) in enumerate(jobs):
        if args.delay and i > 0:
            time.sleep(args.delay)
        run_data, err = fetch_run(job_id, token)
        retries_left = args.retries
        while err and is_transient_error(err) and retries_left > 0:
            retries_left -= 1
            time.sleep(args.retry_delay)
            run_data, err = fetch_run(job_id, token)
        if err:
            print(f"  job_id={job_id} pr={pr_number}: {err}", file=sys.stderr)
            errors += 1
            continue
        if run_data is None:
            continue
        attempt = run_data.get("run_attempt", 1)
        job_url = f"https://github.com/{DEFAULT_REPO}/actions/runs/{job_id}"
        table_rows.append((branch, pr_number, job_url, attempt))
        # Progress every progress_step jobs
        done = i + 1
        retry_count_so_far = sum(1 for _, _, _, a in table_rows if a > 1)
        if done % progress_step == 0 or done == total:
            print(f"Processed {done}/{total} (retries: {retry_count_so_far}, errors: {errors})")

    retry_count = sum(1 for _, _, _, a in table_rows if a > 1)
    print(f"\nTotal jobs: {total}")
    print(f"Retries (run_attempt > 1): {retry_count}")
    if total > 0:
        pct = 100.0 * retry_count / total
        print(f"Share of retries: {pct:.1f}%")
    if errors:
        print(f"API errors: {errors}", file=sys.stderr)

    # Table: branch, pr, job url, retry count (pipe-separated so columns don't stick)
    if table_rows:
        retries_only = [(b, p, u, a) for b, p, u, a in table_rows if a > 1]
        sep = " | "
        header = f"branch{sep}pr{sep}job url{sep}retry count"
        def row_line(branch: str, pr_number: str, job_url: str, attempt: int) -> str:
            return f"{branch}{sep}{pr_number}{sep}{job_url}{sep}{attempt}"
        if retries_only:
            print("\n--- Retries (run_attempt > 1) ---")
            print(header)
            for branch, pr_number, job_url, attempt in retries_only:
                print(row_line(branch, pr_number, job_url, attempt))
        print("\n--- All jobs ---")
        print(header)
        for branch, pr_number, job_url, attempt in table_rows:
            print(row_line(branch, pr_number, job_url, attempt))

        # Save CSV by default (unless --no-csv)
        if not args.no_csv:
            csv_path = args.output
            if not csv_path:
                ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
                csv_path = f"merged_pr_retries_{ts}.csv"
            try:
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow(["branch", "pr", "job url", "retry count"])
                    for branch, pr_number, job_url, attempt in table_rows:
                        w.writerow([branch, pr_number, job_url, attempt])
                print(f"\nSaved CSV: {csv_path}")
            except OSError as e:
                print(f"\nFailed to save CSV to {csv_path}: {e}", file=sys.stderr)

    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
