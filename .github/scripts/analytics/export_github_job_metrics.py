#!/usr/bin/env python3
"""Export GitHub Actions job/step timings as generic CI metrics.

Writes into analytics/ci_metrics via ci_metrics.upsert_metrics.
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote

import requests

from ci_metrics import metrics_from_workflow_run, resolve_table_path, upsert_metrics
from ydb_wrapper import YDBWrapper

DEFAULT_ORG = "ydb-platform"
DEFAULT_REPO = "ydb"
DEFAULT_WORKFLOW = "pr_check.yml"
RETRYABLE_STATUS = frozenset({429, 502, 503, 504})


def _github_headers() -> Dict[str, str]:
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN environment variable is required")
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def github_get(url: str, params: Optional[Dict[str, Any]] = None, retries: int = 5) -> Any:
    backoff = 2.0
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=_github_headers(), params=params, timeout=60)
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        if response.status_code == 200:
            return response.json()
        if response.status_code in RETRYABLE_STATUS and attempt < retries:
            retry_after = response.headers.get("Retry-After")
            sleep_for = float(retry_after) if retry_after and retry_after.isdigit() else backoff
            print(f"GitHub API {response.status_code} for {url}, retry {attempt}/{retries} in {sleep_for:.0f}s")
            time.sleep(sleep_for)
            backoff = min(backoff * 2, 30)
            continue
        raise RuntimeError(f"GitHub API {response.status_code} for {url}: {response.text[:300]}")
    raise RuntimeError(f"GitHub API request failed for {url}: {last_error}")


def iter_workflow_runs(
    org: str,
    repo: str,
    workflow: str,
    created_since: datetime,
    per_page: int = 50,
    max_pages: int = 20,
) -> Iterable[Dict[str, Any]]:
    created = f">={created_since.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    url = f"https://api.github.com/repos/{org}/{repo}/actions/workflows/{quote(workflow)}/runs"
    page = 1
    while page <= max_pages:
        payload = github_get(
            url,
            params={
                "status": "completed",
                "created": created,
                "per_page": per_page,
                "page": page,
            },
        )
        runs = payload.get("workflow_runs") or []
        if not runs:
            break
        for run in runs:
            yield run
        if len(runs) < per_page:
            break
        page += 1
        time.sleep(0.2)


def list_run_jobs(org: str, repo: str, run_id: int, per_page: int = 100) -> List[Dict[str, Any]]:
    url = f"https://api.github.com/repos/{org}/{repo}/actions/runs/{run_id}/jobs"
    jobs: List[Dict[str, Any]] = []
    page = 1
    while True:
        payload = github_get(url, params={"per_page": per_page, "page": page})
        batch = payload.get("jobs") or []
        jobs.extend(batch)
        if len(batch) < per_page:
            break
        page += 1
        time.sleep(0.05)
    return jobs


def collect_rows(org: str, repo: str, workflow: str, created_since: datetime) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    run_count = 0
    for run in iter_workflow_runs(org, repo, workflow, created_since):
        run_id = run.get("id")
        if run_id is None:
            continue
        try:
            jobs = list_run_jobs(org, repo, int(run_id))
        except Exception as exc:  # noqa: BLE001 — keep exporting other runs
            print(f"Warning: failed to list jobs for run {run_id}: {exc}")
            continue
        rows.extend(metrics_from_workflow_run(run, jobs))
        run_count += 1
        if run_count % 20 == 0:
            print(f"Collected {len(rows)} metric rows from {run_count} runs...")
        time.sleep(0.05)
    print(f"Collected {len(rows)} metric rows from {run_count} workflow runs")
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export GitHub job/step timings as CI metrics")
    parser.add_argument("--org", default=os.environ.get("CI_METRICS_ORG", DEFAULT_ORG))
    parser.add_argument("--repo", default=os.environ.get("CI_METRICS_REPO", DEFAULT_REPO))
    parser.add_argument("--workflow", default=os.environ.get("CI_METRICS_WORKFLOW", DEFAULT_WORKFLOW))
    parser.add_argument("--hours", type=int, default=24, help="Lookback window in hours (default 24)")
    parser.add_argument("--table-path", default=None)
    return parser.parse_args()


def main() -> int:
    try:
        args = parse_args()
        created_since = datetime.now(timezone.utc) - timedelta(hours=args.hours)
        print(
            f"Exporting {args.org}/{args.repo} workflow={args.workflow} "
            f"since {created_since.isoformat()}"
        )
        rows = collect_rows(args.org, args.repo, args.workflow, created_since)
        if not rows:
            print("No GitHub job metric rows to upload")
            return 0

        with YDBWrapper() as wrapper:
            if not wrapper.check_credentials():
                print("Env variable CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS is missing, skipping")
                return 0
            table_path = args.table_path or resolve_table_path(wrapper)
            uploaded = upsert_metrics(wrapper, rows, table_path=table_path)
            print(f"Uploaded {uploaded} metric rows to {table_path}")
        return 0
    except Exception as exc:  # noqa: BLE001 — collector must not fail the analytics job
        print(f"Warning: GitHub job metrics export failed: {exc}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
