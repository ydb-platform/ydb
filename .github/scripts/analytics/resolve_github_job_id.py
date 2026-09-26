#!/usr/bin/env python3
"""Resolve GITHUB_NUMERIC_JOB_ID for this runner (paginated)."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from urllib.error import HTTPError, URLError

_ANALYTICS_ROOT = Path(__file__).resolve().parents[1] / "utils" / "analytics"
if str(_ANALYTICS_ROOT) not in sys.path:
    sys.path.insert(0, str(_ANALYTICS_ROOT))

from github_actions.github_api import github_get

PER_PAGE = 100


def pick_job(jobs: List[Dict[str, Any]], runner_name: str) -> Optional[Dict[str, Any]]:
    if not runner_name:
        return None
    matches = [job for job in jobs if str(job.get("runner_name") or "") == runner_name]
    if not matches:
        return None
    running = [job for job in matches if job.get("status") == "in_progress"]
    pool = running or matches
    return max(pool, key=lambda job: str(job.get("started_at") or ""))


def list_run_jobs(
    repository: str,
    run_id: str,
    token: str,
    *,
    get_json: Optional[Callable[..., Any]] = None,
    per_page: int = PER_PAGE,
) -> List[Dict[str, Any]]:
    fetch = get_json or github_get
    url = f"https://api.github.com/repos/{repository}/actions/runs/{run_id}/jobs"
    jobs: List[Dict[str, Any]] = []
    page = 1
    while True:
        payload = fetch(url, {"per_page": per_page, "page": page}, token=token)
        batch = payload.get("jobs") or []
        jobs.extend(item for item in batch if isinstance(item, dict))
        if len(batch) < per_page:
            break
        page += 1
        time.sleep(0.05)
    return jobs


def resolve_github_job(
    runner_name: Optional[str] = None,
    *,
    repository: Optional[str] = None,
    run_id: Optional[str] = None,
    token: Optional[str] = None,
    get_json: Optional[Callable[..., Any]] = None,
) -> Optional[Dict[str, Any]]:
    runner_name = runner_name if runner_name is not None else (os.environ.get("RUNNER_NAME") or "")
    repository = repository or os.environ.get("GITHUB_REPOSITORY") or ""
    run_id = run_id or os.environ.get("GITHUB_RUN_ID") or ""
    token = token or os.environ.get("GITHUB_TOKEN") or ""
    if not runner_name or not repository or not run_id or not token:
        return None
    try:
        jobs = list_run_jobs(repository, run_id, token, get_json=get_json)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, OSError, RuntimeError):
        return None
    return pick_job(jobs, runner_name)


def main(argv: Optional[List[str]] = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if args:
        print("usage: resolve_github_job_id.py", file=sys.stderr)
        return 2
    runner = os.environ.get("RUNNER_NAME") or ""
    job = resolve_github_job(runner)
    if job is None:
        print(f"failed to resolve GITHUB_NUMERIC_JOB_ID for runner {runner}; metrics will be dropped", file=sys.stderr)
        return 0
    print(f"{job.get('id')}\t{job.get('name') or ''}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
