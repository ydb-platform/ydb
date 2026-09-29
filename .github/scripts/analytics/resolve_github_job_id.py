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

# The jobs API lags behind the runner by a few seconds. Without a retry a single
# miss drops every metric row of this job.
RESOLVE_ATTEMPTS = 5
RESOLVE_BACKOFF_SEC = 3.0


def _job_rank(job: Dict[str, Any]) -> tuple:
    steps = job.get("steps") or []
    step_live = any(isinstance(step, dict) and step.get("status") == "in_progress" for step in steps)
    if step_live:
        tier = 2
    elif job.get("status") == "in_progress":
        tier = 1
    else:
        tier = 0
    return (tier, str(job.get("started_at") or ""))


def _describe(job: Dict[str, Any]) -> str:
    return (
        f"id={job.get('id')} name={job.get('name')!r} status={job.get('status')} "
        f"started_at={job.get('started_at')}"
    )


def pick_job(jobs: List[Dict[str, Any]], runner_name: str) -> Optional[Dict[str, Any]]:
    """The job currently running on this runner.

    Runner names are reused on self-hosted fleets, so a completed job with the
    same runner_name belongs to an earlier job and must not win. Only a running
    job can be the caller.
    """
    if not runner_name:
        return None
    matches = [job for job in jobs if str(job.get("runner_name") or "") == runner_name]
    if not matches:
        return None
    live = [job for job in matches if job.get("status") != "completed"]
    if not live:
        # Every job on this runner has finished, so none of them is the caller.
        # The API has not caught up yet: let the caller retry instead of
        # attributing this job's rows to an earlier one.
        print(
            f"Only completed jobs on runner {runner_name} so far; "
            f"candidates: {'; '.join(_describe(job) for job in matches)}",
            file=sys.stderr,
        )
        return None
    if len(live) > 1:
        print(
            f"Ambiguous runner {runner_name}: {len(live)} unfinished jobs; "
            f"picking the most recent. Candidates: "
            f"{'; '.join(_describe(job) for job in live)}",
            file=sys.stderr,
        )
    return max(live, key=_job_rank)


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
    attempts: int = RESOLVE_ATTEMPTS,
    backoff: float = RESOLVE_BACKOFF_SEC,
    sleep: Optional[Callable[[float], None]] = None,
) -> Optional[Dict[str, Any]]:
    runner_name = runner_name if runner_name is not None else (os.environ.get("RUNNER_NAME") or "")
    repository = repository or os.environ.get("GITHUB_REPOSITORY") or ""
    run_id = run_id or os.environ.get("GITHUB_RUN_ID") or ""
    token = token or os.environ.get("GITHUB_TOKEN") or ""
    if not runner_name or not repository or not run_id or not token:
        return None
    wait = sleep or time.sleep
    delay = backoff
    for attempt in range(1, max(attempts, 1) + 1):
        try:
            jobs = list_run_jobs(repository, run_id, token, get_json=get_json)
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, OSError, RuntimeError) as exc:
            print(f"Attempt {attempt}: jobs listing failed: {exc}", file=sys.stderr)
            jobs = []
        job = pick_job(jobs, runner_name) if jobs else None
        if job is not None:
            return job
        if attempt < max(attempts, 1):
            wait(delay)
            delay = min(delay * 2, 30.0)
    return None


def main(argv: Optional[List[str]] = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if args:
        print("usage: resolve_github_job_id.py", file=sys.stderr)
        return 2
    runner = os.environ.get("RUNNER_NAME") or ""
    job = resolve_github_job(runner)
    if job is None:
        # An annotation, because the whole job's metrics are lost without this id
        # and a stderr line in a long log is not noticed.
        print(
            "::warning title=CI analytics::Could not resolve GITHUB_NUMERIC_JOB_ID for runner "
            f"{runner}; every metric row of this job will be dropped"
        )
        print(
            f"failed to resolve GITHUB_NUMERIC_JOB_ID for runner {runner}; metrics will be dropped",
            file=sys.stderr,
        )
        return 0
    print(f"{job.get('id')}\t{job.get('name') or ''}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
