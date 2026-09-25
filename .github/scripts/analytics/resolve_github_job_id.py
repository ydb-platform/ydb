#!/usr/bin/env python3
"""Resolve GITHUB_NUMERIC_JOB_ID from the current workflow run (paginated)."""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any, Callable, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

PER_PAGE = 100


def job_name_matches_preset(name: str, preset: str) -> bool:
    if not name or not preset:
        return False
    return (
        f" {preset} " in f" {name} "
        or name.endswith(f" {preset}")
        or name.split(" ")[-1] == preset
    )


def pick_job(jobs: List[Dict[str, Any]], preset: str) -> Optional[Dict[str, Any]]:
    for job in jobs:
        if job_name_matches_preset(str(job.get("name") or ""), preset):
            return job
    return None


def github_get_json(url: str, token: str, params: Optional[Dict[str, Any]] = None) -> Any:
    query = urlencode({key: value for key, value in (params or {}).items() if value is not None})
    full = f"{url}?{query}" if query else url
    request = Request(
        full,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def list_run_jobs(
    repository: str,
    run_id: str,
    token: str,
    *,
    get_json: Optional[Callable[..., Any]] = None,
    per_page: int = PER_PAGE,
) -> List[Dict[str, Any]]:
    fetch = get_json or github_get_json
    url = f"https://api.github.com/repos/{repository}/actions/runs/{run_id}/jobs"
    jobs: List[Dict[str, Any]] = []
    page = 1
    while True:
        payload = fetch(url, token, {"per_page": per_page, "page": page})
        batch = payload.get("jobs") or []
        jobs.extend(item for item in batch if isinstance(item, dict))
        if len(batch) < per_page:
            break
        page += 1
        time.sleep(0.05)
    return jobs


def resolve_github_job(
    preset: str,
    *,
    repository: Optional[str] = None,
    run_id: Optional[str] = None,
    token: Optional[str] = None,
    get_json: Optional[Callable[..., Any]] = None,
) -> Optional[Dict[str, Any]]:
    repository = repository or os.environ.get("GITHUB_REPOSITORY") or ""
    run_id = run_id or os.environ.get("GITHUB_RUN_ID") or ""
    token = token or os.environ.get("GITHUB_TOKEN") or ""
    if not preset or not repository or not run_id or not token:
        return None
    try:
        jobs = list_run_jobs(repository, run_id, token, get_json=get_json)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, OSError, RuntimeError):
        return None
    return pick_job(jobs, preset)


def main(argv: Optional[List[str]] = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    if len(args) != 1:
        print("usage: resolve_github_job_id.py <build_preset>", file=sys.stderr)
        return 2
    job = resolve_github_job(args[0])
    if job is None:
        print(f"failed to resolve GITHUB_NUMERIC_JOB_ID for preset {args[0]}; metrics will be dropped", file=sys.stderr)
        return 0
    print(f"{job.get('id')}\t{job.get('name') or ''}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
