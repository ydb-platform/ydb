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

from ci_metrics import metrics_from_workflow_run, upload_rows

DEFAULT_ORG = "ydb-platform"
DEFAULT_REPO = "ydb"
ALL_WORKFLOWS = "all"
DEFAULT_WORKFLOWS = (ALL_WORKFLOWS,)
RETRYABLE_STATUS = frozenset({429, 502, 503, 504})


def split_workflows(raw: Any) -> List[str]:
    """Accept a string, comma-separated string, or list of workflow file names."""
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        items = list(raw)
    else:
        items = [raw]
    result: List[str] = []
    seen = set()
    for item in items:
        for part in str(item).split(","):
            name = part.strip()
            if not name or name in seen:
                continue
            seen.add(name)
            result.append(name)
    return result


def resolve_workflows(explicit: Optional[List[str]] = None) -> List[str]:
    if explicit:
        workflows = split_workflows(explicit)
        if workflows:
            return workflows
    env_value = os.environ.get("CI_METRICS_WORKFLOW")
    workflows = split_workflows(env_value) if env_value else []
    return workflows or list(DEFAULT_WORKFLOWS)


def is_all_workflows(workflows: List[str]) -> bool:
    if not workflows:
        return True
    return len(workflows) == 1 and workflows[0].lower() == ALL_WORKFLOWS


def workflow_file_name(path: str) -> str:
    name = str(path or "").rsplit("/", 1)[-1].strip()
    return name


def workflows_from_github_payload(payload: Any) -> List[str]:
    names: List[str] = []
    seen = set()
    for item in (payload or {}).get("workflows") or []:
        if not isinstance(item, dict) or item.get("state") != "active":
            continue
        name = workflow_file_name(str(item.get("path") or ""))
        if not name.endswith((".yml", ".yaml")) or name in seen:
            continue
        seen.add(name)
        names.append(name)
    names.sort()
    return names


def list_active_workflow_files(org: str, repo: str) -> List[str]:
    names: List[str] = []
    seen = set()
    page = 1
    while page <= 10:
        payload = github_get(
            f"https://api.github.com/repos/{org}/{repo}/actions/workflows",
            params={"per_page": 100, "page": page},
        )
        batch = workflows_from_github_payload(payload)
        for name in batch:
            if name in seen:
                continue
            seen.add(name)
            names.append(name)
        workflows = payload.get("workflows") or []
        if len(workflows) < 100:
            break
        page += 1
        time.sleep(0.2)
    names.sort()
    return names


def expand_workflows(org: str, repo: str, explicit: Optional[List[str]] = None) -> List[str]:
    resolved = resolve_workflows(explicit)
    if is_all_workflows(resolved):
        return list_active_workflow_files(org, repo)
    return resolved


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


def pull_refs_from_commit_pulls(pulls: Any) -> List[Dict[str, Any]]:
    """Keep number + target branch from GET /commits/{sha}/pulls."""
    refs: List[Dict[str, Any]] = []
    if not isinstance(pulls, list):
        return refs
    for pull in pulls:
        if not isinstance(pull, dict):
            continue
        base = pull.get("base") if isinstance(pull.get("base"), dict) else {}
        refs.append({"number": pull.get("number"), "base": {"ref": base.get("ref")}})
    return refs


def pull_requests_have_target(pulls: Any) -> bool:
    if not isinstance(pulls, list):
        return False
    for pull in pulls:
        if not isinstance(pull, dict):
            continue
        base = pull.get("base") if isinstance(pull.get("base"), dict) else {}
        if base.get("ref"):
            return True
    return False


def attach_pull_requests(org: str, repo: str, run: Dict[str, Any]) -> Dict[str, Any]:
    """Recover PR target branch from the head SHA when the run payload omits it."""
    if run.get("event") not in ("pull_request", "pull_request_target"):
        return run
    if pull_requests_have_target(run.get("pull_requests")):
        return run
    sha = run.get("head_sha")
    if not sha:
        return run
    try:
        pulls = github_get(f"https://api.github.com/repos/{org}/{repo}/commits/{quote(str(sha))}/pulls")
    except Exception as exc:  # noqa: BLE001 — keep exporting the run
        print(f"Warning: commit pulls for {sha}: {exc}")
        return run
    refs = pull_refs_from_commit_pulls(pulls)
    if not refs:
        return run
    enriched = dict(run)
    enriched["pull_requests"] = refs
    return enriched


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
        rows.extend(metrics_from_workflow_run(attach_pull_requests(org, repo, run), jobs))
        run_count += 1
        if run_count % 20 == 0:
            print(f"Collected {len(rows)} metric rows from {run_count} runs...")
        time.sleep(0.05)
    print(f"Collected {len(rows)} metric rows from {run_count} workflow runs")
    return rows


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export GitHub job/step timings as CI metrics")
    parser.add_argument("--org", default=os.environ.get("CI_METRICS_ORG", DEFAULT_ORG))
    parser.add_argument("--repo", default=os.environ.get("CI_METRICS_REPO", DEFAULT_REPO))
    parser.add_argument(
        "--workflow",
        action="append",
        default=None,
        help="Workflow file name (repeatable or comma-separated). "
        "Default: all active workflows. Use a file name to export one, e.g. pr_check.yml.",
    )
    parser.add_argument("--hours", type=int, default=24, help="Lookback window in hours (default 24)")
    parser.add_argument("--table-path", default=None)
    return parser.parse_args(argv)


def main(argv=None) -> int:
    try:
        args = parse_args(argv)
        workflows = expand_workflows(args.org, args.repo, args.workflow)
        created_since = datetime.now(timezone.utc) - timedelta(hours=args.hours)
        print(
            f"Exporting {args.org}/{args.repo} workflows={workflows} "
            f"since {created_since.isoformat()}"
        )
        rows: List[Dict[str, Any]] = []
        for workflow in workflows:
            try:
                rows.extend(collect_rows(args.org, args.repo, workflow, created_since))
            except Exception as exc:  # noqa: BLE001 — keep other workflows
                print(f"Warning: failed to export workflow {workflow}: {exc}")
        if not rows:
            print("No GitHub job metric rows to upload")
            return 0
        upload_rows(rows, table_path=args.table_path)
        return 0
    except Exception as exc:  # noqa: BLE001 — collector must not fail the analytics job
        print(f"Warning: GitHub job metrics export failed: {exc}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
