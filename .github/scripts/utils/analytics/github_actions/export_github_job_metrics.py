#!/usr/bin/env python3
"""Export GitHub Actions job/step timings as generic CI metrics.

Writes into analytics/ci_metrics via ci_metrics.upsert_metrics.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

_ANALYTICS_ROOT = Path(__file__).resolve().parents[1]
if str(_ANALYTICS_ROOT) not in sys.path:
    sys.path.insert(0, str(_ANALYTICS_ROOT))

from collector.flush import has_send_credentials
from collector.schema import _open_ydb_wrapper
from collector.values import _as_uint, duration_ms_between, parse_datetime
from github_actions.ci_metrics import normalize_metric, resolve_table_path, upsert_metrics

BUILD_PRESET_RE = re.compile(
    r"(relwithdebinfo|release-asan|release-tsan|release-msan|release|debug)"
)


def guess_build_preset(job_name: Optional[str]) -> Optional[str]:
    if not job_name:
        return None
    match = BUILD_PRESET_RE.search(job_name)
    return match.group(1) if match else None


def _first_pr_number(run: Dict[str, Any]) -> Optional[int]:
    pulls = run.get("pull_requests") or []
    if not pulls:
        return None
    return _as_uint(pulls[0].get("number"))


def _first_pr_base(run: Dict[str, Any]) -> Optional[str]:
    pulls = run.get("pull_requests") or []
    if not pulls:
        return None
    base = pulls[0].get("base") or {}
    return base.get("ref")


def metrics_from_workflow_run(run: Dict[str, Any], jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    run_id = _as_uint(run.get("id"))
    if run_id is None:
        return []
    run_created = parse_datetime(run.get("created_at") or run.get("run_started_at"))
    event_name = run.get("event")
    workflow = run.get("name")
    commit = run.get("head_sha")
    run_attempt = _as_uint(run.get("run_attempt"))
    if run_attempt is None:
        return []
    html_url = run.get("html_url")
    branch = run.get("head_branch")
    pr_number = None
    if event_name in ("pull_request", "pull_request_target"):
        branch = run.get("base_branch") or _first_pr_base(run) or branch
        pr_number = _first_pr_number(run)

    rows: List[Dict[str, Any]] = []
    for job in jobs:
        job_id = _as_uint(job.get("id"))
        if job_id is None:
            continue
        job_name = job.get("name") or ""
        job_started = parse_datetime(job.get("started_at"))
        job_completed = parse_datetime(job.get("completed_at"))
        conclusion = job.get("conclusion") or job.get("status")
        preset = guess_build_preset(job_name)
        job_created = parse_datetime(job.get("created_at")) or run_created
        queued_ms = duration_ms_between(job_created, job_started)
        parent = f"job-{job_id}"
        common = {
            "run_id": run_id,
            "github_job_id": job_id,
            "kind": "duration",
            "workflow": workflow,
            "job_name": job_name,
            "event_name": event_name,
            "branch": branch,
            "build_preset": preset,
            "pr_number": pr_number,
            "commit": commit,
            "run_attempt": run_attempt,
            "run_url": html_url,
            "exported_at": now,
        }
        if job_started is not None:
            job_labels: Dict[str, Any] = {}
            if queued_ms is not None:
                job_labels["queued_ms"] = queued_ms
            job_labels["parent_span_id"] = parent
            rows.append(
                normalize_metric(
                    {
                        **common,
                        "span_id": parent,
                        "name": "job",
                        "source": "github_job",
                        "started_at": job_started,
                        "finished_at": job_completed,
                        "value": duration_ms_between(job_started, job_completed),
                        "conclusion": conclusion,
                        "labels": job_labels or None,
                    },
                    now=now,
                )
            )
            if queued_ms is not None and job_created is not None:
                rows.append(
                    normalize_metric(
                        {
                            **common,
                            "span_id": f"queue-{job_id}",
                            "name": "queue",
                            "source": "github_job",
                            "started_at": job_created,
                            "value": queued_ms,
                            "conclusion": conclusion,
                            "labels": {"queued_ms": queued_ms, "parent_span_id": parent},
                        },
                        now=now,
                    )
                )
        for step_index, step in enumerate(job.get("steps") or [], start=1):
            step_name = (step.get("name") or "").strip()
            step_started = parse_datetime(step.get("started_at"))
            if not step_name or step_started is None:
                continue
            step_completed = parse_datetime(step.get("completed_at"))
            rows.append(
                normalize_metric(
                    {
                        **common,
                        "span_id": f"step-{job_id}-{step_index}",
                        "name": step_name,
                        "source": "github_step",
                        "started_at": step_started,
                        "finished_at": step_completed,
                        "value": duration_ms_between(step_started, step_completed),
                        "conclusion": step.get("conclusion") or step.get("status"),
                        "labels": {"parent_span_id": parent},
                    },
                    now=now,
                )
            )
    return [row for row in rows if row is not None]


def last_export_at(table_path: Optional[str] = None) -> Optional[datetime]:
    if not has_send_credentials():
        return None
    try:
        with _open_ydb_wrapper() as wrapper:
            if not wrapper.check_credentials():
                return None
            path = table_path or resolve_table_path(wrapper)
            rows = wrapper.execute_scan_query(
                f"""
                SELECT MAX(exported_at) AS last_export
                FROM `{path}`
                WHERE source IN ("github_job", "github_step")
                """
            )
    except Exception as exc:  # noqa: BLE001 — fall back to --hours
        print(f"Warning: export watermark query failed: {exc}")
        return None
    if not rows:
        return None
    value = rows[0].get("last_export") if isinstance(rows[0], dict) else None
    return parse_datetime(value)


def resolve_created_since(hours: int, table_path: Optional[str] = None) -> datetime:
    floor = datetime.now(timezone.utc) - timedelta(hours=hours)
    last = last_export_at(table_path)
    if last is None:
        return floor
    return max(last - timedelta(minutes=15), floor)


def upload_rows(rows: List[Dict[str, Any]], table_path: Optional[str] = None) -> int:
    if not rows:
        return 0
    if not has_send_credentials():
        print("Analytics YDB credentials are missing, skipping")
        return 0
    try:
        with _open_ydb_wrapper() as wrapper:
            if not wrapper.check_credentials():
                print("Analytics YDB credentials are missing, skipping")
                return 0
            path = table_path or resolve_table_path(wrapper)
            uploaded = upsert_metrics(wrapper, rows, table_path=path)
        print(f"Uploaded {uploaded} metric rows to {path}")
        return uploaded
    except Exception as exc:  # noqa: BLE001 — telemetry must not fail the caller
        print(f"Warning: analytics upload failed: {exc}", file=sys.stderr)
        return 0

RETRYABLE_STATUS = frozenset({429, 502, 503, 504})


def github_headers() -> Dict[str, str]:
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN environment variable is required")
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def github_get(url: str, params: Optional[Dict[str, Any]] = None, retries: int = 5, timeout: int = 60) -> Any:
    query = urlencode({key: value for key, value in (params or {}).items() if value is not None})
    full = f"{url}?{query}" if query else url
    backoff = 2.0
    last_error: Optional[BaseException] = None
    for attempt in range(1, retries + 1):
        try:
            request = Request(full, headers=github_headers())
            with urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            last_error = exc
            snippet = ""
            try:
                snippet = exc.read()[:300].decode("utf-8", errors="replace")
            except Exception:  # noqa: BLE001
                snippet = str(exc)
            if exc.code in RETRYABLE_STATUS and attempt < retries:
                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                sleep_for = float(retry_after) if retry_after and str(retry_after).isdigit() else backoff
                print(f"GitHub API {exc.code} for {url}, retry {attempt}/{retries} in {sleep_for:.0f}s")
                time.sleep(sleep_for)
                backoff = min(backoff * 2, 30)
                continue
            raise RuntimeError(f"GitHub API {exc.code} for {url}: {snippet}") from exc
        except (URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
            last_error = exc
            if attempt >= retries:
                break
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
    raise RuntimeError(f"GitHub API request failed for {url}: {last_error}")

DEFAULT_ORG = "ydb-platform"
DEFAULT_REPO = "ydb"
ALL_WORKFLOWS = "all"
DEFAULT_WORKFLOWS = (ALL_WORKFLOWS,)


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
        created_since = resolve_created_since(args.hours, table_path=args.table_path)
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
