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
from urllib.parse import quote

_ANALYTICS_ROOT = Path(__file__).resolve().parents[1]
if str(_ANALYTICS_ROOT) not in sys.path:
    sys.path.insert(0, str(_ANALYTICS_ROOT))

from collector.flush import has_send_credentials
from collector.schema import _open_ydb_wrapper
from collector.values import _as_uint, duration_ms_between, parse_datetime
from github_actions.ci_metrics import normalize_metric, resolve_table_path, upsert_metrics
from github_actions.github_api import github_get

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


OVERLAP = timedelta(minutes=30)
MAX_LOOKBACK = timedelta(hours=12)
STATE_SOURCE = "export_state"
STATE_NAME = "open_runs"


def completed_since(hours: int, last_export: Optional[datetime] = None) -> datetime:
    """Short GitHub `created` window.

    Cold start uses `--hours`. After a successful export the window starts at
    that timestamp minus 30 minutes. Runs that were already running are tracked
    separately, so this window does not have to cover the longest job.
    """
    if last_export is None:
        return datetime.now(timezone.utc) - timedelta(hours=hours)
    return last_export - OVERLAP


def last_export_at(table_path: Optional[str] = None) -> Optional[datetime]:
    if not has_send_credentials():
        return None
    floor = datetime.now(timezone.utc) - MAX_LOOKBACK
    ts = floor.strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        with _open_ydb_wrapper() as wrapper:
            if not wrapper.check_credentials():
                return None
            path = table_path or resolve_table_path(wrapper)
            rows = wrapper.execute_scan_query(
                f"""
                SELECT MAX(exported_at) AS last_export
                FROM `{path}`
                WHERE event_ts >= Timestamp("{ts}")
                  AND source IN ("github_job", "github_step")
                """
            )
    except Exception as exc:  # noqa: BLE001 — cold-start window
        print(f"Warning: export watermark query failed: {exc}")
        return None
    if not rows or not isinstance(rows[0], dict):
        return None
    try:
        return parse_datetime(rows[0].get("last_export"))
    except (OverflowError, OSError, ValueError) as exc:
        print(f"Warning: export watermark is not a timestamp: {exc}")
        return None


def already_exported(run_id: Any, run_attempt: Any, exported: set) -> bool:
    try:
        return (int(run_id), int(run_attempt)) in exported
    except (TypeError, ValueError):
        return False


def exported_run_ids(since: datetime, table_path: Optional[str] = None) -> set:
    """(run_id, run_attempt) pairs that already have a github_job row in this window.

    The GitHub `created` filter stays at `since`. A failed query exports the
    whole window again; upsert is idempotent.
    """
    if not has_send_credentials():
        return set()
    ts = since.strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        with _open_ydb_wrapper() as wrapper:
            if not wrapper.check_credentials():
                return set()
            path = table_path or resolve_table_path(wrapper)
            rows = wrapper.execute_scan_query(
                f"""
                SELECT run_id, run_attempt
                FROM `{path}`
                WHERE event_ts >= Timestamp("{ts}")
                  AND source = "github_job"
                  AND name = "job"
                """
            )
    except Exception as exc:  # noqa: BLE001 — re-export the window
        print(f"Warning: export watermark query failed: {exc}")
        return set()
    ids = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        try:
            ids.add((int(row["run_id"]), int(row["run_attempt"])))
        except (KeyError, TypeError, ValueError):
            continue
    return ids


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

DEFAULT_ORG = "ydb-platform"
DEFAULT_REPO = "ydb"


def selected_workflows(explicit: Optional[List[str]] = None) -> List[str]:
    raw: List[Any] = list(explicit or [])
    if not raw:
        env_value = os.environ.get("CI_METRICS_WORKFLOW")
        raw = [env_value] if env_value else ["all"]
    names: List[str] = []
    for item in raw:
        for part in str(item).split(","):
            name = part.strip()
            if not name:
                continue
            if name.lower() == "all":
                return ["all"]
            if name not in names:
                names.append(name)
    return names or ["all"]


def active_workflow_files(org: str, repo: str) -> List[str]:
    names: List[str] = []
    seen = set()
    page = 1
    while page <= 10:
        payload = github_get(
            f"https://api.github.com/repos/{org}/{repo}/actions/workflows",
            params={"per_page": 100, "page": page},
        )
        batch = payload.get("workflows") or []
        for item in batch:
            if not isinstance(item, dict) or item.get("state") != "active":
                continue
            name = str(item.get("path") or "").rsplit("/", 1)[-1].strip()
            if name.endswith((".yml", ".yaml")) and name not in seen:
                seen.add(name)
                names.append(name)
        if len(batch) < 100:
            break
        page += 1
        time.sleep(0.2)
    names.sort()
    return names


def workflows_to_export(org: str, repo: str, explicit: Optional[List[str]] = None) -> List[str]:
    names = selected_workflows(explicit)
    if names == ["all"]:
        return active_workflow_files(org, repo)
    return names


def iter_workflow_runs(
    org: str,
    repo: str,
    workflow: str,
    created_since: datetime,
    per_page: int = 50,
    status: str = "completed",
) -> Iterable[Dict[str, Any]]:
    created = f">={created_since.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    url = f"https://api.github.com/repos/{org}/{repo}/actions/workflows/{quote(workflow)}/runs"
    page = 1
    while True:
        payload = github_get(
            url,
            params={
                "status": status,
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


def collect_rows(
    org: str,
    repo: str,
    workflow: str,
    created_since: datetime,
    exported: Optional[set] = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    run_count = 0
    exported = exported or set()
    for run in iter_workflow_runs(org, repo, workflow, created_since):
        run_id = run.get("id")
        if run_id is None or already_exported(run_id, run.get("run_attempt"), exported):
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


def run_ref(run: Dict[str, Any]) -> Optional[tuple]:
    try:
        return int(run["id"]), int(run.get("run_attempt") or 1)
    except (KeyError, TypeError, ValueError):
        return None


def load_open_runs(table_path: Optional[str] = None) -> List[tuple]:
    if not has_send_credentials():
        return []
    floor = (datetime.now(timezone.utc) - MAX_LOOKBACK).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        with _open_ydb_wrapper() as wrapper:
            if not wrapper.check_credentials():
                return []
            path = table_path or resolve_table_path(wrapper)
            rows = wrapper.execute_scan_query(
                f"""
                SELECT event_ts, labels
                FROM `{path}`
                WHERE event_ts >= Timestamp("{floor}")
                  AND source = "{STATE_SOURCE}"
                  AND name = "{STATE_NAME}"
                """
            )
    except Exception as exc:  # noqa: BLE001
        print(f"Warning: open-run watermark read failed: {exc}")
        return []
    latest = None
    latest_ts = None
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        ts = parse_datetime(row.get("event_ts"))
        if latest_ts is None or (ts is not None and ts >= latest_ts):
            latest_ts = ts
            latest = row.get("labels")
    if isinstance(latest, str):
        try:
            latest = json.loads(latest)
        except json.JSONDecodeError:
            return []
    runs = latest.get("runs") if isinstance(latest, dict) else None
    refs = []
    for item in runs or []:
        if isinstance(item, dict):
            ref = run_ref({"id": item.get("id"), "run_attempt": item.get("attempt")})
            if ref:
                refs.append(ref)
    return refs


def save_open_runs(refs: List[tuple], table_path: Optional[str] = None) -> None:
    now = datetime.now(timezone.utc)
    row = {
        "date": now.date(),
        "event_ts": now,
        "run_id": 0,
        "github_job_id": 0,
        "run_attempt": 0,
        "source": STATE_SOURCE,
        "name": STATE_NAME,
        "kind": "event",
        "span_id": f"export-state-{int(now.timestamp())}",
        "labels": json.dumps(
            {"runs": [{"id": run_id, "attempt": attempt} for run_id, attempt in refs]},
            separators=(",", ":"),
        ),
        "exported_at": now,
    }
    upload_rows([row], table_path=table_path)


def fetch_run(org: str, repo: str, run_id: int) -> Dict[str, Any]:
    return github_get(f"https://api.github.com/repos/{org}/{repo}/actions/runs/{run_id}")


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export GitHub job/step timings as CI metrics")
    parser.add_argument("--org", default=os.environ.get("CI_METRICS_ORG", DEFAULT_ORG))
    parser.add_argument("--repo", default=os.environ.get("CI_METRICS_REPO", DEFAULT_REPO))
    parser.add_argument(
        "--workflow",
        action="append",
        default=None,
        help="Workflow file (repeatable or comma-separated). Default: all active workflows.",
    )
    parser.add_argument("--hours", type=int, default=2, help="Cold-start lookback in hours (default 2)")
    parser.add_argument("--table-path", default=None)
    return parser.parse_args(argv)


def remember_ref(refs: List[tuple], ref: Optional[tuple]) -> None:
    if ref and ref not in refs:
        refs.append(ref)


def open_runs_to_save(
    listed: List[tuple],
    held: List[tuple],
    previous: List[tuple],
    list_failed: bool,
    exported: set,
) -> List[tuple]:
    pending: List[tuple] = []
    for ref in list(listed) + list(held):
        remember_ref(pending, ref)
    if not list_failed:
        return pending
    for run_id, attempt in previous:
        if already_exported(run_id, attempt, exported):
            continue
        remember_ref(pending, (run_id, attempt))
    return pending


def main(argv=None) -> int:
    try:
        args = parse_args(argv)
        workflows = workflows_to_export(args.org, args.repo, args.workflow)
        since = completed_since(args.hours, last_export_at(args.table_path))
        exported = exported_run_ids(since, table_path=args.table_path)
        print(
            f"Exporting {args.org}/{args.repo} workflows={workflows} "
            f"since {since.isoformat()} skip={len(exported)}"
        )
        rows: List[Dict[str, Any]] = []
        still_open: List[tuple] = []
        held: List[tuple] = []
        list_failed = False
        open_since = datetime.now(timezone.utc) - MAX_LOOKBACK
        for workflow in workflows:
            try:
                rows.extend(collect_rows(args.org, args.repo, workflow, since, exported))
            except Exception as exc:  # noqa: BLE001 — keep other workflows
                print(f"Warning: failed to export workflow {workflow}: {exc}")
            for status in ("in_progress", "queued"):
                try:
                    for run in iter_workflow_runs(args.org, args.repo, workflow, open_since, status=status):
                        remember_ref(still_open, run_ref(run))
                except Exception as exc:  # noqa: BLE001
                    list_failed = True
                    print(f"Warning: failed to list {status} runs for {workflow}: {exc}")
        previous = load_open_runs(args.table_path)
        for run_id, attempt in previous:
            ref = (run_id, attempt)
            if ref in still_open or already_exported(run_id, attempt, exported):
                continue
            try:
                run = fetch_run(args.org, args.repo, run_id)
            except Exception as exc:  # noqa: BLE001
                print(f"Warning: failed to refresh run {run_id}: {exc}")
                remember_ref(held, ref)
                continue
            if run.get("status") != "completed":
                remember_ref(still_open, run_ref(run) or ref)
                continue
            try:
                jobs = list_run_jobs(args.org, args.repo, run_id)
                rows.extend(metrics_from_workflow_run(attach_pull_requests(args.org, args.repo, run), jobs))
            except Exception as exc:  # noqa: BLE001
                print(f"Warning: failed to export finished run {run_id}: {exc}")
                remember_ref(held, ref)
        pending = open_runs_to_save(still_open, held, previous, list_failed, exported)
        if rows:
            uploaded = upload_rows(rows, table_path=args.table_path)
            if uploaded <= 0:
                print("Warning: metrics were not uploaded, keeping the previous open-run list")
                return 0
        else:
            print("No GitHub job metric rows to upload")
        save_open_runs(pending, args.table_path)
        return 0
    except Exception as exc:  # noqa: BLE001 — collector must not fail the analytics job
        print(f"Warning: GitHub job metrics export failed: {exc}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
