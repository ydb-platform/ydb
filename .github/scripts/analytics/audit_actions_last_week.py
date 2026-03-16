#!/usr/bin/env python3
"""
Audit GitHub Actions usage over recent runs.

Sampling strategy:
- Parse configured actions from local workflow files.
- For each workflow discovered via API, sample up to N runs in time window.
- Parse job logs from sampled runs and map action usage by runner/workflow.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import os
import pathlib
import re
import sys
import time
import urllib.parse
import urllib.request
import urllib.error
import zipfile
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Tuple


API_BASE = "https://api.github.com"
ACTION_DOWNLOAD_RE = re.compile(r"Download action repository '([^']+)'")
ACTION_REF_RE = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[A-Za-z0-9_.:/-]+")
RUNNER_VERSION_RE = re.compile(r"Current runner version:\s*'([^']+)'")
USES_RE = re.compile(r"^\s*uses:\s*([^\s#]+)")


def build_headers(token: str) -> Dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"token {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "runner-action-audit-script",
    }


def parse_link_next(link_header: str) -> Optional[str]:
    if not link_header:
        return None
    for part in link_header.split(","):
        if 'rel="next"' in part:
            m = re.search(r"<([^>]+)>", part)
            if m:
                return m.group(1)
    return None


def http_get_json(url: str, headers: Dict[str, str]) -> Tuple[dict, Optional[str]]:
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=60) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
        return payload, parse_link_next(resp.headers.get("Link"))


def http_get_bytes(url: str, headers: Dict[str, str]) -> bytes:
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.read()


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def http_get_with_optional_redirect_location(
    url: str, headers: Dict[str, str]
) -> Tuple[int, bytes, Optional[str]]:
    opener = urllib.request.build_opener(_NoRedirect())
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with opener.open(req, timeout=120) as resp:
            return int(resp.status), resp.read(), resp.headers.get("Location")
    except urllib.error.HTTPError as e:
        # For 302, urllib raises HTTPError with headers available.
        if int(e.code) in (301, 302, 303, 307, 308):
            return int(e.code), b"", e.headers.get("Location")
        raise


def paginated_get(url: str, headers: Dict[str, str], item_key: Optional[str] = None) -> Iterable[dict]:
    next_url = url
    while next_url:
        payload, next_url = http_get_json(next_url, headers)
        items = payload if item_key is None else payload.get(item_key, [])
        for item in items:
            yield item


def parse_action_ref(action_ref: str) -> Tuple[str, str]:
    if "@" not in action_ref:
        return action_ref, ""
    action, version = action_ref.rsplit("@", 1)
    return action, version


def normalize_action_version(version: str) -> str:
    return version.strip().rstrip(".,;:)]")


def parse_major(version: str) -> Optional[int]:
    m = re.match(r"v(\d+)", version)
    return int(m.group(1)) if m else None


def parse_runner_version(version: str) -> Optional[Tuple[int, int, int]]:
    m = re.search(r"(\d+)\.(\d+)\.(\d+)", version or "")
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def is_runner_version_below(current: str, required: str) -> Optional[bool]:
    cur = parse_runner_version(current)
    req = parse_runner_version(required)
    if cur is None or req is None:
        return None
    return cur < req


def parse_job_log(log_text: str) -> Tuple[set[str], set[str], Optional[str]]:
    lines = log_text.splitlines()
    downloaded = set(ACTION_DOWNLOAD_RE.findall(log_text))
    warned = set()
    for i, line in enumerate(lines):
        if "Node.js 20 actions are deprecated" in line:
            warned.update(ACTION_REF_RE.findall(" ".join(lines[i : i + 6])))
    runner_match = RUNNER_VERSION_RE.search(log_text)
    return downloaded, warned, runner_match.group(1) if runner_match else None


def collect_actions_from_workflows(
    workflows_dir: str,
) -> Tuple[set[str], set[str], int, Dict[str, set[str]], Dict[str, set[str]]]:
    refs: set[str] = set()
    names: set[str] = set()
    file_to_actions: Dict[str, set[str]] = defaultdict(set)
    file_to_local_uses: Dict[str, set[str]] = defaultdict(set)
    files_count = 0

    root = pathlib.Path(workflows_dir)
    cwd = pathlib.Path.cwd().resolve()
    for path in sorted(list(root.rglob("*.yml")) + list(root.rglob("*.yaml"))):
        if not path.is_file():
            continue
        files_count += 1
        rel_path = str(path.resolve().relative_to(cwd))
        with path.open("r", encoding="utf-8", errors="replace") as fp:
            for line in fp:
                m = USES_RE.match(line)
                if not m:
                    continue
                ref = m.group(1).strip()
                if ref.startswith("./"):
                    file_to_local_uses[rel_path].add(ref)
                    continue
                if not ACTION_REF_RE.fullmatch(ref):
                    continue
                refs.add(ref)
                action, _ = parse_action_ref(ref)
                names.add(action)
                file_to_actions[rel_path].add(action)
    return refs, names, files_count, file_to_actions, file_to_local_uses


def detect_workflow_trigger_type(workflow_rel_path: str) -> str:
    p = pathlib.Path(workflow_rel_path)
    if not p.exists() or not p.is_file():
        return "unknown"
    text = p.read_text(encoding="utf-8", errors="replace")
    if re.search(r"^\s*pull_request_target\s*:", text, flags=re.MULTILINE):
        return "pull_request_target"
    if re.search(r"^\s*pull_request\s*:", text, flags=re.MULTILINE):
        return "pull_request"
    if re.search(r"^\s*workflow_dispatch\s*:", text, flags=re.MULTILINE):
        return "workflow_dispatch"
    if re.search(r"^\s*push\s*:", text, flags=re.MULTILINE):
        return "push"
    return "other"


def workflow_branch_hint(trigger_type: str) -> str:
    if trigger_type == "pull_request_target":
        return "update base branch workflow file (not PR head branch)"
    if trigger_type == "pull_request":
        return "update workflow in PR/head branch"
    if trigger_type in ("push", "workflow_dispatch"):
        return "update workflow in branch where run was executed"
    if trigger_type == "unknown":
        return "unknown trigger type; verify manually"
    return "verify trigger semantics for this workflow"


def fetch_workflows(repo: str, headers: Dict[str, str]) -> List[dict]:
    url = f"{API_BASE}/repos/{repo}/actions/workflows?per_page=100"
    return list(paginated_get(url, headers, item_key="workflows"))


def fetch_runs_for_workflow(
    repo: str,
    workflow_id: int,
    since_iso: str,
    max_runs_per_branch: int,
    max_branches_per_workflow: int,
    headers: Dict[str, str],
) -> Tuple[List[dict], int, Dict[str, int], Dict[str, int]]:
    next_url = (
        f"{API_BASE}/repos/{repo}/actions/workflows/{workflow_id}/runs?"
        + urllib.parse.urlencode({"per_page": 100, "created": f">={since_iso}"})
    )
    sampled: List[dict] = []
    branch_total_counts: Dict[str, int] = defaultdict(int)
    branch_sampled_counts: Dict[str, int] = defaultdict(int)
    sampled_branches_in_order: List[str] = []
    sampled_branches_set: set[str] = set()
    total_count = 0
    is_first_page = True
    while next_url:
        payload, next_url = http_get_json(next_url, headers)
        if is_first_page:
            total_count = int(payload.get("total_count", 0))
            is_first_page = False
        for run in payload.get("workflow_runs", []):
            branch = run.get("head_branch") or "(unknown)"
            branch_total_counts[branch] += 1
            if max_branches_per_workflow > 0 and branch not in sampled_branches_set:
                if len(sampled_branches_set) >= max_branches_per_workflow:
                    continue
                sampled_branches_set.add(branch)
                sampled_branches_in_order.append(branch)
            if (run.get("status") or "").lower() != "completed":
                continue
            if (run.get("conclusion") or "").lower() != "success":
                continue
            if branch_sampled_counts[branch] >= max_runs_per_branch:
                continue
            sampled.append(run)
            branch_sampled_counts[branch] += 1
    return sampled, total_count, dict(branch_total_counts), dict(branch_sampled_counts)


def evaluate_action(action_ref: str, warned_refs: set[str]) -> Tuple[str, str]:
    reasons: List[str] = []
    status = "OK"
    if action_ref in warned_refs:
        status = "NOT_OK"
        reasons.append("Node 20 deprecation warning in job log")
    action_name, version = parse_action_ref(action_ref)
    if action_name == "actions/checkout":
        major = parse_major(version)
        if major is not None and major < 5:
            status = "NOT_OK"
            reasons.append("actions/checkout<5")
    if not reasons:
        reasons.append("No Node 20 deprecation warning for this action in this job")
    return status, "; ".join(reasons)


def get_job_log_text(repo: str, job_id: int, headers: Dict[str, str]) -> str:
    status, raw, location = http_get_with_optional_redirect_location(
        f"{API_BASE}/repos/{repo}/actions/jobs/{job_id}/logs", headers
    )

    if status in (301, 302, 303, 307, 308):
        if not location:
            raise RuntimeError("Logs endpoint redirected without Location header")
        # Download signed blob URL WITHOUT GitHub Authorization header.
        raw = http_get_bytes(location, headers={})
    elif status != 200:
        raise RuntimeError(f"Unexpected logs endpoint status: {status}")

    # API may return zip or plain text depending on endpoint behavior.
    if raw[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            parts: List[str] = []
            for name in sorted(zf.namelist()):
                with zf.open(name) as fp:
                    parts.append(fp.read().decode("utf-8", errors="replace"))
            return "\n".join(parts)
    return raw.decode("utf-8", errors="replace")


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit GitHub Actions usage over recent workflow jobs")
    parser.add_argument("--repo", default="ydb-platform/ydb", help="GitHub repo in owner/name format")
    parser.add_argument("--days", type=int, default=7, help="How many days back to inspect")
    parser.add_argument("--max-runs", type=int, default=5000, help="Global maximum workflow runs safety limit")
    parser.add_argument(
        "--max-runs-per-workflow",
        type=int,
        default=10,
        help="Runs to inspect per branch for each workflow (completed+success only)",
    )
    parser.add_argument(
        "--max-jobs-per-branch",
        type=int,
        default=10,
        help="Maximum completed+successful jobs to inspect per (workflow, branch)",
    )
    parser.add_argument(
        "--max-branches-per-workflow",
        type=int,
        default=0,
        help="If > 0, sample only first N branches per workflow (newest-first by runs order)",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=0,
        help="Global maximum jobs to inspect across runs (0 = unlimited)",
    )
    parser.add_argument(
        "--first-workflows",
        type=int,
        default=0,
        help="If > 0, inspect only first N workflows (smoke mode helper)",
    )
    parser.add_argument("--workflows-dir", default=".github/workflows", help="Path to local workflows directory")
    parser.add_argument(
        "--include-unlisted-actions",
        action="store_true",
        help="Include actions seen in logs even if not present in local workflow files",
    )
    parser.add_argument("--output-md", default="action_audit_last_week.md", help="Path to markdown report")
    parser.add_argument("--output-csv", default="action_audit_last_week.csv", help="Path to csv report")
    parser.add_argument(
        "--output-runner-updates-csv",
        default="runner_updates.csv",
        help="Path to csv with runner update candidates",
    )
    parser.add_argument(
        "--output-step-updates-csv",
        default="step_updates.csv",
        help="Path to csv with workflow step/action update candidates",
    )
    parser.add_argument(
        "--output-workflow-branch-coverage-csv",
        default="workflow_branch_coverage.csv",
        help="Path to csv with per-workflow/per-branch sampling coverage",
    )
    parser.add_argument(
        "--min-self-hosted-runner-version",
        default="2.327.1",
        help="Minimum acceptable self-hosted runner version",
    )
    parser.add_argument("--progress-every", type=int, default=20, help="Print progress every N jobs")
    parser.add_argument(
        "--group-by-runner-name",
        action="store_true",
        help="Group rows by runner_name (useful for host-level debugging). Default: aggregate across runner names.",
    )
    parser.add_argument(
        "--workflow-progress-every",
        type=int,
        default=5,
        help="Print workflow sampling progress every N workflows",
    )
    args = parser.parse_args()

    token = os.getenv("GH_TOKEN") or os.getenv("GITHUB_TOKEN")
    if not token:
        print("ERROR: GH_TOKEN or GITHUB_TOKEN is required", file=sys.stderr)
        return 2

    print(f"[0/6] Parsing configured actions in {args.workflows_dir} ...", flush=True)
    configured_refs, configured_names, workflow_files, file_to_actions, file_to_local_uses = collect_actions_from_workflows(
        args.workflows_dir
    )
    print(
        f"[0/6] Workflow files={workflow_files}, configured external action refs={len(configured_refs)}",
        flush=True,
    )

    headers = build_headers(token)
    since = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=args.days)).isoformat().replace("+00:00", "Z")
    print(
        f"[1/6] Fetching workflows and sampling runs since {since} "
        f"(<= {args.max_runs_per_workflow} per branch per workflow, completed+success only)...",
        flush=True,
    )
    if args.max_branches_per_workflow > 0:
        print(f"[1/6] Branch cap per workflow: {args.max_branches_per_workflow}", flush=True)

    workflows = fetch_workflows(args.repo, headers)
    workflows_total_discovered = len(workflows)
    if args.first_workflows > 0:
        workflows = workflows[: args.first_workflows]
        print(
            f"[1/6] Limiting workflows to first {len(workflows)} out of {workflows_total_discovered} (test mode)",
            flush=True,
        )
    sampled_runs: List[dict] = []
    run_to_workflow: Dict[int, dict] = {}
    workflow_coverage: Dict[int, dict] = {}
    workflow_trigger_types: Dict[str, str] = {}
    stage1_started = time.time()

    for idx, wf in enumerate(workflows, start=1):
        wf_id = int(wf["id"])
        wf_name = wf.get("name", "")
        runs, total_count, branch_total_counts, branch_sampled_counts = fetch_runs_for_workflow(
            args.repo, wf_id, since, args.max_runs_per_workflow, args.max_branches_per_workflow, headers
        )
        wf_path = wf.get("path", "")
        trigger_type = workflow_trigger_types.get(wf_path)
        if trigger_type is None:
            trigger_type = detect_workflow_trigger_type(wf_path)
            workflow_trigger_types[wf_path] = trigger_type
        workflow_coverage[wf_id] = {
            "name": wf_name,
            "path": wf_path,
            "trigger_type": trigger_type,
            "state": wf.get("state", ""),
            "runs_total_in_period": total_count,
            "runs_sampled": len(runs),
            "jobs_sampled": 0,
            "sample_cap_hit": any(v > args.max_runs_per_workflow for v in branch_total_counts.values()),
            "branches_in_period": len(branch_total_counts),
            "branches_sampled": len(branch_sampled_counts),
            "branch_total_counts": branch_total_counts,
            "branch_sampled_counts": branch_sampled_counts,
            "configured_actions_count": len(file_to_actions.get(wf_path, set())),
            "configured_local_uses_count": len(file_to_local_uses.get(wf_path, set())),
        }
        if idx == 1 or idx % max(1, args.workflow_progress_every) == 0 or idx == len(workflows):
            elapsed = int(time.time() - stage1_started)
            print(
                f"    workflow-progress: {idx}/{len(workflows)} "
                f"({wf_name}) sampled_runs={len(runs)} total_runs_in_period={total_count} "
                f"branches={len(branch_total_counts)} sampled_branches={len(branch_sampled_counts)} "
                f"global_sampled_runs={len(sampled_runs)} elapsed={elapsed}s",
                flush=True,
            )
        for run in runs:
            run_id = int(run["id"])
            sampled_runs.append(run)
            run_to_workflow[run_id] = wf
            if len(sampled_runs) >= args.max_runs:
                break
        if len(sampled_runs) >= args.max_runs:
            print(f"  - Reached global run cap {args.max_runs}", flush=True)
            break

    print(f"[1/6] Workflows discovered={len(workflows)}, runs sampled={len(sampled_runs)}", flush=True)

    print("[2/6] Fetching jobs and parsing logs ...", flush=True)
    print(f"[2/6] Per-branch jobs cap: {args.max_jobs_per_branch}", flush=True)
    if args.max_jobs > 0:
        print(f"[2/6] Global jobs cap: {args.max_jobs}", flush=True)
    else:
        print("[2/6] Global jobs cap: unlimited", flush=True)
    records: List[dict] = []
    seen_actions_by_workflow: Dict[str, set[str]] = defaultdict(set)
    inspected_jobs = 0
    jobs_by_workflow_branch: Dict[Tuple[int, str], int] = defaultdict(int)

    for run in sampled_runs:
        if args.max_jobs > 0 and inspected_jobs >= args.max_jobs:
            break

        run_id = int(run["id"])
        wf = run_to_workflow.get(run_id, {})
        wf_id = int(wf.get("id", run.get("workflow_id", 0)))
        wf_name = wf.get("name", "")
        wf_path = wf.get("path", "")
        run_name = run.get("name", "")
        run_branch = run.get("head_branch") or "(unknown)"
        print(f"  - Run {run_id} ({wf_name} / {run_name}, branch={run_branch}): fetching jobs", flush=True)
        branch_key = (wf_id, run_branch)
        if jobs_by_workflow_branch[branch_key] >= args.max_jobs_per_branch:
            continue

        jobs_url = f"{API_BASE}/repos/{args.repo}/actions/runs/{run_id}/jobs?per_page=100"
        for job in paginated_get(jobs_url, headers, item_key="jobs"):
            if args.max_jobs > 0 and inspected_jobs >= args.max_jobs:
                break
            if jobs_by_workflow_branch[branch_key] >= args.max_jobs_per_branch:
                break

            # We audit only completed+successful jobs to avoid in-progress/skipped noise.
            if (job.get("status") or "").lower() != "completed":
                continue
            if (job.get("conclusion") or "").lower() != "success":
                continue

            inspected_jobs += 1
            jobs_by_workflow_branch[branch_key] += 1
            if wf_id in workflow_coverage:
                workflow_coverage[wf_id]["jobs_sampled"] += 1

            job_id = int(job["id"])
            if inspected_jobs % max(1, args.progress_every) == 0:
                max_jobs_display = str(args.max_jobs) if args.max_jobs > 0 else "unbounded"
                print(
                    f"    progress: jobs={inspected_jobs}/{max_jobs_display} "
                    f"(run={run_id}, branch={run_branch}, branch_jobs={jobs_by_workflow_branch[branch_key]}/{args.max_jobs_per_branch}, job={job_id})",
                    flush=True,
                )
            try:
                log_text = get_job_log_text(args.repo, job_id, headers)
            except Exception as exc:  # noqa: BLE001
                reason = str(exc)
                if "HTTP Error 401" in reason:
                    reason = (
                        "Unable to fetch job log: HTTP 401 (auth). "
                        "Check GH/GITHUB token scopes for Actions logs access."
                    )
                records.append(
                    {
                        "workflow_name": wf_name,
                        "workflow_path": wf_path,
                        "workflow_trigger_type": workflow_trigger_types.get(wf_path, "unknown"),
                        "effective_workflow_branch_hint": workflow_branch_hint(workflow_trigger_types.get(wf_path, "unknown")),
                        "run_branch": run_branch,
                        "runner_type": "self-hosted" if "self-hosted" in (job.get("labels") or []) else "github-hosted",
                        "runner_group": job.get("runner_group_name") or "",
                        "runner_name": job.get("runner_name") or "",
                        "runner_version": "",
                        "action": "",
                        "version": "",
                        "status": "UNAVAILABLE",
                        "reason": reason,
                        "seen_ok": 0,
                        "seen_not_ok": 0,
                        "seen_unavailable": 1,
                        "sample_url": job.get("html_url", "") or run.get("html_url", ""),
                    }
                )
                continue

            downloaded, warned, runner_version = parse_job_log(log_text)
            action_refs = sorted(downloaded | warned)
            for ref in action_refs:
                action_name, version = parse_action_ref(ref)
                version = normalize_action_version(version)
                if not args.include_unlisted_actions and action_name not in configured_names:
                    continue
                seen_actions_by_workflow[wf_path].add(action_name)
                status, reason = evaluate_action(ref, warned)
                records.append(
                    {
                        "workflow_name": wf_name,
                        "workflow_path": wf_path,
                        "workflow_trigger_type": workflow_trigger_types.get(wf_path, "unknown"),
                        "effective_workflow_branch_hint": workflow_branch_hint(workflow_trigger_types.get(wf_path, "unknown")),
                        "run_branch": run_branch,
                        "runner_type": "self-hosted" if "self-hosted" in (job.get("labels") or []) else "github-hosted",
                        "runner_group": job.get("runner_group_name") or "",
                        "runner_name": job.get("runner_name") or "",
                        "runner_version": runner_version or "",
                        "action": action_name,
                        "version": version,
                        "status": status,
                        "reason": reason,
                        "seen_ok": 1 if status == "OK" else 0,
                        "seen_not_ok": 1 if status == "NOT_OK" else 0,
                        "seen_unavailable": 0,
                        "sample_url": job.get("html_url", "") or run.get("html_url", ""),
                    }
                )

    print(f"[3/6] Aggregating {len(records)} records ...", flush=True)
    agg = defaultdict(
        lambda: {
            "ok": 0,
            "not_ok": 0,
            "unavailable": 0,
            "sample": "",
            "reason": "",
            "runner_names": set(),
        }
    )
    for rec in records:
        grouped_runner_name = rec["runner_name"] if args.group_by_runner_name else ""
        key = (
            rec["workflow_name"],
            rec["workflow_path"],
            rec["workflow_trigger_type"],
            rec["effective_workflow_branch_hint"],
            rec["run_branch"],
            rec["runner_type"],
            rec["runner_group"],
            grouped_runner_name,
            rec["runner_version"],
            rec["action"],
            rec["version"],
        )
        agg[key]["ok"] += rec["seen_ok"]
        agg[key]["not_ok"] += rec["seen_not_ok"]
        agg[key]["unavailable"] += rec.get("seen_unavailable", 0)
        if rec["runner_name"]:
            agg[key]["runner_names"].add(rec["runner_name"])
        if not agg[key]["sample"]:
            agg[key]["sample"] = rec["sample_url"]
        if rec["status"] in ("NOT_OK", "UNAVAILABLE"):
            agg[key]["reason"] = rec["reason"]

    rows = []
    for key, val in agg.items():
        wf_name, wf_path, trigger_type, branch_hint, run_branch, rtype, rgroup, rname, rver, action, version = key
        if val["not_ok"] > 0:
            status = "NOT_OK"
        elif val["unavailable"] > 0:
            status = "UNAVAILABLE"
        else:
            status = "OK"
        rows.append(
            {
                "workflow_name": wf_name,
                "workflow_path": wf_path,
                "workflow_trigger_type": trigger_type,
                "effective_workflow_branch_hint": branch_hint,
                "run_branch": run_branch,
                "runner_type": rtype,
                "runner_group": rgroup,
                "runner_name": rname,
                "runner_names_count": len(val["runner_names"]),
                "runner_version": rver,
                "action": action,
                "version": version,
                "status": status,
                "reason": val["reason"] if status in ("NOT_OK", "UNAVAILABLE") else "OK in sampled jobs",
                "seen_ok": val["ok"],
                "seen_not_ok": val["not_ok"],
                "seen_unavailable": val["unavailable"],
                "sample_url": val["sample"],
            }
        )

    rows.sort(
        key=lambda r: (
            r["status"] != "NOT_OK",
            r["workflow_name"],
            r["workflow_trigger_type"],
            r["run_branch"],
            r["runner_type"],
            r["runner_group"],
            r["action"],
        )
    )

    print(f"[4/6] Writing CSV report to {args.output_csv} ...", flush=True)
    csv_fields = [
        "workflow_name",
        "workflow_path",
        "workflow_trigger_type",
        "effective_workflow_branch_hint",
        "run_branch",
        "runner_type",
        "runner_group",
        "runner_name",
        "runner_names_count",
        "runner_version",
        "action",
        "version",
        "status",
        "reason",
        "seen_ok",
        "seen_not_ok",
        "seen_unavailable",
        "sample_url",
    ]
    with open(args.output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    # Focused report 1: self-hosted runner version posture (all observed entries)
    runner_updates = []
    runner_agg = defaultdict(
        lambda: {
            "workflow_name": "",
            "workflow_path": "",
            "runner_group": "",
            "runner_version": "",
            "runner_names_count": 0,
            "statuses": set(),
            "sample_url": "",
        }
    )
    for row in rows:
        if row["runner_type"] != "self-hosted":
            continue
        key = (
            row["workflow_name"],
            row["workflow_path"],
            row["workflow_trigger_type"],
            row["effective_workflow_branch_hint"],
            row["run_branch"],
            row["runner_group"],
            row["runner_version"],
        )
        bucket = runner_agg[key]
        bucket["workflow_name"] = row["workflow_name"]
        bucket["workflow_path"] = row["workflow_path"]
        bucket["workflow_trigger_type"] = row["workflow_trigger_type"]
        bucket["effective_workflow_branch_hint"] = row["effective_workflow_branch_hint"]
        bucket["run_branch"] = row["run_branch"]
        bucket["runner_group"] = row["runner_group"]
        bucket["runner_version"] = row["runner_version"]
        bucket["runner_names_count"] = max(bucket["runner_names_count"], int(row.get("runner_names_count", 0) or 0))
        bucket["statuses"].add(row["status"])
        if not bucket["sample_url"]:
            bucket["sample_url"] = row["sample_url"]

    for item in runner_agg.values():
        below = is_runner_version_below(item["runner_version"], args.min_self_hosted_runner_version)
        if below is True:
            requires_update = "yes"
            reason = "self-hosted runner below minimum version"
        elif below is False:
            requires_update = "no"
            reason = "self-hosted runner meets minimum version"
        else:
            requires_update = "unknown"
            if item["runner_version"]:
                reason = "unable to parse runner version"
            elif "UNAVAILABLE" in item["statuses"]:
                reason = "runner version unavailable due to log access issues"
            else:
                reason = "runner version is empty in sampled data"

        runner_updates.append(
            {
                "workflow_name": item["workflow_name"],
                "workflow_path": item["workflow_path"],
                "workflow_trigger_type": item["workflow_trigger_type"],
                "effective_workflow_branch_hint": item["effective_workflow_branch_hint"],
                "run_branch": item["run_branch"],
                "runner_group": item["runner_group"],
                "runner_version": item["runner_version"],
                "runner_names_count": item["runner_names_count"],
                "required_min_version": args.min_self_hosted_runner_version,
                "requires_update_version": requires_update,
                "reason": reason,
                "sample_url": item["sample_url"],
            }
        )

    runner_updates.sort(
        key=lambda r: (
            {"yes": 0, "unknown": 1, "no": 2}.get(r["requires_update_version"], 3),
            r["workflow_path"],
            r["workflow_trigger_type"],
            r["run_branch"],
            r["runner_group"],
            r["runner_version"],
        )
    )

    # Focused report 2: where workflow steps/actions should be updated
    step_updates = []
    step_seen = set()
    for row in rows:
        if row["status"] != "NOT_OK":
            continue
        if not row["action"]:
            continue
        key = (
            row["workflow_path"],
            row["workflow_trigger_type"],
            row["run_branch"],
            row["action"],
            row["version"],
            row["reason"],
        )
        if key in step_seen:
            continue
        step_seen.add(key)
        step_updates.append(
            {
                "workflow_name": row["workflow_name"],
                "workflow_path": row["workflow_path"],
                "workflow_trigger_type": row["workflow_trigger_type"],
                "effective_workflow_branch_hint": row["effective_workflow_branch_hint"],
                "run_branch": row["run_branch"],
                "action": row["action"],
                "version": row["version"],
                "reason": row["reason"],
                "sample_url": row["sample_url"],
            }
        )

    with open(args.output_runner_updates_csv, "w", newline="", encoding="utf-8") as f:
        fields = [
            "workflow_name",
            "workflow_path",
            "workflow_trigger_type",
            "effective_workflow_branch_hint",
            "run_branch",
            "runner_group",
            "runner_version",
            "runner_names_count",
            "required_min_version",
            "requires_update_version",
            "reason",
            "sample_url",
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for item in runner_updates:
            writer.writerow(item)

    with open(args.output_step_updates_csv, "w", newline="", encoding="utf-8") as f:
        fields = [
            "workflow_name",
            "workflow_path",
            "workflow_trigger_type",
            "effective_workflow_branch_hint",
            "run_branch",
            "action",
            "version",
            "reason",
            "sample_url",
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for item in step_updates:
            writer.writerow(item)

    # Coverage report by workflow+branch
    workflow_branch_rows = []
    for wf_id, cov in workflow_coverage.items():
        wf_name = cov["name"]
        wf_path = cov["path"]
        branch_total_counts = cov.get("branch_total_counts", {})
        branch_sampled_run_counts = cov.get("branch_sampled_counts", {})
        all_branches = set(branch_total_counts.keys()) | set(branch_sampled_run_counts.keys())
        for branch in sorted(all_branches):
            key = (wf_id, branch)
            workflow_branch_rows.append(
                {
                    "workflow_name": wf_name,
                    "workflow_path": wf_path,
                    "branch": branch,
                    "runs_total_in_period": int(branch_total_counts.get(branch, 0)),
                    "runs_sampled": int(branch_sampled_run_counts.get(branch, 0)),
                    "jobs_sampled": int(jobs_by_workflow_branch.get(key, 0)),
                }
            )

    with open(args.output_workflow_branch_coverage_csv, "w", newline="", encoding="utf-8") as f:
        fields = [
            "workflow_name",
            "workflow_path",
            "branch",
            "runs_total_in_period",
            "runs_sampled",
            "jobs_sampled",
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for item in workflow_branch_rows:
            writer.writerow(item)

    print(f"[5/6] Writing Markdown report to {args.output_md} ...", flush=True)
    not_ok_count = sum(1 for r in rows if r["status"] == "NOT_OK")
    unavailable_count = sum(1 for r in rows if r["status"] == "UNAVAILABLE")
    runner_update_yes = sum(1 for r in runner_updates if r["requires_update_version"] == "yes")
    runner_update_unknown = sum(1 for r in runner_updates if r["requires_update_version"] == "unknown")
    seen_action_versions = {(r["action"], r["version"]) for r in rows}
    missing_configured = sorted(action for action in configured_names if not any(r["action"] == action for r in rows))

    coverage_rows = sorted(
        workflow_coverage.values(),
        key=lambda c: (c["runs_total_in_period"] == 0, -c["runs_total_in_period"], c["name"]),
    )
    md_lines = [
        f"# GitHub Actions Audit (last {args.days} days)",
        "",
        f"- Repository: `{args.repo}`",
        f"- Workflow files parsed locally: `{workflow_files}` from `{args.workflows_dir}`",
        f"- Configured external action refs: `{len(configured_refs)}`",
        f"- Configured external action names: `{len(configured_names)}`",
        f"- Workflows discovered via API: `{workflows_total_discovered}`",
        f"- Workflows inspected: `{len(workflows)}`",
        f"- Runs sampled: `{len(sampled_runs)}` (global cap `{args.max_runs}`, per-branch cap `{args.max_runs_per_workflow}`)",
        f"- Jobs inspected: `{inspected_jobs}` (global cap `{args.max_jobs if args.max_jobs > 0 else 'unlimited'}`, per-branch cap `{args.max_jobs_per_branch}`)",
        f"- Unique action@version seen in sampled jobs: `{len(seen_action_versions)}`",
        f"- NOT_OK entries: `{not_ok_count}`",
        f"- UNAVAILABLE entries (log access/auth issues): `{unavailable_count}`",
        f"- Self-hosted runner entries: `{len(runner_updates)}` (csv: `{args.output_runner_updates_csv}`)",
        f"- Requires runner update: `{runner_update_yes}`, unknown: `{runner_update_unknown}`",
        f"- Step/action update candidates: `{len(step_updates)}` (csv: `{args.output_step_updates_csv}`)",
        f"- Workflow/branch coverage rows: `{len(workflow_branch_rows)}` (csv: `{args.output_workflow_branch_coverage_csv}`)",
        "",
        "## Workflow Coverage",
        "",
        "| Workflow | Path | Branches in period | Branches sampled | Runs in period | Runs sampled | Jobs sampled | Per-branch cap hit? | Configured actions | Seen actions | Local/reusable uses |",
        "|---|---|---:|---:|---:|---:|---:|---|---:|---:|---:|",
    ]
    for cov in coverage_rows:
        seen_count = len(seen_actions_by_workflow.get(cov["path"], set()))
        md_lines.append(
            f"| `{cov['name']}` | `{cov['path']}` | {cov['branches_in_period']} | {cov['branches_sampled']} | "
            f"{cov['runs_total_in_period']} | {cov['runs_sampled']} | {cov['jobs_sampled']} | {'yes' if cov['sample_cap_hit'] else 'no'} | "
            f"{cov['configured_actions_count']} | {seen_count} | {cov['configured_local_uses_count']} |"
        )

    md_lines.extend(
        [
            "",
            "## Action Usage By Workflow and Runner",
            "",
            "| Workflow | Workflow Path | Trigger | Branch Hint | Branch | Runner Type | Runner Group | Runner Name | Runner Names Count | Runner Version | Action | Version | Status | Reason | Seen OK | Seen NOT_OK | Seen UNAVAILABLE | Sample |",
            "|---|---|---|---|---|---|---|---|---:|---|---|---|---|---|---:|---:|---:|---|",
        ]
    )
    for r in rows:
        sample = f"[job]({r['sample_url']})" if r["sample_url"] else ""
        runner_name = r["runner_name"] if r["runner_name"] else "(aggregated)"
        md_lines.append(
            f"| `{r['workflow_name']}` | `{r['workflow_path']}` | `{r['workflow_trigger_type']}` | `{r['effective_workflow_branch_hint']}` | `{r['run_branch']}` | `{r['runner_type']}` | `{r['runner_group']}` | "
            f"`{runner_name}` | {r['runner_names_count']} | `{r['runner_version']}` | `{r['action']}` | `{r['version']}` | "
            f"**{r['status']}** | {r['reason']} | {r['seen_ok']} | {r['seen_not_ok']} | {r['seen_unavailable']} | {sample} |"
        )

    md_lines.extend(
        [
            "",
            "## Configured Actions Not Seen In Sampled Jobs",
            "",
            f"Count: `{len(missing_configured)}`",
            "",
        ]
    )
    for action_name in missing_configured:
        md_lines.append(f"- `{action_name}`")

    with open(args.output_md, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines) + "\n")

    print("[6/6] Done", flush=True)
    print(f"Wrote markdown report: {args.output_md}")
    print(f"Wrote csv report: {args.output_csv}")
    print(f"Wrote runner update report: {args.output_runner_updates_csv}")
    print(f"Wrote step update report: {args.output_step_updates_csv}")
    print(f"Wrote workflow branch coverage report: {args.output_workflow_branch_coverage_csv}")
    print(f"NOT_OK entries: {not_ok_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
