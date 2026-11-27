#!/usr/bin/env python3
"""Merge remote execution JSON with defaults → dispatch_remote `config` output.

repository, workflow_file, host_s3_endpoint, and host_s3_bucket are pinned to
DEFAULT_REMOTE. A caller value that differs is rejected.

Env:
  REMOTE_JSON — optional caller override (object or empty)
  CODE_REF, GITHUB_WORKFLOW (or CALLER_WORKFLOW), CALLER_EVENT, CALLER_PR_NUMBER,
    CALLER_PR_TITLE, DEFAULT_BUILD_TARGET, DEFAULT_BUILD_PRESET,
    ANALYTICS_BRANCH (optional, ignored if remote.code_ref set);
  remote JSON may also set pr_number / pr_title / caller_event (strings).

Display run names:
``{workflow} · {event} · {target_branch} · {build_preset} · #{pr} · {title}``
(omit empty segments; workflow and event are always present when resolve succeeds).
"""
from __future__ import annotations

import json
import os
import re
import sys
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import (  # noqa: E402
    append_summary,
    die,
    host_s3,
    set_outputs,
)

_CALLER_EVENTS = frozenset({"pr", "postcommit", "manual"})
_GITHUB_EVENT_TO_CALLER: dict[str, str] = {
    "pull_request": "pr",
    "pull_request_target": "pr",
    "push": "postcommit",
    "workflow_dispatch": "manual",
}


def format_remote_run_name(
    workflow: str,
    event: str,
    *,
    target_branch: str = "",
    build_preset: str = "",
    pr_number: str = "",
    pr_title: str = "",
) -> str:
    """Assemble host/platform remote run title (middle dot separators)."""
    wf = workflow.strip()
    ev = event.strip()
    if not wf:
        die("caller workflow name is empty (GITHUB_WORKFLOW / CALLER_WORKFLOW)")
    if not ev:
        die("caller_event is empty")
    parts: list[str] = [wf, ev]
    branch = target_branch.strip()
    if branch:
        parts.append(branch)
    preset = build_preset.strip()
    if preset:
        parts.append(preset)
    num = pr_number.strip()
    if num:
        parts.append(f"#{num}")
    title = pr_title.strip()
    if title:
        parts.append(title)
    return " · ".join(parts)


def _normalize_caller_event(raw: str, *, where: str) -> str:
    event = raw.strip().lower()
    if event not in _CALLER_EVENTS:
        die(f"{where} must be one of {sorted(_CALLER_EVENTS)}, got {raw!r}")
    return event


def _caller_event(remote: dict[str, Any]) -> str:
    """Short event token for run-name (pr | postcommit | manual)."""
    from_remote = remote.pop("caller_event", None)
    if from_remote is not None:
        if not isinstance(from_remote, str):
            die("remote JSON .caller_event must be string")
        return _normalize_caller_event(from_remote, where="remote JSON .caller_event")
    env = (os.environ.get("CALLER_EVENT") or "").strip()
    if env:
        return _normalize_caller_event(env, where="CALLER_EVENT")
    gh = (os.environ.get("GITHUB_EVENT_NAME") or "").strip().lower()
    if gh:
        mapped = _GITHUB_EVENT_TO_CALLER.get(gh)
        if not mapped:
            die(f"unsupported GITHUB_EVENT_NAME for caller_event: {gh!r}")
        return mapped
    return "manual"


def _reject_unknown(keys: set[str], allowed: set[str], where: str) -> None:
    unknown = sorted(keys - allowed)
    if unknown:
        die(f"{where} has unknown keys: {unknown}")


def _check_types(values: dict[str, Any], defaults: dict[str, Any], where: str) -> None:
    for key, default in defaults.items():
        v = values[key]
        if type(v) is not type(default):
            die(
                f"{where}.{key} must be {type(default).__name__}, "
                f"got {type(v).__name__}: {v!r}"
            )


_DEFAULT_JOB: dict[str, Any] = {
    "build_target": "ydb/tests/olap/",
    "build_preset": "relwithdebinfo",
    "run_tests": True,
    "increment": False,
    "test_size": "small,medium",
    "test_type": "",
    "test_threads": "64",
    "link_threads": "2",
    "additional_ya_make_args": "",
    "test_retry_count": "",
    "put_build_results_to_cache": True,
    # Match run_tests: local /coredumps loop volume, not a host S3 upload.
    "collect_coredumps": True,
}

# Caller JSON cannot retarget the host repo or its public bucket.
_PINNED_REMOTE_KEYS = (
    "repository",
    "workflow_file",
    "host_s3_endpoint",
    "host_s3_bucket",
)


DEFAULT_REMOTE: dict[str, Any] = {
    "repository": "nebius/ydb",
    "workflow_file": "ci_remote_executor.yml",
    "workflow_name": "CI remote executor",
    "executor_ref": "nebius-ci",
    "host_s3_endpoint": "https://s3.us-east-1.amazonaws.com",
    "host_s3_bucket": "ydb-nebius-ci-public-609722444123-us-east-1",
    "runs_on": ["self-hosted", "linux", "amd64-large"],
    "code_ref": "",
    "analytics_branch": "",
}


def _reject_pinned_overrides(remote: dict[str, Any]) -> None:
    """Identity and host S3 stay on DEFAULT_REMOTE (equal repeats are allowed)."""
    for key in _PINNED_REMOTE_KEYS:
        if key not in remote:
            continue
        expected = DEFAULT_REMOTE[key]
        got = remote[key]
        if type(got) is not type(expected) or got != expected:
            die(
                f"remote JSON .{key} is fixed to {expected!r}; "
                f"refusing override {got!r}"
            )


def _require_single_preset(raw: str, *, where: str) -> str:
    """Remote runs one preset. Comma-separated lists are a caller error."""
    text = str(raw).strip()
    if not text:
        die(f"{where} is empty")
    if "," in text:
        die(
            f"{where} must be a single build preset for remote "
            f"(got {raw!r}). Comma-separated presets are rejected; "
            "pass one preset via remote.job.build_preset or build_preset."
        )
    return text


def _job_input_str(v: Any) -> str:
    """workflow_dispatch inputs are strings; bools become true/false."""
    if isinstance(v, bool):
        return "true" if v else "false"
    return v


def _caller_pr_context(remote: dict[str, Any]) -> tuple[str, str]:
    """Optional display metadata for the host workflow run-name (not claim logic)."""
    pr_number = remote.pop("pr_number", None)
    pr_title = remote.pop("pr_title", None)
    if pr_number is not None and not isinstance(pr_number, (str, int)):
        die("remote JSON .pr_number must be string or int")
    if pr_title is not None and not isinstance(pr_title, str):
        die("remote JSON .pr_title must be string")
    num = str(pr_number).strip() if pr_number is not None else ""
    num = num or (os.environ.get("CALLER_PR_NUMBER") or "").strip()
    title = (pr_title or "").strip() if pr_title is not None else ""
    title = title or (os.environ.get("CALLER_PR_TITLE") or "").strip()
    if num and not re.fullmatch(r"[0-9]+", num):
        die("pr_number must be digits")
    if "\n" in title or "\r" in title:
        die("pr_title must be single-line")
    if len(title) > 200:
        title = title[:197] + "..."
    return num, title


def parse_remote_json(raw: str) -> dict[str, Any]:
    raw = (raw or "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        die(f"remote JSON is not valid JSON: {e}")
    if not isinstance(data, dict):
        die("remote JSON must be an object")
    return data


def build_config() -> dict[str, Any]:
    remote = parse_remote_json(os.environ.get("REMOTE_JSON", ""))
    pr_number, pr_title = _caller_pr_context(remote)
    caller_event = _caller_event(remote)
    remote_job = remote.pop("job", {})
    if remote_job is None:
        remote_job = {}
    if not isinstance(remote_job, dict):
        die("remote JSON .job must be an object")
    _reject_unknown(set(remote), set(DEFAULT_REMOTE), "remote JSON")
    _reject_unknown(set(remote_job), set(_DEFAULT_JOB), "remote JSON .job")
    _reject_pinned_overrides(remote)

    cfg = {**DEFAULT_REMOTE, **remote}
    job = {**_DEFAULT_JOB, **remote_job}
    _check_types(cfg, DEFAULT_REMOTE, "remote JSON")
    _check_types(job, _DEFAULT_JOB, "remote JSON .job")

    # UI seeds override defaults when remote JSON omitted those job keys.
    if "build_target" not in remote_job:
        seed = (os.environ.get("DEFAULT_BUILD_TARGET") or "").strip()
        if seed:
            job["build_target"] = seed
    if "build_preset" not in remote_job:
        seed = (os.environ.get("DEFAULT_BUILD_PRESET") or "").strip()
        if seed:
            job["build_preset"] = _require_single_preset(
                seed, where="DEFAULT_BUILD_PRESET"
            )

    # Prefer REMOTE_JSON.code_ref so callers can pin a SHA while CODE_REF is github.sha.
    remote_code_ref = cfg["code_ref"].strip()
    code_ref = remote_code_ref or (os.environ.get("CODE_REF") or "").strip()
    if not code_ref:
        die("code_ref is empty (pass remote.code_ref or CODE_REF)")
    cfg["code_ref"] = code_ref

    # ANALYTICS_BRANCH describes CODE_REF; ignore it when remote.code_ref overrides the ref.
    analytics = cfg["analytics_branch"].strip()
    if not analytics and not remote_code_ref:
        analytics = (os.environ.get("ANALYTICS_BRANCH") or "").strip()
    if not analytics:
        analytics = (
            "main" if re.fullmatch(r"[0-9a-fA-F]{7,40}", code_ref) else code_ref
        )
    cfg["analytics_branch"] = analytics

    preset = _require_single_preset(job["build_preset"], where="build_preset")
    fetch_depth = "0" if job["increment"] else "2"

    cfg["caller_workflow"] = (
        (os.environ.get("CALLER_WORKFLOW") or os.environ.get("GITHUB_WORKFLOW") or "").strip()
    )
    if not cfg["caller_workflow"]:
        die("caller workflow name is empty (GITHUB_WORKFLOW / CALLER_WORKFLOW)")

    job_inputs = {k: _job_input_str(job[k]) for k in _DEFAULT_JOB}
    job_inputs["build_preset"] = preset
    job_inputs["fetch_depth"] = fetch_depth
    job_inputs["custom_branch_name"] = analytics

    runs_on_str = json.dumps(cfg["runs_on"], separators=(",", ":"))
    cfg["host_s3_endpoint"], cfg["host_s3_bucket"] = host_s3(cfg)

    # Host executor receives only config.inputs (VM → workflow_dispatch).
    # Enqueue stamps live platform_run_id/attempt/repository into inputs.
    cfg["inputs"] = {
        "code_ref": code_ref,
        "job_inputs": job_inputs,
        "runs_on": runs_on_str,
        "caller_workflow": cfg["caller_workflow"],
        "caller_event": caller_event,
        "target_branch": analytics,
        "build_preset": preset,
        "pr_number": pr_number,
        "pr_title": pr_title,
    }
    return cfg


def _fmt_md_value(v: Any) -> str:
    if v is None:
        return "`∅`"
    if isinstance(v, bool):
        return f"`{'true' if v else 'false'}`"
    if isinstance(v, (list, dict)):
        return f"`{json.dumps(v, ensure_ascii=False, separators=(',', ':'))}`"
    s = str(v)
    if s == "":
        return "`\"\"`"
    return f"`{s}`"


def format_launch_summary_md(cfg: dict[str, Any]) -> str:
    """Resolved launch params for the job summary."""
    job = cfg["inputs"]["job_inputs"]

    lines = [
        "<details>",
        "<summary>Resolved launch parameters</summary>",
        "",
        "| key | value |",
        "| --- | --- |",
    ]
    for key, val in cfg.items():
        if key == "inputs":
            continue
        lines.append(f"| {key} | {_fmt_md_value(val)} |")

    lines.extend(["", "### job_inputs", "", "| key | value |", "| --- | --- |"])
    for key, val in job.items():
        lines.append(f"| {key} | {_fmt_md_value(val)} |")
    lines.extend(["", "</details>", ""])
    return "\n".join(lines)


def main() -> int:
    cfg = build_config()
    text = json.dumps(cfg, ensure_ascii=False)
    set_outputs(config=text)
    append_summary(format_launch_summary_md(cfg))
    print(json.dumps(cfg, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
