#!/usr/bin/env python3
"""Merge remote execution JSON with defaults → dispatch_remote `config` output.

Callers (Run-and-debug, PR-check, …) pass optional REMOTE_JSON; this script
emits one opaque config for dispatch_remote.yml (artifact → wait claim → ingest).

Env:
  REMOTE_JSON — optional caller override (object or empty)
  CODE_REF / GITHUB_SHA, GITHUB_WORKFLOW, GITHUB_EVENT_NAME, GITHUB_RUN_ID,
  PULL_NUMBER, ANALYTICS_JOB_NAME (optional top-level overrides)
  GITHUB_OUTPUT — write `config<<EOF...`
"""
from __future__ import annotations

import copy
import json
import os
import re
import sys
from typing import Any


DEFAULT_REMOTE: dict[str, Any] = {
    # Host repo / workflow identity (waiter polls GH; VM dispatches this WF).
    "repository": "nebius/ydb",
    "workflow_file": "ci_remote_executor.yml",
    "workflow_name": "CI remote executor",
    "executor_ref": "nebius-ci",
    "wait": True,
    "fail_on_remote_failure": True,
    # Host public S3 only (claims + build outputs). Platform waiter/ingest read
    # these; the host must never be pointed at platform/YC S3 for launch context.
    "summary_s3_endpoint": "https://s3.us-east-1.amazonaws.com",
    "summary_s3_bucket": "ydb-nebius-ci-public-609722444123-us-east-1",
    "claim_s3_endpoint": "",
    "claim_s3_bucket": "",
    "analytics_job_name": "Run and debug tests",
    "caller_workflow": "",
    "runs_on": ["self-hosted", "linux", "amd64-large"],
    "code_ref": "",
    "analytics_branch": "",
    "platform_repository": "",
    "job": {
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
        "collect_coredumps": False,
    },
}


def die(msg: str) -> None:
    print(f"::error::{msg}", file=sys.stderr)
    sys.exit(1)


def _truthy(v: Any, default: bool = False) -> bool:
    if v is None or v == "":
        return default
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("1", "true", "yes")


def _as_str(v: Any, default: str = "") -> str:
    if v is None:
        return default
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(base)
    for key, val in override.items():
        if isinstance(val, dict) and isinstance(out.get(key), dict):
            out[key] = deep_merge(out[key], val)
        else:
            out[key] = copy.deepcopy(val)
    return out


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
    cfg = deep_merge(DEFAULT_REMOTE, parse_remote_json(os.environ.get("REMOTE_JSON", "")))

    # Optional seeds from the caller WF top-level inputs (when remote JSON omits job.*).
    job = cfg.get("job") if isinstance(cfg.get("job"), dict) else {}
    if not _as_str(job.get("build_target")).strip():
        seed = (os.environ.get("DEFAULT_BUILD_TARGET") or "").strip()
        if seed:
            job = {**job, "build_target": seed}
    if not _as_str(job.get("build_preset")).strip():
        seed = (os.environ.get("DEFAULT_BUILD_PRESET") or "").split(",")[0].strip()
        if seed:
            job = {**job, "build_preset": seed}
    cfg["job"] = job

    code_ref = (os.environ.get("CODE_REF") or "").strip() or _as_str(cfg.get("code_ref")).strip()
    if not code_ref:
        code_ref = (os.environ.get("GITHUB_SHA") or "").strip()
    if not code_ref:
        die("code_ref is empty (pass remote.code_ref or CODE_REF / GITHUB_SHA)")
    cfg["code_ref"] = code_ref

    analytics = _as_str(cfg.get("analytics_branch")).strip()
    if not analytics:
        analytics = (
            "main" if re.fullmatch(r"[0-9a-fA-F]{7,40}", code_ref) else code_ref
        )
    cfg["analytics_branch"] = analytics

    job = cfg.get("job") if isinstance(cfg.get("job"), dict) else {}
    preset = _as_str(job.get("build_preset"), "relwithdebinfo").split(",")[0].strip() or "relwithdebinfo"
    increment = _as_str(job.get("increment"), "false")
    fetch_depth = 0 if _truthy(increment) else 2

    analytics_job = (
        (os.environ.get("ANALYTICS_JOB_NAME") or "").strip()
        or _as_str(cfg.get("analytics_job_name")).strip()
        or "Run and debug tests"
    )
    cfg["analytics_job_name"] = analytics_job
    cfg["caller_workflow"] = (
        (os.environ.get("CALLER_WORKFLOW") or "").strip()
        or _as_str(cfg.get("caller_workflow")).strip()
        or (os.environ.get("GITHUB_WORKFLOW") or "").strip()
    )

    source_event = (os.environ.get("SOURCE_EVENT_NAME") or "").strip() or "workflow_dispatch"
    source_run = (os.environ.get("SOURCE_RUN_ID") or "").strip() or (
        os.environ.get("GITHUB_RUN_ID") or ""
    )
    pull_number = (os.environ.get("PULL_NUMBER") or "").strip()

    job_inputs = {
        "build_target": _as_str(job.get("build_target"), "ydb/"),
        "build_preset": preset,
        "run_tests": _as_str(job.get("run_tests"), "true"),
        "increment": increment,
        "fetch_depth": fetch_depth,
        "test_size": _as_str(job.get("test_size"), "small,medium"),
        "test_type": _as_str(job.get("test_type")),
        "test_threads": _as_str(job.get("test_threads"), "64"),
        "link_threads": _as_str(job.get("link_threads"), "2"),
        "additional_ya_make_args": _as_str(job.get("additional_ya_make_args")),
        "test_retry_count": _as_str(job.get("test_retry_count")),
        "put_build_results_to_cache": _as_str(job.get("put_build_results_to_cache"), "true"),
        "collect_coredumps": _as_str(job.get("collect_coredumps"), "false"),
        "custom_branch_name": analytics,
        "analytics_job_name": analytics_job,
        "source_event_name": source_event,
        "source_run_id": source_run,
        "pull_number": pull_number,
        "enable_ydb_qa": "false",
    }

    runs_on = cfg.get("runs_on")
    if isinstance(runs_on, str):
        try:
            runs_on_obj = json.loads(runs_on)
        except json.JSONDecodeError as e:
            die(f"runs_on must be a JSON array: {e}")
    else:
        runs_on_obj = runs_on
    if not isinstance(runs_on_obj, list):
        die("runs_on must be a JSON array of runner labels")
    runs_on_str = json.dumps(runs_on_obj, separators=(",", ":"))

    cfg["wait"] = _truthy(cfg.get("wait"), True)
    cfg["fail_on_remote_failure"] = _truthy(cfg.get("fail_on_remote_failure"), True)
    cfg.pop("handoff", None)
    cfg.pop("ticket_s3_endpoint", None)
    cfg.pop("ticket_s3_bucket", None)
    cfg["platform_repository"] = (
        _as_str(cfg.get("platform_repository")).strip()
        or (os.environ.get("GITHUB_REPOSITORY") or "").strip()
    )
    if not _as_str(cfg.get("claim_s3_endpoint")).strip():
        cfg["claim_s3_endpoint"] = cfg.get("summary_s3_endpoint") or ""
    if not _as_str(cfg.get("claim_s3_bucket")).strip():
        cfg["claim_s3_bucket"] = cfg.get("summary_s3_bucket") or ""

    # Attempt identity so platform re-runs do not attach to a previous claim.
    platform_run_attempt = (
        (os.environ.get("PLATFORM_RUN_ATTEMPT") or "").strip()
        or (os.environ.get("GITHUB_RUN_ATTEMPT") or "").strip()
        or "1"
    )

    # Only these fields are passed to the host executor (from Actions artifact
    # via VM). Host launch context is never taken from platform S3.
    cfg["inputs"] = {
        "platform_run_id": source_run,
        "platform_run_attempt": platform_run_attempt,
        "platform_repository": cfg["platform_repository"],
        "code_ref": code_ref,
        "job_inputs": job_inputs,
        "runs_on": runs_on_str,
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
    """Full resolved launch params for job summary (no selective omit)."""
    inputs = cfg.get("inputs") if isinstance(cfg.get("inputs"), dict) else {}
    job = inputs.get("job_inputs") if isinstance(inputs.get("job_inputs"), dict) else {}
    if not job and isinstance(cfg.get("job"), dict):
        job = cfg["job"]

    lines = [
        "## Resolved launch parameters",
        "",
        "| key | value |",
        "| --- | --- |",
    ]
    top_keys = (
        "repository",
        "workflow_file",
        "workflow_name",
        "executor_ref",
        "code_ref",
        "analytics_branch",
        "analytics_job_name",
        "caller_workflow",
        "platform_repository",
        "runs_on",
        "wait",
        "fail_on_remote_failure",
        "summary_s3_endpoint",
        "summary_s3_bucket",
        "claim_s3_endpoint",
        "claim_s3_bucket",
    )
    for key in top_keys:
        if key == "code_ref":
            val = inputs.get("code_ref") or cfg.get("code_ref")
        elif key == "runs_on":
            val = inputs.get("runs_on") or cfg.get("runs_on")
        else:
            val = cfg.get(key)
        lines.append(f"| {key} | {_fmt_md_value(val)} |")

    for key in ("platform_run_id", "platform_run_attempt", "platform_repository"):
        if key in inputs:
            lines.append(f"| inputs.{key} | {_fmt_md_value(inputs.get(key))} |")

    lines.extend(["", "### job_inputs", "", "| key | value |", "| --- | --- |"])
    # Stable order: DEFAULT_REMOTE.job keys first, then any extras.
    ordered = list(DEFAULT_REMOTE.get("job", {}).keys())
    for key in job:
        if key not in ordered:
            ordered.append(key)
    # Always include derived fields that live only on job_inputs.
    for key in ("fetch_depth", "custom_branch_name", "source_event_name", "source_run_id",
                "pull_number", "enable_ydb_qa", "analytics_job_name"):
        if key not in ordered:
            ordered.append(key)
    for key in ordered:
        lines.append(f"| {key} | {_fmt_md_value(job.get(key))} |")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    cfg = build_config()
    text = json.dumps(cfg, ensure_ascii=False)
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as f:
            f.write("config<<EOF\n")
            f.write(text)
            f.write("\nEOF\n")
            f.write(f"wait={str(cfg['wait']).lower()}\n")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(format_launch_summary_md(cfg))
    print(json.dumps(cfg, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
