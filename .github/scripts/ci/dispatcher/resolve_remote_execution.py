#!/usr/bin/env python3
"""Merge remote execution JSON with defaults → dispatch_remote `config` output.

Callers (Run-and-debug, PR-check, …) pass optional REMOTE_JSON; this script
emits one opaque config for dispatch_remote.yml (artifact → wait claim → ingest).

Env:
  REMOTE_JSON — optional caller override (object or empty)
  CODE_REF, GITHUB_WORKFLOW, DEFAULT_BUILD_TARGET, DEFAULT_BUILD_PRESET,
  PULL_NUMBER (optional)
  GITHUB_OUTPUT — write `config=...`
  GITHUB_STEP_SUMMARY — optional launch-params table
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
    cfg_str,
    die,
    host_s3,
    set_outputs,
    truthy,
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
    "collect_coredumps": False,
}

DEFAULT_REMOTE: dict[str, Any] = {
    # Host repo / workflow identity (waiter polls GH; VM dispatches this WF).
    "repository": "nebius/ydb",
    "workflow_file": "ci_remote_executor.yml",
    "workflow_name": "CI remote executor",
    "executor_ref": "nebius-ci",
    "fail_on_remote_failure": True,
    # Host public S3 (claims + build outputs). Platform never points host at YC S3.
    "host_s3_endpoint": "https://s3.us-east-1.amazonaws.com",
    "host_s3_bucket": "ydb-nebius-ci-public-609722444123-us-east-1",
    "analytics_job_name": "Run and debug tests",
    "caller_workflow": "",
    "runs_on": ["self-hosted", "linux", "amd64-large"],
    "code_ref": "",
    "analytics_branch": "",
}


def _as_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


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
    remote_job = remote.pop("job", {})
    if remote_job is None:
        remote_job = {}
    if not isinstance(remote_job, dict):
        die("remote JSON .job must be an object")

    cfg = {**DEFAULT_REMOTE, **remote}
    job = {**_DEFAULT_JOB, **remote_job}

    # UI seeds override defaults when remote JSON omitted those job keys.
    if "build_target" not in remote_job:
        seed = (os.environ.get("DEFAULT_BUILD_TARGET") or "").strip()
        if seed:
            job["build_target"] = seed
    if "build_preset" not in remote_job:
        seed = (os.environ.get("DEFAULT_BUILD_PRESET") or "").split(",")[0].strip()
        if seed:
            job["build_preset"] = seed

    # Prefer REMOTE_JSON.code_ref so callers can pin a SHA while CODE_REF is github.sha.
    code_ref = _as_str(remote.get("code_ref")).strip() or (os.environ.get("CODE_REF") or "").strip()
    if not code_ref:
        die("code_ref is empty (pass remote.code_ref or CODE_REF)")
    cfg["code_ref"] = code_ref

    analytics = _as_str(cfg.get("analytics_branch")).strip()
    if not analytics:
        analytics = (
            "main" if re.fullmatch(r"[0-9a-fA-F]{7,40}", code_ref) else code_ref
        )
    cfg["analytics_branch"] = analytics

    preset = _as_str(job["build_preset"]).split(",")[0].strip()
    if not preset:
        die("build_preset is empty")
    fetch_depth = "0" if truthy(job["increment"]) else "2"

    cfg["analytics_job_name"] = cfg_str(cfg, "analytics_job_name")
    cfg["caller_workflow"] = (
        _as_str(cfg.get("caller_workflow")).strip()
        or (os.environ.get("GITHUB_WORKFLOW") or "").strip()
    )
    if not cfg["caller_workflow"]:
        die("caller_workflow is empty")

    pull_number = (os.environ.get("PULL_NUMBER") or "").strip()

    job_inputs = {k: _as_str(job[k]) for k in _DEFAULT_JOB}
    job_inputs["build_preset"] = preset
    job_inputs["fetch_depth"] = fetch_depth
    job_inputs["custom_branch_name"] = analytics
    job_inputs["analytics_job_name"] = cfg["analytics_job_name"]
    job_inputs["pull_number"] = pull_number

    runs_on = cfg.get("runs_on")
    if not isinstance(runs_on, list):
        die("runs_on must be a JSON array of runner labels")
    runs_on_str = json.dumps(runs_on, separators=(",", ":"))

    cfg["fail_on_remote_failure"] = truthy(cfg.get("fail_on_remote_failure"), True)
    cfg["host_s3_endpoint"], cfg["host_s3_bucket"] = host_s3(cfg)

    # Host executor receives only config.inputs (VM → workflow_dispatch).
    # Enqueue stamps live platform_run_id/attempt/repository into inputs.
    cfg["inputs"] = {
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
