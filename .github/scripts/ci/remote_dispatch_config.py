#!/usr/bin/env python3
"""Apply opaque dispatch_remote `config` JSON onto process env for scripts."""
from __future__ import annotations

import json
import os
import sys
from typing import Any


def die(msg: str) -> None:
    print(f"::error::{msg}", file=sys.stderr)
    sys.exit(1)


def load_config(raw: str | None = None) -> dict[str, Any] | None:
    text = (raw if raw is not None else os.environ.get("CONFIG_JSON", "")).strip()
    if not text:
        return None
    try:
        cfg = json.loads(text)
    except json.JSONDecodeError as e:
        die(f"CONFIG_JSON is not valid JSON: {e}")
    if not isinstance(cfg, dict):
        die("CONFIG_JSON must be an object")
    return cfg


def _set(name: str, value: Any, *, overwrite: bool = False) -> None:
    if value is None or value == "":
        return
    if not overwrite and os.environ.get(name):
        return
    if isinstance(value, bool):
        os.environ[name] = "true" if value else "false"
    elif isinstance(value, (dict, list)):
        os.environ[name] = json.dumps(value, ensure_ascii=False)
    else:
        os.environ[name] = str(value)


def apply_to_environ(cfg: dict[str, Any] | None = None, *, overwrite: bool = False) -> dict[str, Any] | None:
    """Map config fields → env used by dispatch_remote_executor / ingest scripts."""
    cfg = cfg if cfg is not None else load_config()
    if cfg is None:
        return None

    repo = (cfg.get("repository") or "").strip()
    _set("REPO", repo or os.environ.get("GITHUB_REPOSITORY", ""), overwrite=overwrite)
    _set("WORKFLOW_FILE", cfg.get("workflow_file"), overwrite=overwrite)
    _set("WORKFLOW_NAME", cfg.get("workflow_name"), overwrite=overwrite)
    _set("REMOTE_WORKFLOW_NAME", cfg.get("workflow_name"), overwrite=overwrite)
    _set("EXECUTOR_REF", cfg.get("executor_ref"), overwrite=overwrite)

    inputs = cfg.get("inputs")
    if inputs is not None:
        _set("INPUTS_JSON", inputs if isinstance(inputs, str) else json.dumps(inputs), overwrite=overwrite)

    wait = cfg.get("wait")
    if wait is not None:
        mode = "wait+mirror" if (wait is True or str(wait).lower() in ("1", "true", "yes")) else "fire-and-forget"
        _set("WAIT_MODE", mode, overwrite=overwrite)
    _set("FAIL_ON_REMOTE_FAILURE", cfg.get("fail_on_remote_failure"), overwrite=overwrite)

    endpoint = (cfg.get("summary_s3_endpoint") or "").strip()
    bucket = (cfg.get("summary_s3_bucket") or "").strip()
    # Config wins over workflow fallbacks (vars.AWS_* pre-set in the job env).
    if endpoint:
        os.environ["AWS_ENDPOINT"] = endpoint
        os.environ["REMOTE_AWS_ENDPOINT"] = endpoint
    if bucket:
        os.environ["AWS_BUCKET"] = bucket
        os.environ["REMOTE_AWS_BUCKET"] = bucket

    _set("ANALYTICS_JOB_NAME", cfg.get("analytics_job_name"), overwrite=overwrite)
    _set("CALLER_WORKFLOW", cfg.get("caller_workflow"), overwrite=overwrite)
    return cfg
