#!/usr/bin/env python3
"""Shared helpers for platform remote dispatcher / ingest scripts."""
from __future__ import annotations

import json
import os
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


def die(msg: str) -> None:
    print(f"::error::{msg}", file=sys.stderr)
    sys.exit(1)


def env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if v is None or v == "":
        die(f"missing env {name}")
    return v


def env_opt(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def truthy(v: Any, default: bool = False) -> bool:
    if v is None or v == "":
        return default
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("1", "true", "yes")


def truthy_env(name: str, default: str = "true") -> bool:
    return truthy(os.environ.get(name, default), default=truthy(default))


def quote_url(url: str) -> str:
    return urllib.parse.quote(url, safe=":/%")


def http_get_bytes(url: str, timeout: int = 60) -> bytes | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.read()
    except Exception as e:
        print(f"GET {url} failed: {e}")
        return None


def http_get_text(url: str, timeout: int = 60) -> str | None:
    raw = http_get_bytes(url, timeout=timeout)
    return None if raw is None else raw.decode()


def append_summary(text: str) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        f.write(text if text.endswith("\n") else text + "\n")


def write_step_summary(text: str) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    with open(path, "w", encoding="utf-8") as f:
        f.write(text if text.endswith("\n") else text + "\n")


def set_outputs(**kwargs: Any) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        for k, v in kwargs.items():
            f.write(f"{k}={v}\n")


def load_config_json(raw: str | None = None) -> dict[str, Any]:
    text = (raw if raw is not None else os.environ.get("CONFIG_JSON", "")).strip()
    if not text:
        return {}
    try:
        cfg = json.loads(text)
    except json.JSONDecodeError as e:
        die(f"CONFIG_JSON is not valid JSON: {e}")
    if not isinstance(cfg, dict):
        die("CONFIG_JSON must be an object")
    return cfg


def discover_attempts(
    public_dir: Path, meta_attempts: list[dict[str, Any]] | None = None
) -> list[dict[str, Any]]:
    """Prefer manifest attempts; else scan try_*/report.json under public_dir."""
    if meta_attempts:
        return list(meta_attempts)
    found: list[dict[str, Any]] = []
    if not public_dir.is_dir():
        return found
    for report in sorted(public_dir.glob("try_*/report.json")):
        m = re.match(r"try_(\d+)$", report.parent.name)
        if not m:
            continue
        retry = int(m.group(1))
        started_file = report.parent / "started_at"
        if started_file.is_file():
            started_at = started_file.read_text(encoding="utf-8").strip() or "0"
        else:
            started_at = str(int(report.stat().st_mtime))
        found.append(
            {
                "retry": retry,
                "started_at": started_at,
                "report_relpath": str(report.relative_to(public_dir)),
                "html_relpath": str((report.parent / "ya-test.html").relative_to(public_dir)),
            }
        )
    return found


def apply_config_env(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """Map resolve config → env for dispatch/ingest.

    S3 endpoints stay in YAML / claim_s3(cfg): resolve fills claim_* from summary_*,
    so scripts do not re-fallback or mirror AWS_* ↔ REMOTE_AWS_*.
    """
    cfg = cfg if cfg is not None else load_config_json()
    if not cfg:
        return cfg

    repo = (cfg.get("repository") or "").strip() or env_opt("GITHUB_REPOSITORY")
    if repo:
        os.environ["REPO"] = repo

    # Host artifact tree is under the executor workflow name.
    workflow_name = (cfg.get("workflow_name") or "").strip()
    if workflow_name:
        os.environ["REMOTE_WORKFLOW_NAME"] = workflow_name

    analytics = (cfg.get("analytics_job_name") or "").strip()
    if analytics:
        os.environ["ANALYTICS_JOB_NAME"] = analytics
    caller_wf = (cfg.get("caller_workflow") or "").strip()
    if caller_wf:
        os.environ["CALLER_WORKFLOW"] = caller_wf

    if "fail_on_remote_failure" in cfg:
        os.environ["FAIL_ON_REMOTE_FAILURE"] = (
            "true" if truthy(cfg.get("fail_on_remote_failure"), True) else "false"
        )
    return cfg
