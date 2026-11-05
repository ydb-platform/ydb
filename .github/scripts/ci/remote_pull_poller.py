#!/usr/bin/env python3
"""Remote-host poller: public tickets → local executor dispatch → claim on local S3.

Env:
  GH_TOKEN              — this repo (github.token)
  GITHUB_REPOSITORY     — this (executor) repo
  PLATFORM_REPOSITORY   — e.g. ydb-platform/ydb
  TICKET_BASE_URL       — public base …/{bucket} (no trailing slash)
  CLAIM_S3_BUCKET       — local public bucket for remote-claims/
  WORKFLOW_FILE         — executor workflow file (default ci_remote_executor.yml)
  EXECUTOR_REF          — ref to run (default: github.ref_name or main)
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


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


def http_get(url: str, timeout: int = 60) -> str | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.read().decode()
    except Exception as e:
        print(f"GET {url} -> {e}")
        return None


def gh(method: str, url: str, body=None, ok=(200, 201, 202, 204)):
    data = None if body is None else json.dumps(body).encode()
    headers = {
        "Authorization": f"Bearer {env('GH_TOKEN')}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read()
            if resp.status not in ok:
                die(f"GitHub {method} {url} -> HTTP {resp.status}")
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        if e.code in ok:
            return e.code, (json.loads(raw) if raw else None)
        die(f"GitHub {method} {url} -> HTTP {e.code}: {raw}")


def coerce_inputs(raw: dict) -> dict:
    out = {}
    for k, v in raw.items():
        if v is None:
            continue
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (dict, list)):
            out[k] = json.dumps(v, separators=(",", ":"))
        else:
            out[k] = str(v)
    return out


def s3cmd_put(path: Path, s3_uri: str) -> None:
    cmd = [
        "s3cmd", "put", "--acl-public", "--no-progress",
        "--no-mime-magic", "--guess-mime-type", str(path), s3_uri,
    ]
    print("+", " ".join(cmd), flush=True)
    subprocess.check_call(cmd)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def list_platform_run_ids(platform_repo: str) -> list[str]:
    """Recent in-progress / queued runs on the platform repo."""
    ids: list[str] = []
    for status in ("in_progress", "queued"):
        url = (
            f"https://api.github.com/repos/{platform_repo}/actions/runs"
            f"?status={status}&per_page=50"
        )
        _, data = gh("GET", url)
        for r in (data or {}).get("workflow_runs") or []:
            rid = str(r.get("id") or "")
            if rid:
                ids.append(rid)
    # unique, keep order
    seen = set()
    out = []
    for rid in ids:
        if rid not in seen:
            seen.add(rid)
            out.append(rid)
    return out


def dispatch_executor(wf_file: str, ref: str, inputs: dict) -> tuple[str, str]:
    host_repo = env("GITHUB_REPOSITORY")
    _, wf = gh("GET", f"https://api.github.com/repos/{host_repo}/actions/workflows/{wf_file}")
    workflow_id = wf.get("id")
    if not workflow_id:
        die(f"workflow {wf_file} not found in {host_repo}")

    before = datetime.now(timezone.utc).timestamp() - 5
    code, _ = gh(
        "POST",
        f"https://api.github.com/repos/{host_repo}/actions/workflows/{workflow_id}/dispatches",
        body={"ref": ref, "inputs": coerce_inputs(inputs)},
    )
    if code != 204:
        die(f"dispatch failed HTTP {code}")

    for _ in range(30):
        time.sleep(2)
        _, runs = gh(
            "GET",
            f"https://api.github.com/repos/{host_repo}/actions/workflows/{workflow_id}/runs"
            f"?branch={urllib.parse.quote(ref)}&event=workflow_dispatch&per_page=10",
        )
        for r in (runs or {}).get("workflow_runs") or []:
            created = datetime.fromisoformat(r["created_at"].replace("Z", "+00:00")).timestamp()
            if created >= before:
                return str(r["id"]), r["html_url"]
    die("could not resolve executor run id after dispatch")


def write_claim(platform_run_id: str, executor_run_id: str, executor_run_url: str) -> None:
    bucket = env("CLAIM_S3_BUCKET")
    key = f"remote-claims/{platform_run_id}.json"
    claim = {
        "version": 1,
        "platform_run_id": platform_run_id,
        "executor_run_id": executor_run_id,
        "executor_run_url": executor_run_url,
        "executor_repository": env("GITHUB_REPOSITORY"),
        "claimed_at": utc_now(),
    }
    with tempfile.TemporaryDirectory(prefix="remote-claim-") as tmp:
        path = Path(tmp) / "claim.json"
        path.write_text(json.dumps(claim, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        s3cmd_put(path, f"s3://{bucket}/{key}")
    print(f"Wrote claim s3://{bucket}/{key}")


def process_ticket(platform_run_id: str, ticket_base: str, wf_file: str, ref: str) -> bool:
    ticket_url = f"{ticket_base.rstrip('/')}/remote-tickets/{platform_run_id}.json"
    cancel_url = f"{ticket_base.rstrip('/')}/remote-tickets/{platform_run_id}.cancel"
    claim_base = env_opt("CLAIM_PUBLIC_BASE")  # optional; else only s3 existence via GET if set

    raw = http_get(ticket_url)
    if not raw:
        return False
    if http_get(cancel_url):
        print(f"skip {platform_run_id}: cancel marker present")
        return False

    # Skip if claim already public (same bucket URL pattern when provided).
    if claim_base:
        if http_get(f"{claim_base.rstrip('/')}/remote-claims/{platform_run_id}.json"):
            print(f"skip {platform_run_id}: already claimed")
            return False

    ticket = json.loads(raw)
    cfg = ticket.get("config") or {}
    inputs = cfg.get("inputs")
    if not isinstance(inputs, dict):
        print(f"::warning::ticket {platform_run_id} missing config.inputs")
        return False

    print(f"Claiming platform_run_id={platform_run_id}")
    executor_run_id, executor_run_url = dispatch_executor(wf_file, ref, inputs)
    write_claim(platform_run_id, executor_run_id, executor_run_url)
    print(f"Dispatched executor {executor_run_id} for ticket {platform_run_id}")
    return True


def main() -> int:
    platform_repo = env("PLATFORM_REPOSITORY")
    ticket_base = env("TICKET_BASE_URL")
    wf_file = env_opt("WORKFLOW_FILE") or "ci_remote_executor.yml"
    ref = env_opt("EXECUTOR_REF") or env_opt("GITHUB_REF_NAME") or "main"

    run_ids = list_platform_run_ids(platform_repo)
    print(f"platform open runs: {len(run_ids)}")
    claimed = 0
    for rid in run_ids:
        try:
            if process_ticket(rid, ticket_base, wf_file, ref):
                claimed += 1
        except Exception as e:
            print(f"::warning::ticket {rid} failed: {e}")
    print(f"claimed={claimed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
