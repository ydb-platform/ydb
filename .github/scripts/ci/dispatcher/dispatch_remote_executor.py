#!/usr/bin/env python3
"""Platform remote dispatcher: publish remote-ticket artifact, wait for claim, cancel.

Reusable from Run-and-debug / PR-check / etc. via dispatch_remote.yml.
Does not start the remote executor — the host VM does that after the webhook.
The executor writes remote-claims/{platform_run_id}.json on *its* public S3.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import (  # noqa: E402
    append_summary,
    cfg_str,
    die,
    env,
    env_opt,
    host_s3,
    http_get_text,
    load_config_json,
    object_url,
    set_outputs,
    truthy,
)

TICKET_FILENAME = "remote-ticket.json"


def api(repo: str) -> str:
    return f"https://api.github.com/repos/{repo}"


def require_gh_token() -> str:
    token = env_opt("GH_TOKEN")
    if not token:
        die("GH_TOKEN required to poll/cancel the host executor (anonymous rate limit is too low)")
    return token


def gh(method, url, token: str, body=None, retries=5, ok_codes=(200, 201, 202, 204)):
    data = None if body is None else json.dumps(body).encode()
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Authorization": f"Bearer {token}",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    last_err = None
    for attempt in range(1, retries + 1):
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                raw = resp.read()
                return resp.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            raw = e.read()
            last_err = (e.code, raw.decode(errors="replace"))
            if e.code in ok_codes:
                return e.code, json.loads(raw) if raw else None
            if e.code in (429, 500, 502, 503, 504) and attempt < retries:
                print(f"HTTP {e.code}, retry {attempt}/{retries} ...")
                print(last_err[1])
                time.sleep(attempt * 5)
                continue
            die(f"GitHub {method} {url} -> HTTP {e.code}: {last_err[1]}")
        except urllib.error.URLError as e:
            if attempt < retries:
                print(f"URL error {e}, retry {attempt}/{retries} ...")
                time.sleep(attempt * 5)
                continue
            die(f"GitHub {method} {url} failed: {e}")
    die(f"GitHub {method} {url} failed: {last_err}")


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_claim(cfg, platform_run_id: str) -> dict | None:
    endpoint, bucket = host_s3(cfg)
    body = http_get_text(object_url(endpoint, bucket, f"remote-claims/{platform_run_id}.json"))
    if not body:
        return None
    claim = json.loads(body)
    return claim if isinstance(claim, dict) else None


def parse_claim(claim: dict[str, Any]) -> tuple[str, str, str]:
    attempt = str(claim.get("platform_run_attempt") or "").strip()
    run_id = str(claim.get("executor_run_id") or "").strip()
    run_url = str(claim.get("executor_run_url") or "").strip()
    return attempt, run_id, run_url


def build_ticket(cfg):
    """Ticket for the host VM: dispatch uses config.inputs only."""
    run_id = env("PLATFORM_RUN_ID")
    attempt = env("PLATFORM_RUN_ATTEMPT")
    repo = env("PLATFORM_REPOSITORY")
    workflow = env("PLATFORM_WORKFLOW")
    run_url = env("PLATFORM_RUN_URL")
    inputs = cfg.get("inputs")
    if not isinstance(inputs, dict):
        die("CONFIG_JSON.inputs missing")
    inputs = dict(inputs)
    inputs["platform_run_id"] = run_id
    inputs["platform_run_attempt"] = attempt
    inputs["platform_repository"] = repo
    cfg = dict(cfg)
    cfg["inputs"] = inputs
    return {
        "version": 1,
        "kind": "remote-ticket",
        # Top-level ids for humans/logs; VM must dispatch from config.inputs.
        "platform_run_id": run_id,
        "platform_run_attempt": attempt,
        "platform_repository": repo,
        "platform_workflow": workflow,
        "platform_run_url": run_url,
        "config": cfg,
        "created_at": utc_now(),
    }


def cmd_enqueue():
    cfg = load_config_json()
    ticket = build_ticket(cfg)
    path = Path(TICKET_FILENAME)
    path.write_text(json.dumps(ticket, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {path} ({path.stat().st_size} bytes)")
    print(f"::notice title=Remote ticket artifact ready::{TICKET_FILENAME} on run {ticket['platform_run_id']}")


def wait_for_claim(cfg):
    platform_run_id = env("PLATFORM_RUN_ID")
    want_attempt = env("PLATFORM_RUN_ATTEMPT")
    endpoint, bucket = host_s3(cfg)
    url = object_url(endpoint, bucket, f"remote-claims/{platform_run_id}.json")
    print(f"Waiting for claim at {url} (attempt={want_attempt}) ...")
    for i in range(2400):
        claim = read_claim(cfg, platform_run_id)
        if claim:
            claim_attempt, run_id, run_url = parse_claim(claim)
            if claim_attempt != want_attempt:
                if i % 10 == 0:
                    print(
                        f"{time.strftime('%H:%M:%S', time.gmtime())} "
                        f"stale claim attempt={claim_attempt!r} (want {want_attempt}); waiting..."
                    )
                time.sleep(30)
                continue
            if run_id and run_url:
                return run_id, run_url
            die(
                f"malformed claim at {url}: "
                f"executor_run_id={run_id!r} executor_run_url={run_url!r}"
            )
        if i % 10 == 0:
            print(f"{time.strftime('%H:%M:%S', time.gmtime())} waiting for claim...")
        time.sleep(30)
    die(f"timed out waiting for claim at {url}")


def wait_executor_done(cfg, token: str, run_id, run_url):
    fail_on_remote = truthy(cfg.get("fail_on_remote_failure"), True)
    repo = cfg_str(cfg, "repository")
    print(f"Waiting for executor run {run_id} ({run_url}) ...")
    status = conclusion = ""
    for _ in range(2400):
        _, state = gh("GET", f"{api(repo)}/actions/runs/{run_id}", token, retries=5)
        status = state.get("status") or ""
        conclusion = state.get("conclusion") or ""
        print(f"{time.strftime('%H:%M:%S', time.gmtime())} status={status} conclusion={conclusion or 'none'}")
        if status == "completed":
            break
        time.sleep(30)
    if status != "completed":
        die(f"timed out waiting for executor run {run_id} (last status={status})")

    set_outputs(executor_conclusion=conclusion)
    append_summary(
        f"Remote executor [{run_id}]({run_url}) finished with `{conclusion}`.\n"
        f"_Resulting summary is published by the ingest job after reupload to caller S3._\n"
    )

    if conclusion == "success":
        return
    # Defence-in-depth if the VM cancels+relaunches on webhook redelivery.
    if conclusion == "cancelled":
        want_attempt = env("PLATFORM_RUN_ATTEMPT")
        for _ in range(40):
            claim = read_claim(cfg, env("PLATFORM_RUN_ID"))
            if claim:
                claim_attempt, new_id, new_url = parse_claim(claim)
                if new_id and new_id != run_id and claim_attempt == want_attempt and new_url:
                    print(f"::notice::executor {run_id} cancelled; following new claim {new_id}")
                    set_outputs(executor_run_id=new_id, executor_run_url=new_url)
                    return wait_executor_done(cfg, token, new_id, new_url)
            time.sleep(15)
    msg = f"executor finished with conclusion={conclusion}"
    if fail_on_remote:
        die(msg)
    print(f"::warning::{msg} (fail_on_remote_failure=false)")


def cmd_wait():
    token = require_gh_token()
    cfg = load_config_json()
    run_id, run_url = wait_for_claim(cfg)
    set_outputs(executor_run_id=run_id, executor_run_url=run_url)
    wait_executor_done(cfg, token, run_id, run_url)


def cmd_cancel():
    token = require_gh_token()
    cfg = load_config_json()
    repo = cfg_str(cfg, "repository")
    run_id = env_opt("EXECUTOR_RUN_ID")
    run_url = env_opt("EXECUTOR_RUN_URL")
    if not run_id:
        platform_run_id = env_opt("PLATFORM_RUN_ID")
        if platform_run_id:
            claim = read_claim(cfg, platform_run_id)
            if claim:
                _, run_id, claim_url = parse_claim(claim)
                run_url = run_url or claim_url
    if not run_id:
        append_summary("### Caller cancelled\n- no executor run id to cancel yet\n")
        return
    run_url = run_url or f"https://github.com/{repo}/actions/runs/{run_id}"
    code, _ = gh(
        "POST",
        f"{api(repo)}/actions/runs/{run_id}/cancel",
        token,
        retries=3,
        ok_codes=(202, 204, 409),
    )
    append_summary(f"### Caller cancelled\n- executor [{run_id}]({run_url}) HTTP {code}\n")


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ("enqueue", "wait", "cancel"):
        die("usage: dispatcher/dispatch_remote_executor.py enqueue|wait|cancel")
    cmd = sys.argv[1]
    if cmd == "enqueue":
        cmd_enqueue()
    elif cmd == "wait":
        cmd_wait()
    else:
        cmd_cancel()


if __name__ == "__main__":
    main()
