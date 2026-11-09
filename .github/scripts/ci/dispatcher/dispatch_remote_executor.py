#!/usr/bin/env python3
"""Platform remote dispatcher: publish remote-ticket artifact, wait for claim, cancel.

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
)

TICKET_FILENAME = "remote-ticket.json"
# Polls are 30s apart: ~60 min for a claim, 24 h for the executor run
# (2880 × 30s), matching the host execute job timeout of 1440 minutes.
CLAIM_WAIT_ATTEMPTS = 120
EXECUTOR_WAIT_ATTEMPTS = 2880


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


def _attempt_int(raw: str, *, label: str) -> int:
    s = str(raw or "").strip()
    if not s.isdigit():
        die(f"{label} must be a positive integer, got {raw!r}")
    n = int(s)
    if n < 1:
        die(f"{label} must be >= 1, got {n}")
    return n


def cmd_enqueue():
    cfg = load_config_json()
    ticket = build_ticket(cfg)
    path = Path(TICKET_FILENAME)
    path.write_text(json.dumps(ticket, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {path} ({path.stat().st_size} bytes)")
    print(f"::notice title=Remote ticket artifact ready::{TICKET_FILENAME} on run {ticket['platform_run_id']}")
    # Pin waiters (incl. Re-run failed jobs) to the attempt that wrote this ticket.
    set_outputs(ticket_attempt=ticket["platform_run_attempt"])


def wait_for_claim(cfg):
    platform_run_id = env("PLATFORM_RUN_ID")
    want_attempt = _attempt_int(env("PLATFORM_RUN_ATTEMPT"), label="PLATFORM_RUN_ATTEMPT")
    endpoint, bucket = host_s3(cfg)
    url = object_url(endpoint, bucket, f"remote-claims/{platform_run_id}.json")
    print(f"Waiting for claim at {url} (ticket_attempt={want_attempt}) ...")
    for i in range(CLAIM_WAIT_ATTEMPTS):
        claim = read_claim(cfg, platform_run_id)
        if claim:
            claim_attempt_raw, run_id, run_url = parse_claim(claim)
            claim_attempt = _attempt_int(claim_attempt_raw, label="claim.platform_run_attempt")
            if claim_attempt > want_attempt:
                die(
                    f"host claimed a newer ticket attempt {claim_attempt} "
                    f"(this waiter wants {want_attempt}); this attempt is obsolete"
                )
            if claim_attempt < want_attempt:
                if i % 10 == 0:
                    print(
                        f"{time.strftime('%H:%M:%S', time.gmtime())} "
                        f"stale claim attempt={claim_attempt} (want {want_attempt}); waiting... "
                        f"(if this is Re-run failed jobs, use Re-run all jobs for a fresh executor)"
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
    die(
        f"ticket not claimed in time ({CLAIM_WAIT_ATTEMPTS}x30s) at {url} "
        f"(ticket_attempt={want_attempt}); check host VM / webhook delivery"
    )


def wait_executor_done(cfg, token: str, run_id, run_url):
    repo = cfg_str(cfg, "repository")
    print(f"Waiting for executor run {run_id} ({run_url}) ...")
    status = conclusion = ""
    for _ in range(EXECUTOR_WAIT_ATTEMPTS):
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
        want_attempt = _attempt_int(env("PLATFORM_RUN_ATTEMPT"), label="PLATFORM_RUN_ATTEMPT")
        for _ in range(40):
            claim = read_claim(cfg, env("PLATFORM_RUN_ID"))
            if claim:
                claim_attempt_raw, new_id, new_url = parse_claim(claim)
                claim_attempt = _attempt_int(
                    claim_attempt_raw, label="claim.platform_run_attempt"
                )
                if new_id and new_id != run_id and claim_attempt == want_attempt and new_url:
                    print(f"::notice::executor {run_id} cancelled; following new claim {new_id}")
                    set_outputs(executor_run_id=new_id, executor_run_url=new_url)
                    return wait_executor_done(cfg, token, new_id, new_url)
            time.sleep(15)
    die(f"executor finished with conclusion={conclusion}")


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


COMMANDS = {"enqueue": cmd_enqueue, "wait": cmd_wait, "cancel": cmd_cancel}


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in COMMANDS:
        die("usage: dispatcher/dispatch_remote_executor.py enqueue|wait|cancel")
    COMMANDS[sys.argv[1]]()


if __name__ == "__main__":
    main()
