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

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import (  # noqa: E402
    append_summary,
    apply_config_env,
    die,
    env,
    env_opt,
    http_get_text,
    load_config_json,
    set_outputs,
    truthy_env,
)

TICKET_ARTIFACT_NAME = "remote-ticket"
TICKET_FILENAME = "remote-ticket.json"


def api():
    return f"https://api.github.com/repos/{env('REPO')}"


def gh(method, url, body=None, retries=1, ok_codes=(200, 201, 202, 204)):
    data = None if body is None else json.dumps(body).encode()
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    token = os.environ.get("GH_TOKEN") or ""
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if data is not None:
        headers["Content-Type"] = "application/json"
    last_err = None
    for attempt in range(1, retries + 1):
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                raw = resp.read()
                code = resp.status
                if code not in ok_codes:
                    die(f"GitHub {method} {url} -> HTTP {code}")
                return code, json.loads(raw) if raw else None
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
            die(f"GitHub {method} {url} failed: {e}")
    die(f"GitHub {method} {url} failed: {last_err}")


def load_config():
    return load_config_json()


def claim_s3(cfg):
    """Host public S3 for claims — resolve already fills claim_* from summary_*."""
    ep = (cfg.get("claim_s3_endpoint") or cfg.get("summary_s3_endpoint") or "").strip() or env_opt(
        "AWS_ENDPOINT"
    )
    bucket = (cfg.get("claim_s3_bucket") or cfg.get("summary_s3_bucket") or "").strip() or env_opt(
        "AWS_BUCKET"
    )
    if not ep or not bucket:
        die("claim S3 endpoint/bucket missing (remote host public S3)")
    return ep.rstrip("/"), bucket


def public_url(endpoint, bucket, key):
    return f"{endpoint}/{bucket}/{key}"


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def platform_run_attempt():
    """Live github.run_attempt — env wins so re-runs ignore stale resolve output."""
    return env_opt("PLATFORM_RUN_ATTEMPT") or env_opt("GITHUB_RUN_ATTEMPT") or "1"


def read_claim(cfg, platform_run_id: str) -> dict | None:
    endpoint, bucket = claim_s3(cfg)
    body = http_get_text(public_url(endpoint, bucket, f"remote-claims/{platform_run_id}.json"))
    if not body:
        return None
    claim = json.loads(body)
    return claim if isinstance(claim, dict) else None


def build_ticket(cfg):
    run_id = env("PLATFORM_RUN_ID")
    attempt = platform_run_attempt()
    repo = env_opt("PLATFORM_REPOSITORY") or env_opt("GITHUB_REPOSITORY")
    workflow = env_opt("PLATFORM_WORKFLOW") or env_opt("GITHUB_WORKFLOW") or "unknown"
    run_url = env_opt("PLATFORM_RUN_URL") or f"https://github.com/{repo}/actions/runs/{run_id}"
    if isinstance(cfg.get("inputs"), dict):
        cfg["inputs"]["platform_run_attempt"] = attempt
    return {
        "version": 1,
        "kind": "remote-ticket",
        "platform_run_id": run_id,
        "platform_run_attempt": attempt,
        "platform_repository": repo,
        "platform_workflow": workflow,
        "platform_run_url": run_url,
        "config": cfg,
        "created_at": utc_now(),
    }


def cmd_enqueue():
    cfg = load_config()
    ticket = build_ticket(cfg)
    run_id = ticket["platform_run_id"]
    run_url = ticket["platform_run_url"]
    path = Path(env_opt("TICKET_PATH") or TICKET_FILENAME)
    path.write_text(json.dumps(ticket, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {path} ({path.stat().st_size} bytes)")

    set_outputs(
        platform_run_id=run_id,
        ticket_path=str(path),
        ticket_artifact=TICKET_ARTIFACT_NAME,
    )
    append_summary(
        f"### Remote enqueue\n"
        f"- **platform run:** [{run_id}]({run_url})\n"
        f"- **ticket artifact:** `{TICKET_ARTIFACT_NAME}`\n"
        f"- **claim (host S3):** `remote-claims/{run_id}.json`\n\n"
    )
    print(f"::notice title=Remote ticket artifact ready::{TICKET_ARTIFACT_NAME} on run {run_id}")


def wait_for_claim(cfg):
    platform_run_id = env("PLATFORM_RUN_ID")
    want_attempt = platform_run_attempt()
    endpoint, bucket = claim_s3(cfg)
    url = public_url(endpoint, bucket, f"remote-claims/{platform_run_id}.json")
    print(f"Waiting for claim at {url} (attempt={want_attempt}) ...")
    for i in range(2400):
        claim = read_claim(cfg, platform_run_id)
        if claim:
            status = str(claim.get("status") or "").strip().lower()
            claim_attempt = str(claim.get("platform_run_attempt") or "").strip()
            if claim_attempt and claim_attempt != want_attempt:
                if i % 10 == 0:
                    print(
                        f"{time.strftime('%H:%M:%S', time.gmtime())} "
                        f"stale claim attempt={claim_attempt} (want {want_attempt}); waiting..."
                    )
                time.sleep(30)
                continue
            if status == "failed":
                die(f"remote claim failed: {claim.get('error') or claim}")
            run_id = str(claim.get("executor_run_id") or "").strip()
            if run_id:
                run_url = str(claim.get("executor_run_url") or "").strip() or (
                    f"https://github.com/{claim.get('executor_repository') or env('REPO')}/actions/runs/{run_id}"
                )
                return run_id, run_url
        if i % 10 == 0:
            print(f"{time.strftime('%H:%M:%S', time.gmtime())} waiting for claim...")
        time.sleep(30)
    die(f"timed out waiting for claim at {url}")


def wait_executor_done(run_id, run_url):
    fail_on_remote = truthy_env("FAIL_ON_REMOTE_FAILURE", "true")
    print(f"Waiting for executor run {run_id} ({run_url}) ...")
    status = conclusion = ""
    for _ in range(2400):
        _, state = gh("GET", f"{api()}/actions/runs/{run_id}")
        status = state.get("status") or ""
        conclusion = state.get("conclusion") or ""
        print(f"{time.strftime('%H:%M:%S', time.gmtime())} status={status} conclusion={conclusion or 'none'}")
        if status == "completed":
            break
        time.sleep(30)
    if status != "completed":
        die(f"timed out waiting for executor run {run_id} (last status={status})")

    set_outputs(executor_conclusion=conclusion or "")
    append_summary(
        f"Remote executor [{run_id}]({run_url}) finished with `{conclusion}`.\n"
        f"_Resulting summary is published by the ingest job after reupload to caller S3._\n"
    )

    if conclusion == "success":
        return
    # Duplicate host dispatch can cancel this run; follow the claim if it moved.
    if conclusion == "cancelled":
        cfg = load_config()
        want_attempt = platform_run_attempt()
        for _ in range(40):
            claim = read_claim(cfg, env("PLATFORM_RUN_ID"))
            if claim:
                claim_attempt = str(claim.get("platform_run_attempt") or "").strip()
                new_id = str(claim.get("executor_run_id") or "").strip()
                if (
                    new_id
                    and new_id != run_id
                    and (not claim_attempt or claim_attempt == want_attempt)
                    and str(claim.get("status") or "").strip().lower() != "failed"
                ):
                    new_url = str(claim.get("executor_run_url") or "").strip() or (
                        f"https://github.com/{claim.get('executor_repository') or env('REPO')}/actions/runs/{new_id}"
                    )
                    print(f"::notice::executor {run_id} cancelled; following new claim {new_id}")
                    set_outputs(executor_run_id=new_id, executor_run_url=new_url)
                    return wait_executor_done(new_id, new_url)
            time.sleep(15)
    msg = f"executor finished with conclusion={conclusion}"
    if fail_on_remote:
        die(msg)
    print(f"::warning::{msg} (fail_on_remote_failure=false)")


def cmd_wait():
    cfg = load_config()
    run_id, run_url = wait_for_claim(cfg)
    set_outputs(executor_run_id=run_id, executor_run_url=run_url)
    os.environ["REPO"] = (cfg.get("repository") or "").strip() or env_opt("REPO")
    os.environ["WORKFLOW_NAME"] = (cfg.get("workflow_name") or "").strip() or env_opt("WORKFLOW_NAME")
    ep, bucket = claim_s3(cfg)
    os.environ["AWS_ENDPOINT"] = ep
    os.environ["AWS_BUCKET"] = bucket
    wait_executor_done(run_id, run_url)


def cmd_cancel():
    run_id = env_opt("EXECUTOR_RUN_ID")
    if not run_id:
        cfg = load_config()
        platform_run_id = env_opt("PLATFORM_RUN_ID")
        if platform_run_id:
            claim = read_claim(cfg, platform_run_id)
            if claim:
                run_id = str(claim.get("executor_run_id") or "").strip()
    if not run_id or not env_opt("GH_TOKEN"):
        append_summary("### Caller cancelled\n- no executor run id to cancel yet\n")
        return
    run_url = env_opt("EXECUTOR_RUN_URL") or f"https://github.com/{env('REPO')}/actions/runs/{run_id}"
    code, _ = gh(
        "POST",
        f"{api()}/actions/runs/{run_id}/cancel",
        retries=3,
        ok_codes=(202, 204, 409),
    )
    append_summary(f"### Caller cancelled\n- executor [{run_id}]({run_url}) HTTP {code}\n")


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ("enqueue", "start", "wait", "cancel"):
        die("usage: dispatcher/dispatch_remote_executor.py enqueue|start|wait|cancel")
    apply_config_env()
    cmd = sys.argv[1]
    if cmd in ("enqueue", "start"):
        cmd_enqueue()
    elif cmd == "wait":
        cmd_wait()
    else:
        cmd_cancel()


if __name__ == "__main__":
    main()
