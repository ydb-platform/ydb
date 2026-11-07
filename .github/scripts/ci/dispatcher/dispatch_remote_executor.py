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
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from remote_dispatch_config import apply_to_environ  # noqa: E402
TICKET_ARTIFACT_NAME = "remote-ticket"
TICKET_FILENAME = "remote-ticket.json"


def die(msg):
    print(f"::error::{msg}", file=sys.stderr)
    sys.exit(1)


def env(name, default=None):
    v = os.environ.get(name, default)
    if v is None or v == "":
        die(f"missing env {name}")
    return v


def env_opt(name, default=""):
    return (os.environ.get(name) or default).strip()


def api():
    return f"https://api.github.com/repos/{env('REPO')}"


def append_summary(text):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(text if text.endswith("\n") else text + "\n")


def set_outputs(**kwargs):
    path = os.environ.get("GITHUB_OUTPUT")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        for k, v in kwargs.items():
            f.write(f"{k}={v}\n")


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


def http_get(url, timeout=60):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.read().decode()
    except Exception as e:
        print(f"GET {url} failed: {e}")
        return None


def quote_url(url):
    return urllib.parse.quote(url, safe=":/%")


def _truthy_env(name, default="true"):
    return str(os.environ.get(name, default)).lower() in ("1", "true", "yes")


def load_config():
    raw = env_opt("CONFIG_JSON")
    if not raw:
        return {}
    cfg = json.loads(raw)
    if not isinstance(cfg, dict):
        die("CONFIG_JSON must be an object")
    return cfg


def claim_s3(cfg):
    ep = (
        (cfg.get("claim_s3_endpoint") or "").strip()
        or (cfg.get("summary_s3_endpoint") or "").strip()
        or env_opt("AWS_ENDPOINT")
    )
    bucket = (
        (cfg.get("claim_s3_bucket") or "").strip()
        or (cfg.get("summary_s3_bucket") or "").strip()
        or env_opt("AWS_BUCKET")
    )
    if not ep or not bucket:
        die("claim S3 endpoint/bucket missing (remote host public S3)")
    return ep.rstrip("/"), bucket


def public_url(endpoint, bucket, key):
    return f"{endpoint}/{bucket}/{key}"


def run_root_file_url(run_id, filename):
    base = (
        f"{env('AWS_ENDPOINT')}/{env('AWS_BUCKET')}/{env('REPO')}/"
        f"{env('WORKFLOW_NAME')}/{run_id}/{filename}"
    )
    return quote_url(base)


def fetch_retries(url, attempts=20, sleep_s=3):
    for _ in range(attempts):
        body = http_get(url)
        if body:
            return body
        time.sleep(sleep_s)
    return None


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def platform_run_attempt(cfg=None):
    """Caller run attempt — distinguishes re-runs that keep the same run_id.

    Env wins over ticket/config so "Re-run failed jobs" (stale resolve output)
    still matches the current github.run_attempt.
    """
    from_env = env_opt("PLATFORM_RUN_ATTEMPT") or env_opt("GITHUB_RUN_ATTEMPT")
    if from_env:
        return from_env
    if cfg and isinstance(cfg.get("inputs"), dict):
        from_cfg = str(cfg["inputs"].get("platform_run_attempt") or "").strip()
        if from_cfg:
            return from_cfg
    return "1"


def build_ticket(cfg):
    run_id = env("PLATFORM_RUN_ID")
    attempt = platform_run_attempt(cfg)
    repo = env_opt("PLATFORM_REPOSITORY") or env_opt("GITHUB_REPOSITORY")
    workflow = env_opt("PLATFORM_WORKFLOW") or env_opt("GITHUB_WORKFLOW") or "unknown"
    run_url = env_opt("PLATFORM_RUN_URL") or f"https://github.com/{repo}/actions/runs/{run_id}"
    # Force live attempt onto config.inputs (override stale resolve on re-run).
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
        executor_run_id="",
        executor_run_url="",
    )
    # Launch params already in resolve_remote / ingest summaries; keep enqueue thin.
    append_summary(
        f"### Remote enqueue\n"
        f"- **platform run:** [{run_id}]({run_url})\n"
        f"- **ticket artifact:** `{TICKET_ARTIFACT_NAME}`\n"
        f"- **claim (host S3):** `remote-claims/{run_id}.json`\n\n"
    )
    print(f"::notice title=Remote ticket artifact ready::{TICKET_ARTIFACT_NAME} on run {run_id}")


def wait_for_claim(cfg):
    platform_run_id = env("PLATFORM_RUN_ID")
    want_attempt = platform_run_attempt(cfg)
    endpoint, bucket = claim_s3(cfg)
    url = public_url(endpoint, bucket, f"remote-claims/{platform_run_id}.json")
    print(f"Waiting for claim at {url} (attempt={want_attempt}) ...")
    for i in range(2400):
        body = http_get(url)
        if body:
            claim = json.loads(body)
            status = str(claim.get("status") or "").strip().lower()
            claim_attempt = str(claim.get("platform_run_attempt") or "").strip()
            # Ignore leftover claims from a previous platform run attempt.
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
    fail_on_remote = _truthy_env("FAIL_ON_REMOTE_FAILURE", "true")
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
    # Thin wait summary — full rewritten summary is the ingest job's step summary.
    append_summary(
        f"Remote executor [{run_id}]({run_url}) finished with `{conclusion}`.\n"
        f"_Resulting summary is published by the ingest job after reupload to caller S3._\n"
    )

    summary_url = run_root_file_url(run_id, "job_summary.md")
    if not fetch_retries(summary_url):
        art_url = run_root_file_url(run_id, "remote_artifacts.json")
        if fetch_retries(art_url, attempts=5, sleep_s=2):
            print("::warning::missing job_summary.md; remote_artifacts.json present")
        else:
            print("::warning::missing job_summary.md and remote_artifacts.json")

    if conclusion == "success":
        return
    # Duplicate host dispatch can cancel this run; follow the claim if it moved.
    if conclusion == "cancelled":
        cfg = load_config()
        want_attempt = platform_run_attempt(cfg)
        endpoint, bucket = claim_s3(cfg)
        claim_url = public_url(endpoint, bucket, f"remote-claims/{env('PLATFORM_RUN_ID')}.json")
        for _ in range(40):  # ~10 min for replacement claim to land
            body = http_get(claim_url)
            if body:
                claim = json.loads(body)
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
        # Claim may not exist yet; try reading it so we can still cancel.
        cfg = load_config()
        platform_run_id = env_opt("PLATFORM_RUN_ID")
        if platform_run_id:
            endpoint, bucket = claim_s3(cfg)
            body = http_get(public_url(endpoint, bucket, f"remote-claims/{platform_run_id}.json"))
            if body:
                run_id = str(json.loads(body).get("executor_run_id") or "").strip()
    if not run_id or not env_opt("GH_TOKEN"):
        append_summary("### Caller cancelled\n- no executor run id to cancel yet\n")
        return
    run_url = env_opt("EXECUTOR_RUN_URL") or f"https://github.com/{env('REPO')}/actions/runs/{run_id}"
    code, _ = gh(
        "POST", f"{api()}/actions/runs/{run_id}/cancel",
        retries=3, ok_codes=(202, 204, 409),
    )
    append_summary(f"### Caller cancelled\n- executor [{run_id}]({run_url}) HTTP {code}\n")


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ("enqueue", "start", "wait", "cancel"):
        die("usage: dispatcher/dispatch_remote_executor.py enqueue|start|wait|cancel")
    apply_to_environ()
    cmd = sys.argv[1]
    # `start` kept as an alias for enqueue (workflow_call step name).
    if cmd in ("enqueue", "start"):
        cmd_enqueue()
    elif cmd == "wait":
        cmd_wait()
    else:
        cmd_cancel()


if __name__ == "__main__":
    main()
