#!/usr/bin/env python3
"""Remote executor handoff: pull (ticket/claim) or push (PAT workflow_dispatch)."""
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

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from remote_dispatch_config import apply_to_environ  # noqa: E402


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
    return urllib.parse.quote(url, safe=":/")


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


def handoff(cfg):
    mode = str(cfg.get("handoff") or "pull").strip().lower()
    return mode if mode in ("pull", "push") else "pull"


def ticket_s3(cfg):
    ep = (cfg.get("ticket_s3_endpoint") or "").strip() or env_opt("TICKET_AWS_ENDPOINT") or env_opt("CALLER_AWS_ENDPOINT") or env_opt("AWS_ENDPOINT")
    bucket = (cfg.get("ticket_s3_bucket") or "").strip() or env_opt("TICKET_AWS_BUCKET") or env_opt("CALLER_AWS_BUCKET") or env_opt("AWS_BUCKET")
    if not ep or not bucket:
        die("ticket S3 endpoint/bucket missing")
    return ep.rstrip("/"), bucket


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
        die("claim S3 endpoint/bucket missing")
    return ep.rstrip("/"), bucket


def public_url(endpoint, bucket, key):
    return f"{endpoint}/{bucket}/{key}"


def s3cmd_put(local: Path, s3_uri: str):
    cmd = [
        "s3cmd", "put", "--acl-public", "--no-progress",
        "--no-mime-magic", "--guess-mime-type", str(local), s3_uri,
    ]
    print("+", " ".join(cmd), flush=True)
    subprocess.check_call(cmd)


def coerce_dispatch_inputs(raw):
    if not isinstance(raw, dict):
        die("inputs_json must be a JSON object")
    out = {}
    for key, val in raw.items():
        if not isinstance(key, str) or not key:
            die(f"invalid inputs_json key: {key!r}")
        if val is None:
            continue
        if isinstance(val, bool):
            out[key] = "true" if val else "false"
        elif isinstance(val, (dict, list)):
            out[key] = json.dumps(val, separators=(",", ":"))
        else:
            out[key] = str(val)
    return out


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


def cmd_enqueue():
    cfg = load_config()
    run_id = env("PLATFORM_RUN_ID")
    repo = env_opt("PLATFORM_REPOSITORY") or env_opt("GITHUB_REPOSITORY")
    workflow = env_opt("PLATFORM_WORKFLOW") or env_opt("GITHUB_WORKFLOW") or "unknown"
    run_url = env_opt("PLATFORM_RUN_URL") or f"https://github.com/{repo}/actions/runs/{run_id}"
    endpoint, bucket = ticket_s3(cfg)
    key = f"remote-tickets/{run_id}.json"
    ticket = {
        "version": 1,
        "platform_run_id": run_id,
        "platform_repository": repo,
        "platform_workflow": workflow,
        "platform_run_url": run_url,
        "config": cfg,
        "created_at": utc_now(),
    }
    url = public_url(endpoint, bucket, key)
    with tempfile.TemporaryDirectory(prefix="remote-ticket-") as tmp:
        path = Path(tmp) / "ticket.json"
        path.write_text(json.dumps(ticket, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        s3cmd_put(path, f"s3://{bucket}/{key}")

    set_outputs(platform_run_id=run_id, ticket_url=url, executor_run_id="", executor_run_url="")
    append_summary(
        f"### Remote pull enqueue\n"
        f"- **platform run:** [{run_id}]({run_url})\n"
        f"- **ticket:** [{url}]({quote_url(url)})\n"
        f"- **claim key:** `remote-claims/{run_id}.json` on remote public S3\n\n"
    )
    print(f"::notice title=Remote ticket published::{url}")


def cmd_start():
    if not env_opt("GH_TOKEN"):
        die("push handoff requires GH_TOKEN")
    executor_ref = env("EXECUTOR_REF")
    wf_file = env("WORKFLOW_FILE")
    wait_mode = env("WAIT_MODE")
    inputs = coerce_dispatch_inputs(json.loads(env("INPUTS_JSON")))

    print(f"Dispatching {wf_file} --ref {executor_ref}")
    _, wf = gh("GET", f"{api()}/actions/workflows/{wf_file}")
    workflow_id = wf.get("id")
    if not workflow_id:
        die(f"workflow {wf_file} not found")

    before = datetime.now(timezone.utc).timestamp() - 5
    code, _ = gh(
        "POST",
        f"{api()}/actions/workflows/{workflow_id}/dispatches",
        body={"ref": executor_ref, "inputs": inputs},
        retries=5,
    )
    if code != 204:
        die(f"dispatch failed HTTP {code}")

    run_id = run_url = None
    for _ in range(30):
        time.sleep(2)
        _, runs = gh(
            "GET",
            f"{api()}/actions/workflows/{workflow_id}/runs"
            f"?branch={urllib.parse.quote(executor_ref)}&event=workflow_dispatch&per_page=10",
        )
        for r in runs.get("workflow_runs") or []:
            created = datetime.fromisoformat(r["created_at"].replace("Z", "+00:00")).timestamp()
            if created >= before:
                run_id, run_url = r["id"], r["html_url"]
                break
        if run_id:
            break
    if not run_id:
        die("could not resolve executor run id after dispatch")

    set_outputs(executor_run_id=run_id, executor_run_url=run_url)
    append_summary(
        f"### Dispatched remote executor\n"
        f"- **workflow:** `{wf_file}`\n"
        f"- **executor run:** [{run_id}]({run_url})\n"
        f"- **mode:** `{wait_mode}`\n\n"
    )
    print(f"::notice title=Remote executor dispatched::{run_url}")


def wait_for_claim(cfg):
    platform_run_id = env("PLATFORM_RUN_ID")
    endpoint, bucket = claim_s3(cfg)
    url = public_url(endpoint, bucket, f"remote-claims/{platform_run_id}.json")
    print(f"Waiting for claim at {url} ...")
    for i in range(2400):
        body = http_get(url)
        if body:
            claim = json.loads(body)
            run_id = str(claim.get("executor_run_id") or "").strip()
            if run_id:
                run_url = str(claim.get("executor_run_url") or "").strip() or (
                    f"https://github.com/{claim.get('executor_repository') or env('REPO')}/actions/runs/{run_id}"
                )
                append_summary(
                    f"### Remote claim\n- **executor run:** [{run_id}]({run_url})\n\n"
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
    append_summary(
        f"## Executor finished\n\n"
        f"- **run:** [{run_id}]({run_url})\n"
        f"- **conclusion:** `{conclusion}`\n\n"
        f"_Enriched summary is attached by the ingest job after reupload._\n\n"
    )

    summary_url = run_root_file_url(run_id, "job_summary.md")
    body = fetch_retries(summary_url)
    if body:
        append_summary(
            f"<details><summary>Original remote job summary</summary>\n\n"
            f"_Source: [{summary_url}]({summary_url})_\n\n"
            + (body if body.endswith("\n") else body + "\n")
            + "\n</details>\n"
        )
    else:
        art_url = run_root_file_url(run_id, "remote_artifacts.json")
        if fetch_retries(art_url, attempts=5, sleep_s=2):
            print(f"::warning::missing job_summary.md; remote_artifacts.json present")
        else:
            print(f"::warning::missing job_summary.md and remote_artifacts.json")

    if conclusion == "success":
        return
    msg = f"executor finished with conclusion={conclusion}"
    if fail_on_remote:
        die(msg)
    print(f"::warning::{msg} (fail_on_remote_failure=false)")


def cmd_wait():
    cfg = load_config()
    if handoff(cfg) == "pull":
        run_id, run_url = wait_for_claim(cfg)
        set_outputs(executor_run_id=run_id, executor_run_url=run_url)
        os.environ["REPO"] = (cfg.get("repository") or "").strip() or env_opt("REPO")
        os.environ["WORKFLOW_NAME"] = (cfg.get("workflow_name") or "").strip() or env_opt("WORKFLOW_NAME")
        ep, bucket = claim_s3(cfg)
        os.environ["AWS_ENDPOINT"] = ep
        os.environ["AWS_BUCKET"] = bucket
        wait_executor_done(run_id, run_url)
        return

    run_id = env("EXECUTOR_RUN_ID")
    run_url = env_opt("EXECUTOR_RUN_URL") or f"https://github.com/{env('REPO')}/actions/runs/{run_id}"
    set_outputs(executor_run_id=run_id, executor_run_url=run_url)
    wait_executor_done(run_id, run_url)


def cmd_cancel():
    cfg = load_config()
    platform_run_id = env_opt("PLATFORM_RUN_ID")
    run_id = env_opt("EXECUTOR_RUN_ID")

    if handoff(cfg) == "pull" and platform_run_id:
        endpoint, bucket = ticket_s3(cfg)
        key = f"remote-tickets/{platform_run_id}.cancel"
        with tempfile.TemporaryDirectory(prefix="remote-cancel-") as tmp:
            path = Path(tmp) / "cancel.json"
            path.write_text(
                json.dumps({"version": 1, "platform_run_id": platform_run_id, "cancelled_at": utc_now()}, indent=2)
                + "\n",
                encoding="utf-8",
            )
            try:
                s3cmd_put(path, f"s3://{bucket}/{key}")
                append_summary(f"### Caller cancelled\n- cancel marker `s3://{bucket}/{key}`\n")
            except Exception as e:
                print(f"::warning::cancel marker failed: {e}")

    if run_id and env_opt("GH_TOKEN"):
        run_url = env_opt("EXECUTOR_RUN_URL") or f"https://github.com/{env('REPO')}/actions/runs/{run_id}"
        code, _ = gh(
            "POST", f"{api()}/actions/runs/{run_id}/cancel",
            retries=3, ok_codes=(202, 204, 409),
        )
        append_summary(f"### Caller cancelled\n- executor [{run_id}]({run_url}) HTTP {code}\n")


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ("enqueue", "start", "wait", "cancel"):
        die("usage: dispatch_remote_executor.py enqueue|start|wait|cancel")
    apply_to_environ()
    cfg = load_config()
    cmd = sys.argv[1]
    if cmd == "enqueue" or (cmd == "start" and handoff(cfg) == "pull"):
        cmd_enqueue()
    elif cmd == "start":
        cmd_start()
    elif cmd == "wait":
        cmd_wait()
    else:
        cmd_cancel()


if __name__ == "__main__":
    main()
