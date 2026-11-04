#!/usr/bin/env python3
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

API = f"https://api.github.com/repos/{os.environ['REPO']}"
WF_FILE = os.environ.get("WORKFLOW_FILE", "run_and_debug_tests.yml")
WF_NAME = os.environ.get("WORKFLOW_NAME", "Run and debug tests")


def die(msg):
    print(f"::error::{msg}", file=sys.stderr)
    sys.exit(1)


def env(name, default=None):
    v = os.environ.get(name, default)
    if v is None or v == "":
        die(f"missing env {name}")
    return v


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


def gh(method, url, body=None, retries=1):
    data = None if body is None else json.dumps(body).encode()
    headers = {
        "Authorization": f"Bearer {os.environ['GH_TOKEN']}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    last_err = None
    for attempt in range(1, retries + 1):
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                raw = resp.read()
                code = resp.status
                return code, json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            raw = e.read()
            last_err = (e.code, raw.decode(errors="replace"))
            if e.code in (429, 500, 502, 503, 504) and attempt < retries:
                print(f"HTTP {e.code}, retry {attempt}/{retries} ...")
                print(last_err[1])
                time.sleep(attempt * 5)
                continue
            die(f"GitHub {method} {url} -> HTTP {e.code}: {last_err[1]}")
        except urllib.error.URLError as e:
            die(f"GitHub {method} {url} failed: {e}")
    die(f"GitHub {method} {url} failed: {last_err}")


def http_get(url):
    try:
        with urllib.request.urlopen(url) as resp:
            return resp.read().decode()
    except Exception:
        return None


def job_summary_url(run_id):
    base = f"{env('AWS_ENDPOINT')}/{env('AWS_BUCKET')}/{env('REPO')}/{WF_NAME}/{run_id}/job_summary.md"
    return urllib.parse.quote(base, safe=":/")


def cmd_start():
    preset = env("BUILD_PRESET").split(",")[0].strip()
    executor_ref = env("EXECUTOR_REF")
    code_ref = env("CODE_REF")
    targets = env("TEST_TARGETS")
    wait_mode = os.environ.get("WAIT_MODE", "unknown")

    print(f"Dispatching {WF_FILE} --ref {executor_ref}")
    print(f"code_ref={code_ref} preset={preset} targets={targets}")

    _, wf = gh("GET", f"{API}/actions/workflows/{WF_FILE}")
    workflow_id = wf.get("id")
    if not workflow_id:
        die(f"workflow {WF_FILE} not found")

    payload = {
        "ref": executor_ref,
        "inputs": {
            "code_ref": code_ref,
            "test_targets": targets,
            "build_preset": preset,
        },
    }
    before = datetime.now(timezone.utc).timestamp() - 5
    code, _ = gh(
        "POST",
        f"{API}/actions/workflows/{workflow_id}/dispatches",
        body=payload,
        retries=5,
    )
    if code != 204:
        die(f"dispatch failed HTTP {code}")

    run_id = None
    run_url = None
    for _ in range(30):
        time.sleep(2)
        _, runs = gh(
            "GET",
            f"{API}/actions/workflows/{workflow_id}/runs"
            f"?branch={urllib.parse.quote(executor_ref)}&event=workflow_dispatch&per_page=10",
        )
        for r in runs.get("workflow_runs") or []:
            created = datetime.fromisoformat(r["created_at"].replace("Z", "+00:00")).timestamp()
            if created >= before:
                run_id = r["id"]
                run_url = r["html_url"]
                break
        if run_id:
            break
    if not run_id:
        die("could not resolve executor run id after dispatch")

    print(f"executor_run_id={run_id}")
    print(f"executor_run_url={run_url}")
    set_outputs(executor_run_id=run_id, executor_run_url=run_url)
    append_summary(
        "\n".join(
            [
                "### Dispatched remote executor",
                f"- **executor ref:** `{executor_ref}`",
                f"- **code ref:** `{code_ref}`",
                f"- **preset:** `{preset}`",
                f"- **targets:** `{targets}`",
                f"- **executor run:** [{run_id}]({run_url})",
                f"- **mode:** `{wait_mode}`",
                "",
            ]
        )
    )
    print(
        f"::notice title=Remote executor dispatched::{run_url} "
        f"(code_ref={code_ref}, preset={preset}, mode={wait_mode})"
    )


def cmd_wait():
    run_id = env("EXECUTOR_RUN_ID")
    run_url = os.environ.get("EXECUTOR_RUN_URL") or f"https://github.com/{env('REPO')}/actions/runs/{run_id}"
    print(f"Waiting for executor run {run_id} ...")
    append_summary(
        f"### Waiting for remote executor\n- **executor run:** [{run_id}]({run_url})\n"
    )

    status = ""
    conclusion = ""
    for _ in range(720):
        _, state = gh("GET", f"{API}/actions/runs/{run_id}")
        status = state.get("status") or ""
        conclusion = state.get("conclusion") or ""
        print(f"{time.strftime('%H:%M:%S', time.gmtime())} status={status} conclusion={conclusion or 'none'}")
        if status == "completed":
            break
        time.sleep(30)
    if status != "completed":
        die(f"timed out waiting for executor run {run_id} (last status={status})")

    append_summary(
        f"## Executor summary\n\n"
        f"_Mirrored from [{run_id}]({run_url}) (conclusion=`{conclusion}`)._\n\n"
    )
    url = job_summary_url(run_id)
    body = None
    for _ in range(20):
        body = http_get(url)
        if body:
            break
        time.sleep(3)
    if not body:
        append_summary(f"_Failed to mirror `job_summary.md`._\n\n[Open executor run]({run_url})\n")
        die(f"missing S3 job_summary.md at {url}")
    print(f"Mirrored {url}")
    append_summary(body if body.endswith("\n") else body + "\n")

    if conclusion == "success":
        print("Executor succeeded")
        return
    die(f"executor finished with conclusion={conclusion}")


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ("start", "wait"):
        die("usage: dispatch_remote_executor.py start|wait")
    if sys.argv[1] == "start":
        cmd_start()
    else:
        cmd_wait()


if __name__ == "__main__":
    main()
