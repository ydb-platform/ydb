#!/usr/bin/env python3
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

def die(msg):
    print(f"::error::{msg}", file=sys.stderr)
    sys.exit(1)


def env(name, default=None):
    v = os.environ.get(name, default)
    if v is None or v == "":
        die(f"missing env {name}")
    return v


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


def http_get(url):
    try:
        with urllib.request.urlopen(url) as resp:
            return resp.read().decode()
    except Exception:
        return None


def job_summary_url(run_id):
    base = (
        f"{env('AWS_ENDPOINT')}/{env('AWS_BUCKET')}/{env('REPO')}/"
        f"{env('WORKFLOW_NAME')}/{run_id}/job_summary.md"
    )
    return urllib.parse.quote(base, safe=":/")


def coerce_dispatch_inputs(raw):
    """workflow_dispatch API requires string values for every input."""
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


def cmd_start():
    executor_ref = env("EXECUTOR_REF")
    wf_file = env("WORKFLOW_FILE")
    wait_mode = env("WAIT_MODE")
    inputs = coerce_dispatch_inputs(json.loads(env("INPUTS_JSON")))

    print(f"Dispatching {wf_file} --ref {executor_ref}")
    print(f"inputs={json.dumps(inputs, ensure_ascii=False)}")

    _, wf = gh("GET", f"{api()}/actions/workflows/{wf_file}")
    workflow_id = wf.get("id")
    if not workflow_id:
        die(f"workflow {wf_file} not found")

    payload = {"ref": executor_ref, "inputs": inputs}
    before = datetime.now(timezone.utc).timestamp() - 5
    code, _ = gh(
        "POST",
        f"{api()}/actions/workflows/{workflow_id}/dispatches",
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
            f"{api()}/actions/workflows/{workflow_id}/runs"
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

    # Pretty-print for summary: expand nested JSON strings (e.g. job_inputs).
    pretty = {}
    for k, v in inputs.items():
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
            except json.JSONDecodeError:
                pretty[k] = v
            else:
                pretty[k] = parsed if isinstance(parsed, (dict, list)) else v
        else:
            pretty[k] = v
    pretty_json = json.dumps(pretty, ensure_ascii=False, indent=2)

    append_summary(
        "\n".join(
            [
                "### Dispatched remote executor",
                f"- **workflow:** `{wf_file}`",
                f"- **executor ref:** `{executor_ref}`",
                f"- **executor run:** [{run_id}]({run_url})",
                f"- **mode:** `{wait_mode}`",
                "",
                "<details><summary>inputs</summary>",
                "",
                "```json",
                pretty_json,
                "```",
                "",
                "</details>",
                "",
            ]
        )
    )
    print(
        f"::notice title=Remote executor dispatched::{run_url} "
        f"(ref={executor_ref}, mode={wait_mode})"
    )


def _truthy_env(name, default="true"):
    v = os.environ.get(name, default)
    return str(v).lower() in ("1", "true", "yes")


def cmd_wait():
    run_id = env("EXECUTOR_RUN_ID")
    run_url = os.environ.get("EXECUTOR_RUN_URL") or f"https://github.com/{env('REPO')}/actions/runs/{run_id}"
    fail_on_remote = _truthy_env("FAIL_ON_REMOTE_FAILURE", "true")
    # Link already in the dispatch job summary; only log here.
    print(f"Waiting for executor run {run_id} ({run_url}) ...")

    status = ""
    conclusion = ""
    # 2400 * 30s = 20h — aligns with dispatch_remote.yml wait job timeout (1200m).
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
    msg = f"executor finished with conclusion={conclusion}"
    if fail_on_remote:
        die(msg)
    print(f"::warning::{msg} (fail_on_remote_failure=false; caller stays green)")


def cmd_cancel():
    run_id = env("EXECUTOR_RUN_ID")
    run_url = os.environ.get("EXECUTOR_RUN_URL") or f"https://github.com/{env('REPO')}/actions/runs/{run_id}"
    print(f"Caller cancelled — cancelling executor run {run_id} ...")
    # 409 = already completed / not cancellable.
    code, _ = gh(
        "POST",
        f"{api()}/actions/runs/{run_id}/cancel",
        retries=3,
        ok_codes=(202, 204, 409),
    )
    print(f"cancel HTTP {code}")
    append_summary(
        f"### Caller cancelled\n- cancelled executor run [{run_id}]({run_url}) (HTTP {code})\n"
    )


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ("start", "wait", "cancel"):
        die("usage: dispatch_remote_executor.py start|wait|cancel")
    {"start": cmd_start, "wait": cmd_wait, "cancel": cmd_cancel}[sys.argv[1]]()


if __name__ == "__main__":
    main()
