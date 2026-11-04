#!/usr/bin/env python3
"""Dispatch a remote workflow_dispatch run; optional approach-A runs-on patch via tmp branch."""
from __future__ import annotations

import base64
import json
import os
import re
import sys
import time
import uuid
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


LOCAL_WF_USES = re.compile(r"^\./\.github/workflows/([^@\s]+)")


def load_workflow_yaml(raw: str) -> dict:
    """Load workflow YAML; keep real bools for required/default, fix PyYAML `on:` → True key."""
    _require_yaml()
    doc = yaml.safe_load(raw)
    if not isinstance(doc, dict):
        die("workflow YAML is not a mapping")
    # YAML 1.1: key `on` loads as boolean True. Restore the workflow trigger key.
    if True in doc and "on" not in doc:
        doc["on"] = doc.pop(True)
    return doc


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


def _truthy_env(name, default="true"):
    v = os.environ.get(name, default)
    return str(v).lower() in ("1", "true", "yes")


def _require_yaml():
    if yaml is None:
        die("PyYAML is required for runs-on patch mode; pip install pyyaml")


def runs_on_looks_self_hosted(value) -> bool:
    """True if this runs-on targets self-hosted / auto-provisioned pools."""
    if value is None:
        return False
    if isinstance(value, str):
        text = value
    else:
        text = json.dumps(value, ensure_ascii=False)
    lowered = text.lower()
    return "self-hosted" in lowered or "auto-provisioned" in lowered


def local_workflow_path_from_uses(uses: str) -> str | None:
    if not isinstance(uses, str):
        return None
    m = LOCAL_WF_USES.match(uses.strip())
    if not m:
        return None
    return f".github/workflows/{m.group(1).lstrip('/')}"


def patch_workflow_doc(doc: dict, runs_on: list, self_hosted_only: bool) -> tuple[dict, list[str], int]:
    """
    Patch job runs-on in a loaded workflow document.
    Returns (doc, nested_local_workflow_paths, n_jobs_patched).
    """
    nested: list[str] = []
    patched = 0
    jobs = doc.get("jobs")
    if not isinstance(jobs, dict):
        return doc, nested, patched

    for _name, job in jobs.items():
        if not isinstance(job, dict):
            continue
        uses = job.get("uses")
        if isinstance(uses, str):
            path = local_workflow_path_from_uses(uses)
            if path:
                nested.append(path)
            continue
        if "runs-on" not in job:
            continue
        cur = job["runs-on"]
        if self_hosted_only and not runs_on_looks_self_hosted(cur):
            continue
        job["runs-on"] = runs_on
        patched += 1
    return doc, nested, patched


def dump_workflow_yaml(doc: dict) -> str:
    _require_yaml()
    if True in doc and "on" not in doc:
        doc["on"] = doc.pop(True)
    body = yaml.safe_dump(
        doc,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
        width=120,
    )
    # safe_dump quotes `on` (bool-looking); GitHub Actions wants a plain `on:` key.
    body = re.sub(r"^['\"]on['\"]:\s*$", "on:", body, count=1, flags=re.M)
    header = (
        "# Generated by dispatch_remote_executor.py (approach A: tmp-branch runs-on patch).\n"
        "# Do not merge this branch; it is deleted after the remote run.\n"
    )
    return header + body


def collect_patched_workflows(
    fetch_text,
    root_workflow_path: str,
    runs_on: list,
    self_hosted_only: bool = True,
) -> dict[str, str]:
    """
    Fetch root workflow (+ nested local reusable workflows), patch runs-on, return path→new YAML.
    fetch_text(path) -> str (raw file contents at source ref).
    """
    _require_yaml()
    out: dict[str, str] = {}
    queue = [root_workflow_path]
    seen: set[str] = set()
    total_jobs = 0

    while queue:
        path = queue.pop(0)
        if path in seen:
            continue
        seen.add(path)
        raw = fetch_text(path)
        try:
            doc = load_workflow_yaml(raw)
        except SystemExit:
            raise
        except Exception as e:
            die(f"failed to parse YAML {path}: {e}")
        doc, nested, n = patch_workflow_doc(doc, runs_on, self_hosted_only)
        total_jobs += n
        if n or path == root_workflow_path:
            # Always rewrite root so the tmp ref is distinct; nested only if patched.
            out[path] = dump_workflow_yaml(doc)
        for npath in nested:
            if npath not in seen:
                queue.append(npath)

    if total_jobs == 0:
        die(
            f"no self-hosted runs-on jobs patched under {root_workflow_path} "
            f"(self_hosted_only={self_hosted_only}); refuse empty patch"
        )
    print(f"Patched {total_jobs} job runs-on across {len(out)} workflow file(s)")
    return out


def workflow_repo_path(workflow_file: str) -> str:
    wf = workflow_file.strip().lstrip("/")
    if wf.startswith(".github/workflows/"):
        return wf
    return f".github/workflows/{wf}"


def resolve_commit_sha(ref: str) -> str:
    # Works for branch names, tags, and full/short SHAs.
    _, commit = gh("GET", f"{api()}/commits/{urllib.parse.quote(ref, safe='')}", retries=3)
    sha = (commit or {}).get("sha")
    if not sha:
        die(f"could not resolve commit for ref={ref!r}")
    return sha


def fetch_file_at_ref(path: str, ref: str) -> str:
    url = (
        f"{api()}/contents/{urllib.parse.quote(path, safe='/')}"
        f"?ref={urllib.parse.quote(ref, safe='')}"
    )
    _, data = gh("GET", url, retries=3)
    if not data or data.get("encoding") != "base64" or not data.get("content"):
        die(f"could not fetch {path} @ {ref}")
    raw = data["content"].replace("\n", "")
    return base64.b64decode(raw).decode("utf-8")


def create_tmp_branch_with_files(base_sha: str, branch: str, files: dict[str, str], message: str) -> str:
    _, base_commit = gh("GET", f"{api()}/git/commits/{base_sha}", retries=3)
    base_tree = (base_commit or {}).get("tree", {}).get("sha")
    if not base_tree:
        die(f"missing tree for commit {base_sha}")

    tree_items = []
    for path, content in sorted(files.items()):
        _, blob = gh(
            "POST",
            f"{api()}/git/blobs",
            body={"content": content, "encoding": "utf-8"},
            retries=3,
        )
        blob_sha = (blob or {}).get("sha")
        if not blob_sha:
            die(f"failed to create blob for {path}")
        tree_items.append({"path": path, "mode": "100644", "type": "blob", "sha": blob_sha})

    _, tree = gh(
        "POST",
        f"{api()}/git/trees",
        body={"base_tree": base_tree, "tree": tree_items},
        retries=3,
    )
    tree_sha = (tree or {}).get("sha")
    if not tree_sha:
        die("failed to create git tree")

    _, new_commit = gh(
        "POST",
        f"{api()}/git/commits",
        body={
            "message": message,
            "tree": tree_sha,
            "parents": [base_sha],
        },
        retries=3,
    )
    new_sha = (new_commit or {}).get("sha")
    if not new_sha:
        die("failed to create git commit")

    gh(
        "POST",
        f"{api()}/git/refs",
        body={"ref": f"refs/heads/{branch}", "sha": new_sha},
        retries=3,
    )
    print(f"Created branch {branch} at {new_sha[:12]} (base {base_sha[:12]})")
    return new_sha


def delete_branch(branch: str) -> None:
    if not branch:
        return
    code, _ = gh(
        "DELETE",
        f"{api()}/git/refs/heads/{urllib.parse.quote(branch, safe='')}",
        retries=3,
        ok_codes=(204, 404),
    )
    print(f"delete branch {branch}: HTTP {code}")


def prepare_patched_ref(workflow_file: str, source_ref: str, runs_on: list) -> str:
    """Create tmp branch with patched workflows; return branch name."""
    root = workflow_repo_path(workflow_file)
    base_sha = resolve_commit_sha(source_ref)
    self_hosted_only = _truthy_env("PATCH_SELF_HOSTED_ONLY", "true")

    def fetch_text(path: str) -> str:
        return fetch_file_at_ref(path, base_sha)

    files = collect_patched_workflows(fetch_text, root, runs_on, self_hosted_only=self_hosted_only)
    # Prefer dev/ prefix — matches platform branch conventions (pr_check, mute bots).
    branch = f"dev/ci-remote-tmp/{uuid.uuid4().hex[:12]}"
    create_tmp_branch_with_files(
        base_sha,
        branch,
        files,
        message=(
            f"ci-remote-tmp: patch runs-on in {root} (+{len(files) - 1} nested) "
            f"from {source_ref} ({base_sha[:12]})"
        ),
    )
    return branch


def cmd_start():
    executor_ref = env("EXECUTOR_REF")
    wf_file = env("WORKFLOW_FILE")
    wait_mode = env("WAIT_MODE")
    inputs = coerce_dispatch_inputs(json.loads(env("INPUTS_JSON")))

    runs_on_raw = (os.environ.get("RUNS_ON") or "").strip()
    tmp_branch = ""
    dispatch_ref = executor_ref

    if runs_on_raw:
        try:
            runs_on = json.loads(runs_on_raw)
        except json.JSONDecodeError as e:
            die(f"runs_on is not valid JSON: {e}")
        if not isinstance(runs_on, list) or not runs_on or not all(isinstance(x, str) for x in runs_on):
            die("runs_on must be a non-empty JSON array of strings")
        source_ref = (os.environ.get("SOURCE_REF") or "").strip() or executor_ref
        print(f"Approach A: patch runs-on={runs_on!r} from source_ref={source_ref}")
        tmp_branch = prepare_patched_ref(wf_file, source_ref, runs_on)
        dispatch_ref = tmp_branch

    print(f"Dispatching {wf_file} --ref {dispatch_ref}")
    print(f"inputs={json.dumps(inputs, ensure_ascii=False)}")

    _, wf = gh("GET", f"{api()}/actions/workflows/{wf_file}")
    workflow_id = wf.get("id")
    if not workflow_id:
        if tmp_branch:
            delete_branch(tmp_branch)
        die(f"workflow {wf_file} not found (must be registered on the default branch)")

    payload = {"ref": dispatch_ref, "inputs": inputs}
    before = datetime.now(timezone.utc).timestamp() - 5
    try:
        code, _ = gh(
            "POST",
            f"{api()}/actions/workflows/{workflow_id}/dispatches",
            body=payload,
            retries=5,
        )
        if code != 204:
            die(f"dispatch failed HTTP {code}")
    except SystemExit:
        if tmp_branch:
            delete_branch(tmp_branch)
        raise

    run_id = None
    run_url = None
    for _ in range(30):
        time.sleep(2)
        _, runs = gh(
            "GET",
            f"{api()}/actions/workflows/{workflow_id}/runs"
            f"?branch={urllib.parse.quote(dispatch_ref)}&event=workflow_dispatch&per_page=10",
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
        if tmp_branch:
            delete_branch(tmp_branch)
        die("could not resolve executor run id after dispatch")

    print(f"executor_run_id={run_id}")
    print(f"executor_run_url={run_url}")
    if tmp_branch:
        print(f"tmp_branch={tmp_branch}")
        # Fire-and-forget: run is already queued on the commit; drop the branch now.
        if wait_mode == "fire-and-forget" and _truthy_env("CLEANUP_TMP_BRANCH", "true"):
            delete_branch(tmp_branch)
            tmp_branch = ""
    set_outputs(
        executor_run_id=run_id,
        executor_run_url=run_url,
        tmp_branch=tmp_branch,
    )

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

    summary_lines = [
        "### Dispatched remote executor",
        f"- **workflow:** `{wf_file}`",
        f"- **dispatch ref:** `{dispatch_ref}`",
        f"- **executor run:** [{run_id}]({run_url})",
        f"- **mode:** `{wait_mode}`",
    ]
    if runs_on_raw:
        summary_lines.append(f"- **approach A runs_on:** `{runs_on_raw}`")
        summary_lines.append(f"- **tmp branch:** `{tmp_branch}`")
        summary_lines.append(f"- **source ref:** `{(os.environ.get('SOURCE_REF') or executor_ref)}`")
    else:
        summary_lines.append(f"- **executor ref:** `{executor_ref}`")
    summary_lines.extend(
        [
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
    append_summary("\n".join(summary_lines))
    print(
        f"::notice title=Remote executor dispatched::{run_url} "
        f"(ref={dispatch_ref}, mode={wait_mode})"
    )


def cmd_wait():
    run_id = env("EXECUTOR_RUN_ID")
    run_url = os.environ.get("EXECUTOR_RUN_URL") or f"https://github.com/{env('REPO')}/actions/runs/{run_id}"
    fail_on_remote = _truthy_env("FAIL_ON_REMOTE_FAILURE", "true")
    tmp_branch = (os.environ.get("TMP_BRANCH") or "").strip()
    cleanup = _truthy_env("CLEANUP_TMP_BRANCH", "true")
    print(f"Waiting for executor run {run_id} ({run_url}) ...")

    status = ""
    conclusion = ""
    # 2400 * 30s = 20h — aligns with dispatch_remote.yml wait job timeout (1200m).
    try:
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
    finally:
        if cleanup and tmp_branch:
            delete_branch(tmp_branch)


def cmd_cancel():
    run_id = env("EXECUTOR_RUN_ID")
    run_url = os.environ.get("EXECUTOR_RUN_URL") or f"https://github.com/{env('REPO')}/actions/runs/{run_id}"
    tmp_branch = (os.environ.get("TMP_BRANCH") or "").strip()
    cleanup = _truthy_env("CLEANUP_TMP_BRANCH", "true")
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
    if cleanup and tmp_branch:
        delete_branch(tmp_branch)


def main():
    if len(sys.argv) == 2 and sys.argv[1] in ("start", "wait", "cancel"):
        {"start": cmd_start, "wait": cmd_wait, "cancel": cmd_cancel}[sys.argv[1]]()
        return
    if len(sys.argv) >= 2 and sys.argv[1] == "self-test-patch":
        # Local unit check without GitHub: python3 dispatch_remote_executor.py self-test-patch
        _self_test_patch()
        return
    die("usage: dispatch_remote_executor.py start|wait|cancel|self-test-patch")


def _self_test_patch():
    _require_yaml()
    root = ".github/workflows/root.yml"
    nested = ".github/workflows/nested.yml"
    files = {
        root: """
name: root
on:
  workflow_dispatch:
    inputs:
      x: { type: string, default: a }
jobs:
  prepare:
    runs-on: ubuntu-latest
    steps:
      - run: echo prepare
  main:
    uses: ./.github/workflows/nested.yml
    with:
      x: ${{ inputs.x }}
""",
        nested: """
name: nested
on:
  workflow_call:
    inputs:
      x: { type: string, required: true }
jobs:
  light:
    runs-on: ubuntu-latest
    steps:
      - run: echo light
  heavy:
    runs-on: [self-hosted, auto-provisioned, build-preset-relwithdebinfo]
    steps:
      - run: echo heavy
""",
    }

    def fetch(path: str) -> str:
        return files[path]

    out = collect_patched_workflows(
        fetch,
        root,
        ["self-hosted", "build-preset-relwithdebinfo-fat"],
        self_hosted_only=True,
    )
    assert nested in out, out.keys()
    doc = load_workflow_yaml(out[nested])
    assert "on" in doc and True not in doc
    assert doc["jobs"]["heavy"]["runs-on"] == [
        "self-hosted",
        "build-preset-relwithdebinfo-fat",
    ]
    assert doc["jobs"]["light"]["runs-on"] == "ubuntu-latest"
    root_doc = load_workflow_yaml(out[root])
    assert "on" in root_doc and True not in root_doc
    assert any(line == "on:" for line in out[root].splitlines())
    assert "true:" not in {ln.strip() for ln in out[root].splitlines()[:15]}
    # required must stay a real bool in the dumped YAML (not the string 'false').
    sample = dump_workflow_yaml(
        {
            "name": "t",
            "on": {"workflow_dispatch": {"inputs": {"pull_number": {"required": False, "default": ""}}}},
            "jobs": {"j": {"runs-on": "ubuntu-latest", "steps": []}},
        }
    )
    assert "required: false" in sample, sample
    assert "required: 'false'" not in sample
    print("self-test-patch OK")


if __name__ == "__main__":
    main()
