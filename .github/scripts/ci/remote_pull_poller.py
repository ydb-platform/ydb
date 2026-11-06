#!/usr/bin/env python3
"""Remote-host poller: platform remote-ticket artifact → local executor → claim.

Webhook/list gives platform_run_id; workflow_dispatch inputs are in the
platform Actions artifact `remote-ticket`.

Claim is taken *before* dispatch (S3 If-None-Match create) so two pollers
do not both start an executor.

Env:
  GH_TOKEN              — PAT/app on THIS repo (workflow_dispatch); github.token will not start workflows
  PLATFORM_GH_TOKEN     — read artifacts/runs on platform (falls back to GH_TOKEN)
  GITHUB_REPOSITORY     — this (executor) repo
  PLATFORM_REPOSITORY   — e.g. ydb-platform/ydb
  PLATFORM_RUN_ID       — optional single run (webhook); else scan open runs
  PLATFORM_WORKFLOW_NAMES — comma-separated workflow names for scan (default: Run and debug tests)
  CLAIM_S3_BUCKET       — local public bucket for remote-claims/
  CLAIM_PUBLIC_BASE     — optional public HTTP base (else s3cmd ls)
  TICKET_BASE_URL       — optional S3 ticket fallback
  TICKET_ARTIFACT_NAME  — default remote-ticket
  WORKFLOW_FILE         — default ci_remote_executor.yml
  EXECUTOR_REF          — default ref if ticket omits executor_ref
  ARTIFACT_WAIT_ATTEMPTS / ARTIFACT_WAIT_SLEEP_S — only when PLATFORM_RUN_ID is set
"""
from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path

TICKET_ARTIFACT_DEFAULT = "remote-ticket"
TICKET_JSON_NAME = "remote-ticket.json"
DEFAULT_SCAN_WORKFLOWS = ("Run and debug tests",)


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


def platform_token() -> str:
    return env_opt("PLATFORM_GH_TOKEN") or env("GH_TOKEN")


def gh(method: str, url: str, body=None, ok=(200, 201, 202, 204), token: str | None = None):
    data = None if body is None else json.dumps(body).encode()
    headers = {
        "Authorization": f"Bearer {token or env('GH_TOKEN')}",
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


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def gh_download_artifact_zip(url: str, token: str) -> bytes:
    """GET artifact zip; follow Location without sending Bearer to the signed URL."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    req = urllib.request.Request(url, headers=headers, method="GET")
    opener = urllib.request.build_opener(_NoRedirect)
    try:
        with opener.open(req, timeout=120) as resp:
            return resp.read()
    except urllib.error.HTTPError as e:
        if e.code not in (301, 302, 303, 307, 308):
            raw = e.read().decode(errors="replace")
            die(f"GitHub GET {url} -> HTTP {e.code}: {raw}")
        loc = e.headers.get("Location")
        if not loc:
            die(f"artifact download HTTP {e.code} without Location")
        with urllib.request.urlopen(loc, timeout=120) as resp:
            return resp.read()


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


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def claim_key(platform_run_id: str) -> str:
    return f"remote-claims/{platform_run_id}.json"


def claim_s3_uri(platform_run_id: str) -> str:
    return f"s3://{env('CLAIM_S3_BUCKET')}/{claim_key(platform_run_id)}"


def claim_http_url(platform_run_id: str) -> str | None:
    base = env_opt("CLAIM_PUBLIC_BASE")
    if not base:
        return None
    return f"{base.rstrip('/')}/{claim_key(platform_run_id)}"


def s3cmd_run(args: list[str], *, check: bool = True) -> subprocess.CompletedProcess:
    cmd = ["s3cmd", *args]
    print("+", " ".join(cmd), flush=True)
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if check and proc.returncode != 0:
        die(f"s3cmd failed ({proc.returncode}): {proc.stderr or proc.stdout}")
    return proc


def s3_object_exists(s3_uri: str) -> bool:
    proc = s3cmd_run(["ls", s3_uri], check=False)
    return proc.returncode == 0 and bool((proc.stdout or "").strip())


def read_claim(platform_run_id: str) -> dict | None:
    url = claim_http_url(platform_run_id)
    if url:
        raw = http_get(url)
        if raw:
            return json.loads(raw)
    if not s3_object_exists(claim_s3_uri(platform_run_id)):
        return None
    with tempfile.TemporaryDirectory(prefix="remote-claim-get-") as tmp:
        path = Path(tmp) / "claim.json"
        proc = s3cmd_run(["get", "--force", claim_s3_uri(platform_run_id), str(path)], check=False)
        if proc.returncode != 0 or not path.is_file():
            return None
        return json.loads(path.read_text(encoding="utf-8"))


def write_claim_object(platform_run_id: str, claim: dict, *, exclusive: bool) -> bool:
    """Put claim JSON. exclusive=True uses If-None-Match:* (create only)."""
    with tempfile.TemporaryDirectory(prefix="remote-claim-put-") as tmp:
        path = Path(tmp) / "claim.json"
        path.write_text(json.dumps(claim, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        args = [
            "put", "--acl-public", "--no-progress",
            "--no-mime-magic", "--guess-mime-type",
        ]
        if exclusive:
            args.append("--add-header=If-None-Match:*")
        args.extend([str(path), claim_s3_uri(platform_run_id)])
        proc = s3cmd_run(args, check=False)
        if proc.returncode == 0:
            return True
        err = (proc.stderr or proc.stdout or "").strip()
        if exclusive and s3_object_exists(claim_s3_uri(platform_run_id)):
            print(f"claim create lost race for {platform_run_id}: {err or 'object exists'}")
            return False
        die(f"s3cmd put claim failed ({proc.returncode}): {err}")


def try_acquire_claim(platform_run_id: str, claim_token: str) -> bool:
    existing = read_claim(platform_run_id)
    exclusive = True
    if existing:
        if str(existing.get("executor_run_id") or "").strip():
            print(f"skip {platform_run_id}: already claimed executor_run_id={existing.get('executor_run_id')}")
            return False
        status = str(existing.get("status") or "").strip().lower()
        if status == "failed":
            print(f"reclaiming failed claim for {platform_run_id}")
            exclusive = False
        else:
            print(f"skip {platform_run_id}: pending claim by token={existing.get('claim_token')}")
            return False

    pending = {
        "version": 1,
        "status": "pending",
        "platform_run_id": platform_run_id,
        "claim_token": claim_token,
        "executor_repository": env("GITHUB_REPOSITORY"),
        "claimed_at": utc_now(),
        "executor_run_id": "",
        "executor_run_url": "",
    }
    if not write_claim_object(platform_run_id, pending, exclusive=exclusive):
        return False

    # Confirm we still own the object (If-None-Match unsupported → last writer check).
    got = read_claim(platform_run_id) or {}
    if got.get("claim_token") != claim_token:
        print(f"skip {platform_run_id}: claim token mismatch after put")
        return False
    return True


def finalize_claim(
    platform_run_id: str,
    claim_token: str,
    executor_run_id: str,
    executor_run_url: str,
) -> None:
    claim = {
        "version": 1,
        "status": "dispatched",
        "platform_run_id": platform_run_id,
        "claim_token": claim_token,
        "executor_run_id": executor_run_id,
        "executor_run_url": executor_run_url,
        "executor_repository": env("GITHUB_REPOSITORY"),
        "claimed_at": utc_now(),
    }
    write_claim_object(platform_run_id, claim, exclusive=False)
    print(f"Wrote claim {claim_s3_uri(platform_run_id)}")


def abandon_claim(platform_run_id: str, claim_token: str, error: str) -> None:
    got = read_claim(platform_run_id) or {}
    if got.get("claim_token") != claim_token:
        return
    claim = {
        **got,
        "status": "failed",
        "error": error[:500],
        "failed_at": utc_now(),
        "executor_run_id": "",
        "executor_run_url": "",
    }
    write_claim_object(platform_run_id, claim, exclusive=False)
    print(f"::warning::abandoned claim {platform_run_id}: {error}")


def scan_workflow_names() -> set[str] | None:
    raw = env_opt("PLATFORM_WORKFLOW_NAMES")
    if raw == "*":
        return None
    if not raw:
        return set(DEFAULT_SCAN_WORKFLOWS)
    return {part.strip() for part in raw.split(",") if part.strip()}


def list_platform_run_ids(platform_repo: str) -> list[str]:
    allow = scan_workflow_names()
    ids: list[str] = []
    tok = platform_token()
    for status in ("in_progress", "queued"):
        url = (
            f"https://api.github.com/repos/{platform_repo}/actions/runs"
            f"?status={status}&per_page=50"
        )
        _, data = gh("GET", url, token=tok)
        for r in (data or {}).get("workflow_runs") or []:
            name = (r.get("name") or "").strip()
            if allow is not None and name not in allow:
                continue
            rid = str(r.get("id") or "")
            if rid:
                ids.append(rid)
    seen: set[str] = set()
    out: list[str] = []
    for rid in ids:
        if rid not in seen:
            seen.add(rid)
            out.append(rid)
    return out


def platform_run_alive(platform_repo: str, run_id: str) -> bool:
    tok = platform_token()
    _, data = gh(
        "GET",
        f"https://api.github.com/repos/{platform_repo}/actions/runs/{run_id}",
        token=tok,
    )
    status = (data or {}).get("status") or ""
    if status == "completed":
        print(
            f"skip {run_id}: platform run completed "
            f"conclusion={(data or {}).get('conclusion')}"
        )
        return False
    return True


def extract_ticket_json(zip_bytes: bytes) -> dict:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = zf.namelist()
        candidate = next(
            (n for n in names if n.rsplit("/", 1)[-1] == TICKET_JSON_NAME),
            None,
        )
        if candidate is None:
            die(f"remote-ticket zip missing {TICKET_JSON_NAME}: {names}")
        raw = zf.read(candidate).decode()
    ticket = json.loads(raw)
    if not isinstance(ticket, dict):
        die("ticket JSON must be an object")
    return ticket


def download_ticket_artifact(platform_repo: str, run_id: str) -> dict | None:
    name = env_opt("TICKET_ARTIFACT_NAME") or TICKET_ARTIFACT_DEFAULT
    tok = platform_token()
    url = (
        f"https://api.github.com/repos/{platform_repo}/actions/runs/{run_id}"
        f"/artifacts?per_page=100"
    )
    _, data = gh("GET", url, token=tok)
    arts = [
        a
        for a in ((data or {}).get("artifacts") or [])
        if a.get("name") == name and not a.get("expired")
    ]
    if not arts:
        return None
    art = arts[0]
    zip_url = art.get("archive_download_url") or (
        f"https://api.github.com/repos/{platform_repo}/actions/artifacts/{art['id']}/zip"
    )
    print(f"Downloading artifact {name} id={art.get('id')} for run {run_id}")
    return extract_ticket_json(gh_download_artifact_zip(zip_url, tok))


def load_ticket(
    platform_repo: str,
    platform_run_id: str,
    ticket_base: str,
    *,
    wait: bool,
) -> dict | None:
    if wait:
        attempts = int(env_opt("ARTIFACT_WAIT_ATTEMPTS") or "20")
        sleep_s = int(env_opt("ARTIFACT_WAIT_SLEEP_S") or "15")
    else:
        attempts, sleep_s = 1, 0

    for i in range(attempts):
        ticket = download_ticket_artifact(platform_repo, platform_run_id)
        if ticket is not None:
            return ticket
        if i + 1 < attempts:
            print(f"artifact not ready for {platform_run_id}; wait {sleep_s}s ({i + 1}/{attempts})")
            time.sleep(sleep_s)

    if ticket_base:
        cancel_url = f"{ticket_base.rstrip('/')}/remote-tickets/{platform_run_id}.cancel"
        if http_get(cancel_url):
            print(f"skip {platform_run_id}: S3 cancel marker")
            return None
        ticket_url = f"{ticket_base.rstrip('/')}/remote-tickets/{platform_run_id}.json"
        raw = http_get(ticket_url)
        if raw:
            print(f"Loaded S3 ticket fallback for {platform_run_id}")
            return json.loads(raw)
    return None


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


def process_run(
    platform_repo: str,
    platform_run_id: str,
    ticket_base: str,
    wf_file: str,
    ref: str,
    *,
    wait_artifact: bool,
) -> bool:
    if not platform_run_alive(platform_repo, platform_run_id):
        return False

    ticket = load_ticket(platform_repo, platform_run_id, ticket_base, wait=wait_artifact)
    if ticket is None:
        print(f"skip {platform_run_id}: no remote-ticket")
        return False

    cfg = ticket.get("config") or {}
    inputs = cfg.get("inputs")
    if not isinstance(inputs, dict):
        print(f"::warning::ticket {platform_run_id} missing config.inputs")
        return False

    claim_token = uuid.uuid4().hex
    if not try_acquire_claim(platform_run_id, claim_token):
        return False

    ticket_wf = str(cfg.get("workflow_file") or "").strip() or wf_file
    ticket_ref = str(cfg.get("executor_ref") or "").strip() or ref

    print(f"Claimed platform_run_id={platform_run_id}; dispatching")
    try:
        executor_run_id, executor_run_url = dispatch_executor(ticket_wf, ticket_ref, inputs)
    except Exception as e:
        abandon_claim(platform_run_id, claim_token, str(e))
        raise

    finalize_claim(platform_run_id, claim_token, executor_run_id, executor_run_url)
    print(f"Dispatched executor {executor_run_id} for {platform_run_id}")
    return True


def main() -> int:
    platform_repo = env("PLATFORM_REPOSITORY")
    ticket_base = env_opt("TICKET_BASE_URL")
    wf_file = env_opt("WORKFLOW_FILE") or "ci_remote_executor.yml"
    ref = env_opt("EXECUTOR_REF") or env_opt("GITHUB_REF_NAME") or "main"

    single = env_opt("PLATFORM_RUN_ID")
    run_ids = [single] if single else list_platform_run_ids(platform_repo)
    wait_artifact = bool(single)
    print(
        f"platform runs to consider: {len(run_ids)}"
        + (f" (single={single}, wait_artifact)" if single else " (scan, no wait)")
    )
    claimed = 0
    for rid in run_ids:
        try:
            if process_run(
                platform_repo, rid, ticket_base, wf_file, ref, wait_artifact=wait_artifact
            ):
                claimed += 1
        except Exception as e:
            print(f"::warning::run {rid} failed: {e}")
    print(f"claimed={claimed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
