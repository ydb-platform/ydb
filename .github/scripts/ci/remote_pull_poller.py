#!/usr/bin/env python3
"""Remote-host poller: platform remote-ticket artifact → local executor → claim.

Webhook / run list gives platform_run_id but not workflow_dispatch inputs.
Those live in the platform Actions artifact `remote-ticket`.

Env:
  GH_TOKEN              — this (executor) repo for local workflow_dispatch
  PLATFORM_GH_TOKEN     — read artifacts/runs on platform (falls back to GH_TOKEN)
  GITHUB_REPOSITORY     — this (executor) repo
  PLATFORM_REPOSITORY   — e.g. ydb-platform/ydb
  PLATFORM_RUN_ID       — optional single run (webhook); else list open runs
  CLAIM_S3_BUCKET       — local public bucket for remote-claims/
  CLAIM_PUBLIC_BASE     — optional public HTTP base to skip already-claimed
  TICKET_BASE_URL       — optional S3 ticket fallback (legacy)
  TICKET_ARTIFACT_NAME  — default remote-ticket
  WORKFLOW_FILE         — executor workflow file (default ci_remote_executor.yml)
  EXECUTOR_REF          — ref to run (default: github.ref_name or main)
  ARTIFACT_WAIT_ATTEMPTS / ARTIFACT_WAIT_SLEEP_S — poll until artifact appears
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
import zipfile
from datetime import datetime, timezone
from pathlib import Path

TICKET_ARTIFACT_DEFAULT = "remote-ticket"
TICKET_JSON_NAME = "remote-ticket.json"


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


def gh_download_artifact_zip(url: str, token: str) -> bytes:
    """Download Actions artifact zip; follow redirect without forwarding the Bearer token."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    req = urllib.request.Request(url, headers=headers, method="GET")
    opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler())
    try:
        # First response is often 302 to a signed URL; urllib follows and strips
        # Authorization on cross-host redirects in modern Python.
        with opener.open(req, timeout=120) as resp:
            return resp.read()
    except urllib.error.HTTPError as e:
        if e.code in (301, 302, 303, 307, 308):
            loc = e.headers.get("Location")
            if not loc:
                die(f"artifact download redirect without Location: HTTP {e.code}")
            with urllib.request.urlopen(loc, timeout=120) as resp:
                return resp.read()
        raw = e.read().decode(errors="replace")
        die(f"GitHub GET {url} -> HTTP {e.code}: {raw}")


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
    tok = platform_token()
    for status in ("in_progress", "queued"):
        url = (
            f"https://api.github.com/repos/{platform_repo}/actions/runs"
            f"?status={status}&per_page=50"
        )
        _, data = gh("GET", url, token=tok)
        for r in (data or {}).get("workflow_runs") or []:
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
    _, data = gh("GET", f"https://api.github.com/repos/{platform_repo}/actions/runs/{run_id}", token=tok)
    status = (data or {}).get("status") or ""
    if status in ("completed",):
        print(f"skip {run_id}: platform run status={status} conclusion={(data or {}).get('conclusion')}")
        return False
    return True


def extract_ticket_json(zip_bytes: bytes) -> dict:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = zf.namelist()
        candidate = None
        for name in names:
            base = name.rsplit("/", 1)[-1]
            if base == TICKET_JSON_NAME or base.endswith("ticket.json"):
                candidate = name
                break
        if candidate is None and len(names) == 1:
            candidate = names[0]
        if candidate is None:
            die(f"remote-ticket zip has no json: {names}")
        raw = zf.read(candidate).decode()
    ticket = json.loads(raw)
    if not isinstance(ticket, dict):
        die("ticket JSON must be an object")
    return ticket


def download_ticket_artifact(platform_repo: str, run_id: str) -> dict | None:
    """Return ticket dict from Actions artifact, or None if not uploaded yet."""
    name = env_opt("TICKET_ARTIFACT_NAME") or TICKET_ARTIFACT_DEFAULT
    tok = platform_token()
    url = f"https://api.github.com/repos/{platform_repo}/actions/runs/{run_id}/artifacts?per_page=100"
    _, data = gh("GET", url, token=tok)
    arts = [
        a for a in ((data or {}).get("artifacts") or [])
        if a.get("name") == name and not a.get("expired")
    ]
    if not arts:
        return None
    art = arts[0]
    zip_url = (
        art.get("archive_download_url")
        or f"https://api.github.com/repos/{platform_repo}/actions/artifacts/{art['id']}/zip"
    )
    print(f"Downloading artifact {name} id={art.get('id')} for run {run_id}")
    return extract_ticket_json(gh_download_artifact_zip(zip_url, tok))


def wait_ticket_artifact(platform_repo: str, run_id: str) -> dict | None:
    attempts = int(env_opt("ARTIFACT_WAIT_ATTEMPTS") or "20")
    sleep_s = int(env_opt("ARTIFACT_WAIT_SLEEP_S") or "15")
    for i in range(attempts):
        ticket = download_ticket_artifact(platform_repo, run_id)
        if ticket is not None:
            return ticket
        if i + 1 < attempts:
            print(f"artifact not ready for {run_id}; wait {sleep_s}s ({i + 1}/{attempts})")
            time.sleep(sleep_s)
    return None


def load_ticket_s3_fallback(platform_run_id: str, ticket_base: str) -> dict | None:
    if not ticket_base:
        return None
    ticket_url = f"{ticket_base.rstrip('/')}/remote-tickets/{platform_run_id}.json"
    cancel_url = f"{ticket_base.rstrip('/')}/remote-tickets/{platform_run_id}.cancel"
    if http_get(cancel_url):
        print(f"skip {platform_run_id}: S3 cancel marker present")
        return None
    raw = http_get(ticket_url)
    if not raw:
        return None
    print(f"Loaded S3 ticket fallback for {platform_run_id}")
    return json.loads(raw)


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


def process_run(platform_repo: str, platform_run_id: str, ticket_base: str, wf_file: str, ref: str) -> bool:
    claim_base = env_opt("CLAIM_PUBLIC_BASE")
    if claim_base:
        if http_get(f"{claim_base.rstrip('/')}/remote-claims/{platform_run_id}.json"):
            print(f"skip {platform_run_id}: already claimed")
            return False

    if not platform_run_alive(platform_repo, platform_run_id):
        return False

    ticket = wait_ticket_artifact(platform_repo, platform_run_id)
    if ticket is None:
        ticket = load_ticket_s3_fallback(platform_run_id, ticket_base)
    if ticket is None:
        print(f"skip {platform_run_id}: no remote-ticket artifact (and no S3 fallback)")
        return False

    cfg = ticket.get("config") or {}
    inputs = cfg.get("inputs")
    if not isinstance(inputs, dict):
        print(f"::warning::ticket {platform_run_id} missing config.inputs")
        return False

    # Prefer workflow_file / executor_ref from ticket config when present.
    ticket_wf = str(cfg.get("workflow_file") or "").strip() or wf_file
    ticket_ref = str(cfg.get("executor_ref") or "").strip() or ref

    print(f"Claiming platform_run_id={platform_run_id} via artifact ticket")
    executor_run_id, executor_run_url = dispatch_executor(ticket_wf, ticket_ref, inputs)
    write_claim(platform_run_id, executor_run_id, executor_run_url)
    print(f"Dispatched executor {executor_run_id} for {platform_run_id}")
    return True


def main() -> int:
    platform_repo = env("PLATFORM_REPOSITORY")
    ticket_base = env_opt("TICKET_BASE_URL")
    wf_file = env_opt("WORKFLOW_FILE") or "ci_remote_executor.yml"
    ref = env_opt("EXECUTOR_REF") or env_opt("GITHUB_REF_NAME") or "main"

    single = env_opt("PLATFORM_RUN_ID")
    run_ids = [single] if single else list_platform_run_ids(platform_repo)
    print(f"platform runs to consider: {len(run_ids)}" + (f" (single={single})" if single else ""))
    claimed = 0
    for rid in run_ids:
        try:
            if process_run(platform_repo, rid, ticket_base, wf_file, ref):
                claimed += 1
        except Exception as e:
            print(f"::warning::run {rid} failed: {e}")
    print(f"claimed={claimed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
