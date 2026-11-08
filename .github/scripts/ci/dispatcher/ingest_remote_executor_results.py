#!/usr/bin/env python3
"""Post-wait ingest: remote-repo artifacts → caller S3 + YDB QA + enriched summary.

Env (from dispatch_remote.yml ingest): CONFIG_JSON (resolve output),
EXECUTOR_RUN_ID / EXECUTOR_RUN_URL / EXECUTOR_CONCLUSION,
CALLER_AWS_ENDPOINT / CALLER_AWS_BUCKET, GITHUB_STEP_SUMMARY.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

_DISPATCHER_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _DISPATCHER_DIR.parents[3]
sys.path.insert(0, str(_DISPATCHER_DIR))
sys.path.insert(0, str(_REPO_ROOT / ".github/scripts/utils/analytics"))

from _common import (  # noqa: E402
    append_summary,
    cfg_str,
    die,
    discover_attempts,
    env,
    env_opt,
    host_s3,
    http_get_bytes_retry,
    load_config_json,
    object_url,
    quote_url,
)
from github_actions.ci_metrics import flush_file  # noqa: E402

_TEXT_SUFFIXES = frozenset(
    ".json .html .htm .txt .md .js .css .xml .yaml .yml .csv .log .out .err".split()
)

_TRY_ARTIFACT_SUFFIXES = (
    "report.json",
    "started_at",
    "index.html",
)


def run(cmd: list[str], **kwargs: Any) -> None:
    print("+", " ".join(cmd), flush=True)
    subprocess.check_call(cmd, **kwargs)


def normalize_manifest_files(meta: dict[str, Any]) -> list[str]:
    """Validate and dedupe manifest `files` (POSIX paths relative to artifact_key_prefix)."""
    raw = meta.get("files")
    if not isinstance(raw, list) or not raw:
        die("remote_artifacts.json missing non-empty files list")
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, str):
            die(f"invalid files entry: {item!r}")
        rel = item.strip()
        if not rel or rel.startswith("/"):
            die(f"invalid file path: {item!r}")
        if "\\" in rel:
            die(f"invalid file path (backslash): {item!r}")
        parts = [p for p in rel.split("/") if p and p != "."]
        if not parts or any(p == ".." for p in parts):
            die(f"invalid file path: {item!r}")
        rel = "/".join(parts)
        if rel in seen:
            continue
        seen.add(rel)
        out.append(rel)
    return out


def load_manifest(
    endpoint: str,
    bucket: str,
    host_repo: str,
    host_workflow: str,
    run_id: str,
) -> dict[str, Any]:
    url = object_url(
        endpoint, bucket, f"{host_repo}/{host_workflow}/{run_id}/remote_artifacts.json"
    )
    print(f"Fetching remote_artifacts.json from {url}")
    # Wait already saw host run completed; host publishes this in its final step.
    raw = http_get_bytes_retry(url)
    if not raw:
        die(f"missing remote_artifacts.json at {url}")
    meta = json.loads(raw.decode())
    if not isinstance(meta, dict) or int(meta.get("version") or 0) != 1:
        die(f"unexpected remote_artifacts.json schema: {meta!r}")
    normalize_manifest_files(meta)
    return meta


def _posix_join(*parts: str) -> str:
    return "/".join(p.strip("/") for p in parts if p and p.strip("/"))


def _safe_under(dest: Path, child_rel: str) -> Path | None:
    """Resolve child_rel under dest; None if it escapes (path traversal)."""
    parts = [p for p in child_rel.split("/") if p and p != "."]
    if any(p == ".." for p in parts):
        return None
    local = dest.joinpath(*parts) if parts else dest
    try:
        local.resolve().relative_to(dest.resolve())
    except ValueError:
        return None
    return local


def _write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def _is_required_artifact(rel: str) -> bool:
    """Paths that must be present when listed in remote_artifacts.json `files`."""
    rel = rel.strip("/")
    if rel in ("index.html", "ci_metrics.jsonl"):
        return True
    return (
        re.match(
            r"try_\d+/(index\.html|report\.json|ya-test\.html|started_at)$",
            rel,
        )
        is not None
    )


def _try_dirs_in_files(files: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for rel in files:
        m = re.match(r"(try_\d+)/", rel)
        if not m:
            continue
        tn = m.group(1)
        if tn in seen:
            continue
        seen.add(tn)
        out.append(tn)
    return sorted(out, key=lambda s: int(s.split("_", 1)[1]))


def validate_downloaded_manifest(
    files: list[str], dest: Path, failed: list[str]
) -> None:
    """Enforce required paths from manifest `files` after GetObject download."""
    failed_set = set(failed)
    if "ci_metrics.jsonl" not in files:
        die("remote_artifacts.json files missing ci_metrics.jsonl")
    for tn in _try_dirs_in_files(files):
        for suffix in _TRY_ARTIFACT_SUFFIXES:
            rel = f"{tn}/{suffix}"
            if rel not in files:
                die(f"remote_artifacts.json files missing {rel}")
    for rel in files:
        if not _is_required_artifact(rel):
            continue
        if rel in failed_set:
            die(f"incomplete remote artifacts: {rel}")
        local = _safe_under(dest, rel)
        if local is None or not local.is_file():
            die(f"incomplete remote artifacts: {rel}")


def download_listed_objects(
    endpoint: str,
    bucket: str,
    key_prefix: str,
    files: list[str],
    dest: Path,
    *,
    fetch=http_get_bytes_retry,
) -> list[str]:
    """Download manifest-listed objects via anonymous GetObject.

    Returns relative paths that failed to download. Dies if any required path
    listed in `files` cannot be fetched (after retries).
    """
    dest.mkdir(parents=True, exist_ok=True)
    root_key = key_prefix.strip("/")
    n_ok = 0
    failed: list[str] = []

    for child_rel in files:
        local_path = _safe_under(dest, child_rel)
        if local_path is None:
            print(f"::warning::refusing unsafe manifest path {child_rel!r}")
            failed.append(child_rel)
            continue
        file_key = _posix_join(root_key, child_rel)
        file_url = object_url(endpoint.rstrip("/"), bucket, file_key)
        data = fetch(file_url)
        if data is None:
            print(f"::warning::failed to download {file_url}")
            failed.append(child_rel)
            continue
        _write_bytes(local_path, data)
        n_ok += 1

    print(
        f"Downloaded {n_ok} object(s) from manifest "
        f"(s3://{bucket}/{root_key}/); failures={len(failed)}"
    )
    if n_ok == 0:
        die(f"manifest download retrieved nothing under s3://{bucket}/{root_key}/")

    validate_downloaded_manifest(files, dest, failed)

    required_failed = [p for p in failed if _is_required_artifact(p)]
    if required_failed:
        shown = ", ".join(required_failed[:20])
        die(f"incomplete remote artifacts: {shown}")
    if failed:
        shown = ", ".join(failed[:20])
        print(f"::warning::{len(failed)} non-required artifact(s) failed to copy: {shown}")
    return failed


def rewrite_url_prefix(root: Path, old: str, new: str) -> int:
    if not old or old == new:
        return 0
    n = 0
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in _TEXT_SUFFIXES:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        if old not in text:
            continue
        path.write_text(text.replace(old, new), encoding="utf-8")
        n += 1
    return n


def regenerate_summary(
    *,
    public_dir: Path,
    public_dir_url: str,
    attempts: list[dict[str, Any]],
    build_preset: str,
    branch: str,
    workflow_run_id: str,
    scratch_dir: Path,
) -> str:
    """Regenerate ya-test.html with YDB history; return enriched summary markdown.

    generate-summary runs with GITHUB_STEP_SUMMARY cleared so it cannot append
    to the ingest job summary.
    """
    summary_links = public_dir / "summary_links.txt"
    stub = f"_No test attempts found._\n\n[Artifacts]({public_dir_url}/index.html)\n"

    last = attempts[-1] if attempts else None
    if not last:
        return stub

    html_path = public_dir / last["html_relpath"]
    report_path = public_dir / last["report_relpath"]
    color_file = scratch_dir / "ingest_summary_color.txt"
    text_file = scratch_dir / "ingest_summary_text.txt"
    try_dir = public_dir / f"try_{last['retry']}"
    oom = try_dir / "oom_dmesg.txt"
    build_failed_md = try_dir / "build_failed.md"

    cmd = [
        sys.executable,
        str(_REPO_ROOT / ".github/scripts/tests/generate-summary.py"),
        "--summary_links",
        str(summary_links),
        "--public_dir",
        str(public_dir),
        "--public_dir_url",
        public_dir_url,
        "--build_preset",
        build_preset,
        "--branch",
        branch,
        "--is_retry",
        "0" if len(attempts) <= 1 else "1",
        "--is_last_retry",
        "1",
        "--is_test_result_ignored",
        "0",
        "--comment_color_file",
        str(color_file),
        "--comment_text_file",
        str(text_file),
        "--workflow_run_id",
        workflow_run_id,
    ]
    if oom.is_file() and oom.stat().st_size > 0:
        cmd.extend(["--oom_dmesg_log", str(oom)])
    cmd.extend(["Tests", str(html_path), str(report_path)])
    child_env = dict(os.environ)
    child_env["GITHUB_STEP_SUMMARY"] = ""
    try:
        run(cmd, cwd=str(_REPO_ROOT), env=child_env)
    except subprocess.CalledProcessError as e:
        print(f"::warning::generate-summary failed ({e}); writing stub job_summary")
        return stub

    parts: list[str] = ["## Summary (enriched)\n\n"]
    if build_failed_md.is_file() and build_failed_md.stat().st_size > 0:
        parts.append(build_failed_md.read_text(encoding="utf-8").rstrip() + "\n\n")
    if text_file.is_file():
        body = text_file.read_text(encoding="utf-8")
        parts.append(body if body.endswith("\n") else body + "\n")
    return "".join(parts)


def rewrite_ci_metrics_attribution(
    metrics_path: Path,
    *,
    run_id: str,
    event_name: str,
    workflow: str,
    job_name: str,
    caller_repo: str,
) -> Path:
    """Rewrite remote-stamped JSONL rows to caller run attribution before YDB flush."""
    out = metrics_path.with_name(metrics_path.name + ".caller")
    run_url = f"https://github.com/{caller_repo}/actions/runs/{run_id}"
    caller_run_id = int(run_id)
    attempt_raw = env_opt("PLATFORM_RUN_ATTEMPT") or env_opt("GITHUB_RUN_ATTEMPT")
    caller_attempt = int(attempt_raw) if attempt_raw.isdigit() else None
    rewritten = 0
    with metrics_path.open(encoding="utf-8") as src, out.open("w", encoding="utf-8") as dst:
        for line in src:
            raw = line.strip()
            if not raw:
                continue
            try:
                row = json.loads(raw)
            except json.JSONDecodeError:
                row = None
            if not isinstance(row, dict):
                dst.write(line if line.endswith("\n") else line + "\n")
                continue
            labels = row.get("labels") if isinstance(row.get("labels"), dict) else {}
            if row.get("run_id") is not None:
                labels.setdefault("remote_executor_run_id", row.get("run_id"))
            if caller_attempt is not None:
                if row.get("run_attempt") is not None:
                    labels.setdefault("remote_executor_run_attempt", row.get("run_attempt"))
                row["run_attempt"] = caller_attempt
            row["run_id"] = caller_run_id
            row["event_name"] = event_name
            row["workflow"] = workflow
            row["job_name"] = job_name
            row["run_url"] = run_url
            if labels:
                row["labels"] = labels
            dst.write(json.dumps(row, ensure_ascii=False) + "\n")
            rewritten += 1
    print(f"Rewrote {rewritten} ci_metrics row(s) for caller run_id={run_id}")
    return out


def flush_ci_metrics(
    public_dir: Path,
    *,
    event_name: str,
    run_id: str,
    caller_repo: str,
    caller_workflow: str,
    job_name: str,
) -> None:
    """Flush remote-buffered ci_metrics.jsonl into caller analytics/ci_metrics."""
    metrics_path = public_dir / "ci_metrics.jsonl"
    if not metrics_path.is_file() or metrics_path.stat().st_size == 0:
        print(f"No ci_metrics at {metrics_path}; skipping analytics flush")
        return

    rewritten = rewrite_ci_metrics_attribution(
        metrics_path,
        run_id=run_id,
        event_name=event_name,
        workflow=caller_workflow,
        job_name=job_name,
        caller_repo=caller_repo,
    )

    print(f"Flushing ci_metrics from {rewritten}")
    uploaded = flush_file(str(rewritten))
    print(f"ci_metrics flush uploaded={uploaded}")


def upload_attempts(
    public_dir: Path,
    attempts: list[dict[str, Any]],
    *,
    branch: str,
    commit: str,
    build_preset: str,
    event_name: str,
    run_id: str,
    job_name: str,
) -> None:
    for att in attempts:
        report = public_dir / att["report_relpath"]
        cmd = [
            sys.executable,
            str(_REPO_ROOT / ".github/scripts/analytics/upload_tests_results.py"),
            "--test-results-file",
            str(report),
            "--run-timestamp",
            att["started_at"],
            "--commit",
            commit,
            "--build-type",
            build_preset,
            "--event-name",
            event_name,
            "--attempt",
            str(int(att["retry"])),
            "--job-name",
            job_name,
            "--job-id",
            run_id,
            "--branch",
            branch,
        ]
        run(cmd, cwd=str(_REPO_ROOT))


def _s3_acl_args() -> list[str]:
    """S3_PUBLIC_ACL_FLAG from s3cmd action; unset → --acl-public, empty → no flag."""
    if "S3_PUBLIC_ACL_FLAG" not in os.environ:
        return ["--acl-public"]
    flag = os.environ["S3_PUBLIC_ACL_FLAG"].strip()
    return [flag] if flag else []


def s3cmd_sync_upload(local: Path, s3_bucket_path: str) -> None:
    run(
        [
            "s3cmd",
            "sync",
            "--follow-symlinks",
            *_s3_acl_args(),
            "--no-progress",
            "--stats",
            "--no-mime-magic",
            "--guess-mime-type",
            "--no-check-md5",
            f"{local}/",
            s3_bucket_path.rstrip("/") + "/",
        ]
    )


def main() -> None:
    cfg = load_config_json()
    host_repo = cfg_str(cfg, "repository")
    host_workflow = cfg_str(cfg, "workflow_name")
    host_ep, host_bucket = host_s3(cfg)
    caller_workflow = cfg_str(cfg, "caller_workflow")
    job_name = cfg_str(cfg, "analytics_job_name")

    inputs = cfg.get("inputs")
    if not isinstance(inputs, dict):
        die("CONFIG_JSON.inputs missing")
    ji = inputs.get("job_inputs")
    if not isinstance(ji, dict) or not ji:
        die("CONFIG_JSON.inputs.job_inputs missing")
    build_preset = cfg_str(ji, "build_preset")
    branch = cfg_str(ji, "custom_branch_name")

    event_name = env("GITHUB_EVENT_NAME")
    caller_run_id = env("GITHUB_RUN_ID")
    run_id = env("EXECUTOR_RUN_ID")
    run_url = env("EXECUTOR_RUN_URL")
    caller_endpoint = env("CALLER_AWS_ENDPOINT").rstrip("/")
    caller_bucket = env("CALLER_AWS_BUCKET")
    caller_repo = env("GITHUB_REPOSITORY")

    meta = load_manifest(host_ep, host_bucket, host_repo, host_workflow, run_id)
    files = normalize_manifest_files(meta)
    key_prefix = str(meta.get("artifact_key_prefix") or "").strip()
    if not key_prefix:
        die("remote_artifacts.json missing artifact_key_prefix")
    old_public = (meta.get("public_dir_url") or "").rstrip("/")
    commit = str(meta.get("commit") or "").strip()
    if not commit:
        die("remote_artifacts.json missing commit")

    leaf = key_prefix.rstrip("/").rsplit("/", 1)[-1]
    run_root_key = f"{caller_repo}/{caller_workflow}/{caller_run_id}/remote-{run_id}"
    dest_key_prefix = f"{run_root_key}/{leaf}"
    new_public = quote_url(f"{caller_endpoint}/{caller_bucket}/{dest_key_prefix}")
    caller_s3_path = f"s3://{caller_bucket}/{dest_key_prefix}"

    executor_conclusion = env_opt("EXECUTOR_CONCLUSION")

    with tempfile.TemporaryDirectory(prefix="remote-ingest-") as tmp:
        tmp_path = Path(tmp)
        public_dir = tmp_path / "results"
        scratch_dir = tmp_path / "scratch"
        scratch_dir.mkdir()

        print(f"Downloading s3://{host_bucket}/{key_prefix} → {public_dir} (manifest files)")
        download_failed = download_listed_objects(
            host_ep, host_bucket, key_prefix, files, public_dir
        )

        n = rewrite_url_prefix(public_dir, old_public, new_public)
        print(f"Rewrote URL prefix in {n} files ({old_public} → {new_public})")

        run(
            [sys.executable, str(_REPO_ROOT / ".github/scripts/Indexer/indexer.py"), "-r", f"{public_dir}/"],
            cwd=str(_REPO_ROOT),
        )

        attempts = discover_attempts(public_dir)
        print(f"attempts={json.dumps(attempts, ensure_ascii=False)}")

        summary_md = regenerate_summary(
            public_dir=public_dir,
            public_dir_url=new_public,
            attempts=attempts,
            build_preset=build_preset,
            branch=branch,
            workflow_run_id=caller_run_id,
            scratch_dir=scratch_dir,
        )
        if download_failed:
            note = (
                f"\n\n_{len(download_failed)} non-required artifact(s) failed to copy "
                f"from the host tree._\n"
            )
            summary_md = summary_md.rstrip() + note

        s3cmd_sync_upload(public_dir, caller_s3_path)
        print(f"::notice title=Caller artifacts uploaded::{new_public}/index.html")

        upload_attempts(
            public_dir=public_dir,
            attempts=attempts,
            branch=branch,
            commit=commit,
            build_preset=build_preset,
            event_name=event_name,
            run_id=caller_run_id,
            job_name=job_name,
        )

        flush_ci_metrics(
            public_dir=public_dir,
            event_name=event_name,
            run_id=caller_run_id,
            caller_repo=caller_repo,
            caller_workflow=caller_workflow,
            job_name=job_name,
        )

        body = summary_md.rstrip() + "\n"
        footer_bits = [f"Remote executor: [{run_id}]({run_url})"]
        if executor_conclusion:
            footer_bits.append(f"`{executor_conclusion}`")
        append_summary(body + "\n---\n" + " · ".join(footer_bits) + "\n")

    print(f"::notice title=Remote ingest complete::{new_public}/index.html")


if __name__ == "__main__":
    main()
