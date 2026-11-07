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
import time
import urllib.parse
from pathlib import Path
from typing import Any

_DISPATCHER_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _DISPATCHER_DIR.parents[3]
if str(_DISPATCHER_DIR) not in sys.path:
    sys.path.insert(0, str(_DISPATCHER_DIR))
_ANALYTICS_UTILS = _REPO_ROOT / ".github/scripts/utils/analytics"
_s = str(_ANALYTICS_UTILS)
if _s not in sys.path:
    sys.path.insert(0, _s)

from _common import (  # noqa: E402
    append_summary,
    cfg_str,
    die,
    discover_attempts,
    env,
    env_opt,
    host_s3,
    http_get_bytes,
    load_config_json,
    object_url,
    quote_url,
)
from github_actions.ci_metrics import flush_file  # noqa: E402

_TEXT_SUFFIXES = frozenset(
    {
        ".json",
        ".html",
        ".htm",
        ".txt",
        ".md",
        ".js",
        ".css",
        ".xml",
        ".yaml",
        ".yml",
        ".csv",
        ".log",
        ".out",
        ".err",
    }
)


def run(cmd: list[str], **kwargs: Any) -> None:
    print("+", " ".join(cmd), flush=True)
    subprocess.check_call(cmd, **kwargs)


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
    raw = None
    for _ in range(20):
        raw = http_get_bytes(url)
        if raw:
            break
        time.sleep(3)
    if not raw:
        die(f"missing remote_artifacts.json at {url}")
    meta = json.loads(raw.decode())
    if not isinstance(meta, dict) or int(meta.get("version") or 0) != 1:
        die(f"unexpected remote_artifacts.json schema: {meta!r}")
    return meta


_INDEX_HREF_RE = re.compile(r"""href=["']([^"']+)["']""", re.IGNORECASE)


def parse_index_hrefs(html: str) -> list[str]:
    """Relative file/dir links from Indexer index.html (skip parent/nav/abs)."""
    out: list[str] = []
    seen: set[str] = set()
    for raw in _INDEX_HREF_RE.findall(html):
        href = urllib.parse.unquote(raw.strip())
        if not href or href.startswith(("#", "?", "mailto:", "javascript:")):
            continue
        if "://" in href:
            continue
        if href.startswith("./"):
            href = href[2:]
        href = href.replace("\\", "/").lstrip("/")
        # Reject parent crumbs and any .. segment (a/../../x).
        if not href or any(p == ".." for p in href.split("/")):
            continue
        if href in seen:
            continue
        seen.add(href)
        out.append(href)
    return out


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


def download_prefix_via_index(endpoint: str, bucket: str, key_prefix: str, dest: Path) -> None:
    """Download a public artifact tree by walking Indexer index.html pages.

    Host public buckets often allow anonymous GetObject but deny ListBucket,
    so signed list sync fails. Indexer listings are public HTML and enumerate
    the same objects.
    """
    dest.mkdir(parents=True, exist_ok=True)
    root_key = key_prefix.strip("/")
    endpoint = endpoint.rstrip("/")

    # BFS over directory keys relative to root_key ("" = leaf root).
    queue: list[str] = [""]
    seen_dirs: set[str] = set()
    n_ok = 0
    n_fail = 0

    while queue:
        rel_dir = queue.pop(0)
        if rel_dir in seen_dirs:
            continue
        seen_dirs.add(rel_dir)

        index_key = _posix_join(root_key, rel_dir, "index.html")
        index_url = object_url(endpoint, bucket, index_key)
        raw = http_get_bytes(index_url)
        if raw is None:
            if rel_dir == "":
                die(f"missing index.html at {index_url}")
            print(f"::warning::missing index.html at {index_url}; skipping subtree")
            n_fail += 1
            continue

        index_rel = _posix_join(rel_dir, "index.html") if rel_dir else "index.html"
        local_index = _safe_under(dest, index_rel)
        if local_index is None:
            print(f"::warning::refusing unsafe index path {index_rel!r}")
            n_fail += 1
            continue
        _write_bytes(local_index, raw)
        n_ok += 1

        try:
            html = raw.decode("utf-8")
        except UnicodeDecodeError:
            html = raw.decode("utf-8", errors="replace")

        for href in parse_index_hrefs(html):
            child_rel = _posix_join(rel_dir, href)

            # Dir link: either "try_1/index.html" (recursive Indexer) or "try_1/".
            if href.endswith("/index.html") or href.endswith("/"):
                sub = (
                    child_rel[: -len("/index.html")]
                    if href.endswith("/index.html")
                    else child_rel.rstrip("/")
                )
                if sub and _safe_under(dest, sub) is not None and sub not in seen_dirs:
                    queue.append(sub)
                continue

            local_path = _safe_under(dest, child_rel)
            if local_path is None:
                print(f"::warning::refusing unsafe href path {child_rel!r}")
                n_fail += 1
                continue
            file_key = _posix_join(root_key, child_rel)
            file_url = object_url(endpoint, bucket, file_key)
            data = http_get_bytes(file_url)
            if data is None:
                print(f"::warning::failed to download {file_url}")
                n_fail += 1
                continue
            _write_bytes(local_path, data)
            n_ok += 1

    print(
        f"Downloaded {n_ok} object(s) via index.html crawl "
        f"(s3://{bucket}/{root_key}/); failures={n_fail}"
    )
    if n_ok == 0:
        die(f"index crawl downloaded nothing under s3://{bucket}/{root_key}/")


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


def build_run_name(event_name: str, run_id: str, pull_number: str, attempt: int) -> str:
    if event_name == "workflow_dispatch":
        base = f"{run_id}_manual"
    elif event_name in ("pull_request", "pull_request_target"):
        base = f"{run_id}_PR_{pull_number}" if pull_number else f"{run_id}_PR"
    elif event_name == "schedule":
        base = f"{run_id}_schedule"
    elif event_name == "push":
        base = f"{run_id}_POST"
    else:
        base = run_id
    return f"{base}_attempt_{attempt}"


def regenerate_summary(
    *,
    public_dir: Path,
    public_dir_url: str,
    attempts: list[dict[str, Any]],
    build_preset: str,
    branch: str,
    pull_number: str,
    workflow_run_id: str,
    scratch_dir: Path,
    job_summary: Path,
) -> Path:
    """Regenerate ya-test.html with YDB history; write enriched job_summary.

    Host publishes job_summary.md at run root for humans; ingest builds its own
    enriched summary from attempt artifacts. generate-summary is run with
    GITHUB_STEP_SUMMARY cleared so it cannot append to the ingest job summary.
    """
    summary_links = public_dir / "summary_links.txt"

    def _stub_summary() -> Path:
        job_summary.write_text(
            f"_No test attempts found._\n\n[Artifacts]({public_dir_url}/index.html)\n",
            encoding="utf-8",
        )
        return job_summary

    last = attempts[-1] if attempts else None
    if not last:
        return _stub_summary()

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
    if pull_number:
        cmd.extend(["--pr_number", pull_number])
    if oom.is_file() and oom.stat().st_size > 0:
        cmd.extend(["--oom_dmesg_log", str(oom)])
    cmd.extend(["Tests", str(html_path), str(report_path)])
    child_env = dict(os.environ)
    child_env["GITHUB_STEP_SUMMARY"] = ""
    try:
        run(cmd, cwd=str(_REPO_ROOT), env=child_env)
    except subprocess.CalledProcessError as e:
        print(f"::warning::generate-summary failed ({e}); writing stub job_summary")
        return _stub_summary()

    parts: list[str] = ["## Summary (enriched)\n\n"]
    if build_failed_md.is_file() and build_failed_md.stat().st_size > 0:
        parts.append(build_failed_md.read_text(encoding="utf-8").rstrip() + "\n\n")
    if text_file.is_file():
        body = text_file.read_text(encoding="utf-8")
        parts.append(body if body.endswith("\n") else body + "\n")
    job_summary.write_text("".join(parts), encoding="utf-8")
    return job_summary


def _as_uint(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def rewrite_ci_metrics_attribution(
    metrics_path: Path,
    *,
    run_id: str,
    event_name: str,
    workflow: str,
    job_name: str,
    pull_number: str,
    caller_repo: str,
) -> Path:
    """Rewrite remote-stamped JSONL rows to caller run attribution before YDB flush."""
    out = metrics_path.with_name(metrics_path.name + ".caller")
    run_url = f"https://github.com/{caller_repo}/actions/runs/{run_id}"
    pr_number = _as_uint(pull_number)
    rewritten = 0
    with metrics_path.open(encoding="utf-8") as src, out.open("w", encoding="utf-8") as dst:
        for line in src:
            raw = line.strip()
            if not raw:
                continue
            try:
                row = json.loads(raw)
            except json.JSONDecodeError:
                dst.write(line if line.endswith("\n") else line + "\n")
                continue
            if not isinstance(row, dict):
                dst.write(line if line.endswith("\n") else line + "\n")
                continue
            labels = row.get("labels") if isinstance(row.get("labels"), dict) else {}
            if row.get("run_id") is not None:
                labels.setdefault("remote_executor_run_id", row.get("run_id"))
            if row.get("github_job_id") is not None:
                labels.setdefault("remote_github_job_id", row.get("github_job_id"))
            row["run_id"] = _as_uint(run_id)
            row["event_name"] = event_name
            row["workflow"] = workflow
            row["job_name"] = job_name
            row["run_url"] = run_url
            if pr_number is not None:
                row["pr_number"] = pr_number
            # Keep host github_job_id; original also in labels.remote_github_job_id.
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
    pull_number: str,
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
        pull_number=pull_number,
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
    pull_number: str,
    job_name: str,
) -> None:
    for att in attempts:
        report = public_dir / att["report_relpath"]
        pull = build_run_name(event_name, run_id, pull_number, int(att["retry"]))
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
            "--pull",
            pull,
            "--job-name",
            job_name,
            "--job-id",
            run_id,
            "--branch",
            branch,
        ]
        run(cmd, cwd=str(_REPO_ROOT))


def s3cmd_sync_upload(local: Path, s3_bucket_path: str) -> None:
    run(
        [
            "s3cmd",
            "sync",
            "--follow-symlinks",
            "--acl-public",
            "--no-progress",
            "--stats",
            "--no-mime-magic",
            "--guess-mime-type",
            "--no-check-md5",
            f"{local}/",
            s3_bucket_path if s3_bucket_path.endswith("/") else s3_bucket_path + "/",
        ]
    )


def s3cmd_put(local: Path, s3_uri: str) -> None:
    run(
        [
            "s3cmd",
            "put",
            "--acl-public",
            "--no-progress",
            "--no-mime-magic",
            "--guess-mime-type",
            str(local),
            s3_uri,
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
    pull_number = str(ji.get("pull_number") or "").strip()
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
    caller_run_root = f"s3://{caller_bucket}/{run_root_key}"
    caller_run_root_url = quote_url(f"{caller_endpoint}/{caller_bucket}/{run_root_key}")

    executor_conclusion = env_opt("EXECUTOR_CONCLUSION")

    with tempfile.TemporaryDirectory(prefix="remote-ingest-") as tmp:
        tmp_path = Path(tmp)
        public_dir = tmp_path / "results"
        scratch_dir = tmp_path / "scratch"
        out_dir = tmp_path / "out"
        scratch_dir.mkdir()
        out_dir.mkdir()
        job_summary = out_dir / "job_summary.md"
        meta_path = out_dir / "remote_artifacts.json"

        print(f"Downloading s3://{host_bucket}/{key_prefix} → {public_dir} (via index.html)")
        download_prefix_via_index(host_ep, host_bucket, key_prefix, public_dir)

        n = rewrite_url_prefix(public_dir, old_public, new_public)
        print(f"Rewrote URL prefix in {n} files ({old_public} → {new_public})")

        run(
            [sys.executable, str(_REPO_ROOT / ".github/scripts/Indexer/indexer.py"), "-r", f"{public_dir}/"],
            cwd=str(_REPO_ROOT),
        )

        attempts = discover_attempts(public_dir)
        print(f"attempts={json.dumps(attempts, ensure_ascii=False)}")

        regenerate_summary(
            public_dir=public_dir,
            public_dir_url=new_public,
            attempts=attempts,
            build_preset=build_preset,
            branch=branch,
            pull_number=pull_number,
            workflow_run_id=caller_run_id,
            scratch_dir=scratch_dir,
            job_summary=job_summary,
        )

        meta_out = {
            "version": 1,
            "commit": commit,
            "caller_public_dir_url": new_public,
            "caller_artifact_key_prefix": dest_key_prefix,
            "caller_run_root_url": caller_run_root_url,
            "executor_run_id": run_id,
            "executor_run_url": run_url,
        }
        meta_path.write_text(json.dumps(meta_out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

        s3cmd_sync_upload(public_dir, caller_s3_path)
        s3cmd_put(job_summary, f"{caller_run_root}/job_summary.md")
        s3cmd_put(meta_path, f"{caller_run_root}/remote_artifacts.json")
        print(f"::notice title=Caller artifacts uploaded::{new_public}/index.html")

        upload_attempts(
            public_dir=public_dir,
            attempts=attempts,
            branch=branch,
            commit=commit,
            build_preset=build_preset,
            event_name=event_name,
            run_id=caller_run_id,
            pull_number=pull_number,
            job_name=job_name,
        )

        flush_ci_metrics(
            public_dir=public_dir,
            event_name=event_name,
            run_id=caller_run_id,
            pull_number=pull_number,
            caller_repo=caller_repo,
            caller_workflow=caller_workflow,
            job_name=job_name,
        )

        body = job_summary.read_text(encoding="utf-8").rstrip() + "\n"
        footer_bits = [f"Remote executor: [{run_id}]({run_url})"]
        if executor_conclusion:
            footer_bits.append(f"`{executor_conclusion}`")
        append_summary(body + "\n---\n" + " · ".join(footer_bits) + "\n")

    print(f"::notice title=Remote ingest complete::{new_public}/index.html")


if __name__ == "__main__":
    main()
