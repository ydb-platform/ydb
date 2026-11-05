#!/usr/bin/env python3
"""Post-wait ingest: remote-repo artifacts → caller S3 + YDB QA + enriched summary.

Env (from dispatch_remote.yml ingest): EXECUTOR_RUN_ID, EXECUTOR_RUN_URL,
REMOTE_AWS_ENDPOINT/BUCKET, REMOTE_WORKFLOW_NAME, CALLER_AWS_ENDPOINT/BUCKET,
optional SOURCE_EVENT_NAME/SOURCE_RUN_ID/PULL_NUMBER/ANALYTICS_JOB_NAME,
GITHUB_REPOSITORY/WORKFLOW/RUN_ID/STEP_SUMMARY, WORKSPACE.
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
import urllib.request
from pathlib import Path
from typing import Any


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


def append_summary(text: str) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        f.write(text if text.endswith("\n") else text + "\n")


def http_get(url: str, timeout: int = 60) -> bytes | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.read()
    except Exception as e:
        print(f"GET {url} failed: {e}")
        return None


def quote_url(url: str) -> str:
    return urllib.parse.quote(url, safe=":/")


def run(cmd: list[str], **kwargs: Any) -> None:
    print("+", " ".join(cmd), flush=True)
    subprocess.check_call(cmd, **kwargs)


def remote_artifacts_url(run_id: str) -> str:
    base = (
        f"{env('REMOTE_AWS_ENDPOINT').rstrip('/')}/{env('REMOTE_AWS_BUCKET')}/"
        f"{env('REPO')}/{env('REMOTE_WORKFLOW_NAME')}/{run_id}/remote_artifacts.json"
    )
    return quote_url(base)


def load_manifest(run_id: str) -> dict[str, Any]:
    url = remote_artifacts_url(run_id)
    print(f"Fetching remote_artifacts.json from {url}")
    raw = None
    for _ in range(20):
        raw = http_get(url)
        if raw:
            break
        time.sleep(3)
    if not raw:
        die(f"missing remote_artifacts.json at {url}")
    meta = json.loads(raw.decode())
    if not isinstance(meta, dict) or int(meta.get("version") or 0) != 1:
        die(f"unexpected remote_artifacts.json schema: {meta!r}")
    return meta


def aws_sync_public(endpoint: str, bucket: str, key_prefix: str, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    src = f"s3://{bucket}/{key_prefix.rstrip('/')}"
    cmd = [
        "aws",
        "s3",
        "sync",
        src,
        str(dest),
        "--no-sign-request",
        "--endpoint-url",
        endpoint.rstrip("/"),
    ]
    try:
        run(cmd)
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        die(f"failed to download remote artifacts ({src}): {e}")


def rewrite_url_prefix(root: Path, old: str, new: str) -> int:
    if not old or old == new:
        return 0
    olds = {old, old.rstrip("/"), quote_url(old), quote_url(old.rstrip("/"))}
    news_map = {
        old: new,
        old.rstrip("/"): new.rstrip("/"),
        quote_url(old): quote_url(new),
        quote_url(old.rstrip("/")): quote_url(new.rstrip("/")),
    }
    n = 0
    text_suffixes = {
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
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in text_suffixes and path.name not in (
            "summary_links.txt",
            "job_summary.md",
            "remote_artifacts.json",
        ):
            continue
        try:
            data = path.read_bytes()
        except OSError:
            continue
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            continue
        orig = text
        for o in olds:
            if o and o in text:
                text = text.replace(o, news_map.get(o, new))
        if text != orig:
            path.write_text(text, encoding="utf-8")
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


def discover_attempts(public_dir: Path, meta: dict[str, Any]) -> list[dict[str, Any]]:
    attempts = list(meta.get("attempts") or [])
    if attempts:
        return attempts
    found = []
    for report in sorted(public_dir.glob("try_*/report.json")):
        m = re.match(r"try_(\d+)$", report.parent.name)
        if not m:
            continue
        retry = int(m.group(1))
        found.append(
            {
                "retry": retry,
                "started_at": str(int(report.stat().st_mtime)),
                "report_relpath": str(report.relative_to(public_dir)),
                "html_relpath": str((report.parent / "ya-test.html").relative_to(public_dir)),
            }
        )
    return found


def regenerate_summary(
    workspace: Path,
    public_dir: Path,
    public_dir_url: str,
    attempts: list[dict[str, Any]],
    build_preset: str,
    branch: str,
    pull_number: str,
    workflow_run_id: str,
) -> Path:
    """Regenerate ya-test.html with YDB history and append markdown to the step summary."""
    summary_links = public_dir / "summary_links.txt"
    if not summary_links.exists():
        summary_links.write_text(f"00 [Artifacts]({public_dir_url}/index.html)\n", encoding="utf-8")

    last = attempts[-1] if attempts else None
    if not last:
        append_summary("_No test attempts found to regenerate._\n")
        return public_dir / "job_summary.md"

    html_rel = last.get("html_relpath") or f"try_{last['retry']}/ya-test.html"
    report_rel = last.get("report_relpath") or f"try_{last['retry']}/report.json"
    html_path = public_dir / html_rel
    report_path = public_dir / report_rel
    if not report_path.is_file():
        die(f"missing report for regenerate: {report_path}")

    html_path.parent.mkdir(parents=True, exist_ok=True)

    color_file = public_dir / "ingest_summary_color.txt"
    text_file = public_dir / "ingest_summary_text.txt"
    status_file = public_dir / "ingest_statusrep.txt"
    oom = public_dir / f"try_{last['retry']}" / "oom_dmesg.txt"

    cmd = [
        sys.executable,
        str(workspace / ".github/scripts/tests/generate-summary.py"),
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
        "--status_report_file",
        str(status_file),
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
    if oom.is_file():
        cmd.extend(["--oom_dmesg_log", str(oom)])
    cmd.extend(["Tests", str(html_path), str(report_path)])
    run(cmd, cwd=str(workspace))

    job_summary = public_dir / "job_summary.md"
    parts: list[str] = [
        "## Summary (enriched)\n",
        f"_Reuploaded from remote executor; links use `{public_dir_url}`._\n\n",
    ]
    if text_file.is_file():
        parts.append(text_file.read_text(encoding="utf-8"))
        if not parts[-1].endswith("\n"):
            parts.append("\n")
    job_summary.write_text("".join(parts), encoding="utf-8")
    append_summary(job_summary.read_text(encoding="utf-8"))
    return job_summary


def flush_ci_metrics(workspace: Path, public_dir: Path, meta: dict[str, Any]) -> None:
    """Flush remote-buffered ci_metrics.jsonl into caller analytics/ci_metrics."""
    rel = (meta.get("ci_metrics_relpath") or "ci_metrics.jsonl").strip() or "ci_metrics.jsonl"
    candidates = [
        public_dir / rel,
        public_dir / "ci_metrics.jsonl",
        public_dir / "analytics.jsonl",
    ]
    metrics_path = next((p for p in candidates if p.is_file() and p.stat().st_size > 0), None)
    if metrics_path is None:
        print("No ci_metrics JSONL in remote artifacts; skipping analytics flush")
        return

    os.environ.pop("CI_DISABLE_YDB_QA", None)
    analytics_dir = workspace / ".github/scripts/utils/analytics"
    ga_dir = analytics_dir / "github_actions"
    for path in (str(analytics_dir), str(ga_dir), str(workspace / ".github/scripts/analytics")):
        if path not in sys.path:
            sys.path.insert(0, path)

    from github_actions.ci_metrics import flush_file  # noqa: E402

    print(f"Flushing ci_metrics from {metrics_path}")
    uploaded = flush_file(str(metrics_path))
    print(f"ci_metrics flush uploaded={uploaded}")
    append_summary(f"- **ci_metrics:** flushed `{metrics_path.name}` (uploaded={uploaded})\n")


def upload_attempts(
    workspace: Path,
    public_dir: Path,
    attempts: list[dict[str, Any]],
    meta: dict[str, Any],
    event_name: str,
    run_id: str,
    pull_number: str,
) -> None:
    job_name = env_opt("ANALYTICS_JOB_NAME") or meta.get("analytics_job_name") or env("GITHUB_WORKFLOW")
    branch = meta.get("branch") or "main"
    commit = meta.get("commit") or ""
    build_preset = meta.get("build_preset") or "relwithdebinfo"
    if not commit:
        die("remote_artifacts.json missing commit")

    for att in attempts:
        report = public_dir / att["report_relpath"]
        if not report.is_file():
            print(f"::warning::skip upload, missing {report}")
            continue
        pull = build_run_name(event_name, run_id, pull_number, int(att["retry"]))
        cmd = [
            sys.executable,
            str(workspace / ".github/scripts/analytics/upload_tests_results.py"),
            "--test-results-file",
            str(report),
            "--run-timestamp",
            str(att.get("started_at") or "0"),
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
        run(cmd, cwd=str(workspace))


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
    run_id = env("EXECUTOR_RUN_ID")
    run_url = env_opt("EXECUTOR_RUN_URL") or f"https://github.com/{env('REPO')}/actions/runs/{run_id}"
    workspace = Path(env("WORKSPACE")).resolve()
    caller_endpoint = env("CALLER_AWS_ENDPOINT").rstrip("/")
    caller_bucket = env("CALLER_AWS_BUCKET")
    caller_repo = env("GITHUB_REPOSITORY")
    caller_workflow = env("GITHUB_WORKFLOW")
    caller_run_id = env("GITHUB_RUN_ID")

    meta = load_manifest(run_id)
    remote_endpoint = (meta.get("s3_endpoint") or env("REMOTE_AWS_ENDPOINT")).rstrip("/")
    remote_bucket = meta.get("s3_bucket") or env("REMOTE_AWS_BUCKET")
    key_prefix = meta.get("artifact_key_prefix") or ""
    if not key_prefix:
        die("remote_artifacts.json missing artifact_key_prefix")
    old_public = (meta.get("public_dir_url") or "").rstrip("/")

    leaf = key_prefix.rstrip("/").rsplit("/", 1)[-1]
    dest_key_prefix = f"{caller_repo}/{caller_workflow}/{caller_run_id}/remote-{run_id}/{leaf}"
    new_public = f"{caller_endpoint}/{caller_bucket}/{dest_key_prefix}"
    caller_s3_path = f"s3://{caller_bucket}/{dest_key_prefix}"
    caller_run_root = f"s3://{caller_bucket}/{caller_repo}/{caller_workflow}/{caller_run_id}/remote-{run_id}"

    event_name = env_opt("SOURCE_EVENT_NAME") or meta.get("source_event_name") or "workflow_dispatch"
    source_run_id = env_opt("SOURCE_RUN_ID") or meta.get("source_run_id") or caller_run_id
    pull_number = env_opt("PULL_NUMBER") or str(meta.get("pull_number") or "")

    append_summary(
        "\n".join(
            [
                "### Remote ingest",
                f"- **executor run:** [{run_id}]({run_url})",
                f"- **remote artifacts:** `{remote_bucket}/{key_prefix}`",
                f"- **caller artifacts:** [{new_public}/index.html]({quote_url(new_public + '/index.html')})",
                f"- **source event:** `{event_name}` run `{source_run_id}`",
                "",
            ]
        )
    )

    with tempfile.TemporaryDirectory(prefix="remote-ingest-") as tmp:
        public_dir = Path(tmp) / "results"
        print(f"Downloading s3://{remote_bucket}/{key_prefix} → {public_dir}")
        aws_sync_public(remote_endpoint, remote_bucket, key_prefix, public_dir)

        n = rewrite_url_prefix(public_dir, old_public, new_public)
        print(f"Rewrote URL prefix in {n} files ({old_public} → {new_public})")

        indexer = workspace / ".github/scripts/Indexer/indexer.py"
        if indexer.is_file():
            run([sys.executable, str(indexer), "-r", f"{public_dir}/"], cwd=str(workspace))

        attempts = discover_attempts(public_dir, meta)
        print(f"attempts={json.dumps(attempts, ensure_ascii=False)}")

        s3cmd_sync_upload(public_dir, caller_s3_path)

        job_summary = regenerate_summary(
            workspace=workspace,
            public_dir=public_dir,
            public_dir_url=new_public,
            attempts=attempts,
            build_preset=meta.get("build_preset") or "relwithdebinfo",
            branch=meta.get("branch") or "main",
            pull_number=pull_number,
            workflow_run_id=source_run_id,
        )

        s3cmd_sync_upload(public_dir, caller_s3_path)
        s3cmd_put(job_summary, f"{caller_run_root}/job_summary.md")

        meta_out = dict(meta)
        meta_out.update(
            {
                "ingested": True,
                "caller_public_dir_url": new_public,
                "caller_artifact_key_prefix": dest_key_prefix,
                "caller_run_root_url": (
                    f"{caller_endpoint}/{caller_bucket}/{caller_repo}/"
                    f"{caller_workflow}/{caller_run_id}/remote-{run_id}"
                ),
                "source_event_name": event_name,
                "source_run_id": source_run_id,
                "pull_number": pull_number,
            }
        )
        meta_path = public_dir / "remote_artifacts.json"
        meta_path.write_text(json.dumps(meta_out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        s3cmd_put(meta_path, f"{caller_run_root}/remote_artifacts.json")

        upload_attempts(
            workspace=workspace,
            public_dir=public_dir,
            attempts=attempts,
            meta=meta,
            event_name=event_name,
            run_id=source_run_id,
            pull_number=pull_number,
        )

        flush_ci_metrics(workspace=workspace, public_dir=public_dir, meta=meta)

    print(f"::notice title=Remote ingest complete::{new_public}/index.html")


if __name__ == "__main__":
    main()
