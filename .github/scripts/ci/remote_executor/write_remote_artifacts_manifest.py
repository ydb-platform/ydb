#!/usr/bin/env python3
"""Write remote_artifacts.json for caller-side post-wait ingest.

Reads S3_BUCKET_PATH / PUBLIC_DIR_URL (and optional analytics env) and writes a
small locator next to job_summary.md at the S3 run root. Discovers try_*/attempts
from PUBLIC_DIR when present.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

_DISPATCHER = Path(__file__).resolve().parents[1] / "dispatcher"
sys.path.insert(0, str(_DISPATCHER))
from _common import discover_attempts, truthy  # noqa: E402


def main() -> int:
    out = os.environ.get("REMOTE_ARTIFACTS_JSON") or (
        sys.argv[1] if len(sys.argv) > 1 else ""
    )
    if not out:
        print("usage: REMOTE_ARTIFACTS_JSON=path write_remote_artifacts_manifest.py", file=sys.stderr)
        return 2

    bucket_path = os.environ["S3_BUCKET_PATH"]  # s3://bucket/key/prefix
    if not bucket_path.startswith("s3://"):
        print(f"S3_BUCKET_PATH must be s3://...; got {bucket_path!r}", file=sys.stderr)
        return 2
    rest = bucket_path[len("s3://") :]
    bucket, _, key_prefix = rest.partition("/")
    run_root_key = key_prefix.rsplit("/", 1)[0]
    public_dir_url = os.environ["PUBLIC_DIR_URL"].rstrip("/")
    run_root_url = public_dir_url.rsplit("/", 1)[0]
    endpoint = os.environ.get("AWS_ENDPOINT") or os.environ.get("S3_ENDPOINT") or ""
    if not endpoint and public_dir_url.startswith("http"):
        parts = public_dir_url.split("/")
        if len(parts) >= 3:
            endpoint = "/".join(parts[:3])

    disabled = truthy(os.environ.get("CI_DISABLE_YDB_QA"))
    public_dir = Path(os.environ.get("PUBLIC_DIR") or Path(out).resolve().parent)
    attempts = discover_attempts(public_dir)

    meta = {
        "version": 1,
        "host_repository": os.environ.get("GITHUB_REPOSITORY", ""),
        "s3_bucket": bucket,
        "s3_endpoint": endpoint,
        "artifact_key_prefix": key_prefix,
        "run_root_key_prefix": run_root_key,
        "public_dir_url": public_dir_url,
        "run_root_url": run_root_url,
        "build_preset": os.environ.get("BUILD_PRESET", ""),
        "branch": os.environ.get("BRANCH_NAME", ""),
        "commit": os.environ.get("ORIGINAL_HEAD", ""),
        "analytics_job_name": os.environ.get("ANALYTICS_JOB_NAME", ""),
        "source_event_name": os.environ.get("SOURCE_EVENT_NAME")
        or os.environ.get("GITHUB_EVENT_NAME", ""),
        "source_run_id": os.environ.get("SOURCE_RUN_ID") or os.environ.get("GITHUB_RUN_ID", ""),
        "pull_number": os.environ.get("PR_NUMBER", ""),
        "executor_run_id": os.environ.get("GITHUB_RUN_ID", ""),
        "executor_workflow": os.environ.get("GITHUB_WORKFLOW", ""),
        "ci_metrics_relpath": "ci_metrics.jsonl" if disabled else "",
        "attempts": attempts,
    }
    with open(out, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
        f.write("\n")
    print(json.dumps(meta, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
