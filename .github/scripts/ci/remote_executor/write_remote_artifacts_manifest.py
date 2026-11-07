#!/usr/bin/env python3
"""Write remote_artifacts.json locator at the S3 run root for platform ingest.

Requires S3_BUCKET_PATH, PUBLIC_DIR_URL, REMOTE_ARTIFACTS_JSON, ORIGINAL_HEAD.
Ingest reads artifact_key_prefix / public_dir_url / commit; attempts are
re-discovered from the downloaded tree.
"""
from __future__ import annotations

import json
import os
import sys


def main() -> int:
    out = os.environ.get("REMOTE_ARTIFACTS_JSON") or ""
    if not out:
        print("usage: REMOTE_ARTIFACTS_JSON=path write_remote_artifacts_manifest.py", file=sys.stderr)
        return 2

    bucket_path = (os.environ.get("S3_BUCKET_PATH") or "").strip()
    if not bucket_path:
        print("S3_BUCKET_PATH missing", file=sys.stderr)
        return 2
    if not bucket_path.startswith("s3://"):
        print(f"S3_BUCKET_PATH must be s3://...; got {bucket_path!r}", file=sys.stderr)
        return 2
    rest = bucket_path[len("s3://") :]
    _, _, key_prefix = rest.partition("/")
    public_dir_url = (os.environ.get("PUBLIC_DIR_URL") or "").rstrip("/")
    if not public_dir_url:
        print("PUBLIC_DIR_URL missing", file=sys.stderr)
        return 2
    commit = (os.environ.get("ORIGINAL_HEAD") or "").strip()
    if not commit:
        print("ORIGINAL_HEAD missing", file=sys.stderr)
        return 2

    meta = {
        "version": 1,
        "artifact_key_prefix": key_prefix,
        "public_dir_url": public_dir_url,
        "commit": commit,
    }
    with open(out, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
        f.write("\n")
    print(json.dumps(meta, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
