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


def require_env(name: str) -> str:
    value = (os.environ.get(name) or "").strip()
    if not value:
        print(f"{name} missing", file=sys.stderr)
        sys.exit(2)
    return value


def main() -> int:
    out = require_env("REMOTE_ARTIFACTS_JSON")
    bucket_path = require_env("S3_BUCKET_PATH")
    if not bucket_path.startswith("s3://"):
        print(f"S3_BUCKET_PATH must be s3://...; got {bucket_path!r}", file=sys.stderr)
        return 2
    _, _, key_prefix = bucket_path[len("s3://") :].partition("/")
    public_dir_url = require_env("PUBLIC_DIR_URL").rstrip("/")
    commit = require_env("ORIGINAL_HEAD")

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
