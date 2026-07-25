"""Optional GitHub issue search by fingerprint (narrow scope)."""

from __future__ import annotations

import json
import shutil
import subprocess
from typing import Any


def search_issues(fingerprint: str | None, *, limit: int = 5) -> dict[str, Any]:
    out: dict[str, Any] = {"enabled": False, "query": None, "items": [], "error": None}
    if not fingerprint:
        out["error"] = "no fingerprint"
        return out
    if not shutil.which("gh"):
        out["error"] = "gh not installed"
        return out
    # Keep query narrow — fingerprint token + ydb-platform/ydb
    q = f"{fingerprint.replace('_', ' ')} repo:ydb-platform/ydb"
    out["enabled"] = True
    out["query"] = q
    try:
        proc = subprocess.run(
            [
                "gh",
                "search",
                "issues",
                q,
                "--limit",
                str(limit),
                "--json",
                "title,url,state,updatedAt",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if proc.returncode != 0:
            out["error"] = (proc.stderr or proc.stdout or "gh failed").strip()[:400]
            return out
        items = json.loads(proc.stdout or "[]")
        out["items"] = items if isinstance(items, list) else []
    except (subprocess.TimeoutExpired, json.JSONDecodeError, OSError) as e:
        out["error"] = str(e)
    return out
