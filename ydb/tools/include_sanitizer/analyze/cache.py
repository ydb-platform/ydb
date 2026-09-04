"""Content-addressed cache for per-file analysis results.

Keying is intentionally pessimistic: any change to the analyzed file or any
header it transitively visits invalidates the cache entry. We don't aim
for the tightness of ya's distributed cache; this cache exists to make
re-running the tool on the same checkout cheap.

Concurrency: the inline-analyze path runs inside every ya compile worker.
Multiple workers will simultaneously try to write the same cache entry
(same header content + same probe-flags hash). We use per-writer unique
temp file names + atomic rename so the writes never collide.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
from pathlib import Path
from typing import Iterable, Optional, Sequence


log = logging.getLogger("cache")


def hash_arguments(arguments: Sequence[str]) -> str:
    h = hashlib.sha256()
    for a in arguments:
        h.update(a.encode("utf-8", errors="replace"))
        h.update(b"\0")
    return h.hexdigest()[:16]


def hash_file(path: Path) -> str:
    try:
        with path.open("rb") as fh:
            h = hashlib.sha256()
            while True:
                chunk = fh.read(65536)
                if not chunk:
                    break
                h.update(chunk)
            return h.hexdigest()[:16]
    except OSError:
        return "missing"


def hash_inputs(paths: Iterable[Path]) -> str:
    h = hashlib.sha256()
    for p in sorted(map(str, paths)):
        h.update(p.encode("utf-8", errors="replace"))
        h.update(b"\0")
        h.update(hash_file(Path(p)).encode("ascii"))
        h.update(b"\0")
    return h.hexdigest()[:16]


def cache_path_for(
    base_dir: Path,
    file_rel: str,
    compile_flags_hash: str,
    input_hash: str,
) -> Path:
    digest = hashlib.sha1(
        (file_rel + "|" + compile_flags_hash + "|" + input_hash).encode("utf-8")
    ).hexdigest()
    sub = digest[:2]
    return base_dir / sub / f"{digest}.json"


def load(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def store(path: Path, data: dict) -> None:
    """Atomically write a JSON cache entry.

    Race-safe under multi-process inline-analyze: every writer creates a
    unique temp file (pid + random suffix), writes the payload, then
    atomically renames it onto ``path``. If a concurrent writer beats us
    to it, its content is identical (same cache key implies same data),
    so last-writer-wins is fine. The unique temp name means we never
    have two processes touching the same intermediate file.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(
        path.suffix + f".tmp.{os.getpid()}.{secrets.token_hex(4)}"
    )
    payload = json.dumps(data, indent=1)
    try:
        tmp.write_text(payload, encoding="utf-8")
        os.replace(tmp, path)
    except OSError:
        # Clean up our own temp file if the rename failed; never leave
        # stray *.tmp.* files behind.
        try:
            tmp.unlink()
        except OSError:
            pass
        raise
