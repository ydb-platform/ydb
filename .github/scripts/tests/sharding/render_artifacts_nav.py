#!/usr/bin/env python3
"""Generate root index.html (Indexer-style folder listing) for parallel PR-check S3."""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_INDEXER = _SCRIPT_DIR.parents[1] / "Indexer" / "indexer.py"


def _parse_shard_ids(raw: str | None) -> list[str]:
    if not raw:
        return []
    raw = raw.strip()
    if not raw:
        return []
    # JSON array: [0,1,2] or CSV / space-separated
    if raw.startswith("["):
        inner = raw.strip("[]").replace(" ", "")
        if not inner:
            return []
        parts = inner.split(",")
    else:
        parts = re.split(r"[\s,]+", raw)
    out: list[str] = []
    for p in parts:
        p = p.strip().strip('"').strip("'")
        if not p:
            continue
        if p.startswith("shard_"):
            p = p.removeprefix("shard_")
        out.append(p)
    return sorted(out, key=lambda x: int(x) if x.isdigit() else x)


def _shard_ids_from_count(shard_count: int | None) -> list[str]:
    if shard_count is None or shard_count <= 0:
        return []
    return [str(i) for i in range(shard_count)]


def _discover_shards_and_tries(
    tries_dir: Path | None,
    *,
    shard_ids: list[str] | None = None,
    shard_count: int | None = None,
) -> tuple[list[str], list[str]]:
    """Return (shard_id strings, try_N folder names)."""
    tries: list[str] = []
    if tries_dir and tries_dir.is_dir():
        try_dirs = sorted(tries_dir.glob("try_*"), key=lambda path: path.name)
        # Prefer tries that have a stitched report; fall back to any try_* dir.
        with_report = [p.name for p in try_dirs if (p / "report.json").is_file()]
        tries = with_report or [p.name for p in try_dirs]

    if shard_ids:
        shards = sorted(shard_ids, key=lambda x: int(x) if x.isdigit() else x)
        return shards, tries

    from_count = _shard_ids_from_count(shard_count)
    if from_count:
        return from_count, tries

    if tries_dir and tries_dir.is_dir():
        try1 = tries_dir / "try_1"
        if try1.is_dir():
            found = sorted(
                (path.stem.removeprefix("shard_") for path in try1.glob("shard_*.json")),
                key=lambda x: int(x) if x.isdigit() else x,
            )
            if found:
                return found, tries
        # meta.txt written by merge job: shard_reports=N
        meta = tries_dir / "meta.txt"
        if meta.is_file():
            for line in meta.read_text(encoding="utf-8").splitlines():
                if line.startswith("shard_reports="):
                    try:
                        n = int(line.split("=", 1)[1].strip())
                    except ValueError:
                        n = 0
                    return _shard_ids_from_count(n), tries

    return [], tries


def _folder_names(
    *,
    tries_dir: Path | None,
    include_build: bool,
    include_plan: bool,
    shard_ids: list[str] | None = None,
    shard_count: int | None = None,
) -> list[str]:
    shards, tries = _discover_shards_and_tries(
        tries_dir, shard_ids=shard_ids, shard_count=shard_count
    )
    names: list[str] = []
    if include_build:
        names.append("build")
    if include_plan:
        names.append("plan")
    names.extend(f"shard_{shard_id}" for shard_id in shards)
    names.extend(tries)
    names.append("final")
    return names


def render_root_index_html(
    *,
    tries_dir: Path | None,
    include_build: bool,
    include_plan: bool = False,
    shard_ids: list[str] | None = None,
    shard_count: int | None = None,
) -> str:
    """Build Indexer-style index.html listing top-level artifact folders.

    Links point at ``name/index.html`` (S3-safe). Stub index files are created
    in staging so recursive Indexer emits the right hrefs; real indexes are
    uploaded by build/plan/shard/merge jobs under the same keys.
    """
    folder_names = _folder_names(
        tries_dir=tries_dir,
        include_build=include_build,
        include_plan=include_plan,
        shard_ids=shard_ids,
        shard_count=shard_count,
    )
    if not folder_names:
        raise ValueError("nothing to list in root artifact index")

    stub = (
        "<!DOCTYPE html><html><head><meta charset='utf-8'>"
        "<meta http-equiv='refresh' content='0; url=./'>"
        "<title>artifacts</title></head>"
        "<body>See job-uploaded index for this folder.</body></html>\n"
    )

    with tempfile.TemporaryDirectory(prefix="parallel-root-index-") as tmp:
        staging = Path(tmp)
        for name in folder_names:
            d = staging / name
            d.mkdir()
            # So Indexer -r links to name/index.html (object storage friendly).
            (d / "index.html").write_text(stub, encoding="utf-8")
        subprocess.run(
            [sys.executable, str(_INDEXER), "-r", str(staging)],
            check=True,
            capture_output=True,
            text=True,
        )
        return (staging / "index.html").read_text(encoding="utf-8")


def render_nav_html(
    *,
    base_url: str,
    tries_dir: Path | None,
    include_build: bool,
    include_plan: bool = False,
    shard_ids: list[str] | None = None,
    shard_count: int | None = None,
) -> str:
    """Backward-compatible alias; base_url is unused (links are relative)."""
    del base_url
    return render_root_index_html(
        tries_dir=tries_dir,
        include_build=include_build,
        include_plan=include_plan,
        shard_ids=shard_ids,
        shard_count=shard_count,
    )


def index_public_subdirs(public_dir: Path) -> None:
    """Generate per-folder index.html for merged tries (same as monolith/shard jobs)."""
    for subdir in sorted(public_dir.glob("try_*"), key=lambda path: path.name):
        if subdir.is_dir():
            subprocess.run([sys.executable, str(_INDEXER), "-r", str(subdir)], check=False)
    final_dir = public_dir / "final"
    if final_dir.is_dir():
        subprocess.run([sys.executable, str(_INDEXER), "-r", str(final_dir)], check=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--base-url",
        default="",
        help="Unused; kept for CLI compatibility (links are relative)",
    )
    parser.add_argument(
        "--tries-dir",
        default="",
        type=Path,
        help="merged/ with try_*/report.json (and optional meta.txt / try_1/shard_*.json)",
    )
    parser.add_argument("--public-dir", default="", type=Path, help="PUBLIC_DIR with try_*/final to index")
    parser.add_argument("--include-build", action="store_true")
    parser.add_argument("--include-plan", action="store_true")
    parser.add_argument(
        "--shard-count",
        type=int,
        default=None,
        help="Number of shards (0..N-1). Preferred when try_1/shard_*.json is absent after merge.",
    )
    parser.add_argument(
        "--shard-ids",
        default="",
        help='Explicit shard ids, e.g. "0,1,2" or JSON "[0,1,2]"',
    )
    args = parser.parse_args()

    tries_dir = args.tries_dir if args.tries_dir else None
    shard_ids = _parse_shard_ids(args.shard_ids) or None
    content = render_root_index_html(
        tries_dir=tries_dir,
        include_build=args.include_build,
        include_plan=args.include_plan,
        shard_ids=shard_ids,
        shard_count=args.shard_count,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(content, encoding="utf-8")

    if args.public_dir:
        index_public_subdirs(args.public_dir)


if __name__ == "__main__":
    main()
