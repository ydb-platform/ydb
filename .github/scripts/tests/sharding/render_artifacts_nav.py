#!/usr/bin/env python3
"""Generate root index.html (Indexer-style folder listing) for sharded PR-check S3."""
from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_INDEXER = _SCRIPT_DIR.parents[1] / "Indexer" / "indexer.py"


def _discover_shards_and_tries(tries_dir: Path) -> tuple[list[str], list[str]]:
    if not tries_dir.is_dir():
        return [], []
    try_dirs = sorted(tries_dir.glob("try_*"), key=lambda path: path.name)
    tries = [path.name for path in try_dirs]
    try1 = tries_dir / "try_1"
    if not try1.is_dir():
        return [], tries
    shards = sorted(
        (path.stem.removeprefix("shard_") for path in try1.glob("shard_*.json")),
        key=int,
    )
    return shards, tries


def _folder_names(
    *,
    tries_dir: Path | None,
    include_params: bool,
    include_build: bool,
    include_plan: bool,
    shard_ids: list[str] | None,
) -> list[str]:
    discovered_shards, tries = _discover_shards_and_tries(tries_dir) if tries_dir else ([], [])
    shards = shard_ids if shard_ids is not None else discovered_shards
    names: list[str] = []
    if include_params:
        names.append("params")
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
    include_params: bool = False,
    include_build: bool = False,
    include_plan: bool = False,
    shard_ids: list[str] | None = None,
) -> str:
    """Build Indexer-style index.html listing top-level artifact folders."""
    folder_names = _folder_names(
        tries_dir=tries_dir,
        include_params=include_params,
        include_build=include_build,
        include_plan=include_plan,
        shard_ids=shard_ids,
    )
    if not folder_names:
        raise ValueError("nothing to list in root artifact index")

    with tempfile.TemporaryDirectory(prefix="shard-root-index-") as tmp:
        staging = Path(tmp)
        for name in folder_names:
            (staging / name).mkdir()
        subprocess.run(
            [sys.executable, str(_INDEXER), str(staging)],
            check=True,
            capture_output=True,
            text=True,
        )
        return (staging / "index.html").read_text(encoding="utf-8")


def index_public_subdirs(public_dir: Path) -> None:
    """Generate per-folder index.html for merged tries (same as monolith/shard jobs)."""
    for subdir in sorted(public_dir.glob("try_*"), key=lambda path: path.name):
        if subdir.is_dir():
            subprocess.run([sys.executable, str(_INDEXER), str(subdir)], check=False)
    final_dir = public_dir / "final"
    if final_dir.is_dir():
        subprocess.run([sys.executable, str(_INDEXER), str(final_dir)], check=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--base-url",
        default="",
        help="Unused; kept for CLI compatibility (links are relative folder paths)",
    )
    parser.add_argument("--tries-dir", default="", type=Path, help="merged/ with try_*/shard_*.json")
    parser.add_argument("--public-dir", default="", type=Path, help="PUBLIC_DIR with try_*/final to index")
    parser.add_argument("--include-params", action="store_true")
    parser.add_argument("--include-build", action="store_true")
    parser.add_argument("--include-plan", action="store_true")
    parser.add_argument(
        "--shard-count",
        type=int,
        default=-1,
        help="If >= 0, list shard_0..shard_{N-1} instead of discovering from tries-dir",
    )
    args = parser.parse_args()
    del args.base_url

    tries_dir = args.tries_dir if args.tries_dir else None
    shard_ids = None
    if args.shard_count >= 0:
        shard_ids = [str(i) for i in range(args.shard_count)]
    content = render_root_index_html(
        tries_dir=tries_dir,
        include_params=args.include_params,
        include_build=args.include_build,
        include_plan=args.include_plan,
        shard_ids=shard_ids,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(content, encoding="utf-8")

    if args.public_dir:
        index_public_subdirs(args.public_dir)


if __name__ == "__main__":
    main()
