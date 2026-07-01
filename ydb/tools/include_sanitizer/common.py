"""Shared helpers: paths, logging, filesystem layout."""

from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Optional


PKG_ROOT = Path(__file__).resolve().parent
# Directory that must be on sys.path to ``import <PKG_NAME>`` (the package's
# parent). PKG_NAME is the package's own top-level name. Computing these from
# __file__ keeps the tool working no matter where it lives in the tree
# (tools/, ydb/tools/, anywhere) and under any import name.
PKG_PARENT = PKG_ROOT.parent
PKG_NAME = PKG_ROOT.name


def _find_marker_root(start: Path) -> Optional[Path]:
    """Walk up from ``start`` to the arcadia root, identified by the
    ``.arcadia.root`` marker. Returns None if no marker is found.

    We deliberately key ONLY on ``.arcadia.root`` (which lives at the true
    repo root and is NOT copied into ya's ephemeral build_root). An earlier
    version also accepted ``build/scripts/clang_wrapper.py`` as a marker,
    but ya stages that file inside the per-TU build_root — so when inline
    analysis runs there (CWD = build_root) it would mis-detect the
    build_root as the repo root and emit absolute, unstable cache keys.
    """
    try:
        cur = start.resolve()
    except OSError:
        cur = start
    for _ in range(60):
        if (cur / ".arcadia.root").exists():
            return cur
        if cur.parent == cur:
            return None
        cur = cur.parent
    return None


def _detect_repo_root() -> Path:
    """Resolve the repository root robustly across execution contexts.

    Priority:
      1. ``YDB_REPO_ROOT`` env override. The ``compdb``/``timetrace``
         drivers set this for the build steps they spawn, where CWD is the
         ephemeral build_root and a bundled binary's ``__file__`` may be
         relative (so package-based detection is unreliable).
      2. Marker search up from the package directory (correct from source).
      3. Marker search up from the current working directory (covers the
         bundled binary launched from within the checkout).
      4. Two levels up from the package as a last resort.
    """
    env = os.environ.get("YDB_REPO_ROOT")
    if env:
        return Path(env).resolve()
    r = _find_marker_root(PKG_ROOT)
    if r is not None:
        return r
    r = _find_marker_root(Path.cwd())
    if r is not None:
        return r
    return PKG_ROOT.parent.parent


REPO_ROOT = _detect_repo_root()

# Where cache/reports are written. Defaults next to the source package
# (the established from-source layout). When the tool runs as a ya-built
# binary, ``__file__`` lives in a read-only runtime dir, so set
# ``YDB_ISAN_DIR`` to a writable working directory.
_WORKDIR = Path(os.environ["YDB_ISAN_DIR"]).resolve() if os.environ.get("YDB_ISAN_DIR") else PKG_ROOT
REPORTS_DIR = _WORKDIR / "reports"
CACHE_DIR = _WORKDIR / ".cache"
DATA_DIR = PKG_ROOT / "data"

DEFAULT_SUBDIR = "ydb"
GENERATED_PREFIXES = (".pb.h", ".pb.cc", ".grpc.pb.h", ".grpc.pb.cc")
SKIP_DIR_PREFIXES = ("contrib/", "util/", "library/cpp/")


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def repo_relative(path: str | Path, repo_root: Path = REPO_ROOT) -> str:
    p = Path(path)
    if not p.is_absolute():
        return str(p)
    try:
        return str(p.relative_to(repo_root))
    except ValueError:
        return str(p)


def is_under_subdir(path: str, subdir: str) -> bool:
    """Whether ``path`` (repo-relative or absolute) is under ``subdir``."""
    rel = repo_relative(path)
    return rel == subdir or rel.startswith(subdir.rstrip("/") + "/")


def is_generated_path(path: str, build_root: Optional[str] = None) -> bool:
    if build_root and path.startswith(build_root.rstrip("/") + "/"):
        return True
    name = os.path.basename(path)
    return any(name.endswith(suf) for suf in GENERATED_PREFIXES)


def is_skipped_path(path: str) -> bool:
    """True for files in contrib/, util/, library/cpp/ — out of scope for v1."""
    rel = repo_relative(path)
    return any(rel.startswith(p) for p in SKIP_DIR_PREFIXES)


def iter_jsonl(path: Path) -> Iterator[dict]:
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def write_jsonl(path: Path, items: Iterable[dict]) -> int:
    n = 0
    with path.open("w", encoding="utf-8") as fh:
        for it in items:
            fh.write(json.dumps(it, separators=(",", ":")))
            fh.write("\n")
            n += 1
    return n


@dataclass(frozen=True)
class Paths:
    repo_root: Path = REPO_ROOT
    reports_dir: Path = REPORTS_DIR
    cache_dir: Path = CACHE_DIR
    data_dir: Path = DATA_DIR

    @property
    def compdb_json(self) -> Path:
        return self.repo_root / "compile_commands.json"

    @property
    def compdb_entries_dir(self) -> Path:
        return self.cache_dir / "compdb_entries"

    @property
    def per_tu_dir(self) -> Path:
        return self.cache_dir / "per_tu"

    @property
    def aggregate_dir(self) -> Path:
        return self.reports_dir

    @property
    def graph_json(self) -> Path:
        return self.reports_dir / "graph.json"

    @property
    def verdicts_jsonl(self) -> Path:
        return self.reports_dir / "verdicts.jsonl"

    @property
    def hot_headers_csv(self) -> Path:
        return self.reports_dir / "hot_headers.csv"

    @property
    def summary_md(self) -> Path:
        return self.reports_dir / "summary.md"

    @property
    def diffs_dir(self) -> Path:
        return self.reports_dir / "diffs"


PATHS = Paths()


def die(msg: str, code: int = 1) -> None:
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(code)
