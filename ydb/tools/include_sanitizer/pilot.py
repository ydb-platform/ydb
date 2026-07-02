"""End-to-end pilot harness for ``ydb/core/base/``.

Real ``compile_commands.json`` and ``clang-include-cleaner`` are slow and
require a working build environment. This harness lets us exercise the
full aggregate-and-report pipeline without them by:

- scanning the real ``#include`` lines of files in ``--subdir`` (so the
  IWYU pragmas, conditional includes, etc. are all real),
- synthesizing an include graph from those scans (assume each spelled
  include resolves to a sibling/known path; unresolved ones are skipped),
- producing a synthetic "cleaner verdict" for each file using a
  configurable policy:

    * ``naive``    - everything is "needed" (used as a smoke test).
    * ``random``   - reproducibly mark X% of header includes as unused.
    * ``override`` - load a YAML/JSON override file with explicit
      unused/suggested per file.

This is a *substitute* for the real clang-include-cleaner pass intended
for development/CI of the tool itself, NOT for producing real cleanup
verdicts. Use ``analyze`` for that on a real build.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set

from .aggregate.graph import build_graph, classify
from .aggregate.run import serialize_graph
from .analyze.schema import FileAnalysis, IncludeLine
from .analyze.source_includes import scan_includes
from .common import (
    PATHS,
    REPO_ROOT,
    die,
    ensure_dir,
    is_generated_path,
    is_skipped_path,
    repo_relative,
    setup_logging,
    write_jsonl,
)
from .report.run import main as report_main


log = logging.getLogger("pilot")


HEADER_SUFFIXES = (".h", ".hh", ".hpp", ".hxx")
TU_SUFFIXES = (".cpp", ".cc", ".cxx", ".c", ".c++")


def iter_files(root: Path, subdir: str) -> List[Path]:
    target = (root / subdir).resolve()
    out: List[Path] = []
    if not target.exists():
        return out
    for dirpath, _dirnames, filenames in os.walk(target):
        for name in filenames:
            if name.endswith(HEADER_SUFFIXES + TU_SUFFIXES):
                full = Path(dirpath) / name
                rel = repo_relative(str(full), root)
                if is_skipped_path(rel) or is_generated_path(rel):
                    continue
                out.append(full)
    return out


def resolve_spelled(
    spelled: str,
    angled: bool,
    including_file: Path,
    repo_root: Path,
    known_paths: Set[str],
) -> Optional[str]:
    """Resolve a spelled include to a repo-relative path, best-effort.

    We try:
    1. ``"x.h"`` relative to the including file's directory.
    2. Repo-rooted (``spelled`` itself).
    3. None if neither resolves.
    """
    if not angled:
        sibling = (including_file.parent / spelled).resolve()
        try:
            rel = repo_relative(str(sibling), repo_root)
            if rel in known_paths:
                return rel
        except ValueError:
            pass

    if spelled in known_paths:
        return spelled
    return None


def synthesize_file_analyses(
    files: List[Path],
    policy: str,
    seed: int,
    repo_root: Path,
) -> Dict[str, FileAnalysis]:
    """Walk every file, produce a FileAnalysis, build include_tree.

    ``policy`` controls how cleaner verdicts are simulated:
      - "naive": no include is unused; nothing is suggested.
      - "random": deterministically mark a fraction of header-side
                  includes as unused (based on the file path).
    """
    known: Set[str] = {repo_relative(str(p), repo_root) for p in files}

    per_file: Dict[str, FileAnalysis] = {}
    edges: Dict[str, List[str]] = {}

    for p in files:
        rel = repo_relative(str(p), repo_root)
        scanned = scan_includes(p)
        kind = "header" if rel.endswith(HEADER_SUFFIXES) else "tu"
        an = FileAnalysis(
            file=rel,
            kind=kind,
            compile_flags_hash="pilot",
            input_hash="pilot",
        )
        children: List[str] = []
        for s in scanned:
            if s.in_conditional:
                continue
            resolved = resolve_spelled(s.spelled, s.angled, p, repo_root, known)
            an.includes.append(IncludeLine(
                line=s.line,
                spelled=s.spelled,
                angled=s.angled,
                resolved=resolved,
                has_iwyu_keep=s.iwyu_keep,
                has_iwyu_export=s.iwyu_export,
            ))
            if resolved:
                children.append(resolved)

        edges[rel] = children
        per_file[rel] = an

    for rel, an in per_file.items():
        an.include_tree = {rel: edges.get(rel, [])}
        for child in edges.get(rel, []):
            if child in per_file:
                an.include_tree.setdefault(child, edges.get(child, []))

    apply_policy(per_file, policy=policy, seed=seed)
    return per_file


def apply_policy(per_file: Dict[str, FileAnalysis], policy: str, seed: int) -> None:
    if policy == "naive":
        return
    if policy == "random":
        for rel, an in per_file.items():
            if an.kind != "header":
                continue
            for inc in an.includes:
                if inc.has_iwyu_keep or inc.has_iwyu_export:
                    continue
                token = f"{seed}|{rel}|{inc.spelled}"
                h = int(hashlib.sha256(token.encode("utf-8")).hexdigest(), 16)
                if h % 4 == 0:
                    if inc.spelled not in an.unused_includes:
                        an.unused_includes.append(inc.spelled)
        return
    raise SystemExit(f"unknown policy {policy!r}")


def write_per_file_cache(per_file: Dict[str, FileAnalysis], out_dir: Path) -> int:
    ensure_dir(out_dir)
    n = 0
    for rel, an in per_file.items():
        digest = hashlib.sha1(rel.encode("utf-8")).hexdigest()
        sub = digest[:2]
        target = out_dir / sub / f"{digest}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(an.to_json(), indent=1), encoding="utf-8")
        n += 1
    return n


def assert_iwyu_pragmas_respected(per_file: Dict[str, FileAnalysis]) -> None:
    """Sanity: every IWYU keep/export line must NOT show in unused_includes."""
    for rel, an in per_file.items():
        for inc in an.includes:
            if (inc.has_iwyu_keep or inc.has_iwyu_export) and inc.spelled in an.unused_includes:
                die(f"pilot policy violated IWYU pragma at {rel}:{inc.line} ({inc.spelled})")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sanitize_includes pilot",
        description="Run an end-to-end pilot WITHOUT clang-include-cleaner.",
    )
    parser.add_argument("--subdir", default="ydb/core/base")
    parser.add_argument("--policy", choices=["naive", "random"], default="random")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--report", action="store_true",
                        help="also run the report subcommand on the pilot data")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    setup_logging(args.verbose)

    target = REPO_ROOT / args.subdir
    if not target.exists():
        die(f"--subdir {args.subdir} not found under repo root")

    files = iter_files(REPO_ROOT, args.subdir)
    log.info("pilot: scanning %d files under %s", len(files), args.subdir)

    per_file_as = synthesize_file_analyses(files, args.policy, args.seed, REPO_ROOT)
    assert_iwyu_pragmas_respected(per_file_as)

    cache_dir = PATHS.per_tu_dir
    n = write_per_file_cache(per_file_as, cache_dir)
    log.info("pilot: wrote %d synthetic per-file analyses to %s",
             n, repo_relative(str(cache_dir)))

    per_file = {rel: an.to_json() for rel, an in per_file_as.items()}
    graph = build_graph(per_file)
    verdicts = classify(per_file, graph)

    ensure_dir(PATHS.reports_dir)
    PATHS.graph_json.write_text(json.dumps(serialize_graph(graph), indent=1), encoding="utf-8")
    write_jsonl(PATHS.verdicts_jsonl, (v.to_json() for v in verdicts))

    counts: Dict[str, int] = {}
    for v in verdicts:
        counts[v.verdict] = counts.get(v.verdict, 0) + 1
    log.info("pilot: %d nodes, %d verdicts: %s",
             len(graph.nodes), len(verdicts),
             ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))

    if args.report:
        return report_main(["--top", "50"] + (["-v"] if args.verbose else []))

    return 0


if __name__ == "__main__":
    sys.exit(main())
