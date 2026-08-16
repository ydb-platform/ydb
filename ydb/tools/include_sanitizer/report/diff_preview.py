"""Per-header unified-diff preview generation.

For each header H with at least one ``remove`` or ``move-to-cpp`` verdict:

- emit a unified diff against the current file with the suggested
  ``#include`` lines removed,
- for ``move-to-cpp`` verdicts, also emit a diff suggesting where to add
  the include in each affected ``.cpp`` consumer.

We **do not** write to the source tree. All output goes into
``reports/diffs/``.
"""

from __future__ import annotations

import difflib
import logging
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from ..aggregate.graph import (
    VERDICT_MOVE_TO_CPP,
    VERDICT_REMOVE,
    Verdict,
)
from ..common import REPO_ROOT, ensure_dir


log = logging.getLogger("report.diff")


def _read_lines(path: Path) -> List[str]:
    try:
        return path.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
    except OSError:
        return []


def _drop_include_lines(lines: List[str], drop_linenos: List[int]) -> List[str]:
    drops = set(drop_linenos)
    out: List[str] = []
    for i, line in enumerate(lines, start=1):
        if i in drops:
            continue
        out.append(line)
    return out


def _insert_after_includes(lines: List[str], new_includes: List[str]) -> List[str]:
    """Append ``new_includes`` after the last existing ``#include`` line.

    Each item in ``new_includes`` is a source-root-relative path like
    ``ydb/foo/bar.h``; we render the angled form ``#include <...>`` so it
    resolves from any TU regardless of directory.
    """
    if not new_includes:
        return lines

    last_include_idx = -1
    for i, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith("#include"):
            last_include_idx = i

    # Moved includes are emitted in source-root angled form. A header
    # may have spelled a sibling relatively (``"foo.h"``); pasting that
    # spelling into a .cpp in a different directory would not resolve.
    # The angled source-root path resolves from any TU.
    insertions = [f'#include <{inc}>\n' for inc in new_includes]
    if last_include_idx < 0:
        return insertions + lines
    head = lines[: last_include_idx + 1]
    tail = lines[last_include_idx + 1 :]
    return head + insertions + tail


def _format_diff(rel_path: str, before: List[str], after: List[str]) -> str:
    if before == after:
        return ""
    return "".join(
        difflib.unified_diff(
            before, after,
            fromfile=f"a/{rel_path}",
            tofile=f"b/{rel_path}",
            n=3,
        )
    )


def render_header_diff(
    header_rel: str,
    repo_root: Path,
    remove_linenos: List[int],
) -> str:
    abs_path = repo_root / header_rel
    before = _read_lines(abs_path)
    after = _drop_include_lines(before, remove_linenos)
    return _format_diff(header_rel, before, after)


def render_tu_addition_diff(
    tu_rel: str,
    repo_root: Path,
    new_includes: List[str],
) -> str:
    abs_path = repo_root / tu_rel
    before = _read_lines(abs_path)
    after = _insert_after_includes(before, new_includes)
    return _format_diff(tu_rel, before, after)


def emit_diffs(
    verdicts: Iterable[Verdict],
    diffs_dir: Path,
    repo_root: Path = REPO_ROOT,
    only: Iterable[str] = (VERDICT_REMOVE, VERDICT_MOVE_TO_CPP),
) -> Tuple[int, int]:
    only_set = set(only)
    ensure_dir(diffs_dir)

    by_header: Dict[str, List[Verdict]] = defaultdict(list)
    for v in verdicts:
        if v.verdict not in only_set:
            continue
        by_header[v.in_file].append(v)

    additions_per_tu: Dict[str, List[str]] = defaultdict(list)
    header_diffs = 0
    tu_diffs = 0

    for header, vs in by_header.items():
        remove_lines = sorted({v.line for v in vs})
        diff = render_header_diff(header, repo_root, remove_lines)
        if diff:
            out_path = diffs_dir / (header.replace("/", "__") + ".patch")
            out_path.write_text(diff, encoding="utf-8")
            header_diffs += 1

        for v in vs:
            if v.verdict != VERDICT_MOVE_TO_CPP:
                continue
            add_path = v.resolved or v.spelled
            for tu in v.consumers_needing:
                if add_path not in additions_per_tu[tu]:
                    additions_per_tu[tu].append(add_path)

    for tu, additions in additions_per_tu.items():
        diff = render_tu_addition_diff(tu, repo_root, sorted(set(additions)))
        if not diff:
            continue
        out_path = diffs_dir / (tu.replace("/", "__") + ".patch")
        out_path.write_text(diff, encoding="utf-8")
        tu_diffs += 1

    return header_diffs, tu_diffs
