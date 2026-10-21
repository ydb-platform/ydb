"""Lightweight ``#include`` scanner used as ground truth for line positions.

clang-include-cleaner's ``--print=changes`` output gives us spelled include
strings and (sometimes) line numbers, but those line numbers are clang's
view and may not match the unmodified file. To produce reliable unified
diffs and to detect ``IWYU pragma`` annotations we re-scan the file
ourselves.

The scanner is intentionally conservative:

- It skips lines inside ``/* ... */`` block comments.
- It honors ``#if 0`` / ``#if !defined(...)`` style guards only at the
  shallowest level by NOT trying to evaluate the preprocessor; lines that
  are technically excluded by ``#if 0`` are still reported (we annotate
  them with ``in_conditional`` so the diff generator can avoid touching
  them).
- ``IWYU pragma`` comments on the same line are detected.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import List


INCLUDE_RE = re.compile(r'^\s*#\s*include\s+([<"])([^">]+)([">])(.*)$')
IWYU_KEEP_RE = re.compile(r"IWYU\s+pragma:\s*keep", re.IGNORECASE)
IWYU_EXPORT_RE = re.compile(r"IWYU\s+pragma:\s*export", re.IGNORECASE)
IWYU_PRIVATE_RE = re.compile(r"IWYU\s+pragma:\s*private", re.IGNORECASE)


@dataclass
class ScannedInclude:
    line: int
    spelled: str
    angled: bool
    iwyu_keep: bool
    iwyu_export: bool
    iwyu_private: bool
    in_conditional: bool
    raw: str


def _strip_line_comments(text: str) -> str:
    out: List[str] = []
    in_block = False
    i = 0
    while i < len(text):
        if in_block:
            if text[i : i + 2] == "*/":
                in_block = False
                i += 2
                continue
            i += 1
            continue
        if text[i : i + 2] == "/*":
            in_block = True
            i += 2
            continue
        if text[i : i + 2] == "//":
            while i < len(text) and text[i] != "\n":
                i += 1
            continue
        out.append(text[i])
        i += 1
    return "".join(out)


def scan_includes(source: Path) -> List[ScannedInclude]:
    """Scan ``source`` for ``#include`` directives and IWYU pragmas.

    Returns one entry per ``#include`` line, in file order.
    """
    try:
        text = source.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []

    results: List[ScannedInclude] = []
    in_block_comment = False
    in_conditional_depth = 0
    if_zero_stack: List[bool] = []

    lines = text.splitlines()
    for lineno, raw in enumerate(lines, start=1):
        line = raw
        if in_block_comment:
            end = line.find("*/")
            if end == -1:
                continue
            line = line[end + 2 :]
            in_block_comment = False

        idx = 0
        clean_chars: List[str] = []
        while idx < len(line):
            ch = line[idx]
            if line[idx : idx + 2] == "//":
                break
            if line[idx : idx + 2] == "/*":
                end = line.find("*/", idx + 2)
                if end == -1:
                    in_block_comment = True
                    break
                idx = end + 2
                continue
            clean_chars.append(ch)
            idx += 1
        clean_line = "".join(clean_chars)

        stripped = clean_line.lstrip()
        if stripped.startswith("#"):
            directive = stripped[1:].lstrip().split(maxsplit=1)
            if directive:
                kind = directive[0]
                if kind in ("if", "ifdef", "ifndef"):
                    in_conditional_depth += 1
                    args = directive[1] if len(directive) > 1 else ""
                    if_zero_stack.append(args.strip() == "0")
                elif kind == "endif":
                    if in_conditional_depth > 0:
                        in_conditional_depth -= 1
                    if if_zero_stack:
                        if_zero_stack.pop()

        m = INCLUDE_RE.match(line)
        if not m:
            continue
        opener, spelled, trailing = m.group(1), m.group(2), m.group(4)
        results.append(
            ScannedInclude(
                line=lineno,
                spelled=spelled,
                angled=(opener == "<"),
                iwyu_keep=bool(IWYU_KEEP_RE.search(trailing)),
                iwyu_export=bool(IWYU_EXPORT_RE.search(trailing)),
                iwyu_private=bool(IWYU_PRIVATE_RE.search(trailing)),
                in_conditional=any(if_zero_stack),
                raw=raw,
            )
        )

    return results
