"""Run clang-include-cleaner and clang ``-H`` and parse their output."""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


log = logging.getLogger("clang_runner")


CHANGE_REMOVE_RE = re.compile(r"^-\s*([<\"])([^>\"]+)([>\"])")
CHANGE_INSERT_RE = re.compile(r"^\+\s*([<\"])([^>\"]+)([>\"])")
HEADER_LINE_RE = re.compile(r"^(\.+)\s+(.+)$")

CLANG_FLAGS_DROP_STANDALONE = {
    "-c",
    "-MD",
    "-MMD",
    "-M",
    "-MM",
    "-MG",
    "-MP",
    "-MJ",
}

CLANG_FLAGS_DROP_PAIR = {
    "-o",
    "-MF",
    "-MT",
    "-MQ",
    "--serialize-diagnostics",
    "-MJ",
}


@dataclass
class CleanerResult:
    removed: List[Tuple[str, bool]]
    inserted: List[Tuple[str, bool]]
    stderr: str
    exit_code: int


@dataclass
class IncludeTree:
    """Mapping ``including_file -> [included_file, ...]`` from ``-H`` output."""

    edges: Dict[str, List[str]]
    visited: List[str]


def find_clang_include_cleaner() -> Optional[str]:
    """Pick the first available clang-include-cleaner binary."""
    env = os.environ.get("YDB_CLANG_INCLUDE_CLEANER")
    if env:
        if Path(env).exists():
            return env
        log.warning("YDB_CLANG_INCLUDE_CLEANER=%s does not exist", env)

    on_path = shutil.which("clang-include-cleaner")
    if on_path:
        return on_path

    ya_root = Path.home() / ".ya" / "tools"
    if ya_root.exists():
        candidates = sorted(ya_root.glob("**/bin/clang-include-cleaner"))
        if candidates:
            return str(candidates[-1])

    return None


def find_clang(compile_arguments: Sequence[str]) -> Optional[str]:
    """The compile command's first arg is typically the clang binary."""
    if not compile_arguments:
        return None
    first = compile_arguments[0]
    if first.endswith("/clang_wrapper.py") or first.endswith("clang_wrapper.py"):
        for a in compile_arguments[1:]:
            if a.endswith("clang") or a.endswith("clang++") or a.endswith("clang-cl"):
                return a
        return None
    if Path(first).exists():
        return first
    return shutil.which(first)


def strip_compile_flags(
    arguments: Sequence[str],
    drop_sources: bool = False,
) -> List[str]:
    """Remove flags that get in the way of running an analysis driver.

    Drops output (-o), dependency-tracking (-M/-MD/-MF/...), and
    serialize-diagnostics flags. Keeps everything else
    (``-I``/``-isystem``/``-D``/``-std``/``-target``/...).

    If ``drop_sources`` is true, also drop source-file positional args
    (``*.cpp``, ``*.cc``, ...). Useful when passing flags after ``--``
    to clang-include-cleaner — the source goes in the positional slot,
    not in the flags.
    """
    out: List[str] = []
    skip_next = False
    for a in arguments:
        if skip_next:
            skip_next = False
            continue
        if a in CLANG_FLAGS_DROP_PAIR:
            skip_next = True
            continue
        if a in CLANG_FLAGS_DROP_STANDALONE:
            continue
        # Glued -o<path>, -MF<path>, -MT<path> forms.
        if (
            (a.startswith("-o") and len(a) > 2)
            or (a.startswith("-MF") and len(a) > 3)
            or (a.startswith("-MT") and len(a) > 3)
            or (a.startswith("-MQ") and len(a) > 3)
            or (a.startswith("-MJ") and len(a) > 3)
        ):
            continue
        if drop_sources and _is_source_arg(a):
            continue
        out.append(a)
    return out


def _is_source_arg(a: str) -> bool:
    lower = a.lower()
    return lower.endswith((".cpp", ".cc", ".cxx", ".c", ".c++"))


def reconstruct_compile_args(
    entry_arguments: Sequence[str],
    clang_path: Optional[str],
) -> List[str]:
    """Filter out the ya-specific wrapper noise around the real clang call."""
    args = list(entry_arguments)
    if args and (args[0].endswith("clang_wrapper.py") or "clang_wrapper.py" in args[0]):
        if len(args) >= 3:
            args = args[3:]
        else:
            args = []
    if "/retry_cc.py" in " ".join(args):
        try:
            i = next(i for i, a in enumerate(args) if "/retry_cc.py" in a)
            args = args[i + 1 :]
        except StopIteration:
            pass
    if clang_path and args and not args[0].endswith(("clang", "clang++", "clang-cl")):
        args = [clang_path] + args
    return args


def parse_print_changes(stderr: str, stdout: str) -> CleanerResult:
    removed: List[Tuple[str, bool]] = []
    inserted: List[Tuple[str, bool]] = []

    for blob in (stdout, stderr):
        for raw in blob.splitlines():
            line = raw.rstrip("\n").rstrip("\r").strip()
            if not line:
                continue
            m = CHANGE_REMOVE_RE.match(line)
            if m:
                removed.append((m.group(2), m.group(1) == "<"))
                continue
            m = CHANGE_INSERT_RE.match(line)
            if m:
                inserted.append((m.group(2), m.group(1) == "<"))
                continue

    return CleanerResult(
        removed=removed,
        inserted=inserted,
        stderr=stderr,
        exit_code=0,
    )


def run_include_cleaner(
    cleaner_bin: str,
    source: Path,
    compile_arguments: Sequence[str],
    mapping_file: Optional[Path] = None,
    ignore_headers: Sequence[str] = (),
    extra_args: Sequence[str] = (),
    timeout: float = 120.0,
) -> CleanerResult:
    """Invoke clang-include-cleaner with ``--print=changes`` on ``source``.

    ``compile_arguments`` is the *compile* command (with clang as argv[0]);
    we strip output/deps/source-file noise and splice the remaining flags
    after ``--`` so clang-include-cleaner uses the same flags.

    The source file is passed once, as a positional argument before
    ``--``; the source(s) in ``compile_arguments`` are removed.
    """
    # Drop argv[0] (clang) and clean up the rest. The source goes in
    # the positional slot below, so drop_sources=True here.
    cleaned_flags = (
        strip_compile_flags(compile_arguments[1:], drop_sources=True)
        if compile_arguments else []
    )

    # Tolerate flag-set drift between the ya-bundled clang and the
    # clang-include-cleaner version (e.g. unknown warnings).
    pacify = ["-Wno-unknown-warning-option", "-Wno-error"]

    cmd = [cleaner_bin, str(source), "--print=changes"]
    if ignore_headers:
        cmd.append(f"--ignore-headers={','.join(ignore_headers)}")
    for ea in extra_args:
        cmd.append(f"--extra-arg={ea}")
    cmd.append("--")
    cmd.extend(cleaned_flags)
    cmd.extend(pacify)

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return CleanerResult(
            removed=[], inserted=[],
            stderr=f"timeout after {timeout}s",
            exit_code=124,
        )
    except OSError as e:
        return CleanerResult(
            removed=[], inserted=[],
            stderr=f"failed to spawn clang-include-cleaner: {e}",
            exit_code=127,
        )

    result = parse_print_changes(proc.stderr, proc.stdout)
    result.exit_code = proc.returncode
    return result


def run_clang_minus_H(
    clang_path: str,
    source: Path,
    compile_arguments: Sequence[str],
    timeout: float = 120.0,
) -> IncludeTree:
    """Reconstruct the include tree by parsing ``clang -H`` stderr.

    The ``-H`` output is one line per header with leading dots indicating
    nesting depth.
    """
    # Drop argv[0] (clang) and the source files (we re-add the source
    # below explicitly). Keep all include paths, defines, language flags.
    args = strip_compile_flags(compile_arguments[1:] if compile_arguments else [],
                               drop_sources=True)
    args = [a for a in args if a != "-H"]
    cmd = [clang_path, "-H", "-fsyntax-only",
           "-Wno-unknown-warning-option", "-Wno-error"]
    cmd.extend(args)
    cmd.append(str(source))

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return IncludeTree(edges={}, visited=[])
    except OSError as e:
        log.debug("clang -H spawn failed: %s", e)
        return IncludeTree(edges={}, visited=[])

    return parse_minus_H(proc.stderr, root=str(source))


def parse_minus_H(stderr: str, root: str) -> IncludeTree:
    """Parse ``clang -H`` nested-dot output into a parent->children map."""
    edges: Dict[str, List[str]] = {root: []}
    visited: List[str] = [root]
    stack: List[Tuple[int, str]] = [(0, root)]

    for raw in stderr.splitlines():
        m = HEADER_LINE_RE.match(raw)
        if not m:
            continue
        depth = len(m.group(1))
        header = m.group(2).strip()
        visited.append(header)
        while stack and stack[-1][0] >= depth:
            stack.pop()
        parent = stack[-1][1] if stack else root
        edges.setdefault(parent, []).append(header)
        edges.setdefault(header, [])
        stack.append((depth, header))

    return IncludeTree(edges=edges, visited=visited)
