"""Compile-command recorder.

Designed to be called from a compile-time wrapper shim (see
``patch_retry_cc`` / ``patch_clang_wrapper`` in
``compdb/generate.py``). It records the compile invocation and, when
asked, also runs clang-include-cleaner *in-line during the build* so
the analyzer sees all generated headers (`.pb.h`, `.grpc.pb.h`,
`.fbs.h`, ...) that live in the ephemeral ya build_root.

Three env vars drive behavior:

- ``YDB_COMPDB_DIR``: enables recording. The script writes one JSON
  file per invocation, named ``<sha1-of-output>.json``, with the
  compilation-database entry::

      {"file": "...", "directory": "...", "arguments": [...]}

- ``YDB_ANALYZE_INLINE=1``: in addition to recording, run
  clang-include-cleaner + ``clang -H`` immediately, while the build is
  still running and generated headers are still on disk. We also
  synthesize probe TUs for every visited header under the active
  subtree and analyze each one inline — this is essential because the
  build_root holding generated ``.pb.h`` files is per-TU and ephemeral.
  Per-TU and per-header results are stored in the same cache that
  ``analyze`` would otherwise populate (``.cache/per_tu/``), so the
  later analyze step becomes a near no-op.

- ``YDB_ANALYZE_INLINE_HEADERS=0``: skip the per-header probe pass and
  do only TU analysis. Faster, but you will probably hit
  ``file not found`` errors on generated headers when ``analyze`` runs
  later.

- ``YDB_ANALYZE_SUBDIR``: only inline-analyze TUs (and probe headers)
  under this repo-relative subdirectory. Defaults to ``ydb``.

- ``YDB_CLEANER_BIN``: path to clang-include-cleaner. Used by
  ``YDB_ANALYZE_INLINE``. Falls back to autodiscovery.

A separate post-processing step (``aggregate_entries``) concatenates
all recorded JSON files into a single ``compile_commands.json``.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple


def _looks_like_source(arg: str) -> bool:
    lower = arg.lower()
    return lower.endswith((".cpp", ".cc", ".cxx", ".c", ".c++"))


def _looks_like_output(arg: str) -> bool:
    lower = arg.lower()
    return lower.endswith((".o", ".obj"))


def split_source_and_output(args: List[str]) -> Tuple[Optional[str], Optional[str]]:
    source: Optional[str] = None
    output: Optional[str] = None
    i = 0
    while i < len(args):
        a = args[i]
        if a == "-c" and i + 1 < len(args):
            cand = args[i + 1]
            if _looks_like_source(cand):
                source = cand
                i += 2
                continue
        if a == "-o" and i + 1 < len(args):
            output = args[i + 1]
            i += 2
            continue
        if a.startswith("-o") and len(a) > 2:
            output = a[2:]
            i += 1
            continue
        if _looks_like_source(a):
            source = a
        i += 1
    return source, output


def record(real_compiler: str, args: List[str], compdb_dir: Path) -> None:
    source, output = split_source_and_output(args)
    if not source:
        return

    entry = {
        "file": os.path.abspath(source),
        "directory": os.getcwd(),
        "arguments": [real_compiler] + list(args),
    }

    key_input = (output or source) + "::" + str(os.getcwd())
    digest = hashlib.sha1(key_input.encode("utf-8")).hexdigest()
    compdb_dir.mkdir(parents=True, exist_ok=True)
    out_path = compdb_dir / f"{digest}.json"
    # Per-writer unique temp name to avoid races between parallel compiles.
    import secrets as _secrets
    tmp_path = out_path.with_suffix(f".json.tmp.{os.getpid()}.{_secrets.token_hex(4)}")
    try:
        with tmp_path.open("w", encoding="utf-8") as fh:
            json.dump(entry, fh)
        os.replace(tmp_path, out_path)
    except OSError:
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise

    if os.environ.get("YDB_ANALYZE_INLINE") == "1":
        try:
            _analyze_inline(entry)
        except Exception as e:
            # Flush: the caller (retry_cc shim) ends with execv(), which
            # discards Python's buffered stderr — without an explicit
            # flush this message would vanish and failures look silent.
            print(f"ydb-include-sanitizer inline-analyze failure: {e}",
                  file=sys.stderr)
            sys.stderr.flush()


def _analyze_inline(entry: dict) -> None:
    """Run clang-include-cleaner now, while the build_root still exists.

    Two passes:
      1. TU pass: analyze the .cpp being compiled.
      2. Header pass: for every header the TU visits (per the live
         ``clang -H`` output, which sees generated ``.pb.h`` files),
         synthesize a probe and analyze it. The build_root is still
         alive at this point, so the per-TU ``-I<build_root>`` paths
         resolve correctly. Headers already in cache are skipped, so
         each unique header is probed at most once globally.

    Imports lazily so non-inline use does not pay the import cost.
    Uses package-relative imports so it works regardless of where the
    package lives or under what top-level name it was imported.
    """
    from ..analyze.per_tu import analyze_tu, analyze_header
    from ..analyze.clang_runner import find_clang_include_cleaner
    from ..common import PATHS, REPO_ROOT, is_under_subdir

    repo_root = REPO_ROOT

    cleaner_bin = os.environ.get("YDB_CLEANER_BIN") or find_clang_include_cleaner()
    if not cleaner_bin:
        return

    cache_dir = PATHS.per_tu_dir
    cache_dir.mkdir(parents=True, exist_ok=True)
    probes_dir = PATHS.cache_dir / "probes"
    probes_dir.mkdir(parents=True, exist_ok=True)

    an, visited_rel, args_map = analyze_tu(
        entry,
        cleaner_bin,
        cache_dir,
        mapping_file=None,
        ignore_headers=(),
        repo_root=repo_root,
        force=False,
        # The build_root is live here; if a previous build cached an
        # error (e.g. a transient timing issue with generated headers
        # being placed late), this re-attempt has a real chance to heal.
        retry_errored=True,
    )

    if os.environ.get("YDB_ANALYZE_INLINE_HEADERS", "1") == "0":
        return

    subdir = os.environ.get("YDB_ANALYZE_SUBDIR", "ydb")
    for header_rel in sorted(visited_rel):
        if not is_under_subdir(header_rel, subdir):
            continue
        try:
            analyze_header(
                header_rel,
                args_map.get(header_rel, []),
                cleaner_bin,
                probes_dir,
                cache_dir,
                mapping_file=None,
                ignore_headers=(),
                repo_root=repo_root,
                force=False,
                retry_errored=True,
            )
        except Exception as e:
            # Defensive: a single bad header must not break the build.
            print(f"ydb-include-sanitizer: header probe failed for "
                  f"{header_rel}: {e}", file=sys.stderr)
            sys.stderr.flush()


def timetrace_flags(args: List[str], tt_dir: Path, granularity: str = "500") -> List[str]:
    """Return ``-ftime-trace`` flags directing the trace to ``tt_dir``.

    Also drops a small ``<digest>.src`` sidecar mapping the trace file
    back to its source path, so the aggregator can name each TU. The
    trace path is explicit (not the default next-to-object location)
    because the object lives in ya's ephemeral build_root.
    """
    source, output = split_source_and_output(args)
    if not source:
        return []
    tt_dir.mkdir(parents=True, exist_ok=True)
    key = (output or source) + "::" + os.getcwd()
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()
    trace_path = tt_dir / f"{digest}.json"
    try:
        (tt_dir / f"{digest}.src").write_text(os.path.abspath(source), encoding="utf-8")
    except OSError:
        pass
    return [f"-ftime-trace={trace_path}", f"-ftime-trace-granularity={granularity}"]


def shim_handle(cmd: List[str], compdb_dir: Optional[str], tt_dir: Optional[str]) -> List[str]:
    """Single entry point for the compile-wrapper shim.

    Records the compile command (when ``compdb_dir`` is set, including
    inline analysis if enabled) and/or appends ``-ftime-trace`` flags
    (when ``tt_dir`` is set). Returns the possibly-augmented command for
    the shim to exec.
    """
    if compdb_dir:
        try:
            record(cmd[0], cmd[1:], Path(compdb_dir))
        except Exception as e:  # never break the build
            print(f"ydb-include-sanitizer record failure: {e}", file=sys.stderr)
    if tt_dir:
        granularity = os.environ.get("YDB_TIMETRACE_GRANULARITY", "500")
        cmd = cmd + timetrace_flags(cmd[1:], Path(tt_dir), granularity)
    return cmd


def main() -> int:
    if len(sys.argv) < 2:
        print("record_cc.py: missing compiler argument", file=sys.stderr)
        return 2

    real_compiler = sys.argv[1]
    args = sys.argv[2:]

    compdb_dir = Path(os.environ.get("YDB_COMPDB_DIR", "/tmp/ydb-compdb"))
    try:
        record(real_compiler, args, compdb_dir)
    except OSError as e:
        print(f"record_cc.py: failed to record: {e}", file=sys.stderr)

    if os.environ.get("YDB_COMPDB_EXEC", "1") == "1":
        cmd = [real_compiler] + args
        rc = subprocess.call(cmd, stdout=sys.stdout, stderr=sys.stderr)
        return rc
    return 0


if __name__ == "__main__":
    sys.exit(main())
