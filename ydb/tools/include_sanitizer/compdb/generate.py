"""High-level compile_commands.json generation.

Three supported modes:

* ``wrapper`` (preferred): patches ``build/scripts/retry_cc.py`` and
  ``build/scripts/clang_wrapper.py`` with a recorder shim, sets
  ``RETRY=yes`` in the environment so that every C++ compile actually
  goes through ``retry_cc.py``, runs the user-supplied ``ya make``
  command, restores both wrappers, then aggregates per-invocation
  records into one ``compile_commands.json``.

  Why both scripts? In OPENSOURCE mode (which is what ydb is):
    - regular ``.cpp -> .o`` compiles do NOT go through
      ``clang_wrapper.py`` (that is only used for ``-emit-ast``,
      ``-emit-llvm``, and ``-target bpf`` flows);
    - they ALSO do not go through any wrapper at all unless
      ``RETRY=yes`` is set, in which case they go through
      ``retry_cc.py``.
  So we patch ``retry_cc.py`` (primary, with ``RETRY=yes`` forced) and
  ``clang_wrapper.py`` (for the AST/LLVM/BPF flows).

  The patch is a strictly additive shim placed before the actual
  ``subprocess`` / ``execv`` call. If the build is interrupted the
  recorder still fires for every compile that ran.

* ``iwyu``: scrapes the ya build cache for ``.iwyujson`` sibling files
  produced by an ``IWYU=yes`` run. ``build/scripts/iwyu.py`` already has the
  filtered clang command in scope; we ship a small companion patch (or read
  pre-existing files when the user opted in to dump them by exporting
  ``YDB_COMPDB_DIR``).

* ``import``: skip recording entirely — point at an existing
  ``compile_commands.json`` (e.g. produced by ``bear``).

The result is a normalized ``compile_commands.json`` at the repo root with
exactly one entry per source file under the requested ``--subdir``.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional

from ..common import (
    PATHS,
    PKG_NAME,
    PKG_PARENT,
    REPO_ROOT,
    die,
    ensure_dir,
    is_skipped_path,
    repo_relative,
    setup_logging,
)


log = logging.getLogger("compdb")

CLANG_WRAPPER = REPO_ROOT / "build" / "scripts" / "clang_wrapper.py"
RETRY_CC = REPO_ROOT / "build" / "scripts" / "retry_cc.py"

PATCH_MARKER_BEGIN = "# >>> ydb-include-sanitizer: record_cc shim begin"
PATCH_MARKER_END = "# <<< ydb-include-sanitizer: record_cc shim end"


SHIM_TEMPLATE_CLANG_WRAPPER = """\
{begin}
import os as _yis_os
if _yis_os.environ.get("YDB_COMPDB_DIR"):
    try:
        import sys as _yis_sys
        _yis_sys.path.insert(0, {pkg_parent!r})
        from {pkg_name}.compdb.record_cc import record as _yis_record
        from pathlib import Path as _yis_Path
        _yis_record(path, args, _yis_Path(_yis_os.environ["YDB_COMPDB_DIR"]))
    except Exception as _yis_e:
        import sys as _yis_sys
        print("ydb-include-sanitizer record failure:", _yis_e, file=_yis_sys.stderr)
{end}
"""


SHIM_TEMPLATE_RETRY_CC = """\
{begin}
import os as _yis_os
_yis_cdb = _yis_os.environ.get("YDB_COMPDB_DIR")
_yis_tt = _yis_os.environ.get("YDB_TIMETRACE_DIR")
if cmd and (_yis_cdb or _yis_tt):
    try:
        import sys as _yis_sys
        _yis_sys.path.insert(0, {pkg_parent!r})
        from {pkg_name}.compdb.record_cc import shim_handle as _yis_handle
        cmd = _yis_handle(list(cmd), _yis_cdb, _yis_tt)
    except Exception as _yis_e:
        import sys as _yis_sys
        print("ydb-include-sanitizer shim failure:", _yis_e, file=_yis_sys.stderr)
    # Bypass retry_cc.py's retry loop entirely (it has a Py3 quirk in
    # need_retry() on empty output). We have already recorded/augmented;
    # exec the compiler directly so the build still works.
    _yis_os.execv(cmd[0], cmd)
{end}
"""


# Binary-routed variant: instead of importing the package from a source
# tree, exec the bundled include-sanitizer binary as the recorder. This
# makes ``compdb``/``timetrace`` collection work from a self-contained
# ya-built binary with no source-tree Python.
SHIM_TEMPLATE_RETRY_CC_BIN = """\
{begin}
import os as _yis_os
if cmd and (_yis_os.environ.get("YDB_COMPDB_DIR") or _yis_os.environ.get("YDB_TIMETRACE_DIR")):
    _yis_os.execv({recorder_bin!r}, [{recorder_bin!r}, "__shim"] + list(cmd))
{end}
"""


def _shim_block(template: str, recorder_bin: Optional[str] = None) -> str:
    return template.format(
        begin=PATCH_MARKER_BEGIN,
        end=PATCH_MARKER_END,
        pkg_parent=str(PKG_PARENT),
        pkg_name=PKG_NAME,
        recorder_bin=recorder_bin or "",
    )


def detect_self_binary() -> Optional[str]:
    """Return the path to this tool's own executable if it is a bundled
    binary (not a plain Python interpreter), else None.

    Lets ``compdb``/``timetrace`` auto-route the build-time shim through
    the binary when the tool is invoked as a ya PY3_PROGRAM.
    """
    exe = os.path.realpath(sys.executable or "")
    base = os.path.basename(exe).lower()
    if not exe or base.startswith(("python", "pypy")):
        return None
    if os.access(exe, os.X_OK):
        return exe
    return None


def _patch_file(target: Path, anchor: str, template: str, indent: str = "",
                recorder_bin: Optional[str] = None) -> None:
    """Inject the recorder shim into ``target`` just before ``anchor``.

    Idempotent: a no-op if markers are already present.
    """
    if not target.exists():
        log.warning("target %s does not exist; skipping", repo_relative(str(target)))
        return
    text = target.read_text(encoding="utf-8")
    if PATCH_MARKER_BEGIN in text:
        log.debug("%s already patched", repo_relative(str(target)))
        return
    if anchor not in text:
        die(
            f"cannot find anchor {anchor!r} in {target}; the script shape "
            "changed and the recorder shim needs updating"
        )
    shim = _shim_block(template, recorder_bin=recorder_bin)
    if indent:
        shim = "".join(indent + line if line.strip() else line
                       for line in shim.splitlines(keepends=True))
    new_text = text.replace(anchor, shim + "\n" + anchor, 1)
    target.write_text(new_text, encoding="utf-8")
    log.info("patched %s with recorder shim", repo_relative(str(target)))


def _unpatch_file(target: Path) -> None:
    if not target.exists():
        return
    text = target.read_text(encoding="utf-8")
    if PATCH_MARKER_BEGIN not in text:
        return
    lines = text.splitlines(keepends=True)
    out: List[str] = []
    skip = False
    skipping_blank_after = False
    for line in lines:
        if PATCH_MARKER_BEGIN in line:
            skip = True
            continue
        if PATCH_MARKER_END in line:
            skip = False
            skipping_blank_after = True
            continue
        if skip:
            continue
        if skipping_blank_after:
            skipping_blank_after = False
            if line.strip() == "":
                continue
        out.append(line)
    new_text = "".join(out)
    target.write_text(new_text, encoding="utf-8")
    log.info("removed recorder shim from %s", repo_relative(str(target)))


def patch_clang_wrapper(wrapper: Path = CLANG_WRAPPER) -> None:
    """Patch ``clang_wrapper.py``.

    Anchor: ``    cmd = [path] + args`` (4-space indent — inside the
    ``if __name__ == '__main__':`` block). We indent the shim by 4 to
    match.
    """
    _patch_file(wrapper, "    cmd = [path] + args", SHIM_TEMPLATE_CLANG_WRAPPER, indent="    ")


def unpatch_clang_wrapper(wrapper: Path = CLANG_WRAPPER) -> None:
    _unpatch_file(wrapper)


def patch_retry_cc(target: Path = RETRY_CC, recorder_bin: Optional[str] = None) -> None:
    """Patch ``retry_cc.py`` so the recorder fires on every ``-c`` compile.

    Anchor: the ``if '-c' in cmd:`` line (4-space indent — inside the
    ``if __name__ == '__main__':`` block). We indent the shim by 4 to
    match; otherwise Python would (silently!) re-parse the original
    compile dispatch as the body of the shim's ``if YDB_COMPDB_DIR:``
    branch, which would skip the compile when the env var is unset.

    When ``recorder_bin`` is given, the shim execs that binary
    (``<bin> __shim ...``) as the recorder instead of importing the
    package from a source tree — enabling a self-contained binary.
    """
    template = SHIM_TEMPLATE_RETRY_CC_BIN if recorder_bin else SHIM_TEMPLATE_RETRY_CC
    _patch_file(target, "    if '-c' in cmd:", template, indent="    ",
                recorder_bin=recorder_bin)


def unpatch_retry_cc(target: Path = RETRY_CC) -> None:
    _unpatch_file(target)


@contextlib.contextmanager
def patched_wrapper(wrapper: Path = CLANG_WRAPPER, retry: Path = RETRY_CC,
                    recorder_bin: Optional[str] = None):
    # In binary mode we route only retry_cc through the binary (it covers
    # every regular C++ compile). clang_wrapper handles only emit-ast /
    # bpf flows, which we skip in binary mode (its import-based shim has
    # no package to import from a bundled binary).
    patched_clang = False
    if not recorder_bin:
        patch_clang_wrapper(wrapper)
        patched_clang = True
    patch_retry_cc(retry, recorder_bin=recorder_bin)
    try:
        yield
    finally:
        unpatch_retry_cc(retry)
        if patched_clang:
            unpatch_clang_wrapper(wrapper)


def aggregate_entries(
    entries_dir: Path,
    subdir: str,
    out_json: Path,
) -> int:
    """Merge per-invocation JSON files into one compile_commands.json.

    Conflict policy: if multiple entries map to the same source file (e.g.
    PIC vs non-PIC, or rebuilt mid-session), keep the most recent one. Skip
    sources that lie outside ``subdir`` or in skipped trees (``contrib/`` &
    co.).
    """
    if not entries_dir.exists():
        die(f"compdb entries dir does not exist: {entries_dir}")

    by_file: Dict[str, dict] = {}
    by_mtime: Dict[str, float] = {}

    n_seen = 0
    n_kept = 0
    n_skipped = 0
    for p in entries_dir.iterdir():
        if not p.name.endswith(".json"):
            continue
        n_seen += 1
        try:
            entry = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            log.warning("ignoring %s: %s", p, e)
            continue

        f = entry.get("file")
        if not f:
            continue
        rel = repo_relative(f)
        if not rel.startswith(subdir.rstrip("/") + "/") and rel != subdir:
            n_skipped += 1
            continue
        if is_skipped_path(rel):
            n_skipped += 1
            continue

        mt = p.stat().st_mtime
        if rel in by_mtime and by_mtime[rel] >= mt:
            continue
        by_mtime[rel] = mt
        by_file[rel] = {
            "file": rel,
            "directory": entry.get("directory") or str(REPO_ROOT),
            "arguments": entry["arguments"],
        }
        n_kept += 1

    ensure_dir(out_json.parent)
    tmp = out_json.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(list(by_file.values()), fh, indent=1)
    os.replace(tmp, out_json)

    log.info(
        "aggregated %d entries (%d unique, %d skipped outside %s) -> %s",
        n_seen, n_kept, n_skipped, subdir, repo_relative(str(out_json)),
    )
    return n_kept


def run_wrapper_mode(
    ya_argv: List[str],
    subdir: str,
    force_retry: bool = True,
    inline_analyze: bool = True,
    cleaner_bin: Optional[str] = None,
    recorder_bin: Optional[str] = None,
) -> int:
    if not ya_argv:
        die("wrapper mode requires a ya make command after '--'")

    if recorder_bin:
        log.info("recorder: routing the build-time shim through the binary %s",
                 recorder_bin)

    entries_dir = ensure_dir(PATHS.compdb_entries_dir)
    log.info("recording compile commands into %s", repo_relative(str(entries_dir)))

    env = os.environ.copy()
    # Pin the repo root for the build-step children. They run with CWD set
    # to ya's ephemeral build_root, where a bundled binary's __file__ /
    # marker search would otherwise mis-resolve REPO_ROOT and produce
    # unstable (absolute) cache keys that never match the later analyze.
    env["YDB_REPO_ROOT"] = str(REPO_ROOT)
    env["YDB_COMPDB_DIR"] = str(entries_dir)
    env.setdefault("YDB_COMPDB_EXEC", "1")
    if force_retry:
        env["RETRY"] = "yes"

    if inline_analyze:
        # Resolve clang-include-cleaner up-front and pass it to the
        # workers via env, so each shim invocation doesn't redo the
        # autodiscovery dance.
        from ..analyze.clang_runner import find_clang_include_cleaner
        resolved = cleaner_bin or find_clang_include_cleaner()
        if not resolved:
            die(
                "inline-analyze is on (default) but clang-include-cleaner "
                "was not found. Either install it (LLVM 17+), set "
                "YDB_CLANG_INCLUDE_CLEANER, or pass --no-inline-analyze to "
                "skip analysis during the build (you will still need a way "
                "to keep generated headers around for a later analyze step)."
            )
        env["YDB_ANALYZE_INLINE"] = "1"
        env["YDB_CLEANER_BIN"] = resolved
        env["YDB_ANALYZE_SUBDIR"] = subdir
        log.info("inline-analyze: ON (clang-include-cleaner=%s, scope=%s); "
                 "the build will be slower but the cache is populated as we go",
                 resolved, subdir)
    else:
        env.pop("YDB_ANALYZE_INLINE", None)
        env.pop("YDB_CLEANER_BIN", None)
        env.pop("YDB_ANALYZE_SUBDIR", None)
        log.warning(
            "inline-analyze: OFF. Note that the analyze step run AFTER the "
            "build typically fails because ya prunes the per-TU build_root "
            "(generated .pb.h headers disappear). Use this only if you "
            "have arranged to keep the build_root alive."
        )

    # If the user is running `./ya make ...`, splice `-DRETRY=yes` after
    # the `make` subcommand so the build graph activates retry_cc.py as a
    # compile wrapper. Without this, in OPENSOURCE builds clang is invoked
    # directly with no wrapper at all and we cannot intercept compiles.
    augmented_argv = list(ya_argv)
    if force_retry and _looks_like_ya_make(augmented_argv) and not _has_retry_define(augmented_argv):
        insert_at = _ya_make_insert_index(augmented_argv)
        augmented_argv = augmented_argv[:insert_at] + ["-DRETRY=yes"] + augmented_argv[insert_at:]
        log.info("auto-inserted -DRETRY=yes so retry_cc.py wraps every compile")

    with patched_wrapper(CLANG_WRAPPER, RETRY_CC, recorder_bin=recorder_bin):
        log.info("running: %s", " ".join(augmented_argv))
        rc = subprocess.call(augmented_argv, env=env)

    n = aggregate_entries(entries_dir, subdir, PATHS.compdb_json)
    if rc != 0:
        log.warning("ya make exited with code %d; collected %d entries anyway", rc, n)
    if n == 0:
        die(
            "no compile entries recorded; this usually means the build had "
            "no compile actions to run (e.g. nothing was rebuilt from cache). "
            "Try forcing a clean rebuild of a small subtree, e.g.:\n"
            "  rm -rf ~/.ya/build/cache  # only if you accept losing the cache\n"
            "  sanitize_includes compdb -- ./ya make --build relwithdebinfo "
            "ydb/core/base\n"
            "or pass --force-rebuild to bypass the local cache."
        )
    return rc


def _looks_like_ya_make(argv: List[str]) -> bool:
    """Return True if ``argv`` looks like an invocation of ``ya make ...``."""
    for i, a in enumerate(argv):
        if a.endswith("/ya") or a == "ya":
            return i + 1 < len(argv) and argv[i + 1] == "make"
    return False


def _ya_make_insert_index(argv: List[str]) -> int:
    """Index at which to insert ``-DRETRY=yes`` in a ``ya make ...`` command."""
    for i, a in enumerate(argv):
        if (a.endswith("/ya") or a == "ya") and i + 1 < len(argv) and argv[i + 1] == "make":
            return i + 2
    return len(argv)


def _has_retry_define(argv: List[str]) -> bool:
    return any(a == "-DRETRY=yes" or a == "-D" and i + 1 < len(argv) and argv[i + 1] == "RETRY=yes"
               for i, a in enumerate(argv))


def run_iwyu_mode(subdir: str) -> int:
    """Scrape compile commands from a previous IWYU=yes build.

    This relies on the user having either:
    - Patched ``build/scripts/iwyu.py`` to dump filtered_clang_cmd into
      a sibling file under ``$YDB_COMPDB_DIR``, or
    - Already populated ``$YDB_COMPDB_DIR`` with entries produced by some
      other means (e.g. an earlier wrapper-mode run).
    """
    entries_dir = PATHS.compdb_entries_dir
    if not entries_dir.exists() or not any(entries_dir.iterdir()):
        die(
            f"no entries found in {entries_dir}; run wrapper mode first, or "
            "populate the directory by patching build/scripts/iwyu.py"
        )
    n = aggregate_entries(entries_dir, subdir, PATHS.compdb_json)
    return 0 if n > 0 else 1


def run_import_mode(source: Path, subdir: str) -> int:
    if not source.exists():
        die(f"--from {source} does not exist")
    try:
        data = json.loads(source.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        die(f"invalid compile_commands.json at {source}: {e}")

    out: List[dict] = []
    for entry in data:
        f = entry.get("file")
        if not f:
            continue
        rel = repo_relative(f)
        if not rel.startswith(subdir.rstrip("/") + "/") and rel != subdir:
            continue
        if is_skipped_path(rel):
            continue
        normalized = {
            "file": rel,
            "directory": entry.get("directory") or str(REPO_ROOT),
        }
        if "arguments" in entry:
            normalized["arguments"] = entry["arguments"]
        elif "command" in entry:
            normalized["command"] = entry["command"]
        else:
            continue
        out.append(normalized)

    ensure_dir(PATHS.compdb_json.parent)
    PATHS.compdb_json.write_text(json.dumps(out, indent=1), encoding="utf-8")
    log.info("imported %d entries -> %s", len(out), repo_relative(str(PATHS.compdb_json)))
    return 0 if out else 1


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sanitize_includes compdb",
        description="Generate compile_commands.json for ydb/",
    )
    parser.add_argument(
        "--mode",
        choices=["wrapper", "iwyu", "import"],
        default="wrapper",
        help="how to obtain compile commands (default: wrapper)",
    )
    parser.add_argument(
        "--subdir",
        default="ydb",
        help="restrict the resulting compile_commands.json to this subtree "
             "(default: ydb)",
    )
    parser.add_argument(
        "--from",
        dest="from_path",
        type=Path,
        help="(import mode) existing compile_commands.json to ingest",
    )
    parser.add_argument(
        "--no-force-retry",
        dest="force_retry",
        action="store_false",
        default=True,
        help="(wrapper mode) do not auto-set RETRY=yes / inject -DRETRY=yes; "
             "use this only if your build already arranges for retry_cc.py "
             "to wrap every C++ compile",
    )
    parser.add_argument(
        "--no-inline-analyze",
        dest="inline_analyze",
        action="store_false",
        default=True,
        help="(wrapper mode) do not run clang-include-cleaner during the "
             "build. The default is to analyze in-line because ya's per-TU "
             "build_root (which holds generated *.pb.h files) is ephemeral "
             "and is typically gone by the time a separate 'analyze' step "
             "runs. Turning this off speeds up the build but means a later "
             "'analyze' will likely fail with 'file not found' on generated "
             "headers.",
    )
    parser.add_argument(
        "--cleaner-bin",
        default=None,
        help="(wrapper mode) path to clang-include-cleaner used by inline "
             "analyze (default: autodiscover)",
    )
    parser.add_argument(
        "--recorder-bin",
        default=None,
        help="(wrapper mode) path to the include-sanitizer binary to route "
             "the build-time shim through (so collection needs no source "
             "Python). Default: auto-detected when running as a bundled "
             "binary; empty (import-from-source) when running from source.",
    )
    parser.add_argument(
        "--no-recorder-bin",
        dest="allow_recorder_bin",
        action="store_false",
        default=True,
        help="(wrapper mode) force the source-import shim even when running "
             "as a bundled binary.",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="debug logging",
    )
    parser.add_argument(
        "ya_argv",
        nargs=argparse.REMAINDER,
        help="(wrapper mode) command to run after '--', e.g. './ya make ...'",
    )

    args = parser.parse_args(argv)
    setup_logging(args.verbose)

    # Strip leading '--' if argparse left it in REMAINDER.
    ya_argv = list(args.ya_argv)
    if ya_argv and ya_argv[0] == "--":
        ya_argv = ya_argv[1:]

    if args.mode == "wrapper":
        recorder_bin = args.recorder_bin
        if recorder_bin is None and args.allow_recorder_bin:
            recorder_bin = detect_self_binary()
        return run_wrapper_mode(
            ya_argv,
            args.subdir,
            force_retry=args.force_retry,
            inline_analyze=args.inline_analyze,
            cleaner_bin=args.cleaner_bin,
            recorder_bin=recorder_bin,
        )
    if args.mode == "iwyu":
        return run_iwyu_mode(args.subdir)
    if args.mode == "import":
        if not args.from_path:
            die("--from PATH is required in import mode")
        return run_import_mode(args.from_path, args.subdir)
    die(f"unknown mode {args.mode}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
