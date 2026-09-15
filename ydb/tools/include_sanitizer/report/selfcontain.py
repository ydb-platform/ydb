"""Per-file "include what you use" edits.

This is the robust alternative to subtractive header cleanup. For every
analyzed file (TU or header) we already know, from clang-include-cleaner:

- ``unused_includes`` — direct includes the file does not use (the ``-``),
- ``suggested_inserts`` — headers it uses transitively and should include
  directly (the ``+``).

Applying BOTH to every file makes each file self-contained: it includes
exactly what it uses and relies on nothing transitively. Once every file
is self-contained, removing an include from a header can no longer break
a consumer, because no consumer depends on transitive reachability.

This module emits one unified-diff patch per file. We deliberately do NOT
modify the tree; the patches are previews you apply with ``git apply``.

Reliability notes:
- The ADD side (``+``) is safe: adding an include cannot break a
  compile, only make it marginally slower. This is the default.
- The REMOVE side (``-``) is NOT reliable. clang-include-cleaner has
  well-known blind spots that produce false-positive removals:
    * macro-only usage (e.g. LWTRACE probes ``lwtrace_*`` / ``LWPROBE``)
      whose defining header gets flagged unused;
    * template member instantiation (e.g. an iterator type whose full
      definition lives in a header the tool thinks is unused);
    * ``namespace X = Y;`` aliases / ``using namespace`` where the
      namespace is defined in a "removed" header.
  Removing these breaks the build. Removals are therefore OByOFF by
  default and gated behind ``--with-removals`` (review every one).
- TU (.cpp) analysis is the most trustworthy (real compile command).
  Header analysis comes from an isolated probe and is less reliable;
  files with an ``error`` (probe-failed / include-cycle) are skipped.
- ``IWYU pragma: keep`` / ``export`` includes are never removed.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from ..analyze.source_includes import scan_includes
from ..common import PATHS, REPO_ROOT, die, ensure_dir, repo_relative, setup_logging
from .diff_preview import (
    _drop_include_lines,
    _format_diff,
    _insert_after_includes,
    _read_lines,
)


log = logging.getLogger("report.selfcontain")


def _render_addition_spelling(spelled: str) -> str:
    """Render a suggested insert as an angled include path.

    clang-include-cleaner emits source-root / standard-library spellings
    (e.g. ``util/system/types.h``, ``utility``, ``ydb/...``); the angled
    form resolves from any file. We strip any stray quotes/brackets the
    cleaner may have included.
    """
    s = spelled.strip().strip('"').strip("<>").strip()
    return s


def render_file_edit(
    file_rel: str,
    fa: dict,
    repo_root: Path,
    do_additions: bool = True,
    do_removals: bool = False,
    include_filter: Optional[Callable[[str], bool]] = None,
) -> Tuple[str, int, int]:
    """Return (unified_diff, n_removed, n_added) for one file.

    Empty diff string means no change. By default only additions are
    emitted (safe); removals require ``do_removals=True`` (risky — see
    module docstring).

    ``include_filter``, if given, restricts both adds and removes to
    include paths for which it returns True (e.g. only ``*.pb.h``).
    """
    def keep(path: str) -> bool:
        return include_filter is None or include_filter(path)

    abs_path = repo_root / file_rel
    before = _read_lines(abs_path)
    if not before:
        return "", 0, 0

    # Decisions come from the cache; POSITIONS come from the CURRENT file
    # (re-scanned now). This keeps the operation idempotent and correct
    # even if the file was already edited since analysis: removals match
    # the include's current line by spelling (so already-removed lines
    # are a no-op, and shifted lines are handled), and additions are
    # de-duplicated against what the file currently includes.
    unused = set(fa.get("unused_includes") or [])
    inserts_set = set(fa.get("suggested_inserts") or [])

    current = scan_includes(abs_path)
    current_spellings = {c.spelled for c in current}

    remove_lines: List[int] = []
    if do_removals:
        for c in current:
            if (
                c.spelled in unused
                # never remove something the same file is told to add
                # (contradictory analysis); leave for human review.
                and c.spelled not in inserts_set
                and not c.iwyu_keep
                and not c.iwyu_export
                and not c.in_conditional
                and keep(c.spelled)
            ):
                remove_lines.append(c.line)

    after = _drop_include_lines(before, remove_lines)

    # Additions: suggested inserts not already present in the CURRENT file.
    adds: List[str] = []
    if do_additions:
        for s in fa.get("suggested_inserts") or []:
            rendered = _render_addition_spelling(s)
            if not rendered:
                continue
            if s in current_spellings or rendered in current_spellings:
                continue
            if not (keep(rendered) or keep(s)):
                continue
            if rendered not in adds:
                adds.append(rendered)

    after = _insert_after_includes(after, sorted(adds))

    diff = _format_diff(file_rel, before, after)
    return diff, len(remove_lines), len(adds)


def emit_self_contained(
    per_file: Dict[str, dict],
    out_dir: Path,
    repo_root: Path,
    kinds: Tuple[str, ...],
    combined: bool = False,
    do_additions: bool = True,
    do_removals: bool = False,
    include_filter: Optional[Callable[[str], bool]] = None,
) -> Tuple[int, int, int, int]:
    """Emit per-file IWYU patches.

    Returns (files_changed, files_skipped_errors, total_removed, total_added).
    """
    ensure_dir(out_dir)
    for p in out_dir.glob("*.patch"):
        try:
            p.unlink()
        except OSError:
            pass

    files_changed = 0
    skipped_errors = 0
    total_removed = 0
    total_added = 0
    combined_chunks: List[str] = []

    for file_rel, fa in sorted(per_file.items()):
        if fa.get("kind") not in kinds:
            continue
        if fa.get("error"):
            # probe-failed / include-cycle: no trustworthy data.
            skipped_errors += 1
            continue
        diff, n_rm, n_add = render_file_edit(
            file_rel, fa, repo_root,
            do_additions=do_additions, do_removals=do_removals,
            include_filter=include_filter,
        )
        if not diff:
            continue
        files_changed += 1
        total_removed += n_rm
        total_added += n_add
        if combined:
            combined_chunks.append(diff)
        else:
            out_path = out_dir / (file_rel.replace("/", "__") + ".patch")
            out_path.write_text(diff, encoding="utf-8")

    if combined and combined_chunks:
        (out_dir / "selfcontain.combined.patch").write_text(
            "".join(combined_chunks), encoding="utf-8"
        )

    return files_changed, skipped_errors, total_removed, total_added


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sanitize_includes selfcontain",
        description="Emit per-file include-what-you-use patches (add missing "
                    "+ remove unused) so every file is self-contained.",
    )
    parser.add_argument("--subdir", default="ydb")
    parser.add_argument(
        "--kind", choices=["tu", "header", "both"], default="tu",
        help="which files to emit edits for. 'tu' (default) is the safest: "
             "TU analysis is the most reliable. 'header' edits are riskier "
             "(isolated-probe analysis). 'both' does everything.",
    )
    parser.add_argument(
        "--combined", action="store_true",
        help="write a single selfcontain.combined.patch instead of one "
             "patch per file.",
    )
    parser.add_argument(
        "--with-removals", action="store_true",
        help="ALSO emit removals of unused includes. OFF by default "
             "because clang-include-cleaner produces false-positive "
             "removals for macro-only usage (e.g. LWTRACE probes) and "
             "template member instantiation, which break the build. "
             "Review every removal if you enable this.",
    )
    parser.add_argument(
        "--removals-only", action="store_true",
        help="emit ONLY removals (no additions). Implies --with-removals. "
             "For when you have already applied the additions.",
    )
    parser.add_argument(
        "--only-pattern", default=None,
        help="restrict adds and removes to include paths matching this "
             "regex (e.g. '\\.pb\\.h$').",
    )
    parser.add_argument(
        "--protobuf", action="store_true",
        help="convenience for --only-pattern '\\.(grpc\\.)?pb\\.h$': target "
             "only generated protobuf/gRPC headers (the heavy, high-fanout "
             "ones). Combine with --kind both for adds and --kind header "
             "--removals-only for the removal step.",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    setup_logging(args.verbose)

    from ..aggregate.run import load_per_file
    per_file = load_per_file(PATHS.per_tu_dir, args.subdir)
    if not per_file:
        die("no analysis cache found; run 'analyze' (or 'compdb' with inline "
            "analysis) first")

    do_removals = args.with_removals or args.removals_only
    do_additions = not args.removals_only

    pattern = args.only_pattern
    if args.protobuf:
        pattern = r"\.(grpc\.)?pb\.h$"
    include_filter: Optional[Callable[[str], bool]] = None
    if pattern:
        rx = re.compile(pattern)
        include_filter = lambda p: bool(rx.search(p))  # noqa: E731

    kinds: Tuple[str, ...] = ("tu", "header") if args.kind == "both" else (args.kind,)
    out_dir = PATHS.reports_dir / "selfcontain"

    changed, skipped, removed, added = emit_self_contained(
        per_file, out_dir, REPO_ROOT, kinds, combined=args.combined,
        do_additions=do_additions, do_removals=do_removals,
        include_filter=include_filter,
    )
    mode = "add-only" if not do_removals else ("remove-only" if not do_additions else "add+remove")
    scope = f", filter={pattern}" if pattern else ""
    log.info(
        "self-contain (kind=%s, mode=%s%s): %d files changed (%d adds, %d removes), "
        "%d skipped (probe errors); patches in %s",
        args.kind, mode, scope, changed, added, removed, skipped,
        repo_relative(str(out_dir)),
    )
    if not do_removals:
        log.info(
            "add-only mode (safe): every TU now includes what it uses. Build "
            "should stay green. To attempt removals (RISKY — false positives "
            "on macros/templates can break the build), re-run with "
            "--with-removals and review each patch."
        )
    else:
        log.warning(
            "removals enabled: clang-include-cleaner has known false "
            "positives (LWTRACE macros, template instantiation, namespace "
            "aliases). Apply incrementally and rebuild; do NOT bulk-apply."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
