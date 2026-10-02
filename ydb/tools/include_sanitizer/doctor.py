"""``doctor`` subcommand: diagnose analyze failures on a single TU.

Picks one entry from ``compile_commands.json`` (or one supplied via
``--file``), prints:

- the resolved clang binary,
- the cleaned compile command we would pass to clang-include-cleaner,
- the actual exit code, stdout and stderr from both clang-include-cleaner
  and ``clang -H``.

Use this when ``analyze`` finishes too fast / produces no edges / no
verdicts: it tells you exactly what clang-include-cleaner is complaining
about for the offending TU.
"""

from __future__ import annotations

import argparse
import json
import logging
import shlex
import sys
from pathlib import Path
from typing import List, Optional, Tuple

from .analyze.clang_runner import (
    find_clang,
    find_clang_include_cleaner,
    reconstruct_compile_args,
    run_clang_minus_H,
    run_include_cleaner,
    strip_compile_flags,
)
from .analyze.per_tu import load_compdb, materialize_arguments
from .common import PATHS, REPO_ROOT, die, repo_relative, setup_logging


log = logging.getLogger("doctor")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sanitize_includes doctor",
        description="Diagnose analyze failures by running clang-include-cleaner "
                    "and clang -H on one TU and printing all output.",
    )
    parser.add_argument("--file",
                        help="repo-relative path of the TU to diagnose; "
                             "defaults to the first entry in compile_commands.json")
    parser.add_argument("--header",
                        help="repo-relative path of a header to probe; "
                             "synthesizes a probe TU that #include's the "
                             "header and diagnoses it the same way as --file. "
                             "Picks compile flags from the first TU that "
                             "transitively reaches this header (per the cache, "
                             "or, if absent, the first TU in the compdb).")
    parser.add_argument("--cleaner",
                        help="path to clang-include-cleaner; defaults to autodiscovery")
    parser.add_argument(
        "--stats", action="store_true",
        help="Survey the per-TU cache and report how many TUs / headers "
             "have non-empty unused_includes and suggested_inserts. Use "
             "this to confirm whether clang-include-cleaner is producing "
             "the `+` insert suggestions that move-to-cpp depends on.",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    setup_logging(args.verbose)

    if args.stats:
        return _doctor_stats()

    cleaner_bin = args.cleaner or find_clang_include_cleaner()
    if not cleaner_bin:
        die("clang-include-cleaner not found; pass --cleaner /path/to/it")
    print(f"clang-include-cleaner: {cleaner_bin}")

    compdb = load_compdb(PATHS.compdb_json)
    if not compdb:
        die("compile_commands.json is empty")

    if args.header:
        return _doctor_header(args.header, compdb, cleaner_bin)

    entry = None
    if args.file:
        for e in compdb:
            rel = repo_relative(e.get("file", ""))
            if rel == args.file:
                entry = e
                break
        if entry is None:
            die(f"no compile_commands.json entry for {args.file!r}; "
                f"first entry is {compdb[0].get('file')!r}")
    else:
        entry = compdb[0]

    file_rel = repo_relative(entry["file"])
    abs_path = REPO_ROOT / file_rel
    print(f"\nFile: {file_rel}")
    print(f"Exists: {abs_path.exists()}")

    raw_args = materialize_arguments(entry)
    print(f"\nRaw arguments[0]: {raw_args[0] if raw_args else '<none>'}")

    clang_path = find_clang(raw_args)
    print(f"Resolved clang path: {clang_path}")
    if clang_path and not Path(clang_path).exists():
        print("  WARNING: clang path does not exist on this machine")

    compile_args = reconstruct_compile_args(raw_args, clang_path)
    cleaned = strip_compile_flags(compile_args[1:] if compile_args else [],
                                  drop_sources=True)
    print(f"\nCleaned flags for cleaner (after --), count={len(cleaned)}:")
    print("  " + " ".join(shlex.quote(a) for a in cleaned[:50]))
    if len(cleaned) > 50:
        print(f"  ... ({len(cleaned) - 50} more)")

    print("\n=== clang -H ===")
    if not clang_path:
        print("  SKIPPED: no clang path")
    elif not Path(clang_path).exists():
        print(f"  SKIPPED: {clang_path} missing")
    else:
        tree = run_clang_minus_H(clang_path, abs_path, compile_args)
        n_kids = sum(len(v) for v in tree.edges.values())
        print(f"  parsed {n_kids} parent->child edges")
        print(f"  visited {len(tree.visited)} headers (first 10):")
        for h in tree.visited[:10]:
            print(f"    {h}")

    print("\n=== clang-include-cleaner ===")
    result = run_include_cleaner(cleaner_bin, abs_path, compile_args)
    print(f"  exit code: {result.exit_code}")
    print(f"  unused: {len(result.removed)}; insert: {len(result.inserted)}")
    if result.removed:
        print("  unused suggestions ('- <spelled>'):")
        for s, _ in result.removed[:50]:
            print(f"    - {s}")
        if len(result.removed) > 50:
            print(f"    ... ({len(result.removed) - 50} more)")
    if result.inserted:
        print("  insert suggestions ('+ <spelled>'):")
        for s, _ in result.inserted[:50]:
            print(f"    + {s}")
        if len(result.inserted) > 50:
            print(f"    ... ({len(result.inserted) - 50} more)")
    print("  stderr (last 60 lines):")
    for line in result.stderr.splitlines()[-60:]:
        print(f"    {line}")
    if not result.removed and not result.inserted and result.exit_code == 0:
        print("  (empty result with zero exit code likely means the cleaner "
              "ran but found nothing — or, in some versions, that compile "
              "errors prevented analysis. Check the stderr above for warnings.)")

    return 0


def _doctor_header(header_rel: str, compdb: list, cleaner_bin: str) -> int:
    """Diagnose a direct-header probe for a single header.

    Picks the first TU in the compdb to borrow compile flags from
    (same flags ya used to compile that TU). We pass the header itself
    as clang-include-cleaner's source file with ``-x c++-header`` so the
    +/- output is about the header's own #includes.
    """
    abs_header = REPO_ROOT / header_rel
    if not abs_header.exists():
        die(f"header {header_rel} does not exist at {abs_header}")

    template_entry = compdb[0]
    template_args = materialize_arguments(template_entry)
    template_clang = find_clang(template_args)
    template_compile_args = reconstruct_compile_args(template_args, template_clang)
    print(f"\nHeader: {header_rel}")
    print(f"Template TU: {repo_relative(template_entry.get('file', ''))}")

    extra = ["-x", "c++"]
    compile_args = list(template_compile_args) + extra

    cleaned = strip_compile_flags(compile_args[1:] if compile_args else [],
                                  drop_sources=True)
    print(f"\nCleaned flags for cleaner (after --), count={len(cleaned)}:")
    import shlex
    print("  " + " ".join(shlex.quote(a) for a in cleaned[:50]))
    if len(cleaned) > 50:
        print(f"  ... ({len(cleaned) - 50} more)")

    if template_clang and Path(template_clang).exists():
        print("\n=== clang -H on the header ===")
        tree = run_clang_minus_H(template_clang, abs_header, compile_args)
        n_kids = sum(len(v) for v in tree.edges.values())
        print(f"  parsed {n_kids} parent->child edges, visited {len(tree.visited)} headers")
        for h in tree.visited[:10]:
            print(f"    {h}")
    else:
        print("\n  (skipping clang -H: template clang missing)")

    print("\n=== clang-include-cleaner on the header (as main file) ===")
    result = run_include_cleaner(cleaner_bin, abs_header, compile_args)
    print(f"  exit code: {result.exit_code}")
    print(f"  unused: {len(result.removed)}; insert: {len(result.inserted)}")
    if result.removed:
        print("  unused suggestions ('- <spelled>'):")
        for s, _ in result.removed[:20]:
            print(f"    - {s}")
    if result.inserted:
        print("  insert suggestions ('+ <spelled>'):")
        for s, _ in result.inserted[:20]:
            print(f"    + {s}")
    print("  stderr (last 60 lines):")
    for line in result.stderr.splitlines()[-60:]:
        print(f"    {line}")
    return 0


def _doctor_stats() -> int:
    """Walk the per-TU/per-header cache and report aggregate stats.

    The purpose: confirm whether clang-include-cleaner is generating the
    `+ ...` insert suggestions that ``move-to-cpp`` verdicts depend on.
    If the TU side is uniformly empty, the classifier *can't* produce
    move-to-cpp regardless of how good our logic is — that points at
    cleaner output, not aggregation.
    """
    import statistics
    from collections import Counter

    cache_dir = PATHS.per_tu_dir
    if not cache_dir.exists():
        die(f"cache dir {cache_dir} does not exist; run `analyze` first")

    tu_stats = {
        "files": 0,
        "with_unused": 0,
        "with_inserts": 0,
        "with_error": 0,
        "total_unused": 0,
        "total_inserts": 0,
        "unused_counts": [],
        "insert_counts": [],
    }
    hdr_stats = {
        "files": 0,
        "with_unused": 0,
        "with_inserts": 0,
        "with_error": 0,
        "total_unused": 0,
        "total_inserts": 0,
        "unused_counts": [],
        "insert_counts": [],
    }

    top_insert_tus: List[Tuple[int, str, List[str]]] = []
    top_insert_headers: List[Tuple[int, str, List[str]]] = []

    all_inserts: Counter = Counter()

    for path in cache_dir.glob("**/*.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        bucket = tu_stats if data.get("kind") == "tu" else hdr_stats
        bucket["files"] += 1
        if data.get("error"):
            bucket["with_error"] += 1
        unused = data.get("unused_includes") or []
        inserts = data.get("suggested_inserts") or []
        bucket["total_unused"] += len(unused)
        bucket["total_inserts"] += len(inserts)
        bucket["unused_counts"].append(len(unused))
        bucket["insert_counts"].append(len(inserts))
        if unused:
            bucket["with_unused"] += 1
        if inserts:
            bucket["with_inserts"] += 1
            tup = (len(inserts), data.get("file") or str(path), list(inserts))
            if data.get("kind") == "tu":
                top_insert_tus.append(tup)
            else:
                top_insert_headers.append(tup)
            for s in inserts:
                all_inserts[s] += 1

    def _fmt_bucket(name: str, b: dict) -> None:
        n = b["files"]
        if n == 0:
            print(f"  {name}: no entries")
            return
        med_unused = statistics.median(b["unused_counts"]) if b["unused_counts"] else 0
        med_inserts = statistics.median(b["insert_counts"]) if b["insert_counts"] else 0
        print(f"  {name}: {n} cache entries")
        print(f"    with error:           {b['with_error']:>6} ({100*b['with_error']/n:5.1f}%)")
        print(f"    with unused_includes: {b['with_unused']:>6} ({100*b['with_unused']/n:5.1f}%)")
        print(f"    with suggested_ins.:  {b['with_inserts']:>6} ({100*b['with_inserts']/n:5.1f}%)")
        print(f"    total unused lines:   {b['total_unused']:>6} (median per file: {med_unused})")
        print(f"    total insert lines:   {b['total_inserts']:>6} (median per file: {med_inserts})")

    print("Cache survey:")
    _fmt_bucket("TUs    ", tu_stats)
    _fmt_bucket("Headers", hdr_stats)

    if tu_stats["with_inserts"] == 0 and tu_stats["files"] > 0:
        print()
        print("DIAGNOSIS: 0 TUs have any suggested_inserts. clang-include-cleaner")
        print("is not emitting `+` suggestions for any TU. As a result the")
        print("aggregator cannot identify move-to-cpp candidates.")
        print()
        print("Possible causes:")
        print("  - cleaner's policy considers transitive-via-header acceptable")
        print("    for the symbols you use (umbrella header pattern)")
        print("  - cleaner needs a --mapping-file to map symbols to their")
        print("    canonical providers")
        print("  - the bundled mapping file at")
        print("    build/config/tests/iwyu/mapping_file.imp may help")
        print("Next step: run `sanitize_includes doctor --file <some_tu.cpp>`")
        print("and check the 'insert suggestions' section. If empty, try")
        print("running clang-include-cleaner manually with `--print=changes`")
        print("on that TU to see whether ANY `+` lines appear.")
        return 1

    if hdr_stats["with_inserts"] == 0 and hdr_stats["files"] > 0:
        print()
        print("Note: 0 headers have suggested_inserts. That means cleaner did")
        print("not suggest any direct-include additions for headers. The")
        print("`keep-deferred` verdict needs this signal, so its absence")
        print("explains 0 keep-deferred verdicts.")

    if top_insert_tus:
        print()
        print(f"Top 10 TUs by insert count (out of {tu_stats['with_inserts']}):")
        for n, f, inserts in sorted(top_insert_tus, reverse=True)[:10]:
            print(f"  {n:>4}  {f}")
            for s in inserts[:5]:
                print(f"         + {s}")
            if len(inserts) > 5:
                print(f"         ... ({len(inserts) - 5} more)")

    if all_inserts:
        print()
        print("Top 20 most-suggested inserts across all files:")
        for spelled, count in all_inserts.most_common(20):
            print(f"  {count:>5}  + {spelled}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
