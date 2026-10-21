"""Per-TU and per-header analysis runner.

Iterates ``compile_commands.json``, runs clang-include-cleaner on every
``.cpp`` TU, runs clang ``-H`` to capture the include tree, and probes
each header visited from those TUs to obtain per-header verdicts.

Outputs one JSON file per analyzed input into ``.cache/per_tu/``. The
aggregator reads those.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import multiprocessing
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

from ..common import (
    PATHS,
    REPO_ROOT,
    die,
    ensure_dir,
    is_generated_path,
    is_skipped_path,
    is_under_subdir,
    repo_relative,
    setup_logging,
)
from .cache import cache_path_for, hash_arguments, hash_file, load, store
from .clang_runner import (
    CleanerResult,
    IncludeTree,
    find_clang,
    find_clang_include_cleaner,
    reconstruct_compile_args,
    run_clang_minus_H,
    run_include_cleaner,
)
from .schema import FileAnalysis, IncludeLine, SCHEMA_VERSION
from .source_includes import scan_includes


log = logging.getLogger("per_tu")


def load_compdb(path: Path) -> List[dict]:
    if not path.exists():
        die(f"compile_commands.json not found at {path}; run 'compdb' first")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        die(f"invalid compile_commands.json: {e}")
    return data


def materialize_arguments(entry: dict) -> List[str]:
    if "arguments" in entry:
        return list(entry["arguments"])
    cmd = entry.get("command", "")
    if not cmd:
        return []
    import shlex
    return shlex.split(cmd)


def populate_includes_from_scan(file_path: Path, an: FileAnalysis) -> None:
    scanned = scan_includes(file_path)
    an.includes = [
        IncludeLine(
            line=s.line,
            spelled=s.spelled,
            angled=s.angled,
            has_iwyu_keep=s.iwyu_keep,
            has_iwyu_export=s.iwyu_export,
        )
        for s in scanned
        if not s.in_conditional
    ]


def apply_cleaner_result(an: FileAnalysis, res: CleanerResult) -> None:
    an.unused_includes = sorted({spelled for spelled, _ in res.removed})
    an.suggested_inserts = sorted({spelled for spelled, _ in res.inserted})


_GENERIC_TRAILING_RE = (
    "Error while processing ",
    "Skipping file ",
    "error generated.",
    " errors generated.",
    "clang-include-cleaner: ",
)


def summarize_stderr(stderr: str) -> Tuple[Optional[str], Optional[str]]:
    """Return (one-line summary, tail).

    The summary is the *first* meaningful diagnostic (file:line:col:
    severity: ...), falling back to the first non-blank line that is
    NOT one of clang-include-cleaner's generic trailing messages.
    The tail is the last ~30 lines of stderr verbatim.
    """
    if not stderr:
        return None, None

    lines = [s for s in stderr.splitlines() if s.strip()]
    if not lines:
        return None, None

    summary: Optional[str] = None
    for line in lines:
        s = line.strip()
        if any(s.startswith(p) or s.endswith(p) for p in _GENERIC_TRAILING_RE):
            continue
        # Pick "path:line:col: severity: ..." style diagnostics first.
        if (": error:" in s) or (": fatal error:" in s) or (": warning:" in s):
            summary = s
            break
    if summary is None:
        for line in lines:
            s = line.strip()
            if not any(s.startswith(p) or s.endswith(p) for p in _GENERIC_TRAILING_RE):
                summary = s
                break
    if summary is None:
        summary = lines[-1].strip()

    tail = "\n".join(lines[-30:])
    return summary, tail


# Markers that indicate the header could not be parsed in isolation because
# it is re-included through its own dependency chain (typically an umbrella
# ``defs.h`` that includes the header back), or it self-includes. In both
# cases the symbol gets defined twice when the header is the analysis main
# file. This is NOT a missing-include problem in the header — it compiles
# fine when a normal .cpp includes it once. We surface it as a distinct,
# non-actionable category so it is not mistaken for a header bug.
_REDEFINITION_MARKERS = (
    "error: redefinition of",
    "error: redeclared",
    "error: redefined",
    "cannot be redefined",
    "previous definition is here",
    "previous declaration is here",
)


def is_cycle_redefinition(stderr: str) -> bool:
    if not stderr:
        return False
    low = stderr
    return any(m in low for m in _REDEFINITION_MARKERS)


CYCLE_ERROR_PREFIX = "include-cycle (not a missing-include problem): "


def apply_include_tree(an: FileAnalysis, tree: IncludeTree) -> None:
    an.include_tree = {k: list(v) for k, v in tree.edges.items()}


def collect_visited_headers(tree: IncludeTree, repo_root: Path) -> Set[str]:
    out: Set[str] = set()
    for h in tree.visited:
        if not h:
            continue
        rel = repo_relative(h, repo_root)
        if rel.startswith("/"):
            continue
        if is_skipped_path(rel):
            continue
        if is_generated_path(rel):
            continue
        if not rel.endswith((".h", ".hh", ".hpp", ".hxx")):
            continue
        out.add(rel)
    return out


def analyze_tu(
    entry: dict,
    cleaner_bin: str,
    cache_dir: Path,
    mapping_file: Optional[Path],
    ignore_headers: Sequence[str],
    repo_root: Path,
    force: bool = False,
    retry_errored: bool = False,
) -> Tuple[FileAnalysis, Set[str], Dict[str, List[str]]]:
    """Analyze one ``.cpp`` TU; return its analysis + visited headers + template-flags map.

    The ``template-flags map`` maps each visited header to the compile
    arguments of this TU; the caller will use it to probe headers later.
    """
    file_rel = repo_relative(entry["file"], repo_root)
    abs_path = repo_root / file_rel

    raw_args = materialize_arguments(entry)
    clang_path = find_clang(raw_args)
    args = reconstruct_compile_args(raw_args, clang_path)

    flags_hash = hash_arguments(args)
    input_hash = hash_file(abs_path)

    cache_path = cache_path_for(cache_dir, file_rel, flags_hash, input_hash)
    cached = None if force else load(cache_path)
    # Errored entries are retried only when ``retry_errored`` is set.
    # The inline path sets this (build_root is alive, retry can heal).
    # The separate analyze path leaves it False: by then ya has pruned
    # the per-TU build_root, so retries can only fail anew, and the
    # original error is more useful than a fresh "file not found".
    if cached and cached.get("error") and retry_errored:
        cached = None
    if cached and cached.get("schema_version") == SCHEMA_VERSION:
        an = FileAnalysis.from_json(cached)
        visited = set(an.include_tree.keys()) | {h for kids in an.include_tree.values() for h in kids}
        visited_rel = {repo_relative(h, repo_root) for h in visited if h}
        visited_rel = {h for h in visited_rel if h.endswith((".h", ".hh", ".hpp", ".hxx"))
                       and not is_skipped_path(h) and not is_generated_path(h)}
        return an, visited_rel, {h: args for h in visited_rel}

    an = FileAnalysis(
        file=file_rel,
        kind="tu",
        compile_flags_hash=flags_hash,
        input_hash=input_hash,
    )
    populate_includes_from_scan(abs_path, an)

    if not clang_path:
        an.error = "could not determine clang path"
        store(cache_path, an.to_json())
        return an, set(), {}

    tree = run_clang_minus_H(clang_path, abs_path, args)
    apply_include_tree(an, tree)

    cleaner = run_include_cleaner(
        cleaner_bin,
        abs_path,
        args,
        mapping_file=mapping_file,
        ignore_headers=ignore_headers,
    )
    if cleaner.exit_code != 0 and not (cleaner.removed or cleaner.inserted):
        an.error, an.stderr_tail = summarize_stderr(cleaner.stderr)
        if not an.error:
            an.error = "cleaner failed"
    apply_cleaner_result(an, cleaner)

    visited_rel = collect_visited_headers(tree, repo_root)
    store(cache_path, an.to_json())
    return an, visited_rel, {h: args for h in visited_rel}


def analyze_header(
    header_rel: str,
    template_arguments: Sequence[str],
    cleaner_bin: str,
    probes_dir: Path,
    cache_dir: Path,
    mapping_file: Optional[Path],
    ignore_headers: Sequence[str],
    repo_root: Path,
    force: bool = False,
    retry_errored: bool = False,
) -> FileAnalysis:
    """Synthesize a probe TU for ``header_rel`` and analyze it."""
    abs_header = repo_root / header_rel

    # The header-probe cache is keyed by header content only, NOT by
    # compile-flags hash: any working set of flags is equivalent for
    # the probe (we are just asking "which of this header's #includes
    # are needed by the header itself?"). Keying by flags would create
    # spurious cache misses when a different TU happens to be the first
    # to probe a given header on a re-run — and any such re-probe would
    # fail because ya's per-TU build_root containing generated *.pb.h
    # files is ephemeral.
    # v2: switched from "probe TU wrapping the header" to passing the
    # header itself as the cleaner's main file. The analysis semantics
    # changed, so old cache entries (which described changes to a
    # wrapper TU, not to the header) are no longer applicable.
    flags_hash = "header-probe-v2-direct"
    input_hash = hash_file(abs_header)
    cache_path = cache_path_for(cache_dir, header_rel, flags_hash, input_hash)
    cached = None if force else load(cache_path)
    if cached and cached.get("error") and retry_errored:
        cached = None
    if cached and cached.get("schema_version") == SCHEMA_VERSION:
        return FileAnalysis.from_json(cached)

    an = FileAnalysis(
        file=header_rel,
        kind="header",
        compile_flags_hash=flags_hash,
        input_hash=input_hash,
    )
    populate_includes_from_scan(abs_header, an)

    template_source = ""
    for a in template_arguments:
        if a.lower().endswith((".cpp", ".cc", ".cxx", ".c", ".c++")):
            template_source = a
            break

    # Pass the header itself as clang-include-cleaner's source file
    # ("main file"). This way the tool reports +/- changes against the
    # header's own #include list, not a wrapping probe TU. We force
    # C++ parsing (``-x c++``) because .h files default to C unless the
    # extension is .hpp/.hh/.hxx. We avoid ``-x c++-header`` because that
    # triggers PCH-generation mode which clang-include-cleaner does not
    # always handle well.
    def _try_header(preamble: Optional[List[str]]) -> "object":
        extra: List[str] = ["-x", "c++"]
        for p in preamble or []:
            extra += ["-include", p]
        compile_args = list(template_arguments) + extra
        return run_include_cleaner(
            cleaner_bin,
            abs_header,
            compile_args,
            mapping_file=mapping_file,
            ignore_headers=ignore_headers,
        )

    cleaner = _try_header(None)
    apply_cleaner_result(an, cleaner)
    if cleaner.exit_code != 0 and not (cleaner.removed or cleaner.inserted):
        if is_cycle_redefinition(cleaner.stderr):
            # The header is re-included through its own dependency chain
            # (e.g. an umbrella defs.h that includes it back) or it
            # self-includes. As the analysis main file it gets defined
            # twice. This is NOT a missing-include problem and a preamble
            # would only make it worse, so don't retry — mark distinctly.
            summary, an.stderr_tail = summarize_stderr(cleaner.stderr)
            an.error = CYCLE_ERROR_PREFIX + (summary or "redefinition")
            store(cache_path, an.to_json())
            return an

        # Fallback: many headers in this codebase are not self-contained
        # — they rely on the including TU having pre-included a sibling
        # first (typical pattern: a templated class declared in foo_fwd.h
        # used by foo_utils.h without explicit include). Retry with the
        # TU's #include prefix forced via `-include`.
        preamble = _tu_preamble_for_header(template_source, header_rel, repo_root)
        if preamble:
            cleaner2 = _try_header(preamble)
            if cleaner2.exit_code == 0 or cleaner2.removed or cleaner2.inserted:
                apply_cleaner_result(an, cleaner2)
                an.notes.append(
                    f"used a {len(preamble)}-entry -include preamble from "
                    f"{repo_relative(template_source, repo_root)}; the "
                    "header is not self-contained."
                )
                an.error = None
                an.stderr_tail = None
            else:
                # If the preamble retry introduced a redefinition (a
                # preamble entry transitively includes this header),
                # that masks the real reason. Always report the original
                # no-preamble diagnostic, which is the genuine missing
                # include we want surfaced.
                an.error, an.stderr_tail = summarize_stderr(cleaner.stderr)
                if not an.error:
                    an.error = "header analysis failed"
        else:
            an.error, an.stderr_tail = summarize_stderr(cleaner.stderr)
            if not an.error:
                an.error = "header analysis failed"

    store(cache_path, an.to_json())
    return an


def _tu_preamble_for_header(
    tu_source: str,
    header_rel: str,
    repo_root: Path,
) -> Optional[List[str]]:
    """Build a preamble of TU's #include lines that precede ``header_rel``.

    For a TU T whose source we can read, scan its ``#include`` directives
    in source order. Return the spelled paths of every include that
    appears BEFORE either (a) a direct include of ``header_rel``, or (b)
    any include whose path basename matches ``header_rel`` (handles
    relative-spelled and absolute-spelled both).

    If we can't find ``header_rel`` in T's directly-spelled includes,
    return all of T's includes minus the very last (assumption: the
    target header is reachable transitively from late includes).
    """
    if not tu_source:
        return None
    tu_path = Path(tu_source)
    if not tu_path.is_absolute():
        tu_path = (repo_root / tu_source).resolve()
    if not tu_path.exists():
        return None
    try:
        scanned = scan_includes(tu_path)
    except Exception:
        return None
    if not scanned:
        return None

    # Try to find the include line that brings in our header.
    header_basename = header_rel.rsplit("/", 1)[-1]
    cutoff = None
    for i, inc in enumerate(scanned):
        if inc.spelled == header_rel or inc.spelled.endswith("/" + header_rel):
            cutoff = i
            break
        if inc.spelled == header_basename or inc.spelled.endswith("/" + header_basename):
            cutoff = i
            break
    if cutoff is None:
        # Use all but the last include; that one is presumed to be (or
        # to transitively pull in) our target.
        cutoff = max(0, len(scanned) - 1)

    preamble = []
    for inc in scanned[:cutoff]:
        if inc.in_conditional:
            continue
        if inc.spelled == header_rel or inc.spelled.endswith("/" + header_rel):
            continue
        preamble.append(inc.spelled)
    return preamble or None


def _tu_worker(
    entry: dict,
    cleaner_bin: str,
    cache_dir_str: str,
    mapping_file_str: Optional[str],
    ignore_headers: Tuple[str, ...],
    repo_root_str: str,
    force: bool,
) -> Tuple[str, dict, Dict[str, List[str]]]:
    cache_dir = Path(cache_dir_str)
    repo_root = Path(repo_root_str)
    mapping_file = Path(mapping_file_str) if mapping_file_str else None
    try:
        an, visited, args_map = analyze_tu(
            entry, cleaner_bin, cache_dir, mapping_file, ignore_headers,
            repo_root, force=force,
        )
        return an.file, an.to_json(), {h: args_map[h] for h in visited}
    except Exception as e:
        log.exception("TU analysis failed for %s", entry.get("file"))
        an = FileAnalysis(file=str(entry.get("file")), kind="tu", error=str(e))
        return an.file, an.to_json(), {}


def _header_worker(
    header_rel: str,
    template_args: List[str],
    cleaner_bin: str,
    probes_dir_str: str,
    cache_dir_str: str,
    mapping_file_str: Optional[str],
    ignore_headers: Tuple[str, ...],
    repo_root_str: str,
    force: bool,
) -> Tuple[str, dict]:
    cache_dir = Path(cache_dir_str)
    probes_dir = Path(probes_dir_str)
    repo_root = Path(repo_root_str)
    mapping_file = Path(mapping_file_str) if mapping_file_str else None
    try:
        an = analyze_header(
            header_rel, template_args, cleaner_bin, probes_dir,
            cache_dir, mapping_file, ignore_headers, repo_root, force=force,
        )
        return header_rel, an.to_json()
    except Exception as e:
        log.exception("Header analysis failed for %s", header_rel)
        an = FileAnalysis(file=header_rel, kind="header", error=str(e))
        return header_rel, an.to_json()


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sanitize_includes analyze",
        description="Run per-TU and per-header include analysis",
    )
    parser.add_argument("--subdir", default="ydb")
    parser.add_argument("--jobs", "-j", type=int, default=max(1, multiprocessing.cpu_count() - 1))
    parser.add_argument("--limit", type=int, default=0, help="cap on TUs (0 = no limit; useful for piloting)")
    parser.add_argument("--skip-headers", action="store_true", help="only analyze TUs, skip header probes")
    parser.add_argument("--force", action="store_true", help="ignore cache")
    parser.add_argument("--mapping-file", type=Path, help="IWYU-style .imp mapping file (optional)")
    parser.add_argument("--ignore-headers", default="",
                        help="comma-separated regexes passed to clang-include-cleaner")
    parser.add_argument("--show-errors", action="store_true",
                        help="print all per-file errors (default: first 5)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    setup_logging(args.verbose)

    cleaner_bin = find_clang_include_cleaner()
    if not cleaner_bin:
        die(
            "clang-include-cleaner not found; set YDB_CLANG_INCLUDE_CLEANER, "
            "put it on PATH, or build it via ./ya tool"
        )
    log.info("clang-include-cleaner: %s", cleaner_bin)

    compdb = load_compdb(PATHS.compdb_json)
    log.info("loaded %d compile commands", len(compdb))

    filtered: List[dict] = []
    for e in compdb:
        rel = repo_relative(e.get("file", ""))
        if not is_under_subdir(rel, args.subdir):
            continue
        if is_skipped_path(rel) or is_generated_path(rel):
            continue
        filtered.append(e)

    if args.limit and len(filtered) > args.limit:
        filtered = filtered[: args.limit]
    log.info("analyzing %d TUs", len(filtered))

    cache_dir = ensure_dir(PATHS.per_tu_dir)
    probes_dir = ensure_dir(PATHS.cache_dir / "probes")
    ignore_headers = tuple(x for x in args.ignore_headers.split(",") if x)

    visited_per_tu: Dict[str, Dict[str, List[str]]] = {}
    tu_results: Dict[str, dict] = {}
    t0 = time.time()
    with concurrent.futures.ProcessPoolExecutor(max_workers=args.jobs) as ex:
        futures = [
            ex.submit(
                _tu_worker,
                entry,
                cleaner_bin,
                str(cache_dir),
                str(args.mapping_file) if args.mapping_file else None,
                ignore_headers,
                str(REPO_ROOT),
                args.force,
            )
            for entry in filtered
        ]
        done = 0
        total = len(futures)
        for fut in concurrent.futures.as_completed(futures):
            file_rel, data, args_map = fut.result()
            visited_per_tu[file_rel] = args_map
            tu_results[file_rel] = data
            done += 1
            if done % 20 == 0 or done == total:
                rate = done / max(0.001, time.time() - t0)
                log.info("TU %d/%d (%.1f/s)", done, total, rate)

    _summarize_errors("TU", tu_results, args.show_errors)

    if args.skip_headers:
        log.info("--skip-headers given; not probing headers")
        return 0

    headers_to_probe: Dict[str, List[str]] = {}
    for tu_rel, m in visited_per_tu.items():
        for h, hargs in m.items():
            if h not in headers_to_probe:
                headers_to_probe[h] = hargs

    headers_to_probe = {
        h: a for h, a in headers_to_probe.items()
        if is_under_subdir(h, args.subdir)
    }
    log.info("probing %d unique headers", len(headers_to_probe))

    header_results: Dict[str, dict] = {}
    t0 = time.time()
    with concurrent.futures.ProcessPoolExecutor(max_workers=args.jobs) as ex:
        futures = [
            ex.submit(
                _header_worker,
                h,
                a,
                cleaner_bin,
                str(probes_dir),
                str(cache_dir),
                str(args.mapping_file) if args.mapping_file else None,
                ignore_headers,
                str(REPO_ROOT),
                args.force,
            )
            for h, a in headers_to_probe.items()
        ]
        done = 0
        total = len(futures)
        for fut in concurrent.futures.as_completed(futures):
            h, data = fut.result()
            header_results[h] = data
            done += 1
            if done % 20 == 0 or done == total:
                rate = done / max(0.001, time.time() - t0)
                log.info("header %d/%d (%.1f/s)", done, total, rate)

    _summarize_errors("header", header_results, args.show_errors)
    return 0


def _summarize_errors(kind: str, results: Dict[str, dict], show_all: bool) -> None:
    """Aggregate per-file ``error`` fields and report counts (with samples)."""
    total = len(results)
    errored: List[Tuple[str, str]] = []
    no_tree: List[str] = []
    for path, data in results.items():
        err = data.get("error")
        if err:
            errored.append((path, err))
            continue
        # For TUs only: an empty include_tree from clang -H is a strong
        # "something is wrong" signal — a real compile visits at least
        # one header. Header probes never populate include_tree (we
        # don't run clang -H on probes), so skipping that check there.
        if kind != "tu":
            continue
        tree = data.get("include_tree") or {}
        kids = [c for kids in tree.values() for c in kids]
        if not kids:
            no_tree.append(path)

    if errored:
        log.warning("%s: %d/%d files reported errors", kind, len(errored), total)
        sample = errored if show_all else errored[:5]
        for p, e in sample:
            log.warning("  %s: %s", p, e)
        if not show_all and len(errored) > 5:
            log.warning("  ... and %d more (re-run with --show-errors for the full list)",
                        len(errored) - 5)
        # Print full stderr_tail for the FIRST failure to give the user
        # something concrete to debug. The single-line `error` field is
        # often a generic suffix from clang-include-cleaner.
        first_path, _ = errored[0]
        first_data = results.get(first_path) or {}
        tail = first_data.get("stderr_tail")
        if tail:
            log.warning(
                "first failure full stderr (use `sanitize_includes doctor "
                "--%s %s` to reproduce):",
                "header" if kind == "header" else "file",
                first_path,
            )
            for line in tail.splitlines():
                log.warning("  | %s", line)
    if no_tree:
        log.warning(
            "%s: %d/%d files compiled cleanly but produced an empty include "
            "tree (clang -H spawned but emitted nothing). Re-run with -v to "
            "see spawn details, or inspect ydb/tools/include_sanitizer/.cache/"
            "per_tu/ JSON files.",
            kind, len(no_tree), total,
        )


if __name__ == "__main__":
    sys.exit(main())
