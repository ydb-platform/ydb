"""``unity`` subcommand: plan and apply JOIN_SRCS unity builds.

Every TU in one ya.make library pays for nearly the same header closure.
Compiling several of them as one TU pays that closure once, which is the
only lever that reduces template-instantiation work without editing any
templates — the cost is spread over tens of thousands of standard-library
specializations, so there is no small set of ``extern template``
declarations that would do it.

Three modes:

``--candidates``
    Rank libraries by how much a unity build would save, from the
    frontend times in ``reports/timing/per_tu.csv``. The shared closure
    is estimated as the 20th percentile of the library's frontend times;
    ``min`` is reported next to it as the conservative bound, because one
    trivial TU in a library drives ``min`` to zero.

``--trivial``
    Census of TUs whose whole body is an ``#include`` of their own
    header. They compile a full closure and emit almost nothing, so they
    are pure tax and the cheapest thing to fold into a bucket.

``--library DIR``
    Emit (or apply) a ya.make rewrite that groups the library's .cpp
    sources into ``JOIN_SRCS`` buckets, following the ``all_<group>.cpp``
    naming util already uses. Sources are bucketed by filename prefix so
    a bucket stays semantically coherent and an edit rebuilds only
    related files.
"""

from __future__ import annotations

import argparse
import logging
import re
import statistics
import sys
from collections import defaultdict
from fnmatch import fnmatch
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from ..common import PATHS, die, repo_relative, setup_logging
from .cost import load_timing, quantile


log = logging.getLogger("analysis.unity")

INCLUDE_LINE_RE = re.compile(r'^#\s*include\s*[<"][^">]+[">]')
EMPTY_NS_RE = re.compile(r'^namespace\s[\w:\s]*\{\s*\}\s*;?$')
SRCS_BLOCK_RE = re.compile(r'(?m)^(?P<indent>[ \t]*)SRCS\(\s*$')

# Sources ya must keep in SRCS: headers (tracked for codegen macros) and
# anything that is not a C++ TU. JOIN_SRCS is for C++ sources only.
JOINABLE_SUFFIXES = (".cpp", ".cc", ".cxx")


def is_trivial_body(body: str) -> bool:
    """True if a file contributes nothing beyond including headers.

    Checked line by line on purpose: a single regex over the whole body
    backtracks pathologically on the comment/include alternation.
    """
    includes = 0
    for raw in body.splitlines():
        line = raw.split("//", 1)[0].strip()
        if not line:
            continue
        if INCLUDE_LINE_RE.match(line):
            includes += 1
            continue
        if EMPTY_NS_RE.match(line):
            continue
        return False
    return includes > 0


def library_of(tu: str, repo_root: Path) -> str:
    """Nearest ancestor directory of ``tu`` that has a ya.make."""
    current = (repo_root / tu).parent
    while len(current.parts) > len(repo_root.parts):
        if (current / "ya.make").exists():
            return str(current.relative_to(repo_root))
        current = current.parent
    return str(Path(tu).parent)


def group_libraries(
    timing: Sequence[Tuple[str, float, float, float, bool]],
    repo_root: Path,
) -> Dict[str, List[Tuple[str, float, float, float]]]:
    libs: Dict[str, List[Tuple[str, float, float, float]]] = defaultdict(list)
    for tu, total, frontend, backend, generated in timing:
        if generated:
            continue
        libs[library_of(tu, repo_root)].append((tu, total, frontend, backend))
    return libs


def report_candidates(libs, total_s: float, top: int) -> None:
    low_sum = high_sum = 0.0
    rows = []
    for lib, rs in libs.items():
        if len(rs) < 2:
            continue
        fes = [r[2] for r in rs]
        lib_total = sum(r[1] for r in rs)
        cap = lib_total * 0.9
        low = min(min(fes) * (len(rs) - 1), cap)
        high = min(quantile(fes, 0.20) * (len(rs) - 1), cap)
        low_sum += low
        high_sum += high
        rows.append((lib, len(rs), lib_total, min(fes),
                     quantile(fes, 0.20), low, high))

    print(f"--- unity-build candidates ({len(rows)} libraries with 2+ TUs) ---")
    print(f"estimated saving: {low_sum:.0f}-{high_sum:.0f} s of {total_s:.0f} s "
          f"({100*low_sum/max(1e-9, total_s):.0f}-"
          f"{100*high_sum/max(1e-9, total_s):.0f}% of non-generated compile time)")
    print(f"{'library':46s} {'TUs':>4s} {'total_s':>8s} {'min':>5s} {'p20':>5s} "
          f"{'save_lo':>8s} {'save_hi':>8s}")
    for lib, n, lib_total, mn, p20, low, high in sorted(
            rows, key=lambda r: -r[6])[:top]:
        print(f"{lib[-46:]:46s} {n:>4d} {lib_total:>8.0f} {mn:>5.1f} "
              f"{p20:>5.1f} {low:>8.0f} {high:>8.0f}")
    print()


def report_trivial(timing, repo_root: Path, total_s: float, top: int) -> None:
    trivial = []
    for tu, total, frontend, backend, generated in timing:
        if generated:
            continue
        path = repo_root / tu
        try:
            body = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        if len(body) < 8192 and is_trivial_body(body):
            trivial.append((tu, total, frontend))
    spent = sum(r[1] for r in trivial)
    print("--- trivial TUs (body is only #include of their own header) ---")
    print(f"count: {len(trivial)}   compile time: {spent:.0f} s "
          f"({100*spent/max(1e-9, total_s):.1f}% of non-generated compile time)")
    if trivial:
        fes = [r[2] for r in trivial]
        print(f"frontend share: "
              f"{100*sum(fes)/max(1e-9, spent):.0f}%   "
              f"median {statistics.median(fes):.1f} s   max {max(fes):.1f} s")
        print(f"{'TU':72s} {'total_s':>8s}")
        for tu, total, _ in sorted(trivial, key=lambda r: -r[1])[:top]:
            print(f"{tu[-72:]:72s} {total:>8.1f}")
    print()


def source_tokens(source: str) -> List[str]:
    return [t for t in re.split(r"_+", Path(source).stem) if t]


DEFINE_RE = re.compile(
    r'^[ \t]*#[ \t]*define[ \t]+(?P<name>\w+)(?P<args>\([^)]*\))?[ \t]*'
    r'(?P<body>.*)$')
QUOTED_INCLUDE_RE = re.compile(r'^[ \t]*#[ \t]*include[ \t]*"(?P<target>[^"]+)"')


TEMPLATE_HEAD_RE = re.compile(r'^[ \t]*template[ \t]*<(?P<params>.*)>[ \t]*$')
TYPE_HEAD_RE = re.compile(r'^[ \t]*(?:class|struct)[ \t]+(?P<name>\w+)\b')


def _template_arity(params: str) -> int:
    depth = 0
    n = 1
    for ch in params:
        if ch in "<([":
            depth += 1
        elif ch in ">)]":
            depth -= 1
        elif ch == "," and depth == 0:
            n += 1
    return n if params.strip() else 0


def collect_facts(path: Path, lib_dir: Path,
                  cache: Dict[Path, Tuple[Dict[str, str], Dict[str, str]]],
                  depth: int = 0) -> Tuple[Dict[str, str], Dict[str, str]]:
    """``(macros, type shapes)`` visible in ``path``.

    Only quoted includes that resolve inside ``lib_dir`` are followed. The
    collisions that break a unity build in practice come from a library's
    own headers: generic macro names (``LOG_D`` and friends) with
    per-subsystem bodies, and — more insidiously — two headers declaring
    unrelated class templates under one name, which is legal only as long
    as no translation unit sees both. Chasing angle-bracket includes into
    contrib would cost far more and add nothing.

    A type's "shape" is just its template arity, which is enough to tell
    two same-named declarations apart without parsing C++.
    """
    if depth == 0:
        # Containment checks below compare against lib_dir, so both sides
        # have to be absolute.
        path = path.resolve()
        lib_dir = lib_dir.resolve()
    if path in cache:
        return cache[path]
    cache[path] = ({}, {})                # break include cycles
    if depth > 8:
        return {}, {}
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return {}, {}

    macros: Dict[str, str] = {}
    types: Dict[str, str] = {}
    pending_arity: Optional[int] = None
    for line in text.splitlines():
        m = DEFINE_RE.match(line)
        if m:
            body = m.group("body").split("//", 1)[0].strip().rstrip("\\").strip()
            macros.setdefault(m.group("name"),
                              f"{m.group('args') or ''}{body}")
            continue

        head = TEMPLATE_HEAD_RE.match(line)
        if head:
            pending_arity = _template_arity(head.group("params"))
            continue
        if pending_arity is not None:
            decl = TYPE_HEAD_RE.match(line)
            if decl and line.rstrip().endswith(("{", ":")):
                types.setdefault(decl.group("name"), f"template/{pending_arity}")
            pending_arity = None
            continue

        inc = QUOTED_INCLUDE_RE.match(line)
        if not inc:
            continue
        target = inc.group("target")
        for candidate in (path.parent / target, lib_dir / target):
            try:
                resolved = candidate.resolve()
            except OSError:
                continue
            if not resolved.is_file():
                continue
            if lib_dir not in resolved.parents and resolved.parent != lib_dir:
                continue
            inner_macros, inner_types = collect_facts(resolved, lib_dir, cache,
                                                      depth + 1)
            for name, body in inner_macros.items():
                macros.setdefault(name, body)
            for name, shape in inner_types.items():
                types.setdefault(name, shape)
            break

    cache[path] = (macros, types)
    return macros, types


TYPE_DECL_RE = re.compile(
    r'^\s*(?:class|struct|union|enum(?:\s+class|\s+struct)?)\s+'
    r'([A-Za-z_]\w*)\s*(?:final\s*)?(?::[^;{]*)?\{')
USING_ALIAS_RE = re.compile(r'^\s*using\s+([A-Za-z_]\w*)\s*=')
FUNC_DEF_RE = re.compile(
    r'^\s*(?:static\s+|inline\s+|constexpr\s+|const\s+)*'
    r'[A-Za-z_][\w:<>,&*\s]*?\b([A-Za-z_]\w*)\s*\([^;]*$')
STATIC_FUNC_RE = re.compile(
    r'^\s*(?:static|inline)\s+[\w:<>,&*\s]*?\b[A-Za-z_]\w*\s*\([^;]*$')
NOT_A_NAME = frozenset((
    "if", "for", "while", "switch", "return", "catch", "sizeof", "throw",
    "case", "else", "do", "new", "delete", "and", "or", "not", "template",
    "namespace", "using", "typedef", "static_assert", "decltype", "noexcept",
))


def collect_local_symbols(path: Path) -> set:
    """Names a source defines that are private to its translation unit.

    Anonymous-namespace types and functions, and ``using`` aliases, are
    file-local by intent, so sibling sources routinely reuse the same name
    — ``TPropose``, ``IsValidLimit`` — for unrelated things. That is legal
    across separate TUs and a redefinition error inside one, which makes it
    the main thing to check before putting two sources in one blob.

    A brace-depth scan rather than a parse: it only needs to be right about
    whether a declaration sits inside ``namespace {``, and being slightly
    over-eager only costs an extra bucket.
    """
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return set()

    names = set()
    depth = 0
    anon_depths: List[int] = []
    pending_template = False
    for raw in text.splitlines():
        line = raw.split("//", 1)[0]
        stripped = line.strip()

        in_anon = bool(anon_depths)
        if in_anon or depth <= 3:
            m = USING_ALIAS_RE.match(stripped)
            if m:
                names.add(m.group(1))

        if TEMPLATE_HEAD_RE.match(line):
            pending_template = True
        elif stripped:
            # A template or a `static`/`inline` free function defined in a
            # .cpp has no external linkage to clash over, so sibling
            # sources reuse such names freely — and then cannot share a TU.
            claim = (in_anon and depth == anon_depths[-1] + 1) or \
                    (pending_template and depth <= 3) or \
                    (in_anon and TYPE_DECL_RE.match(stripped))
            if not claim and depth <= 3:
                claim = bool(STATIC_FUNC_RE.match(stripped))
            if claim:
                m = TYPE_DECL_RE.match(stripped)
                if m:
                    names.add(m.group(1))
                else:
                    m = FUNC_DEF_RE.match(stripped)
                    if m and m.group(1) not in NOT_A_NAME:
                        names.add(m.group(1))
            pending_template = False

        if re.search(r'\bnamespace\s*\{', stripped):
            anon_depths.append(depth)
        depth += line.count("{") - line.count("}")
        while anon_depths and depth <= anon_depths[-1]:
            anon_depths.pop()
    return names


def split_on_conflicts(
    bucket: List[str],
    macros: Dict[str, Dict[str, str]],
    symbols: Dict[str, set],
    types: Dict[str, Dict[str, str]],
) -> List[List[str]]:
    """Partition ``bucket`` so no two members collide in one blob.

    A member joins the first part that agrees with it on every macro body
    and type shape it can see, and shares no file-local symbol name with
    it.
    """
    parts: List[Tuple[List[str], Dict[str, str], set, Dict[str, str]]] = []
    for src in bucket:
        own_macros = macros.get(src, {})
        own_symbols = symbols.get(src, set())
        own_types = types.get(src, {})
        for members, seen_macros, seen_symbols, seen_types in parts:
            if own_symbols & seen_symbols:
                continue
            if any(seen_macros.get(n, b) != b for n, b in own_macros.items()):
                continue
            if any(seen_types.get(n, s) != s for n, s in own_types.items()):
                continue
            members.append(src)
            seen_macros.update(own_macros)
            seen_symbols |= own_symbols
            seen_types.update(own_types)
            break
        else:
            parts.append(([src], dict(own_macros), set(own_symbols),
                          dict(own_types)))
    return [members for members, _, _, _ in parts]


def dominant_first_token(token_lists: Sequence[List[str]]) -> Optional[str]:
    """The leading token most sources share, if it carries no information.

    In a library like ``ydb/core/tx/schemeshard`` nearly every file starts
    with ``schemeshard``, so keeping it would make every bucket name begin
    the same way and waste a level of the grouping below.
    """
    counts: Dict[str, int] = defaultdict(int)
    for tokens in token_lists:
        if tokens:
            counts[tokens[0]] += 1
    if not counts:
        return None
    token, n = max(counts.items(), key=lambda kv: kv[1])
    return token if n * 2 > len(token_lists) else None


def plan_buckets(sources: List[str], max_bucket: int,
                 lib_dir: Optional[Path] = None,
                 max_depth: int = 4,
                 ) -> Tuple[List[Tuple[str, List[str]]], List[str]]:
    """Group ``sources`` into named buckets of at most ``max_bucket`` files.

    Returns ``(buckets, leftovers)``. Leftovers are sources that ended up
    alone — nothing to share a closure with, or a macro conflict with
    every candidate — and must stay in SRCS.

    Grouping is hierarchical on filename tokens: one token first, and a
    group refines to a second or third token only when it is too large for
    a single blob. That keeps a bucket semantically coherent — the
    ``export`` handlers together, the ``operation_create`` subops together
    — so editing one file rebuilds its own family rather than an arbitrary
    neighbourhood.

    Bucket names must be unique because each one becomes a generated
    filename, so leftovers are named after the group they came from and a
    final pass disambiguates any collision that survives.
    """
    sources = sorted(sources)
    tokens = {src: source_tokens(src) for src in sources}
    noise = dominant_first_token(list(tokens.values()))
    if noise:
        for src, toks in tokens.items():
            if toks and toks[0] == noise and len(toks) > 1:
                tokens[src] = toks[1:]

    def refine(files: List[str], depth: int,
               parent: str) -> List[Tuple[str, List[str]]]:
        groups: Dict[str, List[str]] = defaultdict(list)
        for src in files:
            groups["_".join(tokens[src][:depth]) or parent or "misc"].append(src)

        out: List[Tuple[str, List[str]]] = []
        singles: List[str] = []
        for key in sorted(groups):
            members = sorted(groups[key])
            if len(members) == 1 and depth > 1:
                singles.append(members[0])
            elif len(members) <= max_bucket:
                out.append((key, members))
            elif depth < max_depth and len(set(
                    "_".join(tokens[m][:depth + 1]) for m in members)) > 1:
                out.extend(refine(members, depth + 1, key))
            else:
                for i in range(0, len(members), max_bucket):
                    out.append((f"{key}_{i // max_bucket + 1}",
                                members[i:i + max_bucket]))
        # A key matched by a single file would become a one-file blob,
        # which saves nothing; pool those back together under the parent.
        stem = parent or "misc"
        for i in range(0, len(singles), max_bucket):
            out.append((f"{stem}_rest_{i // max_bucket + 1}",
                        singles[i:i + max_bucket]))
        return out

    buckets = refine(sources, 1, "")

    orphans = [files[0] for _, files in buckets if len(files) == 1]
    kept = [(name, files) for name, files in buckets if len(files) > 1]
    for i in range(0, len(orphans), max_bucket):
        kept.append((f"misc_{i // max_bucket + 1}",
                     orphans[i:i + max_bucket]))

    # Applied last so the pooled leftovers above are checked too.
    if lib_dir is not None:
        cache: Dict[Path, Tuple[Dict[str, str], Dict[str, str]]] = {}
        facts = {src: collect_facts(lib_dir / src, lib_dir, cache)
                 for src in sources}
        macros = {src: f[0] for src, f in facts.items()}
        types = {src: f[1] for src, f in facts.items()}
        symbols = {src: collect_local_symbols(lib_dir / src)
                   for src in sources}
        split: List[Tuple[str, List[str]]] = []
        for name, files in kept:
            parts = split_on_conflicts(files, macros, symbols, types)
            if len(parts) == 1:
                split.append((name, files))
                continue
            log.info("%s: split into %d on macro, type or local-name clashes",
                     name, len(parts))
            for i, part in enumerate(parts, 1):
                split.append((f"{name}_m{i}", part))
        kept = split

    # A one-file blob saves nothing, and any source not in a bucket has to
    # be handed back so the caller can keep it in SRCS — dropping it would
    # silently remove it from the build.
    leftovers = sorted(files[0] for _, files in kept if len(files) == 1)
    kept = [(name, files) for name, files in kept if len(files) > 1]

    seen: Dict[str, int] = defaultdict(int)
    unique: List[Tuple[str, List[str]]] = []
    for name, files in kept:
        seen[name] += 1
        unique.append((name if seen[name] == 1 else f"{name}_{seen[name]}",
                       files))
    return unique, leftovers


def find_srcs_block(text: str) -> Optional[Tuple[int, int, List[str]]]:
    """Locate the first ``SRCS(...)`` block: ``(start, end, entries)``.

    ``start``/``end`` bracket the whole macro call including its closing
    paren. Returns None when the block contains anything this rewriter
    should not touch (nested macros, conditionals, variables).
    """
    m = SRCS_BLOCK_RE.search(text)
    if not m:
        return None
    close = text.find("\n)", m.end())
    if close < 0:
        return None
    body = text[m.end():close]
    entries: List[str] = []
    for raw in body.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if any(ch in line for ch in "()$"):
            return None
        entries.append(line)
    # Span the newline that ends the ")" line too, so the replacement text
    # supplies its own trailing separator instead of doubling it.
    end = close + 2
    if text[end:end + 1] == "\n":
        end += 1
    return m.start(), end, entries


def render_ya_make(text: str, max_bucket: int,
                   exclude: Sequence[str] = (),
                   lib_dir: Optional[Path] = None,
                   ) -> Tuple[Optional[str], str]:
    """Rewrite the first SRCS block into JOIN_SRCS buckets.

    ``exclude`` holds fnmatch patterns for sources that must stay in
    SRCS — the escape hatch for families of files that share file-local
    helper names (a very common pattern for per-operation state classes)
    and therefore cannot share a TU until those names are made unique.

    Returns ``(new_text, message)``; ``new_text`` is None when the file
    was left alone, with the reason in ``message``.
    """
    found = find_srcs_block(text)
    if not found:
        return None, ("could not parse a plain SRCS(...) block "
                      "(nested macros, conditionals or variables present)")
    start, end, entries = found

    joinable: List[str] = []
    keep: List[str] = []
    for entry in entries:
        # GLOBAL sources need JOIN_SRCS_GLOBAL and different link
        # semantics; keep them out of this rewrite.
        if entry.startswith("GLOBAL "):
            keep.append(entry)
        elif not entry.endswith(JOINABLE_SUFFIXES):
            keep.append(entry)
        elif any(fnmatch(entry, pattern) for pattern in exclude):
            keep.append(entry)
        else:
            joinable.append(entry)

    if len(joinable) < 2:
        return None, f"only {len(joinable)} joinable source(s); nothing to do"

    if len(joinable) < 2:
        return None, f"only {len(joinable)} joinable source(s) after excludes"

    buckets, leftovers = plan_buckets(joinable, max_bucket, lib_dir)
    if not buckets:
        return None, "bucketing produced no grouping"
    keep.extend(leftovers)

    joined = sum(len(files) for _, files in buckets)
    if joined + len(keep) != len(entries):
        return None, (f"internal error: {joined} joined + {len(keep)} kept "
                      f"!= {len(entries)} original entries")

    chunks: List[str] = []
    if keep:
        chunks.append("SRCS(\n" + "".join(f"    {e}\n" for e in keep) + ")\n")
    for name, files in buckets:
        chunks.append(f"JOIN_SRCS(\n    all_{name}.cpp\n"
                      + "".join(f"    {f}\n" for f in files) + ")\n")

    new_text = text[:start] + "\n".join(chunks) + text[end:]
    msg = (f"{joined} of {len(joinable)} sources -> {len(buckets)} buckets "
           f"(max {max_bucket}/bucket); {len(keep)} entries kept in SRCS")
    return new_text, msg


def run_library(lib: str, max_bucket: int, apply: bool,
                exclude: Sequence[str] = ()) -> int:
    ya_make = PATHS.repo_root / lib / "ya.make"
    if not ya_make.exists():
        die(f"no ya.make at {ya_make}")
    text = ya_make.read_text(encoding="utf-8")
    new_text, msg = render_ya_make(text, max_bucket, exclude, ya_make.parent)
    if new_text is None:
        log.warning("%s: %s", lib, msg)
        return 1
    log.info("%s: %s", lib, msg)
    if apply:
        ya_make.write_text(new_text, encoding="utf-8")
        log.info("rewrote %s", repo_relative(ya_make))
    else:
        print(new_text)
        log.info("dry run; pass --apply to write %s", repo_relative(ya_make))
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sanitize_includes unity",
        description="Plan and apply JOIN_SRCS unity builds.")
    parser.add_argument(
        "--library",
        help="library directory to rewrite, e.g. ydb/core/tx/schemeshard.")
    parser.add_argument("--max-bucket", type=int, default=8,
                        help="most sources per JOIN_SRCS blob (default 8).")
    parser.add_argument(
        "--exclude", action="append", default=[], metavar="GLOB",
        help="keep matching sources in SRCS; repeatable. Use for families "
             "that share file-local helper names, e.g. "
             "'*operation_*.cpp'.")
    parser.add_argument("--apply", action="store_true",
                        help="write the rewritten ya.make instead of printing it.")
    parser.add_argument("--candidates", action="store_true",
                        help="rank libraries by estimated saving.")
    parser.add_argument("--trivial", action="store_true",
                        help="census of one-include TUs.")
    parser.add_argument("--top", type=int, default=22)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    setup_logging(args.verbose)

    if args.library:
        return run_library(args.library, args.max_bucket, args.apply,
                           args.exclude)

    timing_csv = PATHS.reports_dir / "timing" / "per_tu.csv"
    if not timing_csv.exists():
        die(f"no timing report at {timing_csv}; run 'timetrace' then 'timing', "
            "or pass --library to rewrite a ya.make without measurements")
    timing = load_timing(timing_csv)
    total_s = sum(r[1] for r in timing if not r[4])

    if args.trivial:
        report_trivial(timing, PATHS.repo_root, total_s, args.top)
    if args.candidates or not args.trivial:
        report_candidates(group_libraries(timing, PATHS.repo_root),
                          total_s, args.top)
    return 0


if __name__ == "__main__":
    sys.exit(main())
