"""``fanout`` subcommand: rebuild impact of a header, especially protobuf.

The include-sanitizer's other reports answer "how expensive is this header
to *parse*". This one answers the other half of the compile-time problem:
when ``config.proto`` (or any header) changes, *how many translation units
must rebuild*, and *which include sites* are the amplifiers.

It does not need clang-include-cleaner or a compile_commands.json. A
textual scan of ``#include`` plus the ``.proto`` import graph is enough
to compute:

- per-header rebuild fanout (transitive .cpp count);
- the proto-import overlay (changing ``blobstorage.proto`` also
  regenerates every ``.pb.h`` whose ``.proto`` imports it);
- for each direct include of a target header, a cheap classification:
  ``unused`` / ``fwd`` (pointer, reference, smart-pointer only) /
  ``keep`` (needs a complete type).

Forward-declaration suggestions are the piece the original tool
explicitly deferred; this is that piece, scoped to the files that
actually drive rebuild storms.
"""

from __future__ import annotations

import argparse
import csv
import logging
import posixpath
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from ..analyze.source_includes import scan_includes
from ..common import PATHS, ensure_dir, setup_logging


log = logging.getLogger("analysis.fanout")

SOURCE_SUFFIXES = {".h", ".hh", ".hpp", ".hxx", ".cpp", ".cc", ".cxx", ".c",
                   ".inl", ".ipp", ".inc"}
HEADER_SUFFIXES = {".h", ".hh", ".hpp", ".hxx", ".inl", ".ipp", ".inc"}
TU_SUFFIXES = {".cpp", ".cc", ".cxx", ".c"}
SKIP_DIR_NAMES = {
    "contrib", "vendor", ".git", "__pycache__", "canondata", "node_modules",
    ".cache", "third_party",
}
REPO_PREFIXES = ("ydb/", "yql/", "yt/", "library/", "util/", "tools/")

INCLUDE_ROOT_RE = re.compile(
    r"^(ydb|yql|yt|library|util|tools|contrib|kikimr)/"
)
PROTO_IMPORT_RE = re.compile(
    r'^\s*import\s+(?:public\s+|weak\s+)?"([^"]+)"\s*;', re.M
)
PROTO_PACKAGE_RE = re.compile(r"^\s*package\s+([\w.]+)\s*;", re.M)
PROTO_TYPE_RE = re.compile(r"^\s*(message|enum)\s+(\w+)\b", re.M)

# Smart-pointer templates that only require an incomplete type.
FWD_OK_TEMPLATES = (
    "unique_ptr", "shared_ptr", "weak_ptr",
    "THolder", "TIntrusivePtr", "TIntrusiveConstPtr",
    "TPtr", "TAutoPtr", "TSimpleIntrusivePtr",
)


@dataclass
class ProtoInfo:
    path: str
    package: str = ""
    messages: List[str] = field(default_factory=list)
    enums: List[str] = field(default_factory=list)
    imports: List[str] = field(default_factory=list)

    @property
    def pb_h(self) -> str:
        if self.path.endswith(".proto"):
            return self.path[:-len(".proto")] + ".pb.h"
        return self.path + ".pb.h"


@dataclass
class IncludeGraph:
    """Directed graph: including file -> included path (resolved)."""

    out_edges: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))
    in_edges: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))
    files: Set[str] = field(default_factory=set)
    # (including_file, spelled, line, resolved)
    sites: List[Tuple[str, str, int, str]] = field(default_factory=list)
    _tu_cache: Dict[str, Set[str]] = field(default_factory=dict, repr=False)
    _inc_cache: Dict[str, Set[str]] = field(default_factory=dict, repr=False)

    def add(self, src: str, dst: str, spelled: str, line: int) -> None:
        self.files.add(src)
        self.files.add(dst)
        self.out_edges[src].add(dst)
        self.in_edges[dst].add(src)
        self.sites.append((src, spelled, line, dst))
        self._tu_cache.clear()
        self._inc_cache.clear()

    def tus(self) -> List[str]:
        return [p for p in self.files if _is_tu(p)]

    def transitive_consumers(self, start: str) -> Set[str]:
        seen: Set[str] = set()
        stack = [start]
        while stack:
            cur = stack.pop()
            for parent in self.in_edges.get(cur, ()):
                if parent in seen:
                    continue
                seen.add(parent)
                stack.append(parent)
        return seen

    def transitive_includes(self, start: str) -> Set[str]:
        """Files included by ``start``, transitively (the include closure)."""
        cached = self._inc_cache.get(start)
        if cached is not None:
            return cached
        seen: Set[str] = set()
        stack = [start]
        while stack:
            cur = stack.pop()
            for dst in self.out_edges.get(cur, ()):
                if dst in seen:
                    continue
                seen.add(dst)
                stack.append(dst)
        self._inc_cache[start] = seen
        return seen

    def tu_consumers(self, start: str) -> Set[str]:
        cached = self._tu_cache.get(start)
        if cached is not None:
            return cached
        found = {p for p in self.transitive_consumers(start) if _is_tu(p)}
        self._tu_cache[start] = found
        return found

    def header_consumers(self, start: str) -> Set[str]:
        return {p for p in self.transitive_consumers(start) if _is_header(p)}

    def direct_includers(self, start: str) -> Set[str]:
        return set(self.in_edges.get(start, ()))


def _is_tu(path: str) -> bool:
    return Path(path).suffix.lower() in TU_SUFFIXES


def _is_header(path: str) -> bool:
    return Path(path).suffix.lower() in HEADER_SUFFIXES


def _is_pb_h(path: str) -> bool:
    return path.endswith(".pb.h") or path.endswith(".grpc.pb.h")


def _should_skip_dir(name: str) -> bool:
    return name in SKIP_DIR_NAMES or name.startswith(".")


def iter_source_files(repo_root: Path, roots: Sequence[str]) -> Iterable[Path]:
    for root in roots:
        base = repo_root / root
        if not base.exists():
            log.warning("root %s does not exist, skipping", root)
            continue
        for dirpath, dirnames, filenames in os_walk_filtered(base):
            for name in filenames:
                if Path(name).suffix.lower() in SOURCE_SUFFIXES:
                    yield Path(dirpath) / name


def os_walk_filtered(base: Path):
    """os.walk that prunes SKIP_DIR_NAMES in-place."""
    import os
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames[:] = [d for d in dirnames if not _should_skip_dir(d)]
        yield dirpath, dirnames, filenames


def resolve_include(spelled: str, including: str) -> Optional[str]:
    """Map an ``#include`` spelling to a repo-relative graph key.

    Source-root paths (``ydb/...``, ``library/...``) are used as-is, even
    when the file is generated and not on disk (``.pb.h``). Quoted
    relatives are joined with the including file's directory.
    """
    spelled = spelled.strip()
    if not spelled or spelled.startswith("google/") or spelled.startswith("grpcpp/"):
        if spelled.endswith(".pb.h") or spelled.endswith(".proto"):
            return spelled
        return None
    if INCLUDE_ROOT_RE.match(spelled):
        return posixpath.normpath(spelled)
    # Standard-library / unknown angled includes are not rebuild levers.
    if "/" not in spelled and not spelled.endswith((".h", ".hh", ".hpp", ".pb.h")):
        return None
    base = posixpath.dirname(including)
    if not base:
        return spelled
    return posixpath.normpath(posixpath.join(base, spelled))


def proto_path_for_pb(pb_h: str) -> Optional[str]:
    if pb_h.endswith(".grpc.pb.h"):
        return pb_h[: -len(".grpc.pb.h")] + ".proto"
    if pb_h.endswith(".pb.h"):
        return pb_h[: -len(".pb.h")] + ".proto"
    return None


def parse_proto(path: Path, rel: str) -> ProtoInfo:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ProtoInfo(path=rel)
    info = ProtoInfo(path=rel)
    m = PROTO_PACKAGE_RE.search(text)
    if m:
        info.package = m.group(1)
    info.imports = [posixpath.normpath(p) for p in PROTO_IMPORT_RE.findall(text)]
    # Top-level messages/enums only: nested types cannot be forward-declared
    # outside their enclosing class, so they are not useful candidates.
    depth = 0
    for raw in text.splitlines():
        stripped = raw.split("//", 1)[0]
        # crude: count braces after stripping strings poorly; good enough
        # to ignore nested message/enum declarations.
        if depth == 0:
            tm = PROTO_TYPE_RE.match(stripped)
            if tm:
                if tm.group(1) == "message":
                    info.messages.append(tm.group(2))
                else:
                    info.enums.append(tm.group(2))
        depth += stripped.count("{") - stripped.count("}")
        if depth < 0:
            depth = 0
    return info


def load_protos(repo_root: Path, roots: Sequence[str]) -> Dict[str, ProtoInfo]:
    out: Dict[str, ProtoInfo] = {}
    for root in roots:
        base = repo_root / root
        if not base.exists():
            continue
        for dirpath, dirnames, filenames in os_walk_filtered(base):
            for name in filenames:
                if not name.endswith(".proto"):
                    continue
                p = Path(dirpath) / name
                rel = str(p.relative_to(repo_root))
                out[rel] = parse_proto(p, rel)
    return out


def proto_importers(protos: Dict[str, ProtoInfo]) -> Dict[str, Set[str]]:
    """imported proto -> set of protos that import it (directly)."""
    incoming: Dict[str, Set[str]] = defaultdict(set)
    for rel, info in protos.items():
        for imp in info.imports:
            incoming[imp].add(rel)
    return incoming


def transitive_proto_importers(
    start: str, incoming: Dict[str, Set[str]]
) -> Set[str]:
    seen: Set[str] = set()
    stack = [start]
    while stack:
        cur = stack.pop()
        for parent in incoming.get(cur, ()):
            if parent in seen:
                continue
            seen.add(parent)
            stack.append(parent)
    return seen


def scan_graph(repo_root: Path, roots: Sequence[str]) -> IncludeGraph:
    graph = IncludeGraph()
    n_files = 0
    for path in iter_source_files(repo_root, roots):
        try:
            rel = str(path.relative_to(repo_root))
        except ValueError:
            continue
        n_files += 1
        graph.files.add(rel)
        for inc in scan_includes(path):
            if inc.in_conditional:
                continue
            resolved = resolve_include(inc.spelled, rel)
            if not resolved:
                continue
            if not INCLUDE_ROOT_RE.match(resolved) and not _is_pb_h(resolved):
                continue
            graph.add(rel, resolved, inc.spelled, inc.line)
    log.info("scanned %d source files, %d include sites, %d graph nodes",
             n_files, len(graph.sites), len(graph.files))
    return graph


def _strip_comments_and_strings(text: str) -> str:
    """Replace comments and string literals with spaces, keep newlines."""
    out: List[str] = []
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if text[i:i + 2] == "//":
            while i < n and text[i] != "\n":
                out.append(" ")
                i += 1
            continue
        if text[i:i + 2] == "/*":
            i += 2
            out.append("  ")
            while i < n and text[i:i + 2] != "*/":
                out.append("\n" if text[i] == "\n" else " ")
                i += 1
            if i < n:
                out.append("  ")
                i += 2
            continue
        if ch in "\"'":
            quote = ch
            out.append(" ")
            i += 1
            while i < n and text[i] != quote:
                if text[i] == "\\" and i + 1 < n:
                    out.append("  ")
                    i += 2
                    continue
                out.append("\n" if text[i] == "\n" else " ")
                i += 1
            if i < n:
                out.append(" ")
                i += 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _line_of_include_removed(text: str, spelled: str) -> str:
    """Blank out ``#include ... spelled ...`` lines so they don't count as use."""
    lines = text.splitlines(keepends=True)
    out = []
    needle = spelled
    for line in lines:
        if "#include" in line and needle in line:
            out.append("\n" if line.endswith("\n") else "")
        else:
            out.append(line)
    return "".join(out)


_IDENT_BEFORE_RE = re.compile(r"[A-Za-z_]\w*$")


def classify_proto_use(text: str, spelled: str, info: ProtoInfo) -> str:
    """Return ``unused``, ``fwd``, or ``keep`` for one include of a .pb.h.

    Conservative: any use that is not an obvious incomplete-type context
    is ``keep``. Nested-type access (``Type::``) is ``keep``. Enum use is
    ``keep`` (protobuf C++ enums are awkward to forward-declare portably).

    False ``unused`` happens when the file never names a protobuf type
    but still needs a complete type, e.g. ``AppData()->ColumnShardConfig.GetX()``
    (the type is hidden behind a ``TAppData`` member). Treat those as
    ``keep`` when applying patches; or move the access into a ``.cpp``.
    """
    body = _strip_comments_and_strings(text)
    body = _line_of_include_removed(body, spelled)
    names = list(info.messages)
    enum_names = list(info.enums)

    def matches(name: str) -> List[int]:
        return [m.start() for m in re.finditer(r"\b" + re.escape(name) + r"\b", body)]

    if enum_names and any(matches(n) for n in enum_names):
        return "keep"

    hits: List[Tuple[str, int]] = []
    for name in names:
        for pos in matches(name):
            hits.append((name, pos))
    if not hits:
        return "unused"

    for name, pos in hits:
        if _needs_complete_type(body, pos, name):
            return "keep"
    return "fwd"


def _needs_complete_type(body: str, pos: int, name: str) -> bool:
    """True when the occurrence of ``name`` at ``pos`` needs a complete type."""
    end = pos + len(name)
    before = body[:pos]
    after = body[end:]

    # Skip occurrences that are the identifier in ``class Name;`` / ``struct Name;``.
    head = before.rstrip()
    if head.endswith(("class", "struct")):
        rest = after.lstrip()
        if rest.startswith(";") or rest.startswith("{"):
            # ``class TFoo;`` is a fwd. ``class TFoo {`` is a definition, keep.
            return not rest.startswith(";")
        # ``class TFoo *`` still incomplete.
        if rest.startswith("*") or rest.startswith("&"):
            return False

    # Member / nested access: Type:: or ::Type::foo after a namespace is ok
    # if it's Type::Nested or Type::default_instance — that needs complete.
    if after.lstrip().startswith("::"):
        return True
    if before.rstrip().endswith("."):
        return True

    # sizeof(Type) / new Type / delete
    left = before.rstrip()
    if left.endswith("sizeof") or left.endswith("new") or left.endswith("delete"):
        return True
    if re.search(r"\bsizeof\s*$", left):
        return True

    # Inheritance: `: public Type` / `: Type`. Do not match the second
    # colon of a ``Ns::Type`` qualifier.
    if re.search(r"(?<!:):\s*(?:public|private|protected)?\s*$", left):
        return True

    # Template argument: look at the nearest unmatched '<' before pos.
    tmpl = _enclosing_template(before)
    if tmpl is not None:
        return tmpl not in FWD_OK_TEMPLATES

    # Constructor / function-style cast: Type( or Type{
    rest = after.lstrip()
    if rest.startswith("(") or rest.startswith("{"):
        return True

    # Pointer / reference: Type* Type& const Type *  (possibly with const/volatile)
    # Also ``Type const*``.
    if rest.startswith("*") or rest.startswith("&"):
        return False
    # ``const Type *`` already handled via rest. ``Type const *``:
    mconst = re.match(r"\s+const\s*([*&])", after)
    if mconst:
        return False

    # Value declaration / by-value parameter: Type ident  or Type,
    # or Type)  (end of param of form `void f(Type)` — rare, keep).
    if re.match(r"\s+[A-Za-z_~]", after) or re.match(r"\s*[,;)]", after):
        return True

    # Unknown context: keep.
    return True


def _enclosing_template(before: str) -> Optional[str]:
    """If ``before`` ends inside ``Foo< ...`` with no closing ``>``, return Foo."""
    depth = 0
    i = len(before) - 1
    while i >= 0:
        ch = before[i]
        if ch == ">":
            depth += 1
        elif ch == "<":
            if depth == 0:
                ident = []
                j = i - 1
                while j >= 0 and before[j].isspace():
                    j -= 1
                while j >= 0 and (before[j].isalnum() or before[j] == "_"):
                    ident.append(before[j])
                    j -= 1
                return "".join(reversed(ident)) or None
            depth -= 1
        i -= 1
    return None


def used_message_names(text: str, spelled: str, info: ProtoInfo) -> List[str]:
    body = _line_of_include_removed(_strip_comments_and_strings(text), spelled)
    return [n for n in info.messages if re.search(r"\b" + re.escape(n) + r"\b", body)]


APPDATA_H = "ydb/core/base/appdata.h"


@dataclass
class RankRow:
    header: str
    rebuild_tus: int
    include_tus: int
    direct_h: int
    direct_cpp: int
    proto_importers: int
    kind: str
    via_appdata: bool = False


def rebuild_set(
    header: str,
    graph: IncludeGraph,
    protos: Dict[str, ProtoInfo],
    incoming: Dict[str, Set[str]],
) -> Set[str]:
    """TUs that recompile if ``header`` (or its .proto) changes."""
    tus = graph.tu_consumers(header)
    proto = proto_path_for_pb(header)
    if proto and proto in protos:
        for importer in transitive_proto_importers(proto, incoming):
            pb = protos[importer].pb_h
            tus |= graph.tu_consumers(pb)
    return tus


def rank_headers(
    graph: IncludeGraph,
    protos: Dict[str, ProtoInfo],
    incoming: Dict[str, Set[str]],
    only_pb: bool,
) -> List[RankRow]:
    candidates: Set[str] = set()
    if only_pb:
        candidates = {n for n in graph.files if _is_pb_h(n)}
        candidates |= {info.pb_h for info in protos.values()}
    else:
        candidates = {n for n in graph.files if _is_header(n) or _is_pb_h(n)}
        candidates |= {info.pb_h for info in protos.values()}

    appdata_closure = graph.transitive_includes(APPDATA_H)

    rows: List[RankRow] = []
    for header in candidates:
        include_tus = graph.tu_consumers(header)
        proto = proto_path_for_pb(header)
        n_importers = 0
        rebuild = set(include_tus)
        if proto and proto in protos:
            importers = transitive_proto_importers(proto, incoming)
            n_importers = len(importers)
            for importer in importers:
                rebuild |= graph.tu_consumers(protos[importer].pb_h)
        direct = graph.direct_includers(header)
        direct_h = sum(1 for p in direct if _is_header(p))
        direct_cpp = sum(1 for p in direct if _is_tu(p))
        kind = "protobuf" if _is_pb_h(header) else "header"
        if not rebuild and not direct:
            continue
        rows.append(RankRow(
            header=header,
            rebuild_tus=len(rebuild),
            include_tus=len(include_tus),
            direct_h=direct_h,
            direct_cpp=direct_cpp,
            proto_importers=n_importers,
            kind=kind,
            via_appdata=header in appdata_closure,
        ))
    rows.sort(key=lambda r: (-r.rebuild_tus, -r.include_tus, -r.direct_h, r.header))
    return rows


@dataclass
class SiteClass:
    file: str
    line: int
    spelled: str
    verdict: str
    used_types: List[str]
    tu_fanout: int
    is_header: bool


def classify_sites(
    target: str,
    graph: IncludeGraph,
    repo_root: Path,
    protos: Dict[str, ProtoInfo],
) -> List[SiteClass]:
    proto_rel = proto_path_for_pb(target)
    info = protos.get(proto_rel or "", ProtoInfo(path=proto_rel or target))
    out: List[SiteClass] = []
    for src, spelled, line, resolved in graph.sites:
        if resolved != target:
            continue
        path = repo_root / src
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            text = ""
        if info.messages or info.enums:
            verdict = classify_proto_use(text, spelled, info)
            used = used_message_names(text, spelled, info) if verdict != "unused" else []
        else:
            # Non-proto header: we can still flag a complete unused include
            # (the spelled basename never appears) but cannot claim fwd.
            verdict = "unknown"
            used = []
        out.append(SiteClass(
            file=src,
            line=line,
            spelled=spelled,
            verdict=verdict,
            used_types=used,
            tu_fanout=len(graph.tu_consumers(src)) if _is_header(src) else 1,
            is_header=_is_header(src),
        ))
    out.sort(key=lambda s: (-s.is_header, -s.tu_fanout, s.file))
    return out


def fwd_snippet(info: ProtoInfo, used: Sequence[str]) -> str:
    names = [n for n in used if n in info.messages]
    if not names:
        names = list(info.messages[:8])
    ns_parts = info.package.split(".") if info.package else []
    lines = []
    indent = ""
    for ns in ns_parts:
        lines.append(f"{indent}namespace {ns} {{")
        indent += "    "
    for n in names:
        lines.append(f"{indent}class {n};")
    for ns in reversed(ns_parts):
        indent = indent[4:]
        lines.append(f"{indent}}} // namespace {ns}")
    return "\n".join(lines)


def write_rank_csv(path: Path, rows: Sequence[RankRow], top: Optional[int] = None) -> None:
    shown = rows if top is None else rows[:top]
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["header", "kind", "rebuild_tus", "include_tus",
                    "direct_h", "direct_cpp", "proto_importers", "via_appdata"])
        for r in shown:
            w.writerow([r.header, r.kind, r.rebuild_tus, r.include_tus,
                        r.direct_h, r.direct_cpp, r.proto_importers,
                        int(r.via_appdata)])


def write_sites_csv(path: Path, sites: Sequence[SiteClass]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["file", "line", "spelled", "verdict", "tu_fanout",
                    "is_header", "used_types"])
        for s in sites:
            w.writerow([s.file, s.line, s.spelled, s.verdict, s.tu_fanout,
                        int(s.is_header), " ".join(s.used_types)])


def _rank_table_lines(rows: Sequence[RankRow], extra_appdata: bool = False) -> List[str]:
    cols = ("| header | kind | rebuild TUs | C++ include TUs | "
            "direct .h | direct .cpp | proto importers |"
            + (" via appdata.h |" if extra_appdata else ""))
    sep = "|---|---|---:|---:|---:|---:|---:|" + ("---:|" if extra_appdata else "")
    lines = [cols, sep]
    for r in rows:
        line = (
            f"| `{r.header}` | {r.kind} | {r.rebuild_tus} | {r.include_tus} | "
            f"{r.direct_h} | {r.direct_cpp} | {r.proto_importers} |"
        )
        if extra_appdata:
            line += " yes |" if r.via_appdata else " |"
        lines.append(line)
    return lines


def _append_dive(
    lines: List[str],
    header: str,
    sites: Sequence[SiteClass],
    protos: Dict[str, ProtoInfo],
    incoming: Dict[str, Set[str]],
) -> None:
    proto = proto_path_for_pb(header)
    lines.append(f"## Deep dive: `{header}`")
    lines.append("")
    if proto and proto in protos:
        importers = sorted(transitive_proto_importers(proto, incoming))
        lines.append(f"Source proto: `{proto}` "
                     f"(package `{protos[proto].package or '?'}`).")
        if importers:
            lines.append("Protos that import it (their `.pb.h` also "
                         "regenerates on a change):")
            for p in importers[:40]:
                lines.append(f"- `{p}`")
            if len(importers) > 40:
                lines.append(f"- … {len(importers) - 40} more")
        lines.append("")
    by_v: Dict[str, List[SiteClass]] = defaultdict(list)
    for s in sites:
        by_v[s.verdict].append(s)
    lines.append("| verdict | sites | of which headers |")
    lines.append("|---|---:|---:|")
    for v in ("unused", "fwd", "keep", "unknown"):
        ss = by_v.get(v, [])
        lines.append(f"| `{v}` | {len(ss)} | "
                     f"{sum(1 for s in ss if s.is_header)} |")
    lines.append("")
    lines.append("### Amplifier headers (direct `.h` includes, by TU fanout)")
    lines.append("")
    lines.append("| file | line | verdict | TU fanout | used types |")
    lines.append("|---|---:|---|---:|---|")
    header_sites = [s for s in sites if s.is_header]
    for s in header_sites[:60]:
        used = ", ".join(s.used_types) or "—"
        lines.append(f"| `{s.file}`:{s.line} | {s.line} | `{s.verdict}` | "
                     f"{s.tu_fanout} | {used} |")
    if not header_sites:
        lines.append("| *(no header includes this file directly)* | | | | |")
    lines.append("")
    info = protos.get(proto or "", ProtoInfo(path=proto or header))
    fwds = [s for s in header_sites if s.verdict == "fwd"]
    unuseds = [s for s in header_sites if s.verdict == "unused"]
    if unuseds:
        lines.append("### Suggested removals (unused in the header)")
        lines.append("")
        for s in unuseds[:30]:
            lines.append(f"- `{s.file}:{s.line}` — drop `#include <{s.spelled}>` "
                         f"(saves ~{s.tu_fanout} TUs if nothing else pulls it in)")
        lines.append("")
    if fwds and info.package:
        lines.append("### Suggested forward declarations")
        lines.append("")
        lines.append("Replace the protobuf include with:")
        lines.append("")
        lines.append("```cpp")
        used: List[str] = []
        seen: Set[str] = set()
        for s in fwds:
            for n in s.used_types:
                if n not in seen:
                    seen.add(n)
                    used.append(n)
        lines.append(fwd_snippet(info, used))
        lines.append("```")
        lines.append("")
        lines.append("in:")
        for s in fwds[:30]:
            lines.append(f"- `{s.file}:{s.line}` (fanout {s.tu_fanout})")
        lines.append("")
        lines.append("Move the real `#include` into the `.cpp` files that "
                     "need a complete type.")
        lines.append("")


def write_rank_md(
    path: Path,
    rows: Sequence[RankRow],
    top: int,
    dives: Sequence[Tuple[str, Sequence[SiteClass]]],
    protos: Dict[str, ProtoInfo],
    incoming: Dict[str, Set[str]],
) -> None:
    lines: List[str] = []
    lines.append("# Include rebuild fanout")
    lines.append("")
    lines.append("`rebuild_tus` is how many `.cpp` files would recompile if "
                 "this header (or, for a `.pb.h`, its `.proto` **and every "
                 "proto that imports it**) changed. That number is dominated "
                 "by the proto-import overlay: changing `blobstorage.proto` "
                 "also regenerates `config.pb.h`.")
    lines.append("")
    lines.append("`include_tus` is the IWYU lever: how many TUs actually "
                 "`#include` this header, directly or through other headers. "
                 "`direct_h` / `direct_cpp` are the include sites you can "
                 "edit. Cutting a header include with large `tu_fanout` is "
                 "the high-leverage move; cutting a `.cpp` include saves one TU.")
    lines.append("")
    lines.append("`via appdata.h` means `ydb/core/base/appdata.h` transitively "
                 "includes this file, so the protobuf leaks into essentially "
                 "every actor TU even when that TU never names a protobuf type.")
    lines.append("")

    lines.append(f"## Top {min(top, len(rows))} by rebuild impact "
                 "(proto-import overlay)")
    lines.append("")
    lines.extend(_rank_table_lines(rows[:top], extra_appdata=True))
    lines.append("")

    by_include = sorted(
        (r for r in rows if r.include_tus > 0),
        key=lambda r: (-r.include_tus, -r.direct_h, r.header),
    )
    lines.append(f"## Top {min(top, len(by_include))} by C++ include fanout "
                 "(IWYU targets)")
    lines.append("")
    lines.extend(_rank_table_lines(by_include[:top], extra_appdata=True))
    lines.append("")

    via_appdata = [r for r in by_include if r.via_appdata]
    if via_appdata:
        lines.append("## Pulled in by `appdata.h`")
        lines.append("")
        lines.append("These are why a one-line proto change rebuilds half the "
                     "tree: `appdata.h` is in almost every actor, and it "
                     "currently includes headers that include these `.pb.h` "
                     "files (the `#error` in `appdata.h` does not cover them).")
        lines.append("")
        lines.extend(_rank_table_lines(via_appdata[:top], extra_appdata=False))
        lines.append("")

    for header, sites in dives:
        _append_dive(lines, header, sites, protos, incoming)

    path.write_text("\n".join(lines), encoding="utf-8")


def _normalize_header_arg(raw: str) -> str:
    if raw.endswith(".proto"):
        return raw[: -len(".proto")] + ".pb.h"
    return raw


def _sites_stem(header: str) -> str:
    name = posixpath.basename(header)
    for suffix in (".grpc.pb.h", ".pb.h"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    else:
        name = Path(name).stem
    return name


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="sanitize_includes fanout",
        description="Rank headers by rebuild fanout and classify protobuf "
                    "include sites as unused / forward-declarable / keep.",
    )
    parser.add_argument(
        "--roots", nargs="+", default=["ydb"],
        help="source roots to scan (default: ydb).",
    )
    parser.add_argument(
        "--protobuf", action="store_true",
        help="rank generated .pb.h files only.",
    )
    parser.add_argument(
        "--header", action="append", default=[],
        help="deep-dive a header (repeatable; e.g. "
             "ydb/core/protos/config.pb.h). A .proto path is accepted too.",
    )
    parser.add_argument(
        "--classify-top", type=int, default=0, metavar="N",
        help="also classify the top N headers by C++ include fanout "
             "(among those with at least one direct .h include).",
    )
    parser.add_argument("--top", type=int, default=40)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    setup_logging(args.verbose)

    roots = list(args.roots)
    log.info("scanning %s under %s", roots, PATHS.repo_root)
    graph = scan_graph(PATHS.repo_root, roots)
    protos = load_protos(PATHS.repo_root, roots)
    incoming = proto_importers(protos)
    log.info("parsed %d proto files", len(protos))

    only_pb = args.protobuf or any(_is_pb_h(_normalize_header_arg(h))
                                   for h in args.header)
    rows = rank_headers(graph, protos, incoming, only_pb=only_pb)

    out_dir = ensure_dir(PATHS.reports_dir / "fanout")
    write_rank_csv(out_dir / "rank.csv", rows)

    targets: List[str] = []
    seen_t: Set[str] = set()

    def add_target(h: str) -> None:
        h = _normalize_header_arg(h)
        if h not in seen_t:
            seen_t.add(h)
            targets.append(h)

    for h in args.header:
        add_target(h)
    if args.classify_top:
        by_include = sorted(
            (r for r in rows if r.direct_h > 0),
            key=lambda r: (-r.include_tus, -r.direct_h, r.header),
        )
        for r in by_include[:args.classify_top]:
            add_target(r.header)

    dives: List[Tuple[str, List[SiteClass]]] = []
    combined: List[SiteClass] = []
    for target in targets:
        sites = classify_sites(target, graph, PATHS.repo_root, protos)
        dives.append((target, sites))
        combined.extend(sites)
        stem = _sites_stem(target)
        write_sites_csv(out_dir / f"sites-{stem}.csv", sites)
        unused_h = sum(1 for s in sites if s.is_header and s.verdict == "unused")
        fwd_h = sum(1 for s in sites if s.is_header and s.verdict == "fwd")
        keep_h = sum(1 for s in sites if s.is_header and s.verdict == "keep")
        log.info("%s: %d include sites (%d unused-in-header, %d fwd, %d keep)",
                 target, len(sites), unused_h, fwd_h, keep_h)

    if combined:
        write_sites_csv(out_dir / "sites.csv", combined)

    write_rank_md(out_dir / "rank.md", rows, args.top, dives, protos, incoming)

    print(f"{'rebuild':>8s} {'incl_TU':>8s} {'dir.h':>6s} {'dir.cc':>6s}  "
          f"{'appd':>4s}  header")
    if dives:
        by_header = {r.header: r for r in rows}
        for target, sites in dives:
            r = by_header.get(target)
            print(f"--- {target} ---")
            if r:
                print(f"{r.rebuild_tus:8d} {r.include_tus:8d} {r.direct_h:6d} "
                      f"{r.direct_cpp:6d}  {'yes' if r.via_appdata else '':>4s}  "
                      f"{r.header}")
            print()
            print(f"{'fanout':>7s} {'verdict':<8s}  site")
            for s in sites[:40]:
                kind = "h" if s.is_header else "c"
                print(f"{s.tu_fanout:7d} {s.verdict:<8s}  {kind} "
                      f"{s.file}:{s.line}")
            print()
    else:
        for r in rows[:args.top]:
            print(f"{r.rebuild_tus:8d} {r.include_tus:8d} {r.direct_h:6d} "
                  f"{r.direct_cpp:6d}  {'yes' if r.via_appdata else '':>4s}  "
                  f"{r.header}")
    print(f"\nwrote {out_dir / 'rank.md'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
