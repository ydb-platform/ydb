"""Include dependency graph + ``(file, include) -> verdict`` decision logic.

Node = a source or header file (repo-relative path).
Edge = ``including_file -> included_file`` taken from clang ``-H`` output
(direct includes only; the parent map preserves directness).

We classify each *direct* ``#include`` line in headers, and report which
``.cpp`` translation units would need to gain that include if it were
removed from the header.
"""

from __future__ import annotations

import logging
import posixpath
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set


log = logging.getLogger("aggregate.graph")


def resolve_spelling(spelled: str, including_file: str, known_paths: Set[str]) -> Optional[str]:
    """Resolve an ``#include`` spelling to a repo-relative path key.

    clang-include-cleaner spells its inserts its own way (usually
    source-root-relative, e.g. ``ydb/core/base/defs.h``), while a header
    may spell the same include relative to its own directory (e.g.
    ``defs.h`` or ``../base/defs.h``). To compare them we resolve both to
    the same repo-relative path using the set of known analyzed files.

    Returns the repo-relative path if resolvable, else None.
    """
    if not spelled:
        return None
    if spelled in known_paths:
        return spelled
    base = posixpath.dirname(including_file)
    if base:
        cand = posixpath.normpath(posixpath.join(base, spelled))
        if cand in known_paths:
            return cand
    return None


def match_keys(spelled: str, including_file: str, known_paths: Set[str]) -> Set[str]:
    """All keys that should be considered equivalent to ``spelled``.

    Always includes the raw spelling (so exact-spelling matches still
    work) plus the resolved repo-relative path when resolvable.
    """
    keys = {spelled}
    resolved = resolve_spelling(spelled, including_file, known_paths)
    if resolved:
        keys.add(resolved)
    return keys


def resolve_add_target(
    spelled: str,
    angled: bool,
    including_file: str,
    known_paths: Set[str],
) -> str:
    """Compute the include path to ADD to a consumer .cpp.

    The result is rendered later as an angled (source-root) include, so
    it must be a path that resolves from any TU regardless of its
    directory.

    - Angled includes are already source-root spellings (``ydb/...``)
      and are returned as-is. Generated ``*.pb.h`` headers fall here too.
    - Quoted includes are resolved to a repo-relative path: first via the
      analyzed-file set, otherwise by joining with the including header's
      directory (the natural C++ sibling resolution). This converts e.g.
      ``"foo.h"`` in ``ydb/a/b/h.h`` into ``ydb/a/b/foo.h``.
    """
    if angled:
        return spelled
    r = resolve_spelling(spelled, including_file, known_paths)
    if r:
        return r
    base = posixpath.dirname(including_file)
    if base:
        return posixpath.normpath(posixpath.join(base, spelled))
    return spelled


VERDICT_KEEP = "keep"
VERDICT_REMOVE = "remove"
VERDICT_MOVE_TO_CPP = "move-to-cpp"
VERDICT_KEEP_DEFERRED = "keep-deferred"
VERDICT_KEEP_PRAGMA = "keep-pragma"
VERDICT_UNKNOWN = "unknown"
VERDICT_PROBE_FAILED = "probe-failed"
VERDICT_CYCLE = "include-cycle"

# Must match CYCLE_ERROR_PREFIX in analyze/per_tu.py (kept as a literal
# here to avoid a circular import between aggregate and analyze).
_CYCLE_ERROR_MARKER = "include-cycle (not a missing-include problem)"


@dataclass
class Node:
    path: str
    kind: str
    size_bytes: int = 0
    fanin: int = 0
    fanout: int = 0


@dataclass
class Verdict:
    in_file: str
    spelled: str
    line: int
    verdict: str
    reason: str = ""
    consumers_needing: List[str] = field(default_factory=list)
    headers_blocking: List[str] = field(default_factory=list)
    # Source-root-relative path to use when ADDING this include to a
    # consumer .cpp (rendered as an angled include). This is NOT
    # necessarily ``spelled``: a header may spell a sibling relatively
    # (``"foo.h"``), which would not resolve from a .cpp in a different
    # directory. We store the resolved repo-relative path so the moved
    # include works from any TU.
    resolved: Optional[str] = None

    def to_json(self) -> dict:
        return {
            "in_file": self.in_file,
            "spelled": self.spelled,
            "line": self.line,
            "verdict": self.verdict,
            "reason": self.reason,
            "consumers_needing": list(self.consumers_needing),
            "headers_blocking": list(self.headers_blocking),
            "resolved": self.resolved,
        }


@dataclass
class Graph:
    nodes: Dict[str, Node] = field(default_factory=dict)
    out_edges: Dict[str, Set[str]] = field(default_factory=dict)
    in_edges: Dict[str, Set[str]] = field(default_factory=dict)

    def add_edge(self, parent: str, child: str) -> None:
        self.out_edges.setdefault(parent, set()).add(child)
        self.in_edges.setdefault(child, set()).add(parent)

    def ensure_node(self, path: str, kind: str) -> Node:
        n = self.nodes.get(path)
        if n is None:
            n = Node(path=path, kind=kind)
            self.nodes[path] = n
        return n

    def finalize_counts(self) -> None:
        for path, node in self.nodes.items():
            node.fanout = len(self.out_edges.get(path, ()))
            node.fanin = len(self.in_edges.get(path, ()))

    def transitive_consumers(self, header: str) -> Set[str]:
        seen: Set[str] = set()
        stack: List[str] = [header]
        while stack:
            cur = stack.pop()
            for parent in self.in_edges.get(cur, ()):
                if parent in seen:
                    continue
                seen.add(parent)
                stack.append(parent)
        return seen

    def tu_consumers(self, header: str) -> Set[str]:
        return {p for p in self.transitive_consumers(header) if _looks_like_tu(p)}

    def header_consumers(self, header: str) -> Set[str]:
        return {p for p in self.transitive_consumers(header) if _looks_like_header(p)}


def _looks_like_tu(path: str) -> bool:
    return path.lower().endswith((".cpp", ".cc", ".cxx", ".c", ".c++"))


def _looks_like_header(path: str) -> bool:
    return path.lower().endswith((".h", ".hh", ".hpp", ".hxx"))


def _make_node_normalizer(prefixes: Iterable[str]):
    """Return a function mapping an include_tree path to a graph node key.

    ``include_tree`` paths come from ``clang -H`` and are ABSOLUTE
    (``/home/me/work/ydb/...``), whereas ``per_file`` is keyed by
    REPO-RELATIVE paths (``ydb/...``). Without normalization the graph
    edges live in a different namespace than the header keys the
    classifier looks up, so every consumer query returns empty and all
    unused includes collapse to ``remove`` (no move-to-cpp/keep-deferred
    ever fires). We strip any known absolute prefix so both sides share
    one namespace. Paths outside those prefixes (system headers, the ya
    build_root holding generated *.pb.h) are left as-is; they simply
    won't match any analyzed file, which is fine.
    """
    # Longest first so the most specific prefix wins.
    ordered = sorted({p for p in prefixes if p}, key=len, reverse=True)

    def norm(p: str) -> str:
        for pref in ordered:
            if p.startswith(pref):
                return p[len(pref):]
        return p

    return norm


def _derive_abs_prefixes(per_file: Dict[str, dict]) -> Set[str]:
    """Recover the absolute path prefix(es) baked into ``include_tree``.

    The cache stores absolute paths whose root depends on the machine
    that produced it (e.g. ``/home/me/work/``), which may differ from the
    current checkout (worktrees, clones, rsync from another box). We
    recover the prefix from each file's own pair: the include_tree
    contains the file's absolute path, and ``file`` is its repo-relative
    form, so ``abs_path[:-len(rel)]`` is the prefix. Deriving it from the
    data makes us independent of the local repo root.
    """
    prefixes: Set[str] = set()
    for rel, fa in per_file.items():
        tree = fa.get("include_tree") or {}
        if not tree or not rel:
            continue
        for key in tree.keys():
            if key.endswith("/" + rel) or key == rel:
                prefix = key[: len(key) - len(rel)]
                if prefix and prefix != "/" and prefix.startswith("/"):
                    prefixes.add(prefix)
                break
        if len(prefixes) >= 8:
            break
    return prefixes


def build_graph(per_file: Dict[str, dict], repo_root: Optional[str] = None) -> Graph:
    """Build a directed include graph from ``include_tree`` fields.

    ``per_file`` is keyed by repo-relative path of TU/header, value is the
    serialized ``FileAnalysis`` dict. ``include_tree`` paths are absolute
    and get normalized to repo-relative so they share the same node
    namespace as ``per_file`` keys.
    """
    prefixes = _derive_abs_prefixes(per_file)
    if repo_root is None:
        try:
            from ..common import REPO_ROOT
            repo_root = str(REPO_ROOT)
        except Exception:
            repo_root = None
    if repo_root:
        prefixes.add(repo_root.rstrip("/") + "/")
    norm = _make_node_normalizer(prefixes)

    g = Graph()
    for path, fa in per_file.items():
        kind = fa.get("kind", "tu")
        node = g.ensure_node(path, kind)
        try:
            node.size_bytes = (Path.cwd() / path).stat().st_size
        except OSError:
            pass

    for path, fa in per_file.items():
        tree = fa.get("include_tree") or {}
        for parent, kids in tree.items():
            nparent = norm(parent)
            for child in kids:
                if not parent or not child:
                    continue
                nchild = norm(child)
                g.add_edge(nparent, nchild)
                if nparent not in g.nodes:
                    g.ensure_node(nparent, "tu" if _looks_like_tu(nparent) else "header")
                if nchild not in g.nodes:
                    g.ensure_node(nchild, "tu" if _looks_like_tu(nchild) else "header")

    g.finalize_counts()
    return g


def _index_includes(fa: dict) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    for inc in fa.get("includes", []) or []:
        out[inc["spelled"]] = inc
    return out


def _direct_unused_set(fa: dict) -> Set[str]:
    return set(fa.get("unused_includes") or [])


def classify(
    per_file: Dict[str, dict],
    graph: Graph,
) -> List[Verdict]:
    """Produce a verdict for every direct ``#include`` line in every header.

    For each header H:
        for each direct include I in H:
            ``S(H, I)`` is implicit: clang-include-cleaner says whether the
            probe TU for H needs I. If "yes", then ``H_uses(I)`` is True.

            If "no", we check consumers:
                - any header consumer of H also marks I as needed?
                  -> keep-deferred
                - any TU consumer of H marks I as needed (i.e., I appears
                  among the TU's needed includes via H)?
                  -> move-to-cpp (list those TUs)
                - else
                  -> remove
    """
    verdicts: List[Verdict] = []

    header_paths = [p for p, fa in per_file.items() if fa.get("kind") == "header"]

    known_paths: Set[str] = set(per_file.keys())

    def _insert_keys(path: str, fa: dict) -> Set[str]:
        """Resolved + raw keys for a file's suggested_inserts.

        These represent includes clang-include-cleaner says the file
        should add directly (i.e. symbols it uses but doesn't directly
        include). Resolving to repo-relative paths lets us match them
        against a header's include spelling regardless of how each side
        spelled the path.
        """
        out: Set[str] = set()
        for s in fa.get("suggested_inserts") or []:
            out |= match_keys(s, path, known_paths)
        return out

    insert_keys_by_file: Dict[str, Set[str]] = {
        path: _insert_keys(path, fa) for path, fa in per_file.items()
    }

    def _needed_set(fa: dict) -> Set[str]:
        """Headers that the file actually needs.

        Combines:
        - direct includes the cleaner did NOT flag as unused, and
        - ``suggested_inserts``: headers clang-include-cleaner says we
          should directly include (signal that symbols from them are
          consumed, regardless of how we currently reach them).
        """
        direct_unused = _direct_unused_set(fa)
        all_direct = {inc["spelled"] for inc in (fa.get("includes") or [])}
        needed = (all_direct - direct_unused) | set(fa.get("suggested_inserts") or [])
        return needed

    needed_by_header: Dict[str, Set[str]] = {}
    for path, fa in per_file.items():
        if fa.get("kind") != "header":
            continue
        needed_by_header[path] = _needed_set(fa)

    needed_by_tu: Dict[str, Set[str]] = {}
    for path, fa in per_file.items():
        if fa.get("kind") != "tu":
            continue
        needed_by_tu[path] = _needed_set(fa)

    for h in header_paths:
        fa = per_file[h]
        idx = _index_includes(fa)
        unused = _direct_unused_set(fa)
        needed = needed_by_header.get(h, set())

        # If the probe outright failed (clang couldn't even parse the
        # header in isolation), every #include in the header would
        # otherwise fall through to ``keep`` under the safe default —
        # but that is misleading: we have no information either way.
        # Mark them as ``probe-failed`` so the user can distinguish
        # "analyzed and intentionally kept" from "not analyzed".
        err = fa.get("error")
        probe_failed = bool(err)
        is_cycle = bool(err) and _CYCLE_ERROR_MARKER in err

        for spelled, inc in idx.items():
            if probe_failed:
                verdicts.append(Verdict(
                    in_file=h, spelled=spelled, line=inc["line"],
                    verdict=VERDICT_CYCLE if is_cycle else VERDICT_PROBE_FAILED,
                    reason=err or "probe failed",
                ))
                continue

            if inc.get("has_iwyu_keep") or inc.get("has_iwyu_export"):
                verdicts.append(Verdict(
                    in_file=h, spelled=spelled, line=inc["line"],
                    verdict=VERDICT_KEEP_PRAGMA, reason="IWYU pragma present",
                ))
                continue

            if spelled in needed:
                verdicts.append(Verdict(
                    in_file=h, spelled=spelled, line=inc["line"],
                    verdict=VERDICT_KEEP, reason="header itself uses symbols from this include",
                ))
                continue

            if spelled not in unused:
                verdicts.append(Verdict(
                    in_file=h, spelled=spelled, line=inc["line"],
                    verdict=VERDICT_UNKNOWN,
                    reason="cleaner did not flag include either as unused or needed",
                ))
                continue

            # Consumers (header / TU) "need spelled via this header" only
            # if they USE spelled but DON'T directly include it — i.e.
            # spelled is in their suggested_inserts. If a consumer
            # already directly includes spelled, removing it from H is
            # safe for them (they already have it), so they don't
            # promote the verdict to keep-deferred / move-to-cpp.
            #
            # We resolve the header's include spelling to repo-relative
            # path keys and compare against consumers' resolved
            # suggested_inserts. Comparing raw spellings alone fails
            # because the header may spell it relatively ("defs.h") while
            # the cleaner spells the consumer's insert as a source-root
            # path ("ydb/.../defs.h").
            target_keys = match_keys(spelled, h, known_paths)

            header_consumers_needing: List[str] = []
            for parent in graph.header_consumers(h):
                if parent not in per_file:
                    continue
                if insert_keys_by_file.get(parent, set()) & target_keys:
                    header_consumers_needing.append(parent)

            if header_consumers_needing:
                verdicts.append(Verdict(
                    in_file=h, spelled=spelled, line=inc["line"],
                    verdict=VERDICT_KEEP_DEFERRED,
                    reason="other headers use symbols from this include "
                           "transitively via this header",
                    headers_blocking=sorted(header_consumers_needing),
                ))
                continue

            tu_consumers_needing: List[str] = []
            for tu in graph.tu_consumers(h):
                if tu not in per_file:
                    continue
                if insert_keys_by_file.get(tu, set()) & target_keys:
                    tu_consumers_needing.append(tu)

            if tu_consumers_needing:
                verdicts.append(Verdict(
                    in_file=h, spelled=spelled, line=inc["line"],
                    verdict=VERDICT_MOVE_TO_CPP,
                    reason="some .cpp consumers use symbols from this include "
                           "but do not directly include it (cleaner emitted "
                           "a `+` suggestion for them)",
                    consumers_needing=sorted(tu_consumers_needing),
                    resolved=resolve_add_target(
                        spelled, bool(inc.get("angled")), h, known_paths),
                ))
                continue

            verdicts.append(Verdict(
                in_file=h, spelled=spelled, line=inc["line"],
                verdict=VERDICT_REMOVE,
                reason="no consumer (header or TU) needs this include via "
                       "this header — either nobody uses its symbols, or "
                       "everyone who does already includes it directly",
            ))

    return verdicts
