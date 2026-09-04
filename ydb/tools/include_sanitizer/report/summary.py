"""Markdown summary and CSV hot-headers ranking."""

from __future__ import annotations

import csv
import logging
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List

from ..aggregate.graph import (
    Graph,
    VERDICT_CYCLE,
    VERDICT_KEEP,
    VERDICT_KEEP_DEFERRED,
    VERDICT_KEEP_PRAGMA,
    VERDICT_MOVE_TO_CPP,
    VERDICT_PROBE_FAILED,
    VERDICT_REMOVE,
    VERDICT_UNKNOWN,
    Verdict,
)


log = logging.getLogger("report.summary")


def write_hot_headers_csv(graph: Graph, out_path: Path, top: int = 200) -> None:
    """Rank headers by ``fanin * size_bytes`` as a proxy for build-time impact."""
    headers = [n for n in graph.nodes.values() if n.kind == "header"]
    headers.sort(key=lambda n: -(n.fanin * max(1, n.size_bytes)))
    headers = headers[:top]
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["path", "fanin", "fanout", "size_bytes", "impact_score"])
        for n in headers:
            w.writerow([n.path, n.fanin, n.fanout, n.size_bytes, n.fanin * max(1, n.size_bytes)])


def write_summary_md(
    verdicts: Iterable[Verdict],
    graph: Graph,
    out_path: Path,
    top: int = 100,
) -> None:
    by_verdict: Dict[str, int] = defaultdict(int)
    candidates: List[Verdict] = []
    for v in verdicts:
        by_verdict[v.verdict] += 1
        if v.verdict in (VERDICT_REMOVE, VERDICT_MOVE_TO_CPP):
            candidates.append(v)

    def impact(v: Verdict) -> int:
        node = graph.nodes.get(v.in_file)
        fanin = node.fanin if node else 0
        return fanin

    candidates.sort(key=impact, reverse=True)
    candidates = candidates[:top]

    lines: List[str] = []
    lines.append("# ydb include sanitizer report")
    lines.append("")
    lines.append("## Summary by verdict")
    lines.append("")
    lines.append("| verdict | count |")
    lines.append("|---|---:|")
    for k in (
        VERDICT_REMOVE,
        VERDICT_MOVE_TO_CPP,
        VERDICT_KEEP,
        VERDICT_KEEP_DEFERRED,
        VERDICT_KEEP_PRAGMA,
        VERDICT_UNKNOWN,
        VERDICT_PROBE_FAILED,
        VERDICT_CYCLE,
    ):
        lines.append(f"| `{k}` | {by_verdict.get(k, 0)} |")
    lines.append("")

    headers = [n for n in graph.nodes.values() if n.kind == "header"]
    headers.sort(key=lambda n: -(n.fanin * max(1, n.size_bytes)))
    lines.append("## Hot headers (top 25 by fanin*size)")
    lines.append("")
    lines.append("| header | fanin | size B | impact |")
    lines.append("|---|---:|---:|---:|")
    for n in headers[:25]:
        impact_val = n.fanin * max(1, n.size_bytes)
        lines.append(f"| `{n.path}` | {n.fanin} | {n.size_bytes} | {impact_val} |")
    lines.append("")

    lines.append(f"## Top {len(candidates)} candidate cleanups")
    lines.append("")
    lines.append("Each row is a single `#include` directive whose removal "
                 "would not break any header consumer; for `move-to-cpp` "
                 "verdicts a non-empty list of `.cpp` files would need to "
                 "gain the include.")
    lines.append("")
    lines.append("| header | line | included | verdict | TU consumers needing |")
    lines.append("|---|---:|---|---|---:|")
    for v in candidates:
        cons = len(v.consumers_needing)
        lines.append(f"| `{v.in_file}` | {v.line} | `{v.spelled}` | `{v.verdict}` | {cons} |")
    lines.append("")

    lines.append("## How to read this")
    lines.append("")
    lines.append("- `remove`: the include is unused by the header and by every "
                 "downstream consumer reachable through this header.")
    lines.append("- `move-to-cpp`: only some `.cpp` consumers use symbols from "
                 "the included file; suggest moving the include into those "
                 "`.cpp` files. See `reports/diffs/*.patch`.")
    lines.append("- `keep`: the header itself uses symbols from the included file.")
    lines.append("- `keep-deferred`: another header (not a TU) still needs this "
                 "include via this header. Recurse later.")
    lines.append("- `keep-pragma`: an `IWYU pragma: keep` or `export` is present.")
    lines.append("- `unknown`: clang-include-cleaner did not give a definitive "
                 "verdict (often macro-driven; investigate manually).")
    lines.append("- `probe-failed`: clang-include-cleaner could not even parse "
                 "the header in isolation (typical for non-self-contained "
                 "headers that rely on the including TU to pre-include a "
                 "sibling). No recommendation possible.")
    lines.append("- `include-cycle`: the header is re-included through its own "
                 "dependency chain (e.g. an umbrella `defs.h` that includes "
                 "it back) or self-includes, so it cannot be analyzed as a "
                 "standalone main file. This is NOT a header bug — it "
                 "compiles fine when a normal .cpp includes it once.")
    lines.append("")

    out_path.write_text("\n".join(lines), encoding="utf-8")
