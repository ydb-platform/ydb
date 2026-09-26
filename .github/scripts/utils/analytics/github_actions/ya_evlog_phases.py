#!/usr/bin/env python3
"""Intervals inside one ya_make_try, from evlog node-finished events.

Same nodes `ya analyze-make timeline --evlog` draws.
Local compile and link are Compile/Link nodes and Run nodes whose path is an
object or archive (.o, .a, .obj, .so). ya_build is that work before the first
test. ya_rebuild is the same work after tests have started. ya_tests is every
other Run, with those build intervals cut out. Cache fetch and cache put are
not build.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

BUILD_KINDS = (
    "CompileAndLink",
    "SharedLibrary",
    "Preprocess",
    "Compile",
    "Link",
    "Archive",
)
GAP_SEC = 15.0
MIN_SEC = 1.0
Phase = Tuple[str, float, float]


def _span(value: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    raw = value.get("time")
    if not (isinstance(raw, list) and len(raw) == 2):
        return None
    try:
        start = float(raw[0])
        end = float(raw[1])
    except (TypeError, ValueError):
        return None
    if end < start:
        return None
    return start, end


_BUILD_SUFFIXES = (".o", ".a", ".obj", ".so", ".dylib")


def _build_path(name: str) -> bool:
    path = name
    if "$(BUILD_ROOT)" in path:
        path = path.split("$(BUILD_ROOT)", 1)[1]
    path = path.rstrip(")").lower()
    return path.endswith(_BUILD_SUFFIXES)


def _kind(name: str) -> Optional[str]:
    for prefix in BUILD_KINDS:
        if name == prefix or name.startswith(prefix + " ") or name.startswith(prefix + "("):
            return "build"
    if name.startswith("Run("):
        return "build" if _build_path(name) else "test"
    return None


def _merge(spans: Sequence[Tuple[float, float]], gap: float) -> List[Tuple[float, float]]:
    if not spans:
        return []
    ordered = sorted(spans)
    start, end = ordered[0]
    merged: List[Tuple[float, float]] = []
    for nxt_start, nxt_end in ordered[1:]:
        if nxt_start <= end + gap:
            end = max(end, nxt_end)
            continue
        if end - start >= MIN_SEC:
            merged.append((start, end))
        start, end = nxt_start, nxt_end
    if end - start >= MIN_SEC:
        merged.append((start, end))
    return merged


def _subtract(
    spans: Sequence[Tuple[float, float]],
    cuts: Sequence[Tuple[float, float]],
) -> List[Tuple[float, float]]:
    remaining: List[Tuple[float, float]] = []
    for start, end in spans:
        pieces = [(start, end)]
        for cut_start, cut_end in cuts:
            nxt: List[Tuple[float, float]] = []
            for piece_start, piece_end in pieces:
                if cut_end <= piece_start or cut_start >= piece_end:
                    nxt.append((piece_start, piece_end))
                    continue
                if cut_start > piece_start:
                    nxt.append((piece_start, cut_start))
                if cut_end < piece_end:
                    nxt.append((cut_end, piece_end))
            pieces = nxt
        for piece_start, piece_end in pieces:
            if piece_end - piece_start >= MIN_SEC:
                remaining.append((piece_start, piece_end))
    return remaining


def phases_from_events(events: Iterable[Dict[str, Any]], *, gap: float = GAP_SEC) -> List[Phase]:
    builds: List[Tuple[float, float]] = []
    tests: List[Tuple[float, float]] = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        if ev.get("namespace") != "worker_threads" or ev.get("event") != "node-finished":
            continue
        value = ev.get("value") if isinstance(ev.get("value"), dict) else {}
        span = _span(value)
        if span is None:
            continue
        kind = _kind(str(value.get("name") or ""))
        if kind == "test":
            tests.append(span)
        elif kind == "build":
            builds.append(span)
    first_test = min((start for start, _end in tests), default=None)
    early: List[Tuple[float, float]] = []
    late: List[Tuple[float, float]] = []
    for start, end in builds:
        if first_test is None or end <= first_test:
            early.append((start, end))
        elif start >= first_test:
            late.append((start, end))
        else:
            early.append((start, first_test))
            late.append((first_test, end))
    build_spans = _merge(early, gap)
    rebuild_spans = _merge(late, gap)
    test_spans = _subtract(_merge(tests, gap), build_spans + rebuild_spans)
    found: List[Phase] = []
    found.extend(("ya_build", start, end) for start, end in build_spans)
    found.extend(("ya_rebuild", start, end) for start, end in rebuild_spans)
    found.extend(("ya_tests", start, end) for start, end in test_spans)
    found.sort(key=lambda item: item[1])
    return found


def phases_from_path(path: str, *, gap: float = GAP_SEC) -> List[Phase]:
    def events() -> Iterable[Dict[str, Any]]:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                try:
                    ev = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if isinstance(ev, dict):
                    yield ev

    return phases_from_events(events(), gap=gap)


def _parent_span_id(path: str, name: str, ya_attempt: str) -> Optional[str]:
    found = None
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                row = json.loads(text)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict) or row.get("name") != name:
                continue
            labels = row.get("labels") if isinstance(row.get("labels"), dict) else {}
            if ya_attempt and str(labels.get("ya_attempt", "")) != ya_attempt:
                continue
            span_id = row.get("span_id")
            if span_id:
                found = str(span_id)
    return found


def record(path: str, parent_name: str, ya_attempt: str, found: Sequence[Phase]) -> int:
    from ci_metrics import track

    parent = _parent_span_id(path, parent_name, ya_attempt) if path else None
    for name, start, end in found:
        labels = ["ya_attempt=" + ya_attempt] if ya_attempt else []
        if parent:
            labels.append("parent_span_id=" + parent)
        track(
            name,
            {item.split("=", 1)[0]: item.split("=", 1)[1] for item in labels},
            source="ya_phase",
            kind="duration",
            started_epoch=str(start),
            finished_epoch=str(end),
            conclusion="success",
            file=path or None,
        )
    return len(found)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Record ya_build, ya_rebuild, and ya_tests inside a try")
    parser.add_argument("--evlog", required=True)
    parser.add_argument("--parent", required=True, help="ya_make_try span name")
    parser.add_argument("--ya-attempt", default="")
    parser.add_argument("--file", default=None, help="metrics JSONL; default is CI_METRICS_FILE")
    args = parser.parse_args(argv)
    try:
        found = phases_from_path(args.evlog)
        if not found:
            return 0
        from ci_metrics import default_metrics_file

        metrics = args.file or default_metrics_file()
        record(metrics, args.parent, str(args.ya_attempt or ""), found)
    except Exception as exc:  # noqa: BLE001 — telemetry must not fail CI
        print(f"Warning: ya evlog phases failed: {exc}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
