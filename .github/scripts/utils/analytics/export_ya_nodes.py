#!/usr/bin/env python3
"""Copy raw ya/clang node timings into the ci_metrics JSONL buffer.

No ranking, totals, or diffs — those belong in YDB queries.
Sources:
  --evlog         ya evlog (graph nodes: Compile / Link / ...)
  --cpp-json      build_bloat html_cpp_impact/output.json (clang -ftime-trace)
  --headers-json  build_bloat html_headers_impact/output.json

Never fails the caller (exit 0).
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ci_metrics import BUILD_INFO_NAME, track

NODE_KIND_RE = re.compile(
    r"^(CompileAndLink|SharedLibrary|Preprocess|Compile|Link|Archive)\b"
)
KEEP_NODE_KINDS = frozenset(
    {
        "Compile",
        "Link",
        "CompileAndLink",
        "Archive",
        "SharedLibrary",
        "Preprocess",
    }
)
BUILD_ROOT_RE = re.compile(r"\$\(BUILD_ROOT\)/|\$B/")


def parse_node_name(raw: str) -> Tuple[str, str]:
    text = (raw or "").strip()
    match = NODE_KIND_RE.match(text)
    kind = match.group(1) if match else "Node"
    path = text
    if "(" in text and text.endswith(")"):
        path = text[text.find("(") + 1 : -1]
    path = BUILD_ROOT_RE.sub("", path).strip().strip("'\"")
    if path:
        path = path.split()[0]
    return kind, path or text or "unknown"


def _duration_ms_from_time_range(time_range: Any) -> Optional[float]:
    if not (isinstance(time_range, list) and len(time_range) == 2):
        return None
    try:
        start = float(time_range[0])
        end = float(time_range[1])
    except (TypeError, ValueError):
        return None
    if end <= start:
        return None
    # ya evlog `time` is seconds
    return (end - start) * 1000.0


def nodes_from_evlog(events: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Parse ya evlog events into {name, node_kind, duration_ms, raw_name}.

    Prefer `node-finished` rows when present so a mixed chrome+evlog file is
    not written twice. Duration is the raw interval from the event.
    """
    finished: List[Dict[str, Any]] = []
    chrome: List[Dict[str, Any]] = []
    stacks: Dict[Tuple[Any, Any], List[Dict[str, Any]]] = {}

    for ev in events:
        if not isinstance(ev, dict):
            continue

        if "ph" not in ev and ev.get("namespace") == "worker_threads" and ev.get("event") == "node-finished":
            value = ev.get("value") if isinstance(ev.get("value"), dict) else {}
            raw_name = str(value.get("name") or "")
            if raw_name.startswith("Run(") or raw_name.startswith("Result("):
                continue
            kind, path = parse_node_name(raw_name)
            if kind not in KEEP_NODE_KINDS:
                continue
            duration_ms = _duration_ms_from_time_range(value.get("time"))
            if duration_ms is None:
                continue
            finished.append(
                {
                    "name": path,
                    "node_kind": kind,
                    "duration_ms": duration_ms,
                    "raw_name": raw_name,
                }
            )
            continue

        ph = ev.get("ph")
        if ph not in ("B", "E"):
            continue
        key = (ev.get("pid"), ev.get("tid"))
        if ph == "B":
            args = ev.get("args") if isinstance(ev.get("args"), dict) else {}
            raw_name = str(args.get("name") or ev.get("name") or "")
            kind, path = parse_node_name(raw_name or str(ev.get("name") or ""))
            if kind not in KEEP_NODE_KINDS:
                continue
            stacks.setdefault(key, []).append(
                {
                    "ts": ev.get("ts"),
                    "name": path,
                    "node_kind": kind,
                    "raw_name": raw_name or str(ev.get("name") or ""),
                }
            )
            continue
        stack = stacks.get(key)
        if not stack:
            continue
        start = stack.pop()
        try:
            start_ts = float(start.get("ts"))
            end_ts = float(ev.get("ts"))
        except (TypeError, ValueError):
            continue
        # Chrome evlog timestamps are microseconds
        duration_ms = max((end_ts - start_ts) / 1000.0, 0.0)
        if duration_ms <= 0:
            continue
        chrome.append(
            {
                "name": start["name"],
                "node_kind": start["node_kind"],
                "duration_ms": duration_ms,
                "raw_name": start["raw_name"],
            }
        )
    return finished or chrome


def nodes_from_cpp_json(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Per-file clang -ftime-trace rows from html_cpp_impact/output.json."""
    nodes: List[Dict[str, Any]] = []
    for entry in payload.get("cpp_compilation_times") or []:
        if not isinstance(entry, dict):
            continue
        path = str(entry.get("path") or "").strip()
        try:
            time_s = float(entry.get("time_s"))
        except (TypeError, ValueError):
            continue
        if not path or time_s < 0:
            continue
        nodes.append(
            {
                "name": path,
                "node_kind": "Compile",
                "duration_ms": time_s * 1000.0,
                "raw_name": path,
            }
        )
    return nodes


def nodes_from_headers_json(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Raw per-header mean time + inclusion_count from headers output.json."""
    nodes: List[Dict[str, Any]] = []
    for entry in payload.get("headers_compile_duration") or []:
        if not isinstance(entry, dict):
            continue
        path = str(entry.get("path") or "").strip()
        try:
            mean_s = float(entry.get("mean_compilation_time_s"))
        except (TypeError, ValueError):
            continue
        try:
            count = int(entry.get("inclusion_count") or 0)
        except (TypeError, ValueError):
            count = 0
        if not path or mean_s < 0:
            continue
        nodes.append(
            {
                "name": path,
                "node_kind": "Header",
                "duration_ms": mean_s * 1000.0,
                "raw_name": path,
                "inclusion_count": count,
            }
        )
    return nodes


_TRY_DIR_RE = re.compile(r"(?:^|/)try_(\d+)(?:/|$)")


def try_dir_index(path: str) -> int:
    match = _TRY_DIR_RE.search(path.replace("\\", "/"))
    return int(match.group(1)) if match else -1


def resolve_input_files(pattern: str) -> List[str]:
    """Accept a concrete path or a glob; missing patterns yield []."""
    if not pattern:
        return []
    if os.path.isfile(pattern):
        return [pattern]
    return [path for path in sorted(glob.glob(pattern)) if os.path.isfile(path)]


def pick_latest_file(paths: List[str]) -> Optional[str]:
    """Prefer the highest-numbered try_N path when several matches exist."""
    if not paths:
        return None
    return max(paths, key=lambda path: (try_dir_index(path), path))


def load_jsonl_objects(path: str) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    with open(path, encoding="utf-8") as handle:
        first = handle.read(1)
        handle.seek(0)
        if first == "[":
            payload = json.load(handle)
            if isinstance(payload, list):
                events.extend(item for item in payload if isinstance(item, dict))
            return events
        handle.seek(0)
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                item = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                events.append(item)
    return events


def _compact_node(node: Dict[str, Any]) -> Dict[str, Any]:
    compact = {
        "name": node["name"],
        "node_kind": node["node_kind"],
        "duration_ms": node["duration_ms"],
    }
    raw_name = node.get("raw_name")
    if raw_name and raw_name != node["name"]:
        compact["raw_name"] = raw_name
    if node.get("inclusion_count") is not None:
        compact["inclusion_count"] = node["inclusion_count"]
    return compact


def write_build_info(
    nodes: List[Dict[str, Any]],
    *,
    source: str,
    file: Optional[str] = None,
    extra_labels: Optional[Dict[str, Any]] = None,
    origin: Optional[str] = None,
) -> None:
    """One kind=info snapshot of every component (sibling of per-node durations)."""
    payload: Dict[str, Any] = {
        "schema": "ya_nodes",
        "nodes": [_compact_node(node) for node in nodes],
    }
    if origin:
        payload["origin"] = origin
    properties: Dict[str, Any] = {"payload": payload}
    if extra_labels:
        properties.update(extra_labels)
    track(BUILD_INFO_NAME, properties, file=file, kind="info", source=source)


def write_nodes(
    nodes: List[Dict[str, Any]],
    *,
    source: str,
    file: Optional[str] = None,
    extra_labels: Optional[Dict[str, Any]] = None,
    build_info: bool = True,
    origin: Optional[str] = None,
) -> int:
    count = 0
    for node in nodes:
        properties = {"node_kind": node["node_kind"], "raw_name": node.get("raw_name") or node["name"]}
        if node.get("inclusion_count") is not None:
            properties["inclusion_count"] = node["inclusion_count"]
        if extra_labels:
            properties.update(extra_labels)
        track(
            node["name"],
            properties,
            file=file,
            kind="duration",
            source=source,
            value=node["duration_ms"],
            unit="ms",
        )
        count += 1
    if build_info and nodes:
        write_build_info(nodes, source=source, file=file, extra_labels=extra_labels, origin=origin)
        count += 1
    return count


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Copy raw ya/clang node timings to ci_metrics")
    parser.add_argument("--evlog", default=None, help="ya evlog json/jsonl or glob (last match wins)")
    parser.add_argument("--cpp-json", default=None, help="build_bloat html_cpp_impact/output.json")
    parser.add_argument("--headers-json", default=None, help="build_bloat html_headers_impact/output.json")
    parser.add_argument("--source", default="ya_node")
    parser.add_argument("--file", default=None, help="JSONL path (default: $CI_METRICS_FILE)")
    parser.add_argument("--label", action="append", default=[], help="key=value extra labels")
    parser.add_argument(
        "--build-info",
        dest="build_info",
        action="store_true",
        default=True,
        help="Also write one kind=info build_info snapshot of all nodes (default)",
    )
    parser.add_argument(
        "--no-build-info",
        dest="build_info",
        action="store_false",
        help="Skip the build_info snapshot; keep per-node duration rows only",
    )
    return parser.parse_args(argv)


def _labels_from_args(items: List[str]) -> Dict[str, str]:
    labels: Dict[str, str] = {}
    for item in items:
        if not item or "=" not in item:
            continue
        key, value = item.split("=", 1)
        if key.strip():
            labels[key.strip()] = value
    cache_mode = os.environ.get("CI_CACHE_MODE")
    if cache_mode and "cache_mode" not in labels:
        labels["cache_mode"] = cache_mode
    return labels


def main(argv=None) -> int:
    try:
        args = parse_args(argv)
        extra = _labels_from_args(args.label)
        written = 0
        has_input = bool(args.evlog or args.cpp_json or args.headers_json)
        if args.evlog:
            evlog_path = pick_latest_file(resolve_input_files(args.evlog))
            if evlog_path is None:
                print(f"No evlog at {args.evlog!r}, skipping")
            else:
                nodes = nodes_from_evlog(load_jsonl_objects(evlog_path))
                written += write_nodes(
                    nodes,
                    source=args.source,
                    file=args.file,
                    extra_labels=extra,
                    build_info=args.build_info,
                    origin=evlog_path,
                )
                print(f"Wrote {len(nodes)} ya graph nodes from {evlog_path}")
        if args.cpp_json:
            if not os.path.exists(args.cpp_json):
                print(f"No cpp json at {args.cpp_json!r}, skipping")
            else:
                with open(args.cpp_json, encoding="utf-8") as handle:
                    payload = json.load(handle)
                nodes = nodes_from_cpp_json(payload if isinstance(payload, dict) else {})
                written += write_nodes(
                    nodes,
                    source=args.source,
                    file=args.file,
                    extra_labels=extra,
                    build_info=args.build_info,
                    origin=args.cpp_json,
                )
                print(f"Wrote {len(nodes)} cpp files from {args.cpp_json}")
        if args.headers_json:
            if not os.path.exists(args.headers_json):
                print(f"No headers json at {args.headers_json!r}, skipping")
            else:
                with open(args.headers_json, encoding="utf-8") as handle:
                    payload = json.load(handle)
                nodes = nodes_from_headers_json(payload if isinstance(payload, dict) else {})
                written += write_nodes(
                    nodes,
                    source=args.source,
                    file=args.file,
                    extra_labels=extra,
                    build_info=args.build_info,
                    origin=args.headers_json,
                )
                print(f"Wrote {len(nodes)} headers from {args.headers_json}")
        if not has_input:
            print("Nothing to write: pass --evlog, --cpp-json and/or --headers-json")
        elif written == 0:
            print("No raw node timings found")
        return 0
    except Exception as exc:  # noqa: BLE001 — telemetry must not fail CI
        print(f"Warning: export_ya_nodes failed: {exc}", file=sys.stderr)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
