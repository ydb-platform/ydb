#!/usr/bin/env python3
"""Split one ya graph into shard graphs for Run-tests.

Policy lives here and only here. ``shard_count=1`` never calls this module:
the workflow keeps the single ``build_and_test_ya`` job.

A shard is a partition of ``graph.result``. Every result UID is assigned to
exactly one shard. ``ya make --build-custom-json`` runs every UID in
``result``, so the filtered graph's result list is that shard and nothing
else. Dependency nodes stay in the graph so the shard can still build.

Weights are timeout-budget times ya CPU slots (``requirements.cpu``,
``all`` means the job's test thread count). History p90 is intentionally
not used: PR-check history is dominated by lint/import rows and needs a
live YDB connection. The budget is deterministic from the graph alone.

Assignment is deterministic: higher weight first, UID as a tie-break,
and a tied load goes to the lower shard index.
"""
from __future__ import annotations

import argparse
import copy
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

# Ya SIZE default timeouts (build/plugins/lib/test_const TestSize.DefaultTimeouts).
# A node with no size tag is small, matching ya SIZE(SMALL), not medium.
DEFAULT_SIZE_WEIGHTS = {
    "small": 60.0,
    "medium": 600.0,
    "large": 3600.0,
}
DEFAULT_THREADS = 52

_TEST_KIND_LEAVES = frozenset(
    {
        "unittest",
        "py3test",
        "py2test",
        "pytest",
        "gtest",
        "flake8",
        "clang_format",
        "black",
        "import_test",
    }
)
_BUILD_ROOT_PATH_RE = re.compile(
    r"^\$\((?:BUILD_ROOT|SOURCE_ROOT)\)/"
    r"((?:ydb|yql|library|contrib|yt)/(?:[^/$]+(?:/[^/$]+)*))"
)
_TEST_RESULTS_OUT_RE = re.compile(
    r"^\$\((?:BUILD_ROOT|SOURCE_ROOT)\)/"
    r"((?:ydb|yql|library|contrib|yt)/(?:[^/$]+(?:/[^/$]+)*))/test-results/"
)
_DOTFILE_LEAF_RE = re.compile(r"(?:^|/)\.[^/]+$")
_SOURCE_FILE_LEAF_RE = re.compile(
    r"\.(?:py|pyi|json|ya?ml|toml|md|txt|proto|cpp|h|c|cc|hh|hpp|inc|sh)$",
    re.IGNORECASE,
)
_CONTEXT_SIZE_RE = re.compile(rb"SIZE\x94\x8c.([A-Z]+)\x94")


def validate_graph(graph: dict[str, Any]) -> None:
    if not isinstance(graph, dict):
        raise ValueError("graph must be a JSON object")
    if not isinstance(graph.get("result"), list):
        raise ValueError("graph missing 'result' list")
    if not isinstance(graph.get("graph"), list):
        raise ValueError("graph missing 'graph' node list")


def load_graph(path: Path) -> dict[str, Any]:
    graph = json.loads(path.read_text(encoding="utf-8"))
    validate_graph(graph)
    return graph


def result_uids(graph: dict[str, Any]) -> list[str]:
    return [str(uid) for uid in graph["result"]]


def graph_nodes_by_uid(graph: dict[str, Any]) -> dict[str, dict[str, Any]]:
    nodes: dict[str, dict[str, Any]] = {}
    for node in graph["graph"]:
        if isinstance(node, dict) and node.get("uid"):
            nodes[str(node["uid"])] = node
    return nodes


def _dep_uid(dep: Any) -> str | None:
    if isinstance(dep, str):
        return dep
    if isinstance(dep, dict) and dep.get("uid"):
        return str(dep["uid"])
    return None


def cpu_slots(node: dict[str, Any], threads: int) -> int:
    """Ya scheduler slots. ``requirements.cpu: all`` occupies every test thread."""
    slots_cap = max(int(threads), 1)
    req = node.get("requirements") if isinstance(node.get("requirements"), dict) else {}
    raw = req.get("cpu", 1)
    if isinstance(raw, str) and raw.strip().lower() == "all":
        return slots_cap
    try:
        cpu = int(raw)
    except (TypeError, ValueError):
        return 1
    if cpu < 1:
        return 1
    return min(cpu, slots_cap)


def _cmd_args(node: dict[str, Any]) -> list[str]:
    args: list[str] = []
    for cmd in node.get("cmds") or []:
        if isinstance(cmd, dict):
            args.extend(str(arg) for arg in cmd.get("cmd_args") or [])
    return args


def extract_node_test_size(node: dict[str, Any]) -> str | None:
    args = _cmd_args(node)
    for index, arg in enumerate(args):
        if arg == "--test-size" and index + 1 < len(args):
            size = args[index + 1].strip().lower()
            if size in DEFAULT_SIZE_WEIGHTS:
                return size
    return None


def extract_node_timeout_sec(node: dict[str, Any]) -> float | None:
    args = _cmd_args(node)
    for index, arg in enumerate(args):
        if arg == "--timeout" and index + 1 < len(args):
            try:
                value = float(args[index + 1])
            except ValueError:
                continue
            if value > 0:
                return value
    return None


def load_test_sizes_from_context(context: dict[str, Any] | None) -> dict[str, str]:
    """SIZE(...) pickled inside context.json test blobs, keyed by test UID."""
    if not context:
        return {}
    tests = context.get("tests")
    if not isinstance(tests, dict):
        return {}
    sizes: dict[str, str] = {}
    for uid, payload in tests.items():
        if isinstance(payload, str):
            raw = payload.encode("latin1", errors="ignore")
        elif isinstance(payload, bytes):
            raw = payload
        else:
            continue
        match = _CONTEXT_SIZE_RE.search(raw)
        if not match:
            continue
        size = match.group(1).decode("ascii", errors="ignore").lower()
        if size in DEFAULT_SIZE_WEIGHTS:
            sizes[str(uid)] = size
    return sizes


def resolve_node_test_size(
    uid: str,
    node: dict[str, Any],
    size_by_uid: dict[str, str],
) -> str:
    size = extract_node_test_size(node)
    if size is None:
        size = size_by_uid.get(uid)
    if size in DEFAULT_SIZE_WEIGHTS:
        return size
    return "small"


def count_timeout_budget_units(
    node: dict[str, Any],
    nodes_by_uid: dict[str, dict[str, Any]],
    result_set: set[str],
) -> int:
    """Chunked suites keep ``run_test`` nodes out of ``graph.result``.

    Each such dependency is one parallel work unit with the suite timeout.
    A leaf result node counts as one unit.
    """
    units = 0
    for dep in node.get("deps") or []:
        dep_uid = _dep_uid(dep)
        if not dep_uid or dep_uid in result_set:
            continue
        dep_node = nodes_by_uid.get(dep_uid) or {}
        if "run_test" in _cmd_args(dep_node):
            units += 1
    return units if units > 0 else 1


def resolve_timeout_sec(
    uid: str,
    node: dict[str, Any],
    nodes_by_uid: dict[str, dict[str, Any]],
    result_set: set[str],
    size_by_uid: dict[str, str],
) -> tuple[float, str]:
    timeout = extract_node_timeout_sec(node)
    if timeout is None:
        for dep in node.get("deps") or []:
            dep_uid = _dep_uid(dep)
            if not dep_uid or dep_uid in result_set:
                continue
            dep_node = nodes_by_uid.get(dep_uid) or {}
            if "run_test" not in _cmd_args(dep_node):
                continue
            timeout = extract_node_timeout_sec(dep_node)
            if timeout is not None:
                size = resolve_node_test_size(dep_uid, dep_node, size_by_uid)
                return timeout, size
    size = resolve_node_test_size(uid, node, size_by_uid)
    if timeout is None:
        timeout = DEFAULT_SIZE_WEIGHTS[size]
    return timeout, size


def uid_weight(
    uid: str,
    node: dict[str, Any],
    nodes_by_uid: dict[str, dict[str, Any]],
    result_set: set[str],
    size_by_uid: dict[str, str],
    threads: int,
) -> tuple[float, str, int]:
    units = count_timeout_budget_units(node, nodes_by_uid, result_set)
    timeout, size = resolve_timeout_sec(uid, node, nodes_by_uid, result_set, size_by_uid)
    slots = cpu_slots(node, threads)
    return float(units) * float(timeout) * float(slots), size, units


def strip_test_kind_leaf(path: str) -> str:
    cleaned = path.strip().rstrip("/")
    if "/" not in cleaned:
        return cleaned
    parent, leaf = cleaned.rsplit("/", 1)
    if leaf in _TEST_KIND_LEAVES:
        return parent
    return cleaned


def extract_node_path(node: dict[str, Any]) -> str | None:
    """Suite folder for the plan summary. Not used as the partition key."""
    module_dir = (node.get("target_properties") or {}).get("module_dir")
    if isinstance(module_dir, str) and module_dir.strip():
        return module_dir.strip().rstrip("/")

    kv_path = (node.get("kv") or {}).get("path")
    if isinstance(kv_path, str) and kv_path.strip():
        return strip_test_kind_leaf(kv_path)

    for out in node.get("outputs") or []:
        if isinstance(out, str):
            match = _TEST_RESULTS_OUT_RE.match(out)
            if match:
                return match.group(1)

    for inp in node.get("inputs") or []:
        if not isinstance(inp, str):
            continue
        match = _BUILD_ROOT_PATH_RE.match(inp)
        if not match:
            continue
        path = match.group(1)
        if _DOTFILE_LEAF_RE.search(path) or _SOURCE_FILE_LEAF_RE.search(path):
            continue
        return strip_test_kind_leaf(path)
    return None


def bin_pack(weights: dict[str, float], shard_count: int) -> tuple[list[list[str]], list[float]]:
    """Longest-processing-time pack. Sort and tie-break are part of the contract."""
    if shard_count < 1:
        raise ValueError("shard_count must be >= 1")
    ordered = sorted(weights.items(), key=lambda item: (-item[1], item[0]))
    buckets: list[list[str]] = [[] for _ in range(shard_count)]
    loads = [0.0] * shard_count
    for uid, weight in ordered:
        target = min(range(shard_count), key=lambda index: (loads[index], index))
        buckets[target].append(uid)
        loads[target] += weight
    return buckets, loads


def build_plan(
    graph: dict[str, Any],
    shard_count: int,
    *,
    threads: int = DEFAULT_THREADS,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Partition every result UID. Raises if the graph has nothing to run."""
    validate_graph(graph)
    if shard_count < 1:
        raise ValueError("shard_count must be >= 1")
    if threads < 1:
        raise ValueError("threads must be >= 1")

    nodes_by_uid = graph_nodes_by_uid(graph)
    uids = result_uids(graph)
    if not uids:
        raise ValueError("graph.result is empty; refusing to plan zero tests")

    active_shards = min(shard_count, len(uids))
    result_set = set(uids)
    size_by_uid = load_test_sizes_from_context(context)
    weights: dict[str, float] = {}
    size_counts: Counter[str] = Counter()
    total_units = 0
    for uid in uids:
        node = nodes_by_uid.get(uid) or {}
        weight, size, units = uid_weight(uid, node, nodes_by_uid, result_set, size_by_uid, threads)
        weights[uid] = weight
        size_counts[size] += 1
        total_units += units

    buckets, loads = bin_pack(weights, active_shards)
    assignments: dict[str, int] = {}
    shards: list[dict[str, Any]] = []
    for shard_id, shard_uids in enumerate(buckets):
        if not shard_uids:
            raise ValueError(f"internal error: shard {shard_id} is empty")
        sample_paths: list[str] = []
        seen_paths: set[str] = set()
        for uid in shard_uids:
            assignments[uid] = shard_id
            path = extract_node_path(nodes_by_uid.get(uid) or {})
            if path and path not in seen_paths:
                seen_paths.add(path)
                sample_paths.append(path)
        shards.append(
            {
                "id": shard_id,
                "result_node_count": len(shard_uids),
                "balance_weight": round(loads[shard_id], 1),
                "sample_paths": sorted(sample_paths)[:8],
            }
        )

    _assert_partition(uids, assignments, active_shards)
    total_weight = sum(weights.values())
    return {
        "requested_shard_count": shard_count,
        "shard_count": active_shards,
        "threads": threads,
        "total_result_nodes": len(uids),
        "total_weight": round(total_weight, 1),
        "weighting": {
            "mode": "timeout_budget_x_cpu",
            "timeout_budget_units": total_units,
            "size_small": size_counts["small"],
            "size_medium": size_counts["medium"],
            "size_large": size_counts["large"],
        },
        "uid_assignments": assignments,
        "shards": shards,
    }


def _assert_partition(uids: list[str], assignments: dict[str, int], shard_count: int) -> None:
    if len(assignments) != len(uids) or set(assignments) != set(uids):
        raise ValueError("shard plan does not cover every graph.result UID exactly once")
    if len(set(uids)) != len(uids):
        raise ValueError("graph.result contains duplicate UIDs")
    bad = [uid for uid, shard_id in assignments.items() if shard_id < 0 or shard_id >= shard_count]
    if bad:
        raise ValueError(f"assignment outside 0..{shard_count - 1} for {bad[:5]}")


def collect_dep_closure(nodes_by_uid: dict[str, dict[str, Any]], roots: set[str]) -> set[str]:
    stack = list(roots)
    seen: set[str] = set()
    while stack:
        uid = stack.pop()
        if uid in seen:
            continue
        seen.add(uid)
        node = nodes_by_uid.get(uid)
        if not node:
            continue
        for dep in node.get("deps") or []:
            dep_uid = _dep_uid(dep)
            if dep_uid:
                stack.append(dep_uid)
    return seen


def filter_graph_result(graph: dict[str, Any], allowed_uids: set[str]) -> dict[str, Any]:
    """Keep allowed result UIDs and the nodes they depend on."""
    validate_graph(graph)
    filtered = copy.deepcopy(graph)
    filtered_result = [uid for uid in result_uids(graph) if uid in allowed_uids]
    filtered["result"] = filtered_result
    keep = collect_dep_closure(graph_nodes_by_uid(graph), set(filtered_result))
    filtered["graph"] = [
        node
        for node in graph["graph"]
        if isinstance(node, dict) and str(node.get("uid") or "") in keep
    ]
    return filtered


def filter_context_tests(context: dict[str, Any], allowed_test_uids: set[str]) -> dict[str, Any]:
    filtered = copy.deepcopy(context)
    tests = filtered.get("tests")
    if isinstance(tests, dict):
        filtered["tests"] = {
            uid: payload for uid, payload in tests.items() if str(uid) in allowed_test_uids
        }
    return filtered


def assignments_for_shard(plan: dict[str, Any], graph: dict[str, Any], shard_id: int) -> set[str]:
    raw = plan.get("uid_assignments")
    if not isinstance(raw, dict) or not raw:
        raise ValueError("shard plan has no uid_assignments")
    uids = result_uids(graph)
    assignments = {str(uid): int(assigned) for uid, assigned in raw.items()}
    if set(assignments) != set(uids):
        missing = [uid for uid in uids if uid not in assignments]
        extra = [uid for uid in assignments if uid not in set(uids)]
        raise ValueError(
            "plan does not match graph.result "
            f"(missing {len(missing)}, extra {len(extra)}); regenerate the plan"
        )
    allowed = {uid for uid, assigned in assignments.items() if assigned == shard_id}
    if not allowed:
        raise ValueError(f"shard {shard_id} has no graph result nodes")
    return allowed


def render_summary(plan: dict[str, Any]) -> str:
    lines = [
        "## Shard plan",
        "",
        f"**Shards:** {plan['shard_count']} (requested {plan['requested_shard_count']})",
        f"**Result nodes:** {plan['total_result_nodes']}, weight {plan['total_weight']}",
        f"**Weighting:** {plan['weighting']['mode']}, threads {plan['threads']}",
        "",
        "| Shard | Result nodes | Weight | Sample paths |",
        "| ---: | ---: | ---: | --- |",
    ]
    for shard in plan["shards"]:
        sample = ", ".join(f"`{path}`" for path in shard["sample_paths"][:3])
        extra = len(shard["sample_paths"]) - 3
        if extra > 0:
            sample += f", … (+{extra})"
        lines.append(
            f"| {shard['id']} | {shard['result_node_count']} | {shard['balance_weight']} | {sample} |"
        )
    lines.append("")
    return "\n".join(lines)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _cmd_plan(args: argparse.Namespace) -> int:
    graph = load_graph(args.graph)
    context = None
    if args.context:
        context = json.loads(args.context.read_text(encoding="utf-8"))
    plan = build_plan(graph, args.shard_count, threads=args.threads, context=context)
    _write_json(args.output, plan)
    summary = render_summary(plan)
    if args.summary:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        args.summary.write_text(summary, encoding="utf-8")
    sys.stdout.write(summary)
    return 0


def _cmd_filter(args: argparse.Namespace) -> int:
    graph = load_graph(args.graph)
    plan = json.loads(args.plan.read_text(encoding="utf-8"))
    allowed = assignments_for_shard(plan, graph, args.shard_id)
    filtered = filter_graph_result(graph, allowed)
    _write_json(args.output, filtered)
    if args.context or args.context_output:
        if not args.context or not args.context_output:
            print("filter: --context and --context-output are both required", file=sys.stderr)
            return 2
        context = json.loads(args.context.read_text(encoding="utf-8"))
        test_uids = {uid for uid in allowed if uid.startswith("test-")}
        _write_json(args.context_output, filter_context_tests(context, test_uids))
    print(
        f"Shard {args.shard_id}: {len(filtered['result'])}/{len(result_uids(graph))} result nodes",
        file=sys.stderr,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Plan or filter a ya graph for Run-tests shards.")
    sub = parser.add_subparsers(dest="command", required=True)

    plan = sub.add_parser("plan", help="Write shard_plan.json")
    plan.add_argument("--graph", type=Path, required=True)
    plan.add_argument("--context", type=Path)
    plan.add_argument("--shard-count", type=int, required=True)
    plan.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    plan.add_argument("-o", "--output", type=Path, required=True)
    plan.add_argument("--summary", type=Path)
    plan.set_defaults(func=_cmd_plan)

    filt = sub.add_parser("filter", help="Write one shard graph")
    filt.add_argument("--graph", type=Path, required=True)
    filt.add_argument("--plan", type=Path, required=True)
    filt.add_argument("--shard-id", type=int, required=True)
    filt.add_argument("--context", type=Path)
    filt.add_argument("-o", "--output", type=Path, required=True)
    filt.add_argument("--context-output", type=Path)
    filt.set_defaults(func=_cmd_filter)

    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
