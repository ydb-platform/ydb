"""Shared helpers for increment graph sharding (plan + filter)."""
from __future__ import annotations

import copy
import json
import re
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Any

_BUILD_ROOT_PATH_RE = re.compile(
    r"^\$\((?:BUILD_ROOT|SOURCE_ROOT)\)/"
    r"((?:ydb|yql|library|contrib|yt)/(?:[^/$]+(?:/[^/$]+)*))"
)

DEFAULT_SIZE_WEIGHTS = {"small": 60, "medium": 600}


def validate_graph(graph: dict[str, Any]) -> None:
    if not isinstance(graph, dict):
        raise ValueError("graph must be a JSON object")
    if "result" not in graph:
        raise ValueError("graph missing 'result' list")
    if "graph" not in graph:
        raise ValueError("graph missing 'graph' node list")


def load_graph(path: Path | dict[str, Any]) -> dict[str, Any]:
    if isinstance(path, dict):
        graph = path
    else:
        graph = json.loads(path.read_text(encoding="utf-8"))
    validate_graph(graph)
    return graph


def graph_nodes_by_uid(graph: dict[str, Any]) -> dict[str, dict[str, Any]]:
    nodes: dict[str, dict[str, Any]] = {}
    for node in graph.get("graph") or []:
        if isinstance(node, dict) and node.get("uid"):
            nodes[str(node["uid"])] = node
    return nodes


def result_uids(graph: dict[str, Any]) -> list[str]:
    return [str(uid) for uid in graph.get("result") or []]


def dependency_uids_in_result(node: dict[str, Any], result_set: set[str]) -> list[str]:
    deps: list[str] = []
    for dep in node.get("deps") or []:
        uid = dep if isinstance(dep, str) else dep.get("uid") if isinstance(dep, dict) else None
        if uid and str(uid) in result_set:
            deps.append(str(uid))
    return deps


def extract_node_path(node: dict[str, Any]) -> str | None:
    target_props = node.get("target_properties") or {}
    module_dir = target_props.get("module_dir")
    if isinstance(module_dir, str) and module_dir.strip():
        return module_dir.strip()

    for inp in node.get("inputs") or []:
        if not isinstance(inp, str):
            continue
        match = _BUILD_ROOT_PATH_RE.match(inp)
        if match:
            return match.group(1)
    return None


def connected_components(result: list[str], nodes_by_uid: dict[str, dict[str, Any]]) -> list[list[str]]:
    result_set = set(result)
    adjacency: dict[str, set[str]] = defaultdict(set)
    for uid in result:
        node = nodes_by_uid.get(uid)
        if not node:
            continue
        for dep in dependency_uids_in_result(node, result_set):
            adjacency[uid].add(dep)
            adjacency[dep].add(uid)

    components: list[list[str]] = []
    seen: set[str] = set()
    for start in result:
        if start in seen:
            continue
        queue: deque[str] = deque([start])
        seen.add(start)
        component: list[str] = []
        while queue:
            uid = queue.popleft()
            component.append(uid)
            for neighbor in adjacency.get(uid, ()):
                if neighbor not in seen:
                    seen.add(neighbor)
                    queue.append(neighbor)
        components.append(component)
    return components


def longest_history_match(
    path: str,
    duration_p50: dict[str, float],
    *,
    default_weight: float = 600.0,
) -> tuple[float, str, str | None]:
    best_suite: str | None = None
    best_p50: float | None = None
    for suite, p50 in duration_p50.items():
        if path == suite or path.startswith(f"{suite}/"):
            if best_suite is None or len(suite) > len(best_suite):
                best_suite = suite
                best_p50 = float(p50)
    if best_suite is not None and best_p50 is not None:
        return best_p50, "history", best_suite
    return default_weight, "fallback", None


def uid_weight(
    uid: str,
    nodes_by_uid: dict[str, dict[str, Any]],
    duration_p50: dict[str, float],
    *,
    path_counts: Counter[str] | None = None,
    default_weight: float = 600.0,
) -> float:
    """Estimate one result node weight (history p50 split evenly within a path)."""
    node = nodes_by_uid.get(uid)
    if not node:
        return default_weight
    path = extract_node_path(node)
    if not path:
        return default_weight
    weight, _, _ = longest_history_match(path, duration_p50, default_weight=default_weight)
    if path_counts is not None:
        count = path_counts.get(path, 1)
        if count > 1:
            return weight / count
    return weight


def uid_weights(
    uids: list[str],
    nodes_by_uid: dict[str, dict[str, Any]],
    duration_p50: dict[str, float],
    *,
    default_weight: float = 600.0,
) -> dict[str, float]:
    path_counts: Counter[str] = Counter()
    for uid in uids:
        path = extract_node_path(nodes_by_uid.get(uid, {}))
        if path:
            path_counts[path] += 1
    return {
        uid: uid_weight(
            uid,
            nodes_by_uid,
            duration_p50,
            path_counts=path_counts,
            default_weight=default_weight,
        )
        for uid in uids
    }


def component_weight(
    component: list[str],
    nodes_by_uid: dict[str, dict[str, Any]],
    duration_p50: dict[str, float],
    *,
    default_weight: float = 600.0,
) -> tuple[float, dict[str, float]]:
    per_path: dict[str, float] = {}
    for uid in component:
        node = nodes_by_uid.get(uid)
        if not node:
            continue
        path = extract_node_path(node)
        if not path or path in per_path:
            continue
        weight, _, _ = longest_history_match(path, duration_p50, default_weight=default_weight)
        per_path[path] = weight
    return sum(per_path.values()), per_path


def filter_graph_result(graph: dict[str, Any], allowed_uids: set[str]) -> dict[str, Any]:
    filtered = copy.deepcopy(graph)
    filtered["result"] = [uid for uid in result_uids(graph) if uid in allowed_uids]
    return filtered


def filter_context_tests(context: dict[str, Any], allowed_test_uids: set[str]) -> dict[str, Any]:
    filtered = copy.deepcopy(context)
    tests = filtered.get("tests")
    if isinstance(tests, dict):
        filtered["tests"] = {uid: payload for uid, payload in tests.items() if uid in allowed_test_uids}
    return filtered


def suite_to_shard_map(plan: dict[str, Any]) -> dict[str, int]:
    mapping: dict[str, int] = {}
    for shard in plan.get("shards") or []:
        shard_id = shard.get("id")
        if shard_id is None:
            continue
        for suite in shard.get("tests") or []:
            mapping[str(suite)] = int(shard_id)
    return mapping


def shard_ids(plan: dict[str, Any]) -> list[int]:
    ids = [int(shard["id"]) for shard in plan.get("shards") or [] if shard.get("id") is not None]
    if not ids:
        raise ValueError("shard plan contains no shard ids")
    return sorted(ids)


def match_shard_for_path(path: str | None, suite_to_shard: dict[str, int]) -> int | None:
    if not path:
        return None
    best_suite: str | None = None
    best_shard: int | None = None
    for suite, shard_id in suite_to_shard.items():
        if path == suite or path.startswith(f"{suite}/") or suite.startswith(f"{path}/"):
            if best_suite is None or len(suite) > len(best_suite):
                best_suite = suite
                best_shard = shard_id
    return best_shard


def choose_component_shard(
    component: list[str],
    nodes_by_uid: dict[str, dict[str, Any]],
    suite_to_shard: dict[str, int],
    load: Counter[int],
    shard_id_list: list[int],
) -> int:
    votes: Counter[int] = Counter()
    for uid in component:
        node = nodes_by_uid.get(uid)
        if not node:
            continue
        shard = match_shard_for_path(extract_node_path(node), suite_to_shard)
        if shard is not None:
            votes[shard] += 1
    if votes:
        max_votes = max(votes.values())
        candidates = [sid for sid, count in votes.items() if count == max_votes]
        return min(candidates, key=lambda sid: (load[sid], sid))
    return min(shard_id_list, key=lambda sid: (load[sid], sid))


def assign_result_uids_to_shards_legacy(plan: dict[str, Any], graph: dict[str, Any]) -> dict[str, int]:
    """Legacy suite-prefix assignment for suite-based shard plans."""
    suite_map = suite_to_shard_map(plan)
    ids = shard_ids(plan)
    nodes_by_uid = graph_nodes_by_uid(graph)
    uids = result_uids(graph)
    load: Counter[int] = Counter({sid: 0 for sid in ids})
    assignments: dict[str, int] = {}

    for component in connected_components(uids, nodes_by_uid):
        shard = choose_component_shard(component, nodes_by_uid, suite_map, load, ids)
        for uid in component:
            assignments[uid] = shard
            load[shard] += 1
    return assignments


assign_result_uids_to_shards = assign_result_uids_to_shards_legacy
