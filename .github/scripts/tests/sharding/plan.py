#!/usr/bin/env python3
"""Plan graph-replay sharding by partitioning graph.result UIDs across runners.

Combines:
  * Graph helpers (LPT weights, path extraction, CPU scaling, size resolution).
  * Adaptive shard-count heuristics (:func:`choose_shard_count`,
    :func:`min_shards_for_wall_budget`, :func:`enrich_plan_timing_estimate`).
  * The ``split_graph_result`` CLI: bin-pack ``graph.result`` UIDs into shards
    using ``weight_mode=history`` (nightly regression p90 with size fallback).

Adaptive planning is always off-peak here (no peak-hour cap). After the first
plan is built the CLI applies ``MAX_SHARDS`` (env, hard upper bound) and then
re-raises the shard count if the estimated slowest shard exceeds
``MAX_SHARD_WALL_MIN``, so history-mode plans never schedule a job past the
wall-time budget. If the wall-time floor still doesn't fit after replanning,
the CLI fails instead of scheduling an over-budget shard.
"""
from __future__ import annotations

import argparse
import copy
import io
import json
import math
import os
import re
import sys
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
_TESTS_DIR = _SCRIPT_DIR.parent
for _path in (_SCRIPT_DIR, _TESTS_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

# History lookup is imported lazily in main() so unit tests do not need YDB SDK.
DEFAULT_DAYS_BACK = 14


# --------------------------------------------------------------------------- #
# Graph helpers                                                               #
# --------------------------------------------------------------------------- #

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
_CONTEXT_SIZE_RE = re.compile(rb"SIZE\x94\x8c.([A-Z]+)\x94")

# Ya SIZE default timeouts (build/plugins/lib/test_const TestSize.DefaultTimeouts).
DEFAULT_SIZE_WEIGHTS = {
    "small": 60.0,
    "medium": 600.0,
    "large": 3600.0,
}

WEIGHT_MODE_TIMEOUT_BUDGET = "timeout_budget"
WEIGHT_MODE_HISTORY = "history"
DEFAULT_WEIGHT_MODE = WEIGHT_MODE_HISTORY
DEFAULT_THREADS = 52


def cpu_slots(node: dict[str, Any], threads: int = DEFAULT_THREADS) -> int:
    """Ya scheduler slots for a test node: ``requirements.cpu`` (``all`` → threads)."""
    slots_cap = max(int(threads), 1)
    req = node.get("requirements") if isinstance(node.get("requirements"), dict) else {}
    raw = req.get("cpu", 1)
    if raw is None:
        return 1
    if isinstance(raw, str) and raw.strip().lower() == "all":
        return slots_cap
    try:
        cpu = int(raw)
    except (TypeError, ValueError):
        return 1
    if cpu < 1:
        return 1
    return min(cpu, slots_cap)


def scale_weight_by_cpu(
    weight: float,
    node: dict[str, Any],
    threads: int = DEFAULT_THREADS,
) -> float:
    """Convert duration/timeout seconds into slot-seconds for LPT packing."""
    return float(weight) * float(cpu_slots(node, threads))


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


def strip_test_kind_leaf(path: str) -> str:
    """Drop ya test-kind leaf (unittest/flake8/...) so path aligns with suite_folder."""
    cleaned = path.strip().rstrip("/")
    if "/" not in cleaned:
        return cleaned
    parent, leaf = cleaned.rsplit("/", 1)
    if leaf in _TEST_KIND_LEAVES:
        return parent
    return cleaned


def extract_node_test_size(node: dict[str, Any]) -> str | None:
    """Read ya ``--test-size`` from graph node cmds (small/medium/large)."""
    for cmd in node.get("cmds") or []:
        if not isinstance(cmd, dict):
            continue
        args = cmd.get("cmd_args") or []
        for index, arg in enumerate(args):
            if arg == "--test-size" and index + 1 < len(args):
                size = str(args[index + 1]).strip().lower()
                if size in DEFAULT_SIZE_WEIGHTS:
                    return size
    return None


def load_test_sizes_from_context(context: dict[str, Any] | None) -> dict[str, str]:
    """Extract SIZE(SMALL|MEDIUM|LARGE) from ya context.json test blobs."""
    if not context:
        return {}
    tests = context.get("tests")
    if not isinstance(tests, dict):
        return {}
    sizes: dict[str, str] = {}
    for uid, payload in tests.items():
        if isinstance(payload, bytes):
            raw = payload
        elif isinstance(payload, str):
            raw = payload.encode("latin1", errors="ignore")
        else:
            continue
        match = _CONTEXT_SIZE_RE.search(raw)
        if not match:
            continue
        size = match.group(1).decode("ascii", errors="ignore").lower()
        if size in DEFAULT_SIZE_WEIGHTS:
            sizes[str(uid)] = size
    return sizes


def extract_node_path(node: dict[str, Any]) -> str | None:
    """Best-effort suite folder for a graph result node.

    Priority:
    1. target_properties.module_dir
    2. kv.path (strip unittest/flake8/... leaf)
    3. outputs .../suite/test-results/...
    4. SOURCE/BUILD_ROOT inputs, skipping dotfiles and source filenames
    """
    target_props = node.get("target_properties") or {}
    module_dir = target_props.get("module_dir")
    if isinstance(module_dir, str) and module_dir.strip():
        return module_dir.strip().rstrip("/")

    kv_path = (node.get("kv") or {}).get("path")
    if isinstance(kv_path, str) and kv_path.strip():
        return strip_test_kind_leaf(kv_path)

    for out in node.get("outputs") or []:
        if not isinstance(out, str):
            continue
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
    duration_p90: dict[str, float],
) -> tuple[float, str, str | None]:
    best_suite: str | None = None
    best_p90: float | None = None
    for suite, p90 in duration_p90.items():
        if path == suite or path.startswith(f"{suite}/"):
            if best_suite is None or len(suite) > len(best_suite):
                best_suite = suite
                best_p90 = float(p90)
    if best_suite is not None and best_p90 is not None:
        return best_p90, "history", best_suite
    return 0.0, "fallback", None


def resolve_node_test_size(
    uid: str,
    node: dict[str, Any],
    *,
    size_by_uid: dict[str, str] | None = None,
) -> str:
    """Return small/medium/large; default small (ya SIZE(SMALL) semantics)."""
    size = extract_node_test_size(node)
    if size is None and size_by_uid is not None:
        size = size_by_uid.get(uid)
    if size in DEFAULT_SIZE_WEIGHTS:
        return size
    return "small"


def fallback_weight_for_node(
    uid: str,
    node: dict[str, Any],
    *,
    size_weights: dict[str, float] | None = None,
    size_by_uid: dict[str, str] | None = None,
) -> tuple[float, str]:
    """Size-based fallback when suite history is missing."""
    weights = size_weights or DEFAULT_SIZE_WEIGHTS
    size = resolve_node_test_size(uid, node, size_by_uid=size_by_uid)
    return float(weights.get(size, DEFAULT_SIZE_WEIGHTS["small"])), f"size_{size}"


def is_graph_test_node(uid: str, node: dict[str, Any] | None) -> bool:
    """True for ya test result nodes (not build/support peers in graph.result)."""
    if node and node.get("node-type") == "test":
        return True
    return str(uid).startswith("test-")


def _cmd_args(node: dict[str, Any]) -> list[str]:
    args: list[str] = []
    for cmd in node.get("cmds") or []:
        if isinstance(cmd, dict):
            for arg in cmd.get("cmd_args") or []:
                args.append(str(arg))
    return args


def node_has_cmd_token(node: dict[str, Any], token: str) -> bool:
    return token in _cmd_args(node)


def extract_node_timeout_sec(node: dict[str, Any]) -> float | None:
    """Read ya ``--timeout`` seconds from graph node cmds when present."""
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


def count_timeout_budget_units(
    uid: str,
    node: dict[str, Any],
    nodes_by_uid: dict[str, dict[str, Any]],
    result_set: set[str],
) -> int:
    """How many size-timeout budgets this result UID represents.

    Suite accumulators keep chunk ``run_test`` nodes out of ``graph.result``;
    each such dep is one parallel work unit with the suite SIZE timeout.
    Leaf result nodes count as 1.
    """
    units = 0
    for dep in node.get("deps") or []:
        dep_uid = dep if isinstance(dep, str) else dep.get("uid") if isinstance(dep, dict) else None
        if not dep_uid:
            continue
        dep_uid = str(dep_uid)
        if dep_uid in result_set:
            continue
        dep_node = nodes_by_uid.get(dep_uid) or {}
        if node_has_cmd_token(dep_node, "run_test"):
            units += 1
    if units > 0:
        return units
    return 1


def resolve_node_timeout_sec(
    uid: str,
    node: dict[str, Any],
    *,
    size_weights: dict[str, float],
    size_by_uid: dict[str, str] | None = None,
    nodes_by_uid: dict[str, dict[str, Any]] | None = None,
    result_set: set[str] | None = None,
) -> tuple[float, str]:
    """Timeout seconds for budget weighting: cmd --timeout, else SIZE default."""
    timeout = extract_node_timeout_sec(node)
    if timeout is not None:
        size = resolve_node_test_size(uid, node, size_by_uid=size_by_uid)
        return timeout, size

    if nodes_by_uid is not None and result_set is not None:
        for dep in node.get("deps") or []:
            dep_uid = dep if isinstance(dep, str) else dep.get("uid") if isinstance(dep, dict) else None
            if not dep_uid or str(dep_uid) in result_set:
                continue
            dep_node = nodes_by_uid.get(str(dep_uid)) or {}
            if not node_has_cmd_token(dep_node, "run_test"):
                continue
            timeout = extract_node_timeout_sec(dep_node)
            if timeout is not None:
                size = resolve_node_test_size(str(dep_uid), dep_node, size_by_uid=size_by_uid)
                return timeout, size

    size = resolve_node_test_size(uid, node, size_by_uid=size_by_uid)
    return float(size_weights.get(size, DEFAULT_SIZE_WEIGHTS["small"])), size


def _size_rank(size: str, size_weights: dict[str, float]) -> float:
    return float(size_weights.get(size, size_weights.get("small", 60.0)))


def _plan_uid_weights_timeout_budget(
    uids: list[str],
    nodes_by_uid: dict[str, dict[str, Any]],
    *,
    size_weights: dict[str, float],
    size_by_uid: dict[str, str] | None = None,
    threads: int = DEFAULT_THREADS,
) -> tuple[dict[str, float], dict[str, Any]]:
    """Weight = N_work_units * timeout(size) * cpu_slots."""
    result_set = set(uids)
    per_uid: dict[str, float] = {}
    source_counts: Counter[str] = Counter()
    total_units = 0
    missing_path = 0

    for uid in uids:
        node = nodes_by_uid.get(uid) or {}
        if extract_node_path(node) is None and is_graph_test_node(uid, node):
            missing_path += 1
        units = count_timeout_budget_units(uid, node, nodes_by_uid, result_set)
        timeout, size = resolve_node_timeout_sec(
            uid,
            node,
            size_weights=size_weights,
            size_by_uid=size_by_uid,
            nodes_by_uid=nodes_by_uid,
            result_set=result_set,
        )
        weight = scale_weight_by_cpu(float(units) * float(timeout), node, threads)
        per_uid[uid] = weight
        total_units += units
        source_counts[f"timeout_{size}"] += 1
        source_counts["units"] += units

    stats = {
        "mode": "graph_uid_timeout_budget_lpt",
        "history_suite_count": 0,
        "history_uid_count": 0,
        "timeout_budget_units": int(total_units),
        "size_small_uid_count": int(source_counts.get("timeout_small", 0)),
        "size_medium_uid_count": int(source_counts.get("timeout_medium", 0)),
        "size_large_uid_count": int(source_counts.get("timeout_large", 0)),
        "missing_path_uid_count": missing_path,
        "history_weight": 0.0,
        "fallback_weight": round(sum(per_uid.values()), 1),
        "size_weights": {k: float(v) for k, v in sorted(size_weights.items())},
        "threads": int(threads),
        "cpu_scaled": True,
    }
    return per_uid, stats


def _plan_uid_weights_history(
    uids: list[str],
    nodes_by_uid: dict[str, dict[str, Any]],
    duration_p90: dict[str, float],
    *,
    size_weights: dict[str, float],
    size_by_uid: dict[str, str] | None = None,
    threads: int = DEFAULT_THREADS,
) -> tuple[dict[str, float], dict[str, Any]]:
    """History mode: longest suite_folder prefix match, then * cpu_slots."""
    path_by_uid: dict[str, str | None] = {}
    size_by_resolved: dict[str, str] = {}
    match_by_uid: dict[str, tuple[float, str, str | None]] = {}
    history_tests: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))

    for uid in uids:
        node = nodes_by_uid.get(uid) or {}
        path = extract_node_path(node)
        path_by_uid[uid] = path
        size = resolve_node_test_size(uid, node, size_by_uid=size_by_uid)
        size_by_resolved[uid] = size
        if not path or not is_graph_test_node(uid, node):
            match_by_uid[uid] = (0.0, "fallback", None)
            continue
        p90, source, suite = longest_history_match(path, duration_p90)
        match_by_uid[uid] = (p90, source, suite)
        if source == "history" and suite is not None:
            history_tests[suite][size].append(uid)

    owner_size_by_suite: dict[str, str] = {}
    history_uid_counts: Counter[str] = Counter()
    for suite, by_size in history_tests.items():
        owner = max(by_size.keys(), key=lambda s: _size_rank(s, size_weights))
        owner_size_by_suite[suite] = owner
        history_uid_counts[suite] = len(by_size[owner])

    per_uid: dict[str, float] = {}
    source_counts: Counter[str] = Counter()
    history_weight = 0.0
    fallback_weight = 0.0
    missing_path = 0

    for uid in uids:
        node = nodes_by_uid.get(uid) or {}
        p90, source, suite = match_by_uid[uid]
        size = size_by_resolved[uid]
        if (
            source == "history"
            and suite is not None
            and owner_size_by_suite.get(suite) == size
        ):
            weight = scale_weight_by_cpu(
                p90 / max(history_uid_counts[suite], 1),
                node,
                threads,
            )
            per_uid[uid] = weight
            history_weight += weight
            source_counts["history"] += 1
            continue

        if path_by_uid[uid] is None:
            missing_path += 1
        base_weight, fb_source = fallback_weight_for_node(
            uid,
            node,
            size_weights=size_weights,
            size_by_uid=size_by_uid,
        )
        weight = scale_weight_by_cpu(base_weight, node, threads)
        per_uid[uid] = weight
        fallback_weight += weight
        source_counts[fb_source] += 1

    stats = {
        "mode": "graph_uid_history_p90_lpt",
        "history_suite_count": len(duration_p90),
        "history_uid_count": int(source_counts.get("history", 0)),
        "size_small_uid_count": int(source_counts.get("size_small", 0)),
        "size_medium_uid_count": int(source_counts.get("size_medium", 0)),
        "size_large_uid_count": int(source_counts.get("size_large", 0)),
        "missing_path_uid_count": missing_path,
        "history_weight": round(history_weight, 1),
        "fallback_weight": round(fallback_weight, 1),
        "size_weights": {k: float(v) for k, v in sorted(size_weights.items())},
        "threads": int(threads),
        "cpu_scaled": True,
    }
    return per_uid, stats


def plan_uid_weights(
    uids: list[str],
    nodes_by_uid: dict[str, dict[str, Any]],
    duration_p90: dict[str, float],
    *,
    size_weights: dict[str, float] | None = None,
    size_by_uid: dict[str, str] | None = None,
    weight_mode: str = DEFAULT_WEIGHT_MODE,
    threads: int = DEFAULT_THREADS,
) -> tuple[dict[str, float], dict[str, Any]]:
    """Weight result UIDs for LPT packing (slot-seconds: duration * cpu).

    Default ``history``: longest suite_folder prefix match on test nodes; p90
    from nightly regression jobs goes to the heaviest SIZE at that key. Other
    sizes / non-test peers use size fallback.

    Opt-in ``timeout_budget``: ``N * timeout(size)`` where N is the number of
    chunk ``run_test`` deps outside ``graph.result`` (else 1).

    Both modes multiply by ``requirements.cpu`` (``all`` → ``threads``) so pack
    load approximates ya slot occupancy; wall estimate remains max_load/threads.
    """
    weights_cfg = dict(DEFAULT_SIZE_WEIGHTS)
    if size_weights:
        weights_cfg.update(size_weights)

    mode = (weight_mode or DEFAULT_WEIGHT_MODE).strip().lower()
    if mode == WEIGHT_MODE_HISTORY:
        return _plan_uid_weights_history(
            uids,
            nodes_by_uid,
            duration_p90,
            size_weights=weights_cfg,
            size_by_uid=size_by_uid,
            threads=threads,
        )
    return _plan_uid_weights_timeout_budget(
        uids,
        nodes_by_uid,
        size_weights=weights_cfg,
        size_by_uid=size_by_uid,
        threads=threads,
    )


def uid_weights(
    uids: list[str],
    nodes_by_uid: dict[str, dict[str, Any]],
    duration_p90: dict[str, float],
    *,
    default_weight: float = 600.0,
    size_weights: dict[str, float] | None = None,
    size_by_uid: dict[str, str] | None = None,
    threads: int = DEFAULT_THREADS,
) -> dict[str, float]:
    """Compatibility wrapper around :func:`plan_uid_weights`."""
    weights_cfg = dict(size_weights or DEFAULT_SIZE_WEIGHTS)
    if size_weights is None and default_weight != DEFAULT_SIZE_WEIGHTS["medium"]:
        weights_cfg["medium"] = float(default_weight)
    weights, _ = plan_uid_weights(
        uids,
        nodes_by_uid,
        duration_p90,
        size_weights=weights_cfg,
        size_by_uid=size_by_uid,
        weight_mode=DEFAULT_WEIGHT_MODE,
        threads=threads,
    )
    return weights


def component_weight(
    component: list[str],
    nodes_by_uid: dict[str, dict[str, Any]],
    duration_p90: dict[str, float],
    *,
    default_weight: float = 600.0,
    size_weights: dict[str, float] | None = None,
    size_by_uid: dict[str, str] | None = None,
    threads: int = DEFAULT_THREADS,
) -> tuple[float, dict[str, float]]:
    """Sum planned weights for UIDs in a component (same rules as packing)."""
    weights_cfg = dict(size_weights or DEFAULT_SIZE_WEIGHTS)
    if size_weights is None and default_weight != DEFAULT_SIZE_WEIGHTS["medium"]:
        weights_cfg["medium"] = float(default_weight)
    per_uid = uid_weights(
        component,
        nodes_by_uid,
        duration_p90,
        size_weights=weights_cfg,
        size_by_uid=size_by_uid,
        threads=threads,
    )
    per_path: dict[str, float] = defaultdict(float)
    for uid, weight in per_uid.items():
        path = extract_node_path(nodes_by_uid.get(uid, {})) or uid
        per_path[path] += weight
    return sum(per_uid.values()), dict(per_path)


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


# --------------------------------------------------------------------------- #
# Shard-count heuristics (choose_shard_count)                                 #
# --------------------------------------------------------------------------- #

DEFAULT_LIGHT_THRESHOLD_MIN = 60.0
DEFAULT_TIERS: tuple[tuple[float, int], ...] = (
    (120.0, 4),
    (200.0, 8),
    (float("inf"), 12),
)
DEFAULT_PEAK_HOURS_UTC = range(9, 17)
DEFAULT_PEAK_CAP = 4
# Slowest shard estimated wall time must stay within this budget.
DEFAULT_MAX_SHARD_WALL_MIN = 240.0

SHARD_PROFILES: dict[str, dict[str, Any]] = {
    "pr": {
        "light_threshold_min": DEFAULT_LIGHT_THRESHOLD_MIN,
        "tiers": DEFAULT_TIERS,
        "peak_cap": DEFAULT_PEAK_CAP,
    },
    "soft": {
        "light_threshold_min": DEFAULT_LIGHT_THRESHOLD_MIN,
        "tiers": ((120.0, 2), (200.0, 4), (float("inf"), 8)),
        "peak_cap": DEFAULT_PEAK_CAP,
    },
}
DEFAULT_PROFILE = "pr"


def resolve_profile(name: str | None) -> dict[str, Any]:
    key = (name or DEFAULT_PROFILE).strip().lower()
    if key not in SHARD_PROFILES:
        known = ", ".join(sorted(SHARD_PROFILES))
        raise ValueError(f"unknown shard profile {name!r}; expected one of: {known}")
    return SHARD_PROFILES[key]


def estimate_single_job_minutes(total_weight_sec: float, threads: int) -> float:
    if threads <= 0:
        raise ValueError("threads must be positive")
    return total_weight_sec / 60.0 / threads


def estimate_critical_path_minutes(shard_weights_sec: list[float], threads: int) -> float:
    """Wall-clock lower bound: slowest shard slot-seconds / threads."""
    if not shard_weights_sec:
        return 0.0
    return estimate_single_job_minutes(max(shard_weights_sec), threads)


def min_shards_for_wall_budget(
    total_weight_sec: float,
    *,
    threads: int = DEFAULT_THREADS,
    max_shard_wall_min: float = DEFAULT_MAX_SHARD_WALL_MIN,
) -> int:
    """Lower bound on shard count for ideal wall time within the budget."""
    if max_shard_wall_min <= 0:
        raise ValueError("max_shard_wall_min must be positive")
    estimate_min = estimate_single_job_minutes(total_weight_sec, threads)
    if estimate_min <= 0:
        return 1
    return max(1, math.ceil(estimate_min / max_shard_wall_min))


def enrich_plan_timing_estimate(plan: dict, threads: int) -> dict:
    """Attach estimated_* timing fields from shard balance weights."""
    shards = plan.get("shards") or []
    loads = [float(shard.get("balance_weight") or 0) for shard in shards]
    total_weight = float(plan.get("total_weight") or sum(loads) or 0.0)
    max_load = max(loads) if loads else 0.0
    plan["estimate_threads"] = threads
    plan["estimated_max_shard_weight_sec"] = round(max_load, 1)
    plan["estimated_critical_path_min"] = round(
        estimate_critical_path_minutes(loads, threads), 1
    )
    plan["estimated_single_job_min"] = round(
        estimate_single_job_minutes(total_weight, threads), 1
    )
    return plan


def choose_shard_count(
    total_weight_sec: float,
    *,
    threads: int = DEFAULT_THREADS,
    light_threshold_min: float | None = None,
    peak_cap: int | None = None,
    is_peak: bool = False,
    max_shards: int = 0,
    max_shard_wall_min: float = DEFAULT_MAX_SHARD_WALL_MIN,
    profile: str = DEFAULT_PROFILE,
    tiers: tuple[tuple[float, int], ...] | None = None,
) -> tuple[int, float]:
    """Return (shard_count, estimated_single_job_minutes)."""
    prof = resolve_profile(profile)
    light = float(prof["light_threshold_min"] if light_threshold_min is None else light_threshold_min)
    use_tiers = tiers if tiers is not None else tuple(prof["tiers"])
    use_peak_cap = int(prof["peak_cap"] if peak_cap is None else peak_cap)

    estimate_min = estimate_single_job_minutes(total_weight_sec, threads)
    wall_floor = min_shards_for_wall_budget(
        total_weight_sec,
        threads=threads,
        max_shard_wall_min=max_shard_wall_min,
    )
    if estimate_min < light:
        count = 1
    else:
        count = int(use_tiers[-1][1])
        for upper_min, tier_count in use_tiers:
            if estimate_min < float(upper_min):
                count = int(tier_count)
                break
    count = max(count, wall_floor)
    if is_peak and count > use_peak_cap:
        count = use_peak_cap
    if max_shards > 0:
        count = min(count, max_shards)
    count = max(count, wall_floor)
    return max(count, 1), estimate_min


def is_peak_hour_utc(hour: int) -> bool:
    return hour in DEFAULT_PEAK_HOURS_UTC


# --------------------------------------------------------------------------- #
# Bin packing / plan building                                                 #
# --------------------------------------------------------------------------- #


def collect_unique_paths(graph: dict[str, Any]) -> list[str]:
    nodes_by_uid = graph_nodes_by_uid(graph)
    paths: set[str] = set()
    for uid in result_uids(graph):
        node = nodes_by_uid.get(uid)
        if not node:
            continue
        path = extract_node_path(node)
        if path:
            paths.add(path)
    return sorted(paths)


def bin_pack_uids(
    weights: dict[str, float],
    shard_count: int,
) -> tuple[list[list[str]], list[float]]:
    if shard_count < 1:
        raise ValueError("shard_count must be >= 1")
    indexed = sorted(weights.items(), key=lambda item: item[1], reverse=True)
    buckets: list[list[str]] = [[] for _ in range(shard_count)]
    loads = [0.0] * shard_count
    for uid, weight in indexed:
        target = min(range(shard_count), key=lambda i: (loads[i], i))
        buckets[target].append(uid)
        loads[target] += weight
    return buckets, loads


def build_plan(
    graph: dict[str, Any],
    shard_count: int,
    *,
    duration_p90: dict[str, float],
    size_weights: dict[str, float] | None = None,
    size_by_uid: dict[str, str] | None = None,
    plan_mode: str = "increment_graph",
    threads: int = DEFAULT_THREADS,
    days_back: int | None = None,
    build_type: str | None = None,
    branch: str | None = None,
    weight_mode: str = DEFAULT_WEIGHT_MODE,
) -> dict[str, Any]:
    nodes_by_uid = graph_nodes_by_uid(graph)
    uids = result_uids(graph)
    per_uid, weighting_stats = plan_uid_weights(
        uids,
        nodes_by_uid,
        duration_p90,
        size_weights=size_weights,
        size_by_uid=size_by_uid,
        weight_mode=weight_mode,
        threads=threads,
    )
    total_weight = sum(per_uid.values())
    buckets, loads = bin_pack_uids(per_uid, shard_count)
    components = connected_components(uids, nodes_by_uid)
    assignments: dict[str, int] = {}
    shards: list[dict[str, Any]] = []
    for shard_id, shard_uids in enumerate(buckets):
        if not shard_uids:
            continue
        sample_paths: list[str] = []
        seen_paths: set[str] = set()
        for uid in shard_uids:
            assignments[uid] = shard_id
            path = extract_node_path(nodes_by_uid.get(uid, {}))
            if path and path not in seen_paths:
                seen_paths.add(path)
                sample_paths.append(path)
        sample_paths.sort()
        shards.append(
            {
                "id": shard_id,
                "tests": [],
                "graph_uids": shard_uids,
                "result_node_count": len(shard_uids),
                "balance_weight": round(loads[shard_id], 1),
                "sample_paths": sample_paths[:8],
            }
        )

    weighting = dict(weighting_stats)
    weighting["max_shard_weight_ratio"] = (
        round(max(loads) / total_weight, 3) if total_weight else 0.0
    )
    if days_back is not None:
        weighting["days_back"] = days_back
    if build_type is not None:
        weighting["build_type"] = build_type
    if branch is not None:
        weighting["branch"] = branch

    plan = {
        "plan_mode": plan_mode,
        "requested_shard_count": shard_count,
        "shard_count": len(shards),
        "total_graph_nodes": len(uids),
        "total_components": len(components),
        "total_weight": round(total_weight, 1),
        "weighting": weighting,
        "uid_assignments": assignments,
        "shards": shards,
    }
    return enrich_plan_timing_estimate(plan, threads)


# --------------------------------------------------------------------------- #
# Summary rendering                                                           #
# --------------------------------------------------------------------------- #


def print_summary(plan: dict[str, Any], *, title: str = "Shard plan") -> None:
    """Print a short markdown summary of ``plan`` to stdout.

    Meant for redirecting into ``$GITHUB_STEP_SUMMARY``.
    """
    lines: list[str] = [f"## {title}", ""]

    shard_count = int(plan.get("shard_count") or len(plan.get("shards") or []))
    requested = plan.get("requested_shard_count")
    if requested is not None and int(requested) != shard_count:
        lines.append(f"**Shard count:** {shard_count} (requested {requested})")
    else:
        lines.append(f"**Shard count:** {shard_count}")

    critical = plan.get("estimated_critical_path_min")
    if critical is not None:
        threads = plan.get("estimate_threads", DEFAULT_THREADS)
        max_weight = plan.get("estimated_max_shard_weight_sec")
        line = f"**Estimated wall time (slowest shard):** ~{float(critical):.0f} min ({threads} threads"
        if max_weight is not None:
            line += f", max shard weight {max_weight}s"
        line += ")"
        lines.append(line)
        single = plan.get("estimated_single_job_min")
        if single is not None and shard_count > 1:
            lines.append(f"**Monolith equivalent:** ~{float(single):.0f} min (total weight / threads)")

    plan_mode = plan.get("plan_mode") or "graph"
    total_nodes = plan.get("total_graph_nodes")
    total_weight = plan.get("total_weight")
    graph_label = "Full graph" if plan_mode == "full_graph" else "Increment graph"
    total_line = f"**{graph_label}:** {total_nodes or 0} result nodes"
    if total_weight is not None:
        total_line += f", weight {total_weight}"
    lines.append(total_line)

    weighting = plan.get("weighting") or {}
    if weighting:
        bits: list[str] = []
        mode = weighting.get("mode")
        if mode:
            bits.append(f"mode={mode}")
        hist = weighting.get("history_uid_count")
        if hist is not None and int(hist or 0) > 0:
            bits.append(f"history_uids={hist}")
        small = int(weighting.get("size_small_uid_count") or 0)
        medium = int(weighting.get("size_medium_uid_count") or 0)
        large = int(weighting.get("size_large_uid_count") or 0)
        if small or medium or large:
            bits.append(
                f"size_uids={small + medium + large} "
                f"(small={small}, medium={medium}, large={large})"
            )
        days = weighting.get("days_back")
        if days is not None:
            bits.append(f"days_back={days}")
        if bits:
            lines.append("**Weighting:** " + ", ".join(bits))

    lines.append("")
    lines.append("| Shard | Graph nodes | Weight | Sample paths |")
    lines.append("| ---: | ---: | ---: | --- |")
    for shard in plan.get("shards") or []:
        shard_id = shard.get("id", "?")
        node_count = shard.get("result_node_count", len(shard.get("graph_uids") or []))
        load_value = shard.get("balance_weight", "")
        sample_paths = shard.get("sample_paths") or []
        sample = ", ".join(f"`{s}`" for s in sample_paths[:3])
        if len(sample_paths) > 3:
            sample += f", … (+{len(sample_paths) - 3})"
        lines.append(f"| {shard_id} | {node_count} | {load_value} | {sample} |")
    lines.append("")

    sys.stdout.write("\n".join(lines) + "\n")


# --------------------------------------------------------------------------- #
# CLI                                                                         #
# --------------------------------------------------------------------------- #


def _empty_plan(plan_mode: str, threads: int) -> dict[str, Any]:
    return enrich_plan_timing_estimate(
        {
            "plan_mode": plan_mode,
            "requested_shard_count": 0,
            "shard_count": 0,
            "total_graph_nodes": 0,
            "total_components": 0,
            "total_weight": 0,
            "uid_assignments": {},
            "shards": [],
        },
        threads,
    )


def _write_plan(plan: dict[str, Any], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _resolve_shard_count(
    total_weight: float,
    shard_count_arg: str,
    *,
    threads: int,
    profile: str,
    weight_mode: str,
    max_shards: int,
    max_shard_wall_min: float,
) -> tuple[int, float]:
    if shard_count_arg == "auto":
        choose_kwargs: dict[str, Any] = {
            "threads": threads,
            "profile": profile,
            "is_peak": False,
            "max_shards": max_shards,
        }
        # timeout_budget is a worst-case CPU ceiling, not an expected-duration
        # estimate — do not inflate shard count via wall_floor.
        if weight_mode == WEIGHT_MODE_TIMEOUT_BUDGET:
            choose_kwargs["max_shard_wall_min"] = 1e9
        else:
            choose_kwargs["max_shard_wall_min"] = max_shard_wall_min
        return choose_shard_count(total_weight, **choose_kwargs)
    manual = max(int(shard_count_arg), 1)
    if max_shards > 0:
        manual = min(manual, max_shards)
    estimate_min = estimate_single_job_minutes(total_weight, threads)
    return manual, estimate_min


def _read_int_env(name: str) -> int:
    raw = os.environ.get(name)
    if not raw:
        return 0
    try:
        return max(int(raw), 0)
    except ValueError:
        return 0


def _read_float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("graph", type=Path, help="Increment cut-graph JSON")
    parser.add_argument(
        "--shard-count",
        required=True,
        help='"auto" or integer shard count',
    )
    parser.add_argument(
        "--profile",
        default=DEFAULT_PROFILE,
        choices=sorted(SHARD_PROFILES),
        help="Adaptive profile when --shard-count=auto: pr (4/8/12) or soft (2/4/8)",
    )
    parser.add_argument("-o", "--output", type=Path, required=True, help="shard_plan.json")
    parser.add_argument("--build-type", default="relwithdebinfo")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--days-back", type=int, default=DEFAULT_DAYS_BACK)
    parser.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    parser.add_argument(
        "--context",
        type=Path,
        default=None,
        help="Optional context.json to read per-test SIZE when cmds lack --test-size",
    )
    parser.add_argument(
        "--default-weight-sec",
        type=float,
        default=DEFAULT_SIZE_WEIGHTS["medium"],
        help="Fallback weight for medium tests when history is missing",
    )
    parser.add_argument(
        "--small-weight-sec",
        type=float,
        default=DEFAULT_SIZE_WEIGHTS["small"],
        help="Fallback weight for small tests when history is missing",
    )
    parser.add_argument(
        "--large-weight-sec",
        type=float,
        default=DEFAULT_SIZE_WEIGHTS["large"],
        help="Fallback weight for large tests when history is missing",
    )
    parser.add_argument(
        "--plan-mode",
        default="increment_graph",
        choices=("increment_graph", "full_graph"),
        help="Plan mode stored in shard_plan.json (default: increment_graph)",
    )
    parser.add_argument(
        "--max-shards",
        type=int,
        default=None,
        help="Hard upper bound (0 = none). Defaults to MAX_SHARDS env or 0",
    )
    parser.add_argument(
        "--max-shard-wall-min",
        type=float,
        default=None,
        help=(
            "Keep enough shards so ideal per-shard wall time stays within this "
            "many minutes. Defaults to MAX_SHARD_WALL_MIN env or "
            f"{DEFAULT_MAX_SHARD_WALL_MIN:.0f}"
        ),
    )
    parser.add_argument(
        "--weight-mode",
        default=DEFAULT_WEIGHT_MODE,
        choices=(WEIGHT_MODE_TIMEOUT_BUDGET, WEIGHT_MODE_HISTORY),
        help="UID weight source (default: history from nightly regression p90)",
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=None,
        help="Optional path to write a markdown plan summary (also printed to stdout)",
    )
    args = parser.parse_args()

    max_shards = args.max_shards if args.max_shards is not None else _read_int_env("MAX_SHARDS")
    max_shard_wall_min = (
        args.max_shard_wall_min
        if args.max_shard_wall_min is not None
        else _read_float_env("MAX_SHARD_WALL_MIN", DEFAULT_MAX_SHARD_WALL_MIN)
    )

    size_weights = {
        "small": float(args.small_weight_sec),
        "medium": float(args.default_weight_sec),
        "large": float(args.large_weight_sec),
    }

    graph = load_graph(args.graph)
    if not result_uids(graph):
        plan = _empty_plan(args.plan_mode, args.threads)
        _write_plan(plan, args.output)
        print("Empty increment graph: shard_count=0", file=sys.stderr)
        if args.summary:
            args.summary.parent.mkdir(parents=True, exist_ok=True)
            args.summary.write_text("", encoding="utf-8")
        return 0

    size_by_uid: dict[str, str] = {}
    if args.context is not None:
        if not args.context.is_file():
            print(f"warning: context not found: {args.context}", file=sys.stderr)
        else:
            context = json.loads(args.context.read_text(encoding="utf-8"))
            size_by_uid = load_test_sizes_from_context(context)
            print(f"Loaded test sizes from context for {len(size_by_uid)} tests", file=sys.stderr)

    duration_p90: dict[str, float] = {}
    if args.weight_mode == WEIGHT_MODE_HISTORY:
        from get_test_duration_estimates import get_suite_duration_p90  # noqa: E402

        unique_paths = collect_unique_paths(graph)
        try:
            duration_p90 = get_suite_duration_p90(
                unique_paths,
                args.days_back,
                args.build_type,
                args.branch,
            )
        except Exception as exc:
            print(f"warning: duration history lookup failed: {exc}", file=sys.stderr)
            duration_p90 = {}

    preview_nodes = graph_nodes_by_uid(graph)
    preview_uids = result_uids(graph)
    preview_weights, _ = plan_uid_weights(
        preview_uids,
        preview_nodes,
        duration_p90,
        size_weights=size_weights,
        size_by_uid=size_by_uid,
        weight_mode=args.weight_mode,
        threads=args.threads,
    )
    total_weight = sum(preview_weights.values())

    initial_count, estimate_min = _resolve_shard_count(
        total_weight,
        str(args.shard_count),
        threads=args.threads,
        profile=args.profile,
        weight_mode=args.weight_mode,
        max_shards=max_shards,
        max_shard_wall_min=max_shard_wall_min,
    )
    if args.shard_count == "auto":
        print(
            f"Adaptive shard count (profile={args.profile}) from graph weight {total_weight:.0f}s "
            f"(~{estimate_min:.1f} min single job) -> {initial_count} "
            f"(peak cap off, MAX_SHARDS={max_shards or 'unset'})",
            file=sys.stderr,
        )

    plan = build_plan(
        graph,
        initial_count,
        duration_p90=duration_p90,
        size_weights=size_weights,
        size_by_uid=size_by_uid,
        plan_mode=args.plan_mode,
        threads=args.threads,
        days_back=args.days_back if args.weight_mode == WEIGHT_MODE_HISTORY else None,
        build_type=args.build_type if args.weight_mode == WEIGHT_MODE_HISTORY else None,
        branch=args.branch if args.weight_mode == WEIGHT_MODE_HISTORY else None,
        weight_mode=args.weight_mode,
    )

    # MAX_SHARDS may shrink the plan; wall-time floor may raise it back so the
    # slowest shard stays within MAX_SHARD_WALL_MIN. timeout_budget weights are
    # worst-case CPU ceilings, not expected duration — do not inflate.
    timeout_budget_mode = args.weight_mode == WEIGHT_MODE_TIMEOUT_BUDGET
    desired = int(plan.get("shard_count") or 0)
    if desired > 0:
        if max_shards > 0 and desired > max_shards:
            print(
                f"Pool capacity prefers {max_shards} shards (plan had {desired})",
                file=sys.stderr,
            )
            desired = max_shards
        plan_weight = float(plan.get("total_weight") or 0.0)
        if not timeout_budget_mode:
            wall_floor = min_shards_for_wall_budget(
                plan_weight,
                threads=args.threads,
                max_shard_wall_min=max_shard_wall_min,
            )
            if desired < wall_floor:
                single = float(plan.get("estimated_single_job_min") or 0.0)
                print(
                    f"Raising shards from {desired} to {wall_floor} "
                    f"to keep estimated shard wall <= {max_shard_wall_min:.0f} min "
                    f"(single-job ~{single:.1f} min)",
                    file=sys.stderr,
                )
                desired = wall_floor

        if desired != int(plan.get("shard_count") or 0):
            print(
                f"Replanning graph shards: {plan['shard_count']} -> {desired}",
                file=sys.stderr,
            )
            plan = build_plan(
                graph,
                desired,
                duration_p90=duration_p90,
                size_weights=size_weights,
                size_by_uid=size_by_uid,
                plan_mode=args.plan_mode,
                threads=args.threads,
                days_back=args.days_back if args.weight_mode == WEIGHT_MODE_HISTORY else None,
                build_type=args.build_type if args.weight_mode == WEIGHT_MODE_HISTORY else None,
                branch=args.branch if args.weight_mode == WEIGHT_MODE_HISTORY else None,
                weight_mode=args.weight_mode,
            )

    _write_plan(plan, args.output)

    # Wall-time check: hard fail for history/expected weights; warn-only for
    # timeout_budget (N * SIZE timeout is a packing ceiling, not ETA).
    count = int(plan.get("shard_count") or 0)
    crit = float(plan.get("estimated_critical_path_min") or 0.0)
    if count > 0 and crit > max_shard_wall_min:
        msg = (
            f"estimated slowest shard ~{crit:.1f} min exceeds "
            f"max_shard_wall_min={max_shard_wall_min:.0f} with {count} shards"
        )
        if timeout_budget_mode:
            print(
                f"WARNING: {msg} (timeout_budget is worst-case packing weight; continuing)",
                file=sys.stderr,
            )
        else:
            print(
                f"ERROR: {msg}. "
                "Increase shard_count / free pool capacity, or shrink test scope.",
                file=sys.stderr,
            )
            return 1

    loads = Counter(plan["uid_assignments"].values())
    weighting = plan.get("weighting") or {}
    size_uid_total = (
        int(weighting.get("size_small_uid_count") or 0)
        + int(weighting.get("size_medium_uid_count") or 0)
        + int(weighting.get("size_large_uid_count") or 0)
    )
    print(
        f"Graph replay plan ({args.plan_mode}): {plan['shard_count']} shards, "
        f"{plan['total_graph_nodes']} nodes, {plan['total_components']} components, "
        f"weight={plan['total_weight']}, mode={weighting.get('mode')}, "
        f"timeout_units={weighting.get('timeout_budget_units', 0)}, "
        f"history_uids={weighting.get('history_uid_count', 0)}, "
        f"size_uids={size_uid_total}, "
        f"est. critical path ~{float(plan.get('estimated_critical_path_min') or 0):.1f} min "
        f"-> {args.output}",
        file=sys.stderr,
    )
    print(f"Shard loads (nodes): {dict(sorted(loads.items()))}", file=sys.stderr)

    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if args.summary is not None or step_summary:
        buffer = io.StringIO()
        old_stdout = sys.stdout
        sys.stdout = buffer
        try:
            print_summary(plan)
        finally:
            sys.stdout = old_stdout
        text = buffer.getvalue()

        if args.summary is not None:
            args.summary.parent.mkdir(parents=True, exist_ok=True)
            args.summary.write_text(text, encoding="utf-8")

        if step_summary:
            with open(step_summary, "a", encoding="utf-8") as fp:
                fp.write(text)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
