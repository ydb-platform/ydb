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
_TEST_RESULTS_OUT_RE = re.compile(
    r"^\$\((?:BUILD_ROOT|SOURCE_ROOT)\)/"
    r"((?:ydb|yql|library|contrib|yt)/(?:[^/$]+(?:/[^/$]+)*))/test-results/"
)
_DOTFILE_LEAF_RE = re.compile(r"(?:^|/)\.[^/]+$")
_SOURCE_FILE_LEAF_RE = re.compile(
    r"\.(?:py|pyi|json|ya?ml|toml|md|txt|proto|cpp|h|c|cc|hh|hpp|inc|sh)$",
    re.IGNORECASE,
)

# Leaf names that ya puts after the suite folder in kv.path / test-results/.
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
    # History split must use the component alone only when this is the full
    # result set; callers that need global split should use plan_uid_weights.
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
