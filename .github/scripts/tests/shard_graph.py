#!/usr/bin/env python3
"""Split one ya graph into shard graphs for Run-tests.

Policy lives here and only here. ``shard_count=1`` never calls this module:
the workflow keeps the single ``build_and_test_ya`` job.

A shard is a partition of ``graph.result``. Every result UID is assigned to
exactly one shard. ``ya make --build-custom-json`` runs every UID in
``result``, so the filtered graph's result list is that shard and nothing
else. Dependency nodes stay in the graph so the shard can still build.

Weights prefer, for each suite in the graph, the sum of per-test p90
durations from YDB history (history branch, same build type, last 14
days, skipped rows left out). A suite with no history row keeps the
graph timeout budget. Both are multiplied by ya CPU slots
(``requirements.cpu``; ``all`` means the job's test thread count).

Assignment is deterministic: higher weight first, UID as a tie-break,
and a tied load goes to the lower shard index.
"""
from __future__ import annotations

import argparse
import copy
import fnmatch
import json
import math
import os
import re
import subprocess
import sys
import tempfile
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
MAX_SHARDS = 16
# Target wall time per host. Hosts = ceil(estimated minutes / this).
MAX_SHARD_WALL_MIN = 60.0

# VM size per runner label. Live free capacity comes from Compute quota, not from these numbers.
_CAPACITY_RESOURCES = ("vcpu", "ram_gb", "nrd_ssd_gb")
_QUOTA_SCALE = {
    "compute.instances.count": ("instances", 1.0),
    "compute.instanceCores.count": ("vcpu", 1.0),
    "compute.instanceMemory.size": ("ram_gb", float(1024**3)),
    "compute.ssdNonReplicatedDisks.size": ("nrd_ssd_gb", float(1024**3)),
}
_SA_KEY_ENV = "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"

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


def _size_seconds(p90_by_suite: dict[str, dict[str, float]] | None, path: str, size: str) -> float | None:
    if not p90_by_suite:
        return None
    bucket = p90_by_suite.get(path)
    if not isinstance(bucket, dict):
        return None
    value = bucket.get(size)
    if value is None or float(value) <= 0:
        return None
    return float(value)


def suite_history_seconds(
    path: str | None,
    p90_by_suite: dict[str, dict[str, float]] | None,
    result_keys: set[tuple[str, str]],
    size: str,
) -> float | None:
    """Seconds from history for one graph path and test size, before descendant rows.

    Exact ``suite_folder`` of this size wins. Otherwise a path whose parent is a
    suite of this size in history uses that suite. If the parent is also a result
    node of this size, the tail contributes nothing.
    """
    if not path or not p90_by_suite:
        return None
    path = path.strip("/")
    direct = _size_seconds(p90_by_suite, path, size)
    if direct is not None:
        return direct
    if "/" not in path:
        return None
    parent = path.rsplit("/", 1)[0]
    parent_seconds = _size_seconds(p90_by_suite, parent, size)
    if parent_seconds is None:
        return None
    if (parent, size) in result_keys:
        return 0.0
    return parent_seconds


def unclaimed_descendant_seconds(
    path: str,
    p90_by_suite: dict[str, dict[str, float]],
    claimed: set[tuple[str, str]],
    size: str,
) -> tuple[float, list[str]] | None:
    """Sum same-size history rows strictly longer than ``path`` that no graph node took yet."""
    prefix = path.strip("/") + "/"
    taken = [
        key
        for key, bucket in p90_by_suite.items()
        if key.startswith(prefix) and (key, size) not in claimed and _size_seconds(p90_by_suite, key, size) is not None
    ]
    if not taken:
        return None
    return sum(float(p90_by_suite[key][size]) for key in taken), taken


def suite_p90_seconds(
    node: dict[str, Any],
    p90_by_suite: dict[str, dict[str, float]] | None,
    *,
    result_keys: set[tuple[str, str]] | None = None,
    size: str = "small",
) -> float | None:
    """Observed suite seconds for this node's test size, or None when history has no row."""
    path = extract_node_path(node)
    return suite_history_seconds(path, p90_by_suite, result_keys or set(), size)


def is_test_result_node(node: dict[str, Any]) -> bool:
    """True when this result node is a test ya will run.

    ``graph.result`` also contains build outputs. Those share the suite folder
    via ``module_dir`` but do not add duration: the test node depends on them,
    and the shard downloads the binary from the remote cache.
    """
    if node.get("node-type") == "test":
        return True
    args = _cmd_args(node)
    return "run_test" in args or "--test-suite-name" in args or "--test-size" in args


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


def volume_shard_count(total_weight_sec: float, threads: int) -> int:
    """How many hosts the graph weight wants, before pool and caps.

    Minutes = total slot-seconds / 60 / threads. One host while that fits in
    one hour. After that, the smallest host count that keeps the same budget.
    """
    if threads < 1:
        raise ValueError("threads must be >= 1")
    minutes = float(total_weight_sec) / 60.0 / float(threads)
    if minutes <= 0:
        return 1
    return max(1, math.ceil(minutes / MAX_SHARD_WALL_MIN))


def choose_host_count(
    *,
    result_nodes: int,
    total_weight_sec: float,
    threads: int,
    free_runners: int | None = None,
    explicit: int | None = None,
    max_shards: int = MAX_SHARDS,
) -> int:
    """Hosts for one preset.

    ``explicit`` wins over auto. Auto is volume, then capped by free runners
    when that number is known: 0 or 1 free host forces a single job. Unknown
    availability (``None``) does not cap. The result is at least 1 when there
    is work, at most ``max_shards``, and never above ``result_nodes``.
    """
    if result_nodes < 1:
        raise ValueError("no result nodes")
    if explicit is not None:
        if explicit < 1:
            raise ValueError("explicit shard_count must be >= 1")
        desired = explicit
    else:
        desired = volume_shard_count(total_weight_sec, threads)
        if free_runners is not None:
            if free_runners <= 1:
                desired = 1
            else:
                desired = min(desired, free_runners)
    return max(1, min(desired, max_shards, result_nodes))


def footprints_config_path() -> Path:
    return Path(__file__).resolve().parents[2] / "config" / "runners_footprints.yml"


def load_simple_yaml(text: str) -> dict[str, Any]:
    """Indent-based mappings only. runners_footprints.yml has no lists."""
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, root)]
    for raw in text.splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        line = raw.strip()
        if line.startswith("- "):
            raise ValueError(f"lists are not supported in runner config: {line}")
        key, sep, value = line.partition(":")
        if not sep:
            continue
        key = key.strip()
        value = value.strip()
        if " #" in value:
            value = value.split(" #", 1)[0].strip()
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if value == "":
            child: dict[str, Any] = {}
            parent[key] = child
            stack.append((indent, child))
        else:
            parent[key] = _yaml_scalar(value)
    return root


def _yaml_scalar(value: str) -> Any:
    if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
        return value[1:-1]
    try:
        if any(ch in value for ch in ".eE"):
            return float(value)
        return int(value)
    except ValueError:
        return value


def load_runner_footprints(path: Path | None = None) -> dict[str, Any]:
    config_path = path or footprints_config_path()
    config = load_simple_yaml(config_path.read_text(encoding="utf-8"))
    footprints = config.get("footprints")
    default = config.get("default_footprint")
    cloud_id = config.get("quota_cloud_id")
    if not isinstance(footprints, dict) or not isinstance(default, dict) or not cloud_id:
        raise ValueError(f"{config_path} must set footprints, default_footprint, and quota_cloud_id")
    return config


def footprint_for(config: dict[str, Any], preset_label: str) -> dict[str, int]:
    found = (config.get("footprints") or {}).get(preset_label) or config["default_footprint"]
    if not isinstance(found, dict):
        raise ValueError(f"footprint for {preset_label} is not a mapping")
    return {res: int(found[res]) for res in _CAPACITY_RESOURCES}


def runners_that_fit(free: dict[str, float], footprint: dict[str, int]) -> int:
    """How many VMs of this footprint fit in quota limit-usage. The tightest resource wins."""
    fits = [float(free["instances"])]
    for res in _CAPACITY_RESOURCES:
        need = float(footprint[res])
        if need <= 0:
            raise ValueError(f"footprint {res} must be positive")
        fits.append(float(free[res]) / need)
    return max(int(math.floor(min(fits))), 0)


def quota_free(payload: dict[str, Any]) -> dict[str, float]:
    limits = payload.get("quotaLimits") or payload.get("quota_limits") or []
    found: dict[str, float] = {}
    for item in limits:
        if not isinstance(item, dict):
            continue
        quota_id = item.get("quotaId") or item.get("quota_id")
        scale = _QUOTA_SCALE.get(str(quota_id))
        if scale is None:
            continue
        name, divisor = scale
        found[name] = (float(item["limit"]) - float(item["usage"])) / divisor
    missing = [name for name, _divisor in _QUOTA_SCALE.values() if name not in found]
    if missing:
        raise KeyError(f"compute quota response missing {missing}")
    return found


def yc_compute_quota(cloud_id: str, key_file: str) -> dict[str, float]:
    """``yc quota-manager quota-limit list`` using the CI service-account key.

    yc reads that key itself. A private config under a temporary HOME keeps the
    call off the operator's logged-in profile.
    """
    with tempfile.TemporaryDirectory() as tmp:
        home = Path(tmp)
        cfg_dir = home / ".config" / "yandex-cloud"
        cfg_dir.mkdir(parents=True)
        (cfg_dir / "config.yaml").write_text(
            "current: sa\n"
            "profiles:\n"
            "  sa:\n"
            f"    service-account-key: {key_file}\n"
            f"    cloud-id: {cloud_id}\n",
            encoding="utf-8",
        )
        env = os.environ.copy()
        env["HOME"] = str(home)
        env.pop("YC_TOKEN", None)
        env.pop("YC_IAM_TOKEN", None)
        proc = subprocess.run(
            [
                "yc",
                "quota-manager",
                "quota-limit",
                "list",
                "--service",
                "compute",
                "--resource-type",
                "resource-manager.cloud",
                "--resource-id",
                cloud_id,
                "--format",
                "json",
            ],
            env=env,
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
    payload = json.loads(proc.stdout)
    if not isinstance(payload, dict):
        raise ValueError("yc quota response is not an object")
    return quota_free(payload)


def lookup_free_runners(preset_label: str) -> int | None:
    """How many more VMs of this preset fit in live Compute quota. None if quota cannot be read."""
    key_file = os.environ.get(_SA_KEY_ENV)
    if not key_file:
        print(
            f"runner availability unknown ({_SA_KEY_ENV} is unset); not capping by quota",
            file=sys.stderr,
        )
        return None
    try:
        config = load_runner_footprints()
        free = yc_compute_quota(str(config["quota_cloud_id"]), key_file)
        footprint = footprint_for(config, preset_label)
        count = runners_that_fit(free, footprint)
    except subprocess.CalledProcessError as exc:
        detail = [
            line
            for line in (exc.stderr or "").splitlines()
            if line.strip() and "PRIVATE" not in line and "BEGIN" not in line
        ]
        tail = detail[-1] if detail else f"exit {exc.returncode}"
        print(f"yc quota failed ({tail}); fallback to test volume only", file=sys.stderr)
        return None
    except (OSError, json.JSONDecodeError, KeyError, TimeoutError, ValueError, subprocess.TimeoutExpired) as exc:
        print(f"quota lookup failed ({exc}); fallback to test volume only", file=sys.stderr)
        return None
    printable = {name: round(value, 2) for name, value in free.items()}
    print(
        f"free runners for {preset_label}: {count} "
        f"(quota free {printable}; footprint {footprint})",
        file=sys.stderr,
    )
    return count


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
    p90_by_suite: dict[str, dict[str, float]] | None = None,
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
    duration_uids = {
        uid for uid in uids if is_test_result_node(nodes_by_uid.get(uid) or {})
    }
    result_keys = {
        (path.strip("/"), resolve_node_test_size(uid, nodes_by_uid.get(uid) or {}, size_by_uid))
        for uid in duration_uids
        if (path := extract_node_path(nodes_by_uid.get(uid) or {}))
    }
    weights: dict[str, float] = {}
    size_counts: Counter[str] = Counter()
    total_units = 0
    history_nodes = 0
    timeout_nodes = 0
    history_slot_seconds = 0.0
    observed_by_uid: dict[str, float | None] = {}
    path_by_uid: dict[str, str | None] = {}
    size_by_result: dict[str, str] = {}
    claimed: set[tuple[str, str]] = set()
    for uid in uids:
        node = nodes_by_uid.get(uid) or {}
        path = extract_node_path(node)
        path = path.strip("/") if path else None
        size = resolve_node_test_size(uid, node, size_by_uid)
        path_by_uid[uid] = path
        size_by_result[uid] = size
        if uid not in duration_uids:
            observed_by_uid[uid] = None
            continue
        observed = suite_history_seconds(path, p90_by_suite, result_keys, size)
        observed_by_uid[uid] = observed
        if not path or not p90_by_suite or observed is None:
            continue
        if _size_seconds(p90_by_suite, path, size) is not None:
            claimed.add((path, size))
        elif "/" in path and _size_seconds(p90_by_suite, path.rsplit("/", 1)[0], size) is not None:
            claimed.add((path.rsplit("/", 1)[0], size))
    if p90_by_suite:
        absorbing = [uid for uid in duration_uids if path_by_uid[uid]]
        absorbing.sort(key=lambda uid: len(path_by_uid[uid] or ""), reverse=True)
        for uid in absorbing:
            path = path_by_uid[uid] or ""
            size = size_by_result[uid]
            found = unclaimed_descendant_seconds(path, p90_by_suite, claimed, size)
            has_descendants = any(
                key.startswith(path + "/") and size in bucket
                for key, bucket in p90_by_suite.items()
            )
            if found is not None:
                total, taken = found
                claimed.update((key, size) for key in taken)
                current = observed_by_uid[uid]
                observed_by_uid[uid] = total if current is None else current + total
            elif has_descendants and observed_by_uid[uid] is None:
                observed_by_uid[uid] = 0.0
        # Several test nodes can share one suite folder and size. The p90 is the
        # folder total, so it is attached once.
        best_for_key: dict[tuple[str, str], str] = {}
        for uid in duration_uids:
            path = path_by_uid[uid]
            observed = observed_by_uid[uid]
            if not path or observed is None or observed <= 0:
                continue
            key = (path, size_by_result[uid])
            current = best_for_key.get(key)
            if current is None or observed > (observed_by_uid[current] or 0) or (
                observed == observed_by_uid[current] and uid < current
            ):
                best_for_key[key] = uid
        for uid in duration_uids:
            path = path_by_uid[uid]
            observed = observed_by_uid[uid]
            if not path or observed is None or observed <= 0:
                continue
            if best_for_key.get((path, size_by_result[uid])) != uid:
                observed_by_uid[uid] = 0.0
    for uid in uids:
        node = nodes_by_uid.get(uid) or {}
        if uid not in duration_uids:
            weights[uid] = 0.0
            continue
        weight, size, units = uid_weight(uid, node, nodes_by_uid, result_set, size_by_uid, threads)
        observed = observed_by_uid[uid]
        if observed is not None:
            weight = observed * float(cpu_slots(node, threads))
            history_slot_seconds += weight
            history_nodes += 1
        else:
            timeout_nodes += 1
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
            "mode": "history_p90_else_timeout" if p90_by_suite is not None else "timeout_budget_x_cpu",
            "history_nodes": history_nodes,
            "timeout_nodes": timeout_nodes,
            "history_slot_seconds": round(history_slot_seconds, 1),
            "expected_wall_min": round(history_slot_seconds / 60.0 / float(threads), 1),
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


def failed_report_keys(report: dict[str, Any]) -> tuple[list[str], set[str]]:
    """Suite paths and node uids of FAILED/ERROR rows.

    Ya reports identify a test by ``path`` and sometimes by ``uid``. A path
    plus test ``name`` is kept too, because the graph node often stops at the
    suite directory.
    """
    paths: list[str] = []
    uids: set[str] = set()
    for result in report.get("results") or []:
        if not isinstance(result, dict) or result.get("status") not in ("FAILED", "ERROR"):
            continue
        uid = str(result.get("uid") or "").strip()
        if uid:
            uids.add(uid)
        path = str(result.get("path") or "").strip().strip("/")
        name = str(result.get("name") or "").strip().strip("/")
        if path:
            paths.append(path)
        if path and name:
            paths.append(f"{path}/{name}")
        elif name:
            paths.append(name)
    return paths, uids


def result_uids_matching_paths(graph: dict[str, Any], paths: list[str], uids: set[str] | None = None) -> set[str]:
    """Result UIDs whose suite path or uid is one of the failed tests."""
    failed_uids = uids or set()
    if not paths and not failed_uids:
        return set()
    nodes = graph_nodes_by_uid(graph)
    matched: set[str] = set()
    for uid in result_uids(graph):
        if uid in failed_uids:
            matched.add(uid)
            continue
        node_path = (extract_node_path(nodes.get(uid) or {}) or "").strip("/")
        if not node_path:
            continue
        for path in paths:
            if node_path == path or node_path.startswith(path + "/") or path.startswith(node_path + "/"):
                matched.add(uid)
                break
    return matched


def matrix_rows_from_plans(plans: list[tuple[str, dict[str, Any]]]) -> tuple[list[dict[str, Any]], list[str]]:
    """Build shard-job rows from saved plans.

    A plan with one shard is not a matrix row: that preset already ran as a
    single job. A broken plan is reported and skipped so the other presets
    still get rows.
    """
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    for name, plan in plans:
        if not isinstance(plan, dict) or plan.get("_error"):
            errors.append(f"{name}: {plan.get('_error') if isinstance(plan, dict) else 'not an object'}")
            continue
        try:
            count = int(plan.get("shard_count") or 0)
        except (TypeError, ValueError):
            errors.append(f"{name}: shard_count is not an integer")
            continue
        if count <= 1:
            continue
        run = plan.get("run") if isinstance(plan.get("run"), dict) else {}
        preset = str(run.get("build_preset") or "")
        target = str(run.get("build_target") or "")
        size = str(run.get("test_size") or "")
        try:
            threads = int(run.get("threads") or plan.get("threads") or 0)
        except (TypeError, ValueError):
            threads = 0
        shards = plan.get("shards")
        if not preset or not target or not size or threads < 1 or not isinstance(shards, list) or not shards:
            errors.append(f"{name}: plan is missing the matrix row or its shard list")
            continue
        try:
            for shard in shards:
                rows.append(
                    {
                        "build_preset": preset,
                        "build_target": target,
                        "test_size": size,
                        "threads_count": threads,
                        "shard_id": int(shard["id"]),
                    }
                )
        except (KeyError, TypeError, ValueError) as exc:
            errors.append(f"{name}: {exc}")
    return rows, errors


def narrow_graph_to_report(graph: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    """Keep only failed result nodes. An empty match returns the graph unchanged.

    ``--build-custom-json`` ignores the test blacklist, so a shard retry has to
    shrink ``graph.result`` itself. Missing the failed suite must not drop the shard.
    """
    paths, uids = failed_report_keys(report)
    allowed = result_uids_matching_paths(graph, paths, uids)
    if not allowed or allowed == set(result_uids(graph)):
        return graph
    return filter_graph_result(graph, allowed)


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
    ]
    weighting = plan.get("weighting") or {}
    if "history_nodes" in weighting:
        lines.insert(
            5,
            f"**Expected:** {weighting.get('expected_wall_min', 0)} min on one host "
            f"from {weighting['history_nodes']} suites matched to p90, "
            f"timeout fallback {weighting['timeout_nodes']}",
        )
    policy = plan.get("host_policy") or {}
    if policy:
        lines.append(
            f"**Hosts:** mode={policy.get('mode')}, "
            f"volume={policy.get('volume_shards')}, "
            f"free_runners={policy.get('free_runners')}, "
            f"chosen={policy.get('chosen')}, "
            f"capacity={policy.get('capacity')}, "
            f"availability={policy.get('availability')}"
        )
        lines.append("")
    lines += [
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


def load_blacklist_patterns(path: Path) -> list[str]:
    """Paths from a ya ``--test-blacklist-path`` file (``- path: ...`` lines)."""
    patterns: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "path:" not in line:
            continue
        value = line.split("path:", 1)[1].strip().strip("'\"")
        if value:
            patterns.append(value.strip("/"))
    return patterns


def _matches_blacklist(node_path: str, pattern: str) -> bool:
    node = node_path.strip("/")
    pat = pattern.strip("/")
    if not node or not pat:
        return False
    if node == pat or node.startswith(pat + "/"):
        return True
    return fnmatch.fnmatch(node, pat) or fnmatch.fnmatch(node, pat + "/*")


def without_blacklisted(graph: dict[str, Any], patterns: list[str]) -> dict[str, Any]:
    """Drop result nodes covered by the blacklist.

    ``--build-custom-json`` ignores ``--test-blacklist-path`` at replay time,
    so the saved graph itself must not list those tests.
    """
    if not patterns:
        return graph
    nodes = graph_nodes_by_uid(graph)
    drop: set[str] = set()
    for uid in result_uids(graph):
        node_path = extract_node_path(nodes.get(uid) or {}) or ""
        if node_path and any(_matches_blacklist(node_path, pattern) for pattern in patterns):
            drop.add(uid)
    if not drop:
        return graph
    keep = set(result_uids(graph)) - drop
    if not keep:
        raise ValueError("blacklist removed every result node")
    return filter_graph_result(graph, keep)


_BUILD_TYPE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_BRANCH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,200}$")
_HISTORY_DAYS = 14


def suite_p90_query(table: str, build_type: str, branch: str) -> str:
    """Sum of per-test p90 durations for each suite. Lint and chunk duplicates are left out."""
    if not _BUILD_TYPE_RE.fullmatch(build_type):
        raise ValueError(f"build type is not safe to interpolate: {build_type!r}")
    if not _BRANCH_RE.fullmatch(branch):
        raise ValueError(f"branch is not safe to interpolate: {branch!r}")
    if not table or "`" in table or any(ch.isspace() for ch in table):
        raise ValueError(f"table path is not safe to interpolate: {table!r}")
    return f"""
        SELECT suite_folder, test_size, SUM(test_p90) AS p90
        FROM (
            SELECT
                suite_folder,
                test_size,
                test_name,
                PERCENTILE(duration, 0.9) AS test_p90
            FROM (
                SELECT
                    suite_folder,
                    test_name,
                    JSON_VALUE(metadata, "$.size") AS test_size,
                    duration
                FROM `{table}`
                WHERE run_timestamp >= CurrentUtcTimestamp() - Interval("P{_HISTORY_DAYS}D")
                  AND branch = '{branch}'
                  AND build_type = '{build_type}'
                  AND status != 'skipped'
                  AND duration > 0
                  AND job_id IS NOT NULL
                  AND suite_folder IS NOT NULL
                  AND suite_folder != ''
                  AND test_name IS NOT NULL
                  AND test_name != ''
                  AND JSON_VALUE(metadata, "$.size") IN ("small", "medium", "large")
                  AND String::Contains(test_name, '.flake8') = FALSE
                  AND String::Contains(test_name, 'clang-format') = FALSE
                  AND String::Contains(test_name, 'clang_format') = FALSE
                  AND String::Contains(test_name, '.black') = FALSE
                  AND String::Contains(test_name, 'import_test') = FALSE
                  AND String::Contains(test_name, 'sole chunk') = FALSE
                  AND String::Contains(test_name, 'chunk+chunk') = FALSE
                  AND String::Contains(test_name, '[chunk]') = FALSE
            )
            GROUP BY suite_folder, test_size, test_name
        )
        GROUP BY suite_folder, test_size
    """


def _row_field(row: Any, name: str) -> Any:
    if isinstance(row, dict):
        return row.get(name)
    return getattr(row, name, None)


def load_suite_p90(build_type: str, branch: str) -> dict[str, dict[str, float]] | None:
    """Suite path to per-size sum of test p90 seconds. None if YDB QA cannot be read."""
    try:
        analytics = Path(__file__).resolve().parents[1] / "analytics"
        if str(analytics) not in sys.path:
            sys.path.insert(0, str(analytics))
        from ydb_wrapper import YDBWrapper

        with YDBWrapper(silent=True) as wrapper:
            table = wrapper.get_table_path("test_results")
            rows = wrapper.execute_scan_query(
                suite_p90_query(table, build_type, branch),
                query_name="suite_p90",
            )
    except Exception as exc:
        print(f"suite p90 unavailable ({exc}); using graph timeouts", file=sys.stderr)
        return None
    found: dict[str, dict[str, float]] = {}
    for row in rows or []:
        folder = _row_field(row, "suite_folder")
        size = _row_field(row, "test_size")
        p90 = _row_field(row, "p90")
        if not isinstance(folder, str) or not folder.strip() or not isinstance(size, str) or p90 is None:
            continue
        size = size.strip().lower()
        if size not in DEFAULT_SIZE_WEIGHTS:
            continue
        try:
            seconds = float(p90)
        except (TypeError, ValueError):
            continue
        if seconds > 0:
            found.setdefault(folder.strip("/"), {})[size] = seconds
    print(
        f"suite p90: {sum(len(sizes) for sizes in found.values())} suite-sizes, "
        f"branch={branch}, build_type={build_type}, "
        f"last {_HISTORY_DAYS} days",
        file=sys.stderr,
    )
    return found


def _resolve_requested_count(raw: str) -> int | None:
    text = raw.strip().lower()
    if text == "auto":
        return None
    if not text.isdigit() or int(text) < 1:
        raise ValueError("shard-count must be 'auto' or a positive integer")
    return int(text)


def _cmd_plan(args: argparse.Namespace) -> int:
    graph = load_graph(args.graph)
    if args.blacklist:
        graph = without_blacklisted(graph, load_blacklist_patterns(args.blacklist))
    context = None
    if args.context:
        context = json.loads(args.context.read_text(encoding="utf-8"))
    explicit = _resolve_requested_count(str(args.shard_count))
    if args.build_preset and args.branch:
        p90_by_suite = load_suite_p90(args.build_preset, args.branch)
    else:
        print(
            "suite p90 skipped (need --build-preset and --branch); using graph timeouts",
            file=sys.stderr,
        )
        p90_by_suite = None
    probe = build_plan(
        graph,
        1,
        threads=args.threads,
        context=context,
        p90_by_suite=p90_by_suite,
    )
    free_runners = None
    if explicit is None and args.preset_label:
        free_runners = lookup_free_runners(args.preset_label)
    chosen = choose_host_count(
        result_nodes=int(probe["total_result_nodes"]),
        total_weight_sec=float(probe["total_weight"]),
        threads=args.threads,
        free_runners=free_runners,
        explicit=explicit,
    )
    plan = probe if chosen == 1 else build_plan(
        graph,
        chosen,
        threads=args.threads,
        context=context,
        p90_by_suite=p90_by_suite,
    )
    plan["host_policy"] = {
        "mode": "auto" if explicit is None else "explicit",
        "volume_shards": volume_shard_count(float(probe["total_weight"]), args.threads),
        "free_runners": free_runners,
        "chosen": chosen,
        "capacity": "compute quota limit-usage; footprints from runners_footprints.yml",
        "availability": (
            "explicit"
            if explicit is not None
            else "api"
            if free_runners is not None
            else "volume-only fallback"
        ),
    }
    # The caller records the matrix row that produced this plan. Shard jobs
    # read it back instead of keeping a second copy of the preset list.
    if args.build_preset or args.build_target or args.test_size or args.branch:
        plan["run"] = {
            "build_preset": args.build_preset,
            "branch": args.branch,
            "build_target": args.build_target,
            "test_size": args.test_size,
            "threads": args.threads,
        }
    _write_json(args.output, plan)
    summary = render_summary(plan)
    if args.summary:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        args.summary.write_text(summary, encoding="utf-8")
    sys.stdout.write(summary)
    return 0


def _cmd_narrow_retry(args: argparse.Namespace) -> int:
    graph = load_graph(args.graph)
    report = json.loads(args.report.read_text(encoding="utf-8"))
    if not isinstance(report, dict):
        raise ValueError("report must be a JSON object")
    narrowed = narrow_graph_to_report(graph, report)
    _write_json(args.output, narrowed)
    if args.context or args.context_output:
        if not args.context or not args.context_output:
            print("narrow-retry: --context and --context-output are both required", file=sys.stderr)
            return 2
        context = json.loads(args.context.read_text(encoding="utf-8"))
        allowed = {uid for uid in result_uids(narrowed) if uid.startswith("test-")}
        _write_json(args.context_output, filter_context_tests(context, allowed))
    print(
        f"Retry graph: {len(result_uids(narrowed))}/{len(result_uids(graph))} result nodes",
        file=sys.stderr,
    )
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
    plan.add_argument("--shard-count", required=True, help="'auto' or an explicit positive integer")
    plan.add_argument(
        "--preset-label",
        default="",
        help="Runner label for the pool cap, e.g. build-preset-relwithdebinfo. Used when shard-count=auto.",
    )
    plan.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    plan.add_argument("--blacklist", type=Path, default=None, help="ya test blacklist; applied before packing")
    plan.add_argument("--build-preset", default="")
    plan.add_argument("--branch", default="", help="Test-history branch, e.g. main or stable-26-1")
    plan.add_argument("--build-target", default="")
    plan.add_argument("--test-size", default="")
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

    narrow = sub.add_parser("narrow-retry", help="Shrink a shard graph to failed suites")
    narrow.add_argument("--graph", type=Path, required=True)
    narrow.add_argument("--report", type=Path, required=True)
    narrow.add_argument("--context", type=Path)
    narrow.add_argument("-o", "--output", type=Path, required=True)
    narrow.add_argument("--context-output", type=Path)
    narrow.set_defaults(func=_cmd_narrow_retry)

    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
