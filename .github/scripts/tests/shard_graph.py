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
import math
import os
import re
import sys
import urllib.error
import urllib.request
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
# Single-job minutes under this stay on one host (PR 47597 "pr" profile).
VOLUME_LIGHT_MINUTES = 60.0
VOLUME_TIERS: tuple[tuple[float, int], ...] = (
    (120.0, 4),
    (200.0, 8),
    (math.inf, 12),
)
# Keep the slowest shard's ideal wall time inside the PR-check budget.
MAX_SHARD_WALL_MIN = 240.0

# Folder quota snapshot from PR 47597 (.github/config/runner_capacity.yml, 2026-06-13).
# Free hosts = how many more VMs of this preset fit after queued/in-progress jobs.
# If the Actions API cannot be read, availability is unknown and only volume applies.
RUNNER_CAPACITY: dict[str, Any] = {
    "quotas": {"vcpu": 5400, "ram_gb": 23000, "instances": 110, "nrd_ssd_gb": 200000},
    "reserved": {"vcpu": 200, "ram_gb": 600, "instances": 22, "nrd_ssd_gb": 16000},
    "headroom_fraction": 0.9,
    "footprints": {
        "build-preset-relwithdebinfo": {"vcpu": 64, "ram_gb": 256, "nrd_ssd_gb": 2417},
        "build-preset-release-asan": {"vcpu": 96, "ram_gb": 288, "nrd_ssd_gb": 2417},
        "build-preset-release-msan": {"vcpu": 64, "ram_gb": 320, "nrd_ssd_gb": 2417},
        "build-preset-release-tsan": {"vcpu": 64, "ram_gb": 320, "nrd_ssd_gb": 2417},
    },
    "default_footprint": {"vcpu": 96, "ram_gb": 320, "nrd_ssd_gb": 2417},
}
_CAPACITY_RESOURCES = ("vcpu", "ram_gb", "nrd_ssd_gb")

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


def volume_shard_count(total_weight_sec: float, threads: int) -> int:
    """How many hosts the graph weight wants, before pool and caps.

    Minutes = total slot-seconds / 60 / threads. Under 60 minutes stays one
    job. Then the PR 47597 tiers: 4, 8, 12. A wall floor raises that so the
    ideal slowest shard stays within 240 minutes.
    """
    if threads < 1:
        raise ValueError("threads must be >= 1")
    minutes = float(total_weight_sec) / 60.0 / float(threads)
    if minutes < VOLUME_LIGHT_MINUTES:
        count = 1
    else:
        count = int(VOLUME_TIERS[-1][1])
        for upper, tier in VOLUME_TIERS:
            if minutes < float(upper):
                count = int(tier)
                break
    if minutes <= 0:
        return 1
    wall = max(1, math.ceil(minutes / MAX_SHARD_WALL_MIN))
    return max(count, wall)


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


def compute_max_new_runners(
    demand: Counter[str],
    preset_label: str,
    config: dict[str, Any] | None = None,
) -> int:
    """How many more runners of ``preset_label`` fit in the folder quota.

    Same arithmetic as PR 47597 ``compute_max_new_runners``: subtract busy
    jobs' footprints and the static reserve from the quota, then see how
    many VMs of this preset fit in the tightest resource.
    """
    cfg = config or RUNNER_CAPACITY
    quotas = cfg["quotas"]
    reserved = cfg.get("reserved") or {}
    headroom = float(cfg.get("headroom_fraction", 1.0))
    footprints = cfg["footprints"]
    default_footprint = cfg["default_footprint"]

    def footprint(label: str) -> dict[str, int]:
        found = footprints.get(label) or default_footprint
        return {res: int(found[res]) for res in _CAPACITY_RESOURCES}

    used = {res: 0.0 for res in _CAPACITY_RESOURCES}
    used_instances = 0
    for label, count in demand.items():
        fp = footprint(label)
        for res in _CAPACITY_RESOURCES:
            used[res] += fp[res] * count
        used_instances += count

    fits = [((quotas["instances"] - reserved.get("instances", 0)) * headroom) - used_instances]
    target = footprint(preset_label)
    for res in _CAPACITY_RESOURCES:
        free = ((quotas[res] - reserved.get(res, 0)) * headroom) - used[res]
        fits.append(free / target[res])
    return max(int(math.floor(min(fits))), 0)


def lookup_free_runners(preset_label: str) -> int | None:
    """Busy Actions jobs vs RUNNER_CAPACITY. None if the pool cannot be read."""
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    repo = os.environ.get("GITHUB_REPOSITORY")
    if not token or not repo:
        print("runner availability unknown (no token); not capping by pool", file=sys.stderr)
        return None
    try:
        demand = _count_busy_runner_jobs(repo, token)
        free = compute_max_new_runners(demand, preset_label)
    except (OSError, urllib.error.URLError, json.JSONDecodeError, KeyError, TimeoutError) as exc:
        print(f"runner availability unknown ({exc}); not capping by pool", file=sys.stderr)
        return None
    print(f"free runners for {preset_label}: {free} (busy={dict(demand)})", file=sys.stderr)
    return free


def busy_labels_in_jobs(payload: dict[str, Any], known: set[str] | None = None) -> Counter[str]:
    """Count queued or in-progress jobs that occupy a build-preset runner."""
    labels = known if known is not None else set(RUNNER_CAPACITY["footprints"])
    demand: Counter[str] = Counter()
    for job in payload.get("jobs") or []:
        if not isinstance(job, dict) or job.get("status") not in ("queued", "in_progress"):
            continue
        for label in job.get("labels") or []:
            if label in labels or str(label).startswith("build-preset-"):
                demand[str(label)] += 1
                break
    return demand


def _count_busy_runner_jobs(repo: str, token: str) -> Counter[str]:
    demand: Counter[str] = Counter()
    known = set(RUNNER_CAPACITY["footprints"])
    for status in ("queued", "in_progress"):
        run_pages = _github_pages(
            f"https://api.github.com/repos/{repo}/actions/runs?status={status}&per_page=100",
            token,
        )
        for payload in run_pages:
            for run in payload.get("workflow_runs") or []:
                if not isinstance(run, dict) or "id" not in run:
                    continue
                for jobs in _github_pages(
                    f"https://api.github.com/repos/{repo}/actions/runs/{run['id']}/jobs?per_page=100",
                    token,
                ):
                    demand.update(busy_labels_in_jobs(jobs, known))
    return demand


def next_link(link_header: str) -> str:
    """Return the URL marked rel=next in a GitHub Link header, or ''."""
    for part in (link_header or "").split(","):
        bits = [bit.strip() for bit in part.split(";")]
        if not bits or not bits[0].startswith("<") or not bits[0].endswith(">"):
            continue
        rels = {bit for bit in bits[1:]}
        if 'rel="next"' in rels or "rel=next" in rels:
            return bits[0][1:-1]
    return ""


def _github_get(url: str, token: str) -> tuple[dict[str, Any], str]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "ydb-shard-hosts",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.load(response)
        link = response.headers.get("Link", "")
    if not isinstance(payload, dict):
        raise ValueError(f"unexpected response from {url}")
    return payload, str(link)


def _github_pages(url: str, token: str) -> list[dict[str, Any]]:
    pages: list[dict[str, Any]] = []
    seen: set[str] = set()
    while url and url not in seen:
        seen.add(url)
        payload, link = _github_get(url, token)
        pages.append(payload)
        url = next_link(link)
    return pages


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


def failed_suite_paths(report: dict[str, Any]) -> list[str]:
    """Suite paths of FAILED/ERROR rows in a ya build-results report."""
    paths: list[str] = []
    for result in report.get("results") or []:
        if not isinstance(result, dict) or result.get("status") not in ("FAILED", "ERROR"):
            continue
        path = str(result.get("path") or "").strip().strip("/")
        if path:
            paths.append(path)
    return paths


def result_uids_matching_paths(graph: dict[str, Any], paths: list[str]) -> set[str]:
    """Result UIDs whose suite path is a failed suite, or contains one."""
    if not paths:
        return set()
    nodes = graph_nodes_by_uid(graph)
    matched: set[str] = set()
    for uid in result_uids(graph):
        node_path = (extract_node_path(nodes.get(uid) or {}) or "").strip("/")
        if not node_path:
            continue
        for path in paths:
            if node_path == path or node_path.startswith(path + "/") or path.startswith(node_path + "/"):
                matched.add(uid)
                break
    return matched


def narrow_graph_to_report(graph: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    """Keep only failed result nodes. An empty match returns the graph unchanged.

    ``--build-custom-json`` ignores the test blacklist, so a shard retry has to
    shrink ``graph.result`` itself. Missing the failed suite must not drop the shard.
    """
    allowed = result_uids_matching_paths(graph, failed_suite_paths(report))
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
    policy = plan.get("host_policy") or {}
    if policy:
        lines.append(
            f"**Hosts:** mode={policy.get('mode')}, "
            f"volume={policy.get('volume_shards')}, "
            f"free_runners={policy.get('free_runners')}, "
            f"chosen={policy.get('chosen')}"
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


def _resolve_requested_count(raw: str) -> int | None:
    text = raw.strip().lower()
    if text == "auto":
        return None
    if not text.isdigit() or int(text) < 1:
        raise ValueError("shard-count must be 'auto' or a positive integer")
    return int(text)


def _cmd_plan(args: argparse.Namespace) -> int:
    graph = load_graph(args.graph)
    context = None
    if args.context:
        context = json.loads(args.context.read_text(encoding="utf-8"))
    explicit = _resolve_requested_count(str(args.shard_count))
    probe = build_plan(graph, 1, threads=args.threads, context=context)
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
    plan = probe if chosen == 1 else build_plan(graph, chosen, threads=args.threads, context=context)
    plan["host_policy"] = {
        "mode": "auto" if explicit is None else "explicit",
        "volume_shards": volume_shard_count(float(probe["total_weight"]), args.threads),
        "free_runners": free_runners,
        "chosen": chosen,
    }
    # The caller records the matrix row that produced this plan. Shard jobs
    # read it back instead of keeping a second copy of the preset list.
    if args.build_preset or args.build_target or args.test_size:
        plan["run"] = {
            "build_preset": args.build_preset,
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
    plan.add_argument("--build-preset", default="")
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
