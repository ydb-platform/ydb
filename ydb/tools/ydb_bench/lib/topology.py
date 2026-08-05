import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


AFFINITY_MODES = (
    "none",
    "one-whole-numa",
    "one-whole-chiplet",
    "multi-chiplet",
)


@dataclass(frozen=True)
class CpuTopology:
    allowed_cpus: tuple
    numa_nodes: tuple
    chiplets: tuple


@dataclass(frozen=True)
class AffinityPlacement:
    mode: str
    cpus: Optional[tuple]
    reason: Optional[str] = None

    @property
    def supported(self):
        return self.reason is None


def parse_cpu_list(value):
    cpus = set()
    for item in value.strip().split(","):
        if not item:
            continue
        bounds = item.split("-", 1)
        first = int(bounds[0])
        last = int(bounds[-1])
        if last < first:
            raise ValueError("invalid CPU range: {}".format(item))
        cpus.update(range(first, last + 1))
    return tuple(sorted(cpus))


def _read_cpu_list(path):
    try:
        return parse_cpu_list(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return ()


def _allowed_cpus():
    if hasattr(os, "sched_getaffinity"):
        return tuple(sorted(os.sched_getaffinity(0)))
    return tuple(range(os.cpu_count() or 1))


def discover_topology(sys_root=Path("/sys/devices/system"), allowed_cpus=None):
    sys_root = Path(sys_root)
    allowed = tuple(sorted(set(_allowed_cpus() if allowed_cpus is None else allowed_cpus)))
    allowed_set = set(allowed)

    nodes = []
    for node_path in sorted((sys_root / "node").glob("node[0-9]*")):
        cpus = tuple(cpu for cpu in _read_cpu_list(node_path / "cpulist") if cpu in allowed_set)
        if cpus:
            nodes.append((int(node_path.name[4:]), cpus))
    if not nodes:
        nodes = [(0, allowed)]

    chiplet_sets = set()
    for cpu in allowed:
        cache_root = sys_root / "cpu" / "cpu{}".format(cpu) / "cache"
        for cache_path in sorted(cache_root.glob("index[0-9]*")):
            try:
                level = cache_path.joinpath("level").read_text(encoding="utf-8").strip()
            except OSError:
                continue
            if level == "3":
                shared = tuple(
                    candidate
                    for candidate in _read_cpu_list(cache_path / "shared_cpu_list")
                    if candidate in allowed_set
                )
                if shared:
                    chiplet_sets.add(shared)

    if not chiplet_sets:
        die_groups = {}
        for node_id, node_cpus in nodes:
            for cpu in node_cpus:
                try:
                    die_id = int(
                        (sys_root / "cpu" / "cpu{}".format(cpu) / "topology" / "die_id")
                        .read_text(encoding="utf-8")
                        .strip()
                    )
                except (OSError, ValueError):
                    die_id = 0
                die_groups.setdefault((node_id, die_id), []).append(cpu)
        chiplet_sets.update(tuple(cpus) for cpus in die_groups.values())

    chiplets = []
    for cpus in sorted(chiplet_sets):
        node_id = next(
            (node_id for node_id, node_cpus in nodes if set(cpus).issubset(node_cpus)),
            next((node_id for node_id, node_cpus in nodes if cpus[0] in node_cpus), -1),
        )
        chiplets.append((node_id, cpus))
    return CpuTopology(allowed_cpus=allowed, numa_nodes=tuple(nodes), chiplets=tuple(chiplets))


def _spread(groups, count):
    selected = []
    offset = 0
    while len(selected) < count:
        progressed = False
        for group in groups:
            if offset < len(group):
                selected.append(group[offset])
                progressed = True
                if len(selected) == count:
                    break
        if not progressed:
            break
        offset += 1
    return tuple(sorted(selected))


def _unsupported(mode, reason):
    return AffinityPlacement(mode=mode, cpus=None, reason=reason)


def plan_affinity(mode, topology, required_cpus):
    if mode == "none":
        return AffinityPlacement(mode=mode, cpus=None)
    if not hasattr(os, "sched_setaffinity"):
        return _unsupported(mode, "CPU affinity is not supported by this operating system")

    if mode == "one-whole-numa":
        node = next((cpus for _, cpus in topology.numa_nodes if cpus), None)
        if node is None:
            return _unsupported(mode, "no NUMA node contains allowed CPUs")
        return AffinityPlacement(mode=mode, cpus=node)

    if mode == "one-whole-chiplet":
        chiplet = next((cpus for _, cpus in topology.chiplets if cpus), None)
        if chiplet is None:
            return _unsupported(mode, "no chiplet contains allowed CPUs")
        return AffinityPlacement(mode=mode, cpus=chiplet)

    if mode == "multi-chiplet":
        by_node = {}
        for node_id, cpus in topology.chiplets:
            by_node.setdefault(node_id, []).append(cpus)
        groups = next(
            (
                node_chiplets
                for _, node_chiplets in sorted(by_node.items())
                if len(node_chiplets) >= 2 and sum(map(len, node_chiplets)) >= required_cpus
            ),
            None,
        )
        if required_cpus < 2 or groups is None:
            return _unsupported(
                mode,
                "at least two chiplets in one NUMA node and {} allowed CPUs are required".format(required_cpus),
            )
        return AffinityPlacement(mode=mode, cpus=_spread(groups, required_cpus))

    raise ValueError("unknown affinity mode: {}".format(mode))


def topology_record(topology):
    return {
        "allowed_cpus": list(topology.allowed_cpus),
        "numa_nodes": [
            {"id": node_id, "cpus": list(cpus)} for node_id, cpus in topology.numa_nodes
        ],
        "chiplets": [
            {"numa_node": node_id, "cpus": list(cpus)} for node_id, cpus in topology.chiplets
        ],
    }
