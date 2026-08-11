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
    version: int = 2
    physical_cores: tuple = ()
    smt_siblings: tuple = ()
    hierarchy_reasons: tuple = ()


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


def _read_int(path):
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def _allowed_cpus():
    if hasattr(os, "sched_getaffinity"):
        return tuple(sorted(os.sched_getaffinity(0)))
    return tuple(range(os.cpu_count() or 1))


def discover_topology(sys_root=Path("/sys/devices/system"), allowed_cpus=None):
    sys_root = Path(sys_root)
    allowed = tuple(sorted(set(_allowed_cpus() if allowed_cpus is None else allowed_cpus)))
    allowed_set = set(allowed)

    reasons = []
    nodes = []
    for node_path in sorted((sys_root / "node").glob("node[0-9]*")):
        cpus = tuple(cpu for cpu in _read_cpu_list(node_path / "cpulist") if cpu in allowed_set)
        if cpus:
            nodes.append((int(node_path.name[4:]), cpus))
    if not nodes:
        nodes = [(0, allowed)]
        reasons.append(("numa", "NUMA node cpulists are unavailable; using synthetic node 0"))

    chiplet_sets = set()
    l3_cpus = set()
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
                    l3_cpus.update(shared)

    # A partially described cache hierarchy is no more useful for the CPUs it
    # omits than a missing one.  Fill those CPUs from die topology too, so
    # every allowed CPU has a deterministic place in the hierarchy.
    missing_l3 = allowed_set - l3_cpus
    if missing_l3:
        reasons.append(("chiplet", "L3 cache groups are unavailable for some allowed CPUs; using die groups"))
        die_groups = {}
        missing_die_id = False
        for node_id, node_cpus in nodes:
            for cpu in node_cpus:
                if cpu not in missing_l3:
                    continue
                topology_root = sys_root / "cpu" / "cpu{}".format(cpu) / "topology"
                die_id = _read_int(topology_root / "die_id")
                # package_id prevents the synthetic die 0 from combining CPUs
                # from separate sockets when die_id is absent.
                package_id = _read_int(topology_root / "physical_package_id")
                if die_id is None:
                    # An unknown die must not be presented as a shared die.
                    # A CPU-sized fallback is deterministic and conservative.
                    missing_die_id = True
                    die_id = ("cpu", cpu)
                die_groups.setdefault((node_id, package_id, die_id), []).append(cpu)
        chiplet_sets.update(tuple(cpus) for cpus in die_groups.values())
        if missing_die_id:
            reasons.append(("chiplet", "die_id is unavailable; affected CPUs are singleton chiplet groups"))

    chiplets = []
    for cpus in sorted(chiplet_sets):
        node_id = next(
            (node_id for node_id, node_cpus in nodes if set(cpus).issubset(node_cpus)),
            next((node_id for node_id, node_cpus in nodes if cpus[0] in node_cpus), -1),
        )
        chiplets.append((node_id, cpus))
    core_data = {}
    sibling_sets = set()
    incomplete_core_data = False
    incomplete_smt_data = False
    for cpu in allowed:
        topology_root = sys_root / "cpu" / "cpu{}".format(cpu) / "topology"
        package_id = _read_int(topology_root / "physical_package_id")
        core_id = _read_int(topology_root / "core_id")
        siblings = tuple(candidate for candidate in _read_cpu_list(topology_root / "thread_siblings_list")
                         if candidate in allowed_set)
        if not siblings or cpu not in siblings:
            incomplete_smt_data = True
            siblings = (cpu,)
            sibling_sets.add(siblings)
        else:
            sibling_sets.add(siblings)
        if package_id is None or core_id is None:
            incomplete_core_data = True
            core_data[cpu] = None
        elif siblings == (cpu,) and (topology_root / "thread_siblings_list").exists():
            core_data[cpu] = (package_id, core_id)
        elif siblings != (cpu,):
            core_data[cpu] = (package_id, core_id)
        else:
            # Missing SMT data must not cause CPUs with matching partial IDs
            # to be merged into a guessed physical core.
            incomplete_smt_data = True
            core_data[cpu] = None

    core_groups = {}
    for cpu in allowed:
        key = core_data[cpu]
        # Unknown topology gets a CPU-specific key: this is deliberately
        # conservative and can never merge two distinct physical cores.
        core_groups.setdefault(key if key is not None else ("cpu", cpu), []).append(cpu)
    if incomplete_core_data:
        reasons.append(("physical_core", "physical_package_id or core_id is unavailable; affected CPUs are singleton cores"))
    if incomplete_smt_data:
        reasons.append(("smt", "thread_siblings_list is incomplete; affected CPUs are singleton SMT groups"))

    return CpuTopology(
        allowed_cpus=allowed,
        numa_nodes=tuple(nodes),
        chiplets=tuple(chiplets),
        physical_cores=tuple(sorted(tuple(cpus) for cpus in core_groups.values())),
        smt_siblings=tuple(sorted(sibling_sets)),
        hierarchy_reasons=tuple(reasons),
    )


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
        "version": topology.version,
        "allowed_cpus": list(topology.allowed_cpus),
        "numa_nodes": [
            {"id": node_id, "cpus": list(cpus)} for node_id, cpus in topology.numa_nodes
        ],
        "chiplets": [
            {"numa_node": node_id, "cpus": list(cpus)} for node_id, cpus in topology.chiplets
        ],
        "physical_cores": [list(cpus) for cpus in topology.physical_cores],
        "smt_siblings": [list(cpus) for cpus in topology.smt_siblings],
        "hierarchy_reasons": [
            {"level": level, "reason": reason} for level, reason in topology.hierarchy_reasons
        ],
    }
