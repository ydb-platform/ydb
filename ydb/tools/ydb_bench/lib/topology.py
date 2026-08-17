import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


AFFINITY_MODES = (
    "none",
    "pack-numa",
    "pack-numa-pack-chiplet",
    "spread-numa-pack-chiplet",
    "pack-numa-pack-chiplet-pack-core",
    "pack-numa-pack-chiplet-spread-core",
    "pack-numa-spread-chiplet-pack-core",
    "pack-numa-spread-chiplet-spread-core",
    "spread-numa-pack-chiplet-pack-core",
    "spread-numa-pack-chiplet-spread-core",
    "spread-numa-spread-chiplet-pack-core",
    "spread-numa-spread-chiplet-spread-core",
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
    # A non-empty value means the discovered L3 groups cannot safely drive a
    # chiplet-aware placement.  The raw hierarchy remains useful for display,
    # while plan_affinity() rejects only modes which rely on this level.
    chiplet_topology_reason: Optional[str] = None


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

    # L3 is the primary chiplet contract.  Only synthesize die groups when it
    # is wholly unavailable: combining partial L3 data with inferred groups
    # invents a hierarchy (and reasons) for CPUs which sysfs did not describe.
    if not l3_cpus:
        reasons.append(("chiplet", "L3 cache groups are unavailable; using die groups"))
        die_groups = {}
        missing_die_id = False
        for node_id, node_cpus in nodes:
            for cpu in node_cpus:
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

    chiplet_topology_reasons = []
    if l3_cpus:
        missing_l3_cpus = tuple(sorted(allowed_set - l3_cpus))
        if missing_l3_cpus:
            chiplet_topology_reasons.append(
                "L3 cache groups do not cover all allowed CPUs (missing: {})".format(
                    ", ".join(str(cpu) for cpu in missing_l3_cpus)
                )
            )

    chiplets = []
    for cpus in sorted(chiplet_sets):
        node_ids = tuple(
            node_id for node_id, node_cpus in nodes if set(cpus).issubset(node_cpus)
        )
        if len(node_ids) != 1:
            # The chiplet record has a NUMA-node identifier, so assigning this
            # group to the node of its first CPU would fabricate a hierarchy.
            # Keep chiplet-based modes unavailable until this topology is
            # understood; NUMA-only modes do not rely on the L3 grouping.
            chiplet_topology_reasons.append(
                "L3 cache group {} does not belong to exactly one NUMA node".format(
                    ", ".join(str(cpu) for cpu in cpus)
                )
            )
            continue
        node_id = node_ids[0]
        chiplets.append((node_id, cpus))
    chiplet_topology_reason = "; ".join(chiplet_topology_reasons) or None
    if chiplet_topology_reason:
        reasons.append(("chiplet", chiplet_topology_reason))
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
        chiplet_topology_reason=chiplet_topology_reason,
    )


def _round_robin(groups):
    result = []
    offset = 0
    while True:
        progressed = False
        for group in groups:
            if offset < len(group):
                result.append(group[offset])
                progressed = True
        if not progressed:
            return result
        offset += 1


def _unsupported(mode, reason):
    return AffinityPlacement(mode=mode, cpus=None, reason=reason)


def _parse_mode(mode):
    if mode not in AFFINITY_MODES:
        raise ValueError("unknown affinity mode: {}".format(mode))
    if mode == "none":
        return ()
    parts = mode.split("-")
    return tuple((parts[index + 1], parts[index]) for index in range(0, len(parts), 2))


def _core_groups(topology):
    allowed = set(topology.allowed_cpus)
    groups = topology.physical_cores or tuple((cpu,) for cpu in topology.allowed_cpus)
    return tuple(sorted(tuple(sorted(set(group) & allowed)) for group in groups if set(group) & allowed))


def _select_units(units, required_cpus):
    selected = []
    for unit in units:
        selected.extend(unit)
        if len(selected) >= required_cpus:
            return tuple(sorted(selected))
    return ()


def _plan_units(mode, topology):
    """Build ordered CPU bundles by composing NUMA, chiplet, and core policies."""
    policy = dict(_parse_mode(mode))
    cores = _core_groups(topology)

    def cores_in(cpus):
        return [core for core in cores if set(core).issubset(cpus)]

    def chiplets_in(cpus):
        return [
            chiplet
            for _, chiplet in sorted(topology.chiplets, key=lambda item: (item[0], item[1]))
            if set(chiplet).issubset(cpus)
        ]

    def core_units(cpus):
        groups = cores_in(cpus)
        if policy.get("core") == "spread" or all(len(group) == 1 for group in groups):
            # Pick one sibling from every core before returning to sibling zero.
            # SMT-disabled cores use the same interleaved core order as the
            # sibling lanes on SMT hosts.
            if all(len(group) == 1 for group in groups):
                return groups[::2] + groups[1::2]
            return [(cpu,) for cpu in _round_robin(groups)]
        return groups

    def chiplet_units(cpus):
        groups = chiplets_in(cpus)
        if not groups:
            return []
        if "core" not in policy:
            return [tuple(group) for group in groups]
        nested = [core_units(set(group)) for group in groups]
        if policy["chiplet"] == "spread":
            return _round_robin(nested)
        return [unit for group in nested for unit in group]

    node_groups = [cpus for _, cpus in sorted(topology.numa_nodes) if cpus]
    if "chiplet" not in policy:
        return [tuple(group) for group in node_groups]
    nested = [chiplet_units(set(group)) for group in node_groups]
    if policy["numa"] == "spread":
        return _round_robin(nested)
    return [unit for group in nested for unit in group]


def plan_affinity(mode, topology, required_cpus):
    if mode == "none":
        return AffinityPlacement(mode=mode, cpus=None)
    if not hasattr(os, "sched_setaffinity"):
        return _unsupported(mode, "CPU affinity is not supported by this operating system")
    if required_cpus < 1:
        return _unsupported(mode, "at least one CPU is required")
    policy = dict(_parse_mode(mode))
    if policy.get("chiplet") and topology.chiplet_topology_reason:
        return _unsupported(
            mode,
            "chiplet-based affinity is unavailable: {}".format(
                topology.chiplet_topology_reason
            ),
        )
    numa_nodes = [cpus for _, cpus in topology.numa_nodes if cpus]
    if policy.get("numa") == "spread" and len(numa_nodes) < 2:
        return _unsupported(mode, "spread-numa requires at least two NUMA nodes with allowed CPUs")
    if policy.get("chiplet") == "spread":
        chiplets_by_node = {}
        for node_id, cpus in topology.chiplets:
            chiplets_by_node.setdefault(node_id, []).append(cpus)
        if not any(len(groups) >= 2 for groups in chiplets_by_node.values()):
            return _unsupported(mode, "spread-chiplet requires at least two chiplets in a NUMA node")

    units = _plan_units(mode, topology)
    cpus = _select_units(units, required_cpus)
    if not cpus:
        return _unsupported(mode, "{} allowed CPUs are required by this placement".format(required_cpus))
    return AffinityPlacement(mode=mode, cpus=cpus)


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
