import os
import plistlib
import subprocess
import sys
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
    chiplet_labels: tuple = ()
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


@dataclass(frozen=True)
class BackgroundPlacement:
    mode: str
    cpus: Optional[tuple]
    groups: tuple = ()
    workers: int = 0
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


def _device_tree_integer(value):
    if isinstance(value, int):
        return value
    if isinstance(value, bytes) and value:
        return int.from_bytes(value, byteorder="little")
    return None


def _parse_darwin_topology(data, allowed):
    try:
        roots = plistlib.loads(data)
        entries = roots[0]["IORegistryEntryChildren"]
    except (IndexError, KeyError, TypeError, ValueError, plistlib.InvalidFileException):
        return None

    allowed_set = set(allowed)
    cpus = {}
    for entry in entries:
        cpu = _device_tree_integer(entry.get("logical-cpu-id"))
        cluster = _device_tree_integer(entry.get("cluster-id"))
        cluster_type = entry.get("cluster-type")
        if isinstance(cluster_type, bytes):
            cluster_type = cluster_type.rstrip(b"\0").decode("ascii", errors="replace")
        if cpu in allowed_set and cluster is not None and cluster_type in ("E", "P"):
            placement = (cluster, cluster_type)
            if cpu in cpus and cpus[cpu] != placement:
                return None
            cpus[cpu] = placement
    if set(cpus) != allowed_set:
        return None

    groups = {}
    for cpu, (cluster, cluster_type) in cpus.items():
        groups.setdefault((cluster, cluster_type), []).append(cpu)
    ordered_groups = sorted(groups.items())
    type_counts = {kind: sum(1 for (_, current), _ in ordered_groups if current == kind) for kind in ("E", "P")}
    type_indexes = {"E": 0, "P": 0}
    labels = []
    chiplets = []
    for (_, kind), group_cpus in ordered_groups:
        type_indexes[kind] += 1
        name = "Efficiency" if kind == "E" else "Performance"
        suffix = " {}".format(type_indexes[kind]) if type_counts[kind] > 1 else ""
        group = tuple(sorted(group_cpus))
        chiplets.append((0, group))
        labels.append((group, "{} cluster{}".format(name, suffix)))
    cores = tuple((cpu,) for cpu in allowed)
    return CpuTopology(
        allowed_cpus=allowed,
        numa_nodes=((0, allowed),),
        chiplets=tuple(chiplets),
        physical_cores=cores,
        smt_siblings=cores,
        hierarchy_reasons=(("numa", "macOS does not expose NUMA placement; using package 0"),),
        chiplet_labels=tuple(labels),
    )


def _discover_darwin_topology(allowed):
    try:
        result = subprocess.run(
            ("ioreg", "-a", "-l", "-p", "IODeviceTree", "-r", "-n", "cpus"),
            check=True,
            capture_output=True,
            timeout=10,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None
    return _parse_darwin_topology(result.stdout, allowed)


def discover_topology(sys_root=Path("/sys/devices/system"), allowed_cpus=None):
    sys_root = Path(sys_root)
    allowed = tuple(sorted(set(_allowed_cpus() if allowed_cpus is None else allowed_cpus)))
    allowed_set = set(allowed)

    if sys.platform == "darwin" and sys_root == Path("/sys/devices/system"):
        darwin_topology = _discover_darwin_topology(allowed)
        if darwin_topology is not None:
            return darwin_topology

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
        node_ids = tuple(node_id for node_id, node_cpus in nodes if set(cpus).issubset(node_cpus))
        if len(node_ids) != 1:
            # The chiplet record has a NUMA-node identifier, so assigning this
            # group to the node of its first CPU would fabricate a hierarchy.
            # Keep chiplet-based modes unavailable until this topology is
            # understood; NUMA-only modes do not rely on the L3 grouping.
            chiplet_topology_reasons.append(
                "L3 cache group {} does not belong to exactly one NUMA node".format(", ".join(str(cpu) for cpu in cpus))
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
        siblings = tuple(
            candidate
            for candidate in _read_cpu_list(topology_root / "thread_siblings_list")
            if candidate in allowed_set
        )
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
        reasons.append(
            ("physical_core", "physical_package_id or core_id is unavailable; affected CPUs are singleton cores")
        )
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
            "chiplet-based affinity is unavailable: {}".format(topology.chiplet_topology_reason),
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


def _background_unsupported(mode, reason):
    return BackgroundPlacement(mode=mode, cpus=None, reason=reason)


def plan_background_load(mode, topology, foreground_cpus, foreground_threads):
    """Place background workers on physical cores not used by the foreground."""
    if mode == "none":
        return BackgroundPlacement(mode=mode, cpus=(), workers=0)
    cores = _core_groups(topology)
    if foreground_cpus is None:
        if mode not in ("memory-bandwidth", "coherence-all-numa"):
            return _background_unsupported(
                mode,
                "{} requires an explicit foreground affinity".format(mode),
            )
        if mode == "coherence-all-numa" and len([cpus for _, cpus in topology.numa_nodes if cpus]) < 2:
            return _background_unsupported(mode, "coherence-all-numa requires at least two NUMA nodes")
        workers = max(0, len(cores) - foreground_threads)
        if workers < 1:
            return _background_unsupported(mode, "no estimated physical-core capacity remains")
        if mode == "coherence-all-numa" and workers < 2:
            return _background_unsupported(mode, "coherence-all-numa requires at least two workers")
        if mode == "coherence-all-numa":
            workers -= workers % 2
        groups = (tuple(range(workers)),) if mode == "coherence-all-numa" else ()
        return BackgroundPlacement(mode=mode, cpus=None, groups=groups, workers=workers)

    occupied = set(foreground_cpus)
    free_cores = [core for core in cores if not occupied.intersection(core)]
    representatives = [core[0] for core in free_cores]
    if not representatives:
        return _background_unsupported(mode, "no unused physical cores remain")
    if mode == "memory-bandwidth":
        groups = tuple((cpu,) for cpu in representatives)
        return BackgroundPlacement(mode=mode, cpus=tuple(representatives), groups=groups, workers=len(groups))

    cpu_to_node = {cpu: node_id for node_id, cpus in topology.numa_nodes for cpu in cpus}
    cpu_to_chiplet = {cpu: (node_id, index) for index, (node_id, cpus) in enumerate(topology.chiplets) for cpu in cpus}
    groups = []
    if mode == "coherence-chiplet":
        by_chiplet = {}
        for cpu in representatives:
            if cpu in cpu_to_chiplet:
                by_chiplet.setdefault(cpu_to_chiplet[cpu], []).append(cpu)
        groups.extend(tuple(cpus) for _, cpus in sorted(by_chiplet.items()) if len(cpus) >= 2)
    elif mode == "coherence-numa":
        by_node = {}
        for cpu in representatives:
            chiplet = cpu_to_chiplet.get(cpu)
            if chiplet is not None:
                by_node.setdefault(chiplet[0], {}).setdefault(chiplet, []).append(cpu)
        for chiplets in by_node.values():
            pools = [list(cpus) for _, cpus in sorted(chiplets.items())]
            if len(pools) >= 2:
                groups.append(tuple(_round_robin(pools)))
    elif mode == "coherence-all-numa":
        by_node = {}
        for cpu in representatives:
            by_node.setdefault(cpu_to_node.get(cpu), []).append(cpu)
        pools = [list(cpus) for node, cpus in sorted(by_node.items()) if node is not None]
        if len(pools) >= 2:
            groups.append(tuple(_round_robin(pools)))
    else:
        return _background_unsupported(mode, "unknown background load mode")
    if not groups:
        requirements = {
            "coherence-chiplet": "two unused physical cores in one chiplet",
            "coherence-numa": "unused physical cores in two chiplets of one NUMA node",
            "coherence-all-numa": "unused physical cores in two NUMA nodes",
        }
        return _background_unsupported(mode, "requires {}".format(requirements[mode]))
    cpus = tuple(cpu for group in groups for cpu in group)
    return BackgroundPlacement(mode=mode, cpus=cpus, groups=tuple(groups), workers=len(cpus))


def topology_record(topology):
    chiplet_labels = dict(topology.chiplet_labels)
    return {
        "version": topology.version,
        "allowed_cpus": list(topology.allowed_cpus),
        "numa_nodes": [{"id": node_id, "cpus": list(cpus)} for node_id, cpus in topology.numa_nodes],
        "chiplets": [
            {
                "numa_node": node_id,
                "cpus": list(cpus),
                **({"label": chiplet_labels[cpus]} if cpus in chiplet_labels else {}),
            }
            for node_id, cpus in topology.chiplets
        ],
        "physical_cores": [list(cpus) for cpus in topology.physical_cores],
        "smt_siblings": [list(cpus) for cpus in topology.smt_siblings],
        "hierarchy_reasons": [{"level": level, "reason": reason} for level, reason in topology.hierarchy_reasons],
    }
