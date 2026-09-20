"""Resolve saved placement into a host-local execution plan.

The browser preview is informative only. Workers resolve CPU masks from their
own topology while the host is reserved, then the coordinator freezes the plan.
"""

from ydb.tools.ydb_bench.lib.cluster_templates import validate_affinity, validate_template
from ydb.tools.ydb_bench.lib.common import BenchmarkError
from ydb.tools.ydb_bench.lib.topology import plan_affinity
from ydb.tools.ydb_bench.lib.cluster_config import execution_config, validate_placement, tenant_configs


def execution_template(value, host_ids, target_tenant, multiple_cli=False):
    template = validate_template(value, host_ids)
    execution_config(template.get("ydb_config", {}))
    tenant_configs(template.get('ydb_tenant_configs', {}), [t['path'] for t in template['tenants']], execution=True)
    nodes = template["nodes"]
    cli_count = sum(node["role"] == "cli" for node in nodes)
    if not cli_count or (not multiple_cli and cli_count != 1):
        raise BenchmarkError("Distributed YDB requires exactly one CLI generator")
    if not any(node["role"] == "static" for node in nodes):
        raise BenchmarkError("Distributed YDB requires at least one static node")
    if not isinstance(target_tenant, str) or target_tenant not in {t["path"] for t in template["tenants"]}:
        raise BenchmarkError("Select an existing workload target tenant")
    if not any(node["role"] == "dynamic" and node["tenant"] == target_tenant for node in nodes):
        raise BenchmarkError("The workload target tenant has no dynamic nodes")
    for node in nodes:
        if node["role"] == "static" and not node["disks"]:
            raise BenchmarkError("Static node {} requires at least one disk before execution".format(node["name"]))
        if node["role"] != "cli" and not all(node["location"].values()):
            raise BenchmarkError("Node {} requires a DC and rack before execution".format(node["name"]))
        if node["role"] == "dynamic" and not node["tenant"]:
            raise BenchmarkError("Dynamic node {} requires a tenant before execution".format(node["name"]))
    media = {disk["media"] for node in nodes if node["role"] == "static" for disk in node["disks"]}
    validate_placement(template.get('ydb_config', {}), nodes)
    domains = template.get('ydb_config', {}).get('domains_config', {}).get('domain', [{}])
    pools = template.get('ydb_config', {}).get('storage_pool_types', domains[0].get('storage_pool_types', []))
    if pools and any(t['storage_kind'] not in {p['kind'] for p in pools} for t in template['tenants']):
        raise BenchmarkError('Tenant storage kind has no configured storage pool')
    if any(tenant["storage_kind"] not in media for tenant in template["tenants"]):
        raise BenchmarkError("Tenant storage kind has no matching disks")
    return template


def placement_scope(affinity):
    if affinity["kind"] == "manual":
        return "exclusive"
    mode = affinity["mode"]
    if mode == "pack-numa":
        return "numa"
    return "chiplet" if mode.endswith("-chiplet") else "exclusive"


def resolve_host_placement(nodes, topology):
    """Resolve all nodes on one host together, preserving explicit overlaps.

    Fixed masks win over strategies, and chiplet-scoped accounting wins over
    more flexible NUMA-scoped accounting. Shared reservations narrow capacity
    accounting only: the actual mask still covers every selected whole domain.
    """
    if len({node["host_id"] for node in nodes}) > 1:
        raise BenchmarkError("Host placement must contain nodes from only one host")
    if len({node["name"] for node in nodes}) != len(nodes):
        raise BenchmarkError("Host placement node names must be unique")
    affinities = {node["name"]: validate_affinity(node["affinity"]) for node in nodes}
    allowed = set(topology.allowed_cpus)
    used = {cpu for a in affinities.values() if a["kind"] == "manual" for cpu in a["cpus"]}
    if not used.issubset(allowed):
        raise BenchmarkError("Manual affinity contains CPUs unavailable on this host")
    priority = {"exclusive": 0, "chiplet": 1, "numa": 2}
    ordered = sorted(nodes, key=lambda node: priority[placement_scope(affinities[node["name"]])])
    result = {}
    for node in ordered:
        affinity = affinities[node["name"]]
        scope = placement_scope(affinity)
        if affinity["kind"] == "manual":
            mask = reserved = list(affinity["cpus"])
        elif affinity["mode"] == "none":
            mask, reserved = None, []
        elif scope == "exclusive":
            placement = plan_affinity(affinity["mode"], topology, affinity["count"], excluded_cpus=used)
            if not placement.supported:
                raise BenchmarkError("{}: {}".format(node["name"], placement.reason))
            mask = reserved = list(placement.cpus)
        else:
            if scope == "chiplet" and topology.chiplet_topology_reason:
                raise BenchmarkError(topology.chiplet_topology_reason)
            groups = topology.numa_nodes if scope == "numa" else topology.chiplets
            units = [(numa, [cpu for cpu in cpus if cpu in allowed]) for numa, cpus in groups]

            def order(unit):
                numa, cpus = unit
                occupied = len(used.intersection(cpus))
                return (occupied, numa) if affinity["mode"].startswith("spread-numa") else (numa, occupied)

            reserved, selected = [], set()
            for _, cpus in sorted(units, key=order):
                free = set(cpus) - used - set(reserved)
                if not free:
                    continue
                # Adjacent SMT siblings are accounting slots, not a narrower mask.
                candidates = list(
                    dict.fromkeys(
                        [cpu for core in topology.physical_cores for cpu in core if cpu in free]
                        + [cpu for cpu in cpus if cpu in free]
                    )
                )
                reserved.extend(candidates[: affinity["count"] - len(reserved)])
                selected.update(cpus)
                if len(reserved) == affinity["count"]:
                    break
            if len(reserved) != affinity["count"]:
                raise BenchmarkError("{}: not enough free CPU capacity for {} affinity".format(node["name"], scope))
            mask = sorted(selected)
        result[node["name"]] = {"cpus": mask, "reserved_cpus": reserved, "scope": scope}
        used.update(reserved)
    return result
