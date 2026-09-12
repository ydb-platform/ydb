"""Saved placement specifications; deliberately does not launch cluster processes."""

import json
import re
import threading
import uuid
from datetime import datetime, timezone

from ydb.tools.ydb_bench.lib.common import BenchmarkError, atomic_write_json
from ydb.tools.ydb_bench.lib.topology import AFFINITY_MODES


def _text(value, label, limit=200):
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise BenchmarkError("{} must contain 1 to {} characters".format(label, limit))
    return value.strip()


def _integer(value, label, maximum=65536):
    if type(value) is not int or not 1 <= value <= maximum:
        raise BenchmarkError("{} must be between 1 and {}".format(label, maximum))
    return value


def validate_affinity(value):
    if not isinstance(value, dict):
        raise BenchmarkError("Affinity must be an object")
    if value.get("kind") == "manual":
        cpus = value.get("cpus")
        if (
            not isinstance(cpus, list)
            or not 1 <= len(cpus) <= 65536
            or any(type(cpu) is not int or not 0 <= cpu <= 1048575 for cpu in cpus)
            or len(set(cpus)) != len(cpus)
        ):
            raise BenchmarkError("Select unique logical CPU IDs")
        return {"kind": "manual", "cpus": sorted(cpus)}
    if value.get("kind") != "strategy" or value.get("mode") not in AFFINITY_MODES:
        raise BenchmarkError("Unknown affinity strategy")
    scope = value.get("scope", "exclusive")
    if scope not in ("exclusive", "chiplet", "numa"):
        raise BenchmarkError("Unknown affinity scope")
    mode = value["mode"]
    # Migrate the experimental independent scope to the corresponding placement.
    if scope == "numa":
        mode = "pack-numa"
    result = {
        "kind": "strategy",
        "mode": mode,
        "count": _integer(value.get("count"), "Affinity CPU count"),
    }
    return result


def _names(value, label):
    if not isinstance(value, list) or len(value) > 64:
        raise BenchmarkError("{} must be a list of at most 64 entries".format(label))
    result = [_text(item, label, 80) for item in value]
    if len(set(result)) != len(result):
        raise BenchmarkError("{} must be unique".format(label))
    return result


def validate_template(value, host_ids):
    if not isinstance(value, dict):
        raise BenchmarkError("Template must be an object")
    name = _text(value.get("name"), "Template name")
    nodes = value.get("nodes")
    if not isinstance(nodes, list) or not 1 <= len(nodes) <= 64:
        raise BenchmarkError("A template must contain 1 to 64 nodes")
    selected_hosts = value.get("host_ids")
    if selected_hosts is None:
        selected_hosts = list(
            dict.fromkeys(
                node["host_id"] for node in nodes if isinstance(node, dict) and isinstance(node.get("host_id"), str)
            )
        )
    selected_hosts = _names(selected_hosts, "Template hosts")
    if any(host not in host_ids for host in selected_hosts):
        raise BenchmarkError("Select registered template hosts")
    centers = value.get("data_centers", [])
    if not isinstance(centers, list) or len(centers) > 64 or any(not isinstance(dc, dict) for dc in centers):
        raise BenchmarkError("Data centers must be a list of at most 64 objects")
    dc_names = _names([dc.get("name") for dc in centers], "Data center names")
    centers = [
        {"name": name, "racks": _names(dc.get("racks", []), "Rack names")} for name, dc in zip(dc_names, centers)
    ]
    racks = {dc["name"]: dc["racks"] for dc in centers}
    tenants = value.get("tenants", [])
    if not isinstance(tenants, list) or len(tenants) > 64 or any(not isinstance(t, dict) for t in tenants):
        raise BenchmarkError("Tenants must be a list of at most 64 objects")
    paths = _names([t.get("path") for t in tenants], "Tenant paths")
    normalized_tenants = []
    for path, tenant in zip(paths, tenants):
        if not re.fullmatch(r"/Root/[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*", path):
            raise BenchmarkError("Tenant path must start with /Root/ and contain valid path components")
        if tenant.get("storage_kind") not in ("ssd", "hdd"):
            raise BenchmarkError("Tenant storage kind must be ssd or hdd")
        normalized_tenants.append(
            {
                "path": path,
                "storage_kind": tenant["storage_kind"],
                "storage_groups": _integer(tenant.get("storage_groups"), "Storage groups", 64),
            }
        )
    result, names = [], set()
    for node in nodes:
        if not isinstance(node, dict):
            raise BenchmarkError("Node must be an object")
        node_name = _text(node.get("name"), "Node name", 80)
        if node_name in names:
            raise BenchmarkError("Node names must be unique")
        names.add(node_name)
        role, host = node.get("role"), node.get("host_id")
        if role not in ("static", "dynamic", "cli"):
            raise BenchmarkError("Unknown node role")
        if not isinstance(host, str) or host not in selected_hosts:
            raise BenchmarkError("Select a registered host for every node")
        item = {
            "name": node_name,
            "role": role,
            "host_id": host,
            "binary": _text(node.get("binary"), "Binary path or version", 1000),
            "affinity": validate_affinity(node.get("affinity")),
        }
        location = node.get("location", {})
        if not isinstance(location, dict):
            raise BenchmarkError("Logical location must be an object")
        dc, rack, body = (location.get(key, "") for key in ("data_center", "rack", "body"))
        if any(not isinstance(part, str) for part in (dc, rack, body)):
            raise BenchmarkError("Logical location fields must be strings")
        if role == "cli" and any((dc, rack, body)):
            raise BenchmarkError("CLI load generators cannot have a logical location")
        if (dc and dc not in racks) or (rack and rack not in racks.get(dc, [])) or (body and not rack):
            raise BenchmarkError("Select an existing data center and rack for the logical server")
        if dc and not rack:
            if not racks[dc]:
                racks[dc].append(dc + "-R1")
            rack = racks[dc][0]
        item["location"] = {"data_center": dc, "rack": rack, "body": node_name if rack else ""}
        tenant = node.get("tenant", "")
        if not isinstance(tenant, str) or (tenant and (tenant not in paths or role != "dynamic")):
            raise BenchmarkError("Only dynamic nodes can be assigned to an existing tenant")
        item["tenant"] = tenant
        if role == "static":
            disk = node.get("sector_map")
            if not isinstance(disk, dict):
                raise BenchmarkError("Static nodes require SectorMap settings")
            item["sector_map"] = {
                "count": _integer(disk.get("count"), "SectorMap count", 64),
                "size_gib": _integer(disk.get("size_gib"), "SectorMap size", 1048576),
            }
        result.append(item)
    return {
        "schema_version": 3,
        "name": name,
        "nodes": result,
        "host_ids": selected_hosts,
        "data_centers": centers,
        "tenants": normalized_tenants,
    }


class ClusterTemplateStore:
    def __init__(self, output):
        self.path = output / ".cluster-templates.json"
        self.lock = threading.RLock()

    def list(self):
        with self.lock:
            if not self.path.exists():
                return []
            try:
                if self.path.stat().st_size > 16 * 1024 * 1024:
                    raise ValueError("too large")
                records = json.loads(self.path.read_text(encoding="utf-8"))
                if not isinstance(records, list) or any(not isinstance(item, dict) for item in records):
                    raise ValueError("invalid records")
                return records
            except (OSError, ValueError) as error:
                raise BenchmarkError("Cannot read cluster templates; file was not changed") from error

    def save(self, value, host_ids):
        record = validate_template(value, host_ids)
        with self.lock:
            records = self.list()
            previous = next((item for item in records if item["id"] == value.get("id")), None)
            if value.get("id") and previous is None:
                raise BenchmarkError("Template no longer exists")
            if previous and value.get("revision") != previous["revision"]:
                raise BenchmarkError("Template changed elsewhere; reload before saving")
            if not previous and len(records) >= 100:
                raise BenchmarkError("At most 100 cluster templates can be saved")
            record.update(
                id=previous["id"] if previous else uuid.uuid4().hex,
                revision=previous["revision"] + 1 if previous else 1,
                updated_at=datetime.now(timezone.utc).isoformat(),
            )
            atomic_write_json(self.path, [record] + [item for item in records if item["id"] != record["id"]])
            return record

    def delete(self, value):
        if not isinstance(value, dict):
            raise BenchmarkError("Template reference must be an object")
        with self.lock:
            records = self.list()
            record = next((item for item in records if item["id"] == value.get("id")), None)
            if record is None or record["revision"] != value.get("revision"):
                raise BenchmarkError("Template changed or no longer exists; reload first")
            atomic_write_json(self.path, [item for item in records if item["id"] != record["id"]])
            return {"deleted": record["id"]}
