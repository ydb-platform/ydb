"""Saved placement specifications; deliberately does not launch cluster processes."""

import copy
import json
import posixpath
import re
import threading
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse

from ydb.tools.ydb_bench.lib.common import BenchmarkError, atomic_write_json
from ydb.tools.ydb_bench.lib.topology import AFFINITY_MODES
from ydb.tools.ydb_bench.lib import cluster_config


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


def validate_disks(node):
    disks = node.get("disks")
    if disks is None:
        legacy = node.get("sector_map")
        if not isinstance(legacy, dict):
            raise BenchmarkError("Static nodes require disks")
        count = _integer(legacy.get("count"), "SectorMap count", 64)
        size = _integer(legacy.get("size_gib"), "SectorMap size", 1048576)
        disks = [{"source": "sector_map", "media": "ssd", "size_gib": size} for _ in range(count)]
    if not isinstance(disks, list) or len(disks) > 64:
        raise BenchmarkError("Disks must be a list of at most 64 entries")
    result = []
    for disk in disks:
        if not isinstance(disk, dict) or disk.get("source") not in ("sector_map", "file", "block_device", "partlabel"):
            raise BenchmarkError("Unknown disk source")
        source = disk["source"]
        if disk.get("media") not in ("ssd", "hdd"):
            raise BenchmarkError("Disk media must be ssd or hdd")
        item = {"source": source, "media": disk["media"]}
        if source in ("sector_map", "file"):
            item["size_gib"] = _integer(disk.get("size_gib"), "Disk size", 1048576)
        if source == "file" and "temporary" in disk:
            if type(disk["temporary"]) is not bool:
                raise BenchmarkError("Temporary disk flag must be boolean")
            item["temporary"] = disk["temporary"]
        if source == "partlabel":
            label = _text(disk.get("label"), "Partition label", 255)
            if label in (".", "..") or any(c in label for c in ("/", "\\", "\x00", "\n", "\r")):
                raise BenchmarkError("Partition label must be a single path component")
            item["label"] = label
        elif source == "file" and not item.get("temporary") and "name" in disk:
            name = _text(disk["name"], "File disk name", 200)
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", name):
                raise BenchmarkError("File disk name must contain only letters, digits, dots, underscores and hyphens")
            item["name"] = name
        elif source in ("file", "block_device") and not item.get("temporary"):
            path = _text(disk.get("path"), "Disk path", 4096)
            if not path.startswith("/") or any(c in path for c in ("\x00", "\n", "\r")):
                raise BenchmarkError("Disk path must be absolute")
            path = posixpath.normpath(path)
            if path == "/":
                raise BenchmarkError("Disk path must not be the root directory")
            item["path"] = path
        result.append(item)
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
    domain = cluster_config.domain_name(cluster_config.validate(value.get('ydb_config', {}))[0])
    for path, tenant in zip(paths, tenants):
        if not re.fullmatch('/' + re.escape(domain) + r'/[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*', path):
            raise BenchmarkError('Tenant path must start with /' + domain + '/ and contain valid path components')
        if tenant.get("storage_kind") not in ("ssd", "hdd"):
            raise BenchmarkError("Tenant storage kind must be ssd or hdd")
        normalized_tenants.append(
            {
                "path": path,
                "storage_kind": tenant["storage_kind"],
                "storage_groups": _integer(tenant.get("storage_groups"), "Storage groups", 64),
            }
        )
    result, names, disk_paths = [], set(), set()
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
            item["disks"] = validate_disks(node)
            for disk in item["disks"]:
                path = disk.get("path")
                if disk["source"] == "file" and "name" in disk:
                    path = "file-disks/" + disk["name"]
                if disk["source"] == "partlabel":
                    path = "/dev/disk/by-partlabel/" + disk["label"]
                if path:
                    key = (host, path)
                    if key in disk_paths:
                        raise BenchmarkError("A disk path can only be assigned once per host")
                    disk_paths.add(key)
        result.append(item)
    return {
        "schema_version": 4,
        "name": name,
        "nodes": result,
        "host_ids": selected_hosts,
        "data_centers": centers,
        "tenants": normalized_tenants,
        **({"ydb_config": cluster_config.validate(value["ydb_config"])[0]} if "ydb_config" in value else {}),
        **(
            {'ydb_tenant_configs': cluster_config.tenant_configs(value['ydb_tenant_configs'], paths)}
            if 'ydb_tenant_configs' in value
            else {}
        ),
        **(
            {
                'ydb_tenant_replacements': cluster_config.tenant_replacements(
                    value['ydb_tenant_replacements'], value.get('ydb_tenant_configs', {})
                )
            }
            if 'ydb_tenant_replacements' in value
            else {}
        ),
    }


def apply_configuration_yaml(template, text, hosts):
    """Reconcile referenced entities in a detached draft; never register hosts."""
    if not isinstance(template, dict):
        raise BenchmarkError("Expected a cluster template")
    result = cluster_config.parse_document(text)
    draft = copy.deepcopy(template)
    draft.update(
        ydb_config=result['config'],
        ydb_tenant_configs=result['tenant_configs'],
        ydb_tenant_replacements=result['tenant_replacements'],
    )
    selected = draft.setdefault('host_ids', list(dict.fromkeys(n['host_id'] for n in draft.get('nodes', []))))
    centers = draft.setdefault('data_centers', [])
    tenants = draft.setdefault('tenants', [])
    additions = {'hosts': [], 'data_centers': [], 'racks': [], 'tenants': []}
    aliases = {}
    for host in hosts:
        for alias in (host['id'], host.get('name'), urlparse(host.get('endpoint', '')).hostname):
            if alias:
                aliases.setdefault(alias.lower().rstrip('.'), set()).add(host['id'])
    config = result['config']
    locations = config.get('hosts', [])
    nameservice = config.get('nameservice_config', {}).get('node', [])
    if not isinstance(locations, list) or any(not isinstance(item, dict) for item in locations):
        raise BenchmarkError('hosts must be a list of objects')
    missing, ambiguous = [], []
    for item in [*locations, *nameservice]:
        name = item.get('host') or item.get('interconnect_host')
        if not isinstance(name, str) or not name.strip():
            raise BenchmarkError('Every YAML host must have a host name')
        matches = aliases.get(name.lower().rstrip('.'), set())
        if not matches:
            missing.append(name)
        elif len(matches) != 1:
            ambiguous.append(name)
        else:
            host_id = next(iter(matches))
            if host_id not in selected:
                selected.append(host_id)
                additions['hosts'].append(name)
        location = item.get('location', {})
        if not isinstance(location, dict):
            raise BenchmarkError('Host location must be an object')
        dc, rack = location.get('data_center', ''), location.get('rack', '')
        if not isinstance(dc, str) or not isinstance(rack, str):
            raise BenchmarkError('Data center and rack names must be strings')
        if rack and not dc:
            raise BenchmarkError('Rack requires a data center: ' + rack)
        if dc:
            center = next((item for item in centers if item['name'] == dc), None)
            if center is None:
                center = {'name': dc, 'racks': []}
                centers.append(center)
                additions['data_centers'].append(dc)
            rack = rack or (center['racks'][0] if center['racks'] else dc + '-R1')
            if rack not in center['racks']:
                center['racks'].append(rack)
                additions['racks'].append(dc + ' / ' + rack)
    if missing or ambiguous:
        problems = []
        if missing:
            problems.append('Unregistered hosts: ' + ', '.join(dict.fromkeys(missing)))
        if ambiguous:
            problems.append('Ambiguous hosts: ' + ', '.join(dict.fromkeys(ambiguous)))
        raise BenchmarkError('; '.join(problems) + '. Register hosts or use their exact names/IDs. Template unchanged.')
    paths = list(result['tenant_configs'])
    paths.extend(
        slot['tenant_name'] for slot in config.get('tenant_pool_config', {}).get('slots', []) if slot.get('tenant_name')
    )
    for path in dict.fromkeys(paths):
        if not any(tenant['path'] == path for tenant in tenants):
            tenants.append({'path': path, 'storage_kind': 'ssd', 'storage_groups': 1})
            additions['tenants'].append(path)
    normalized = validate_template(draft, {host['id'] for host in hosts})
    draft.update(normalized)
    return {**result, 'template': draft, 'added': additions}


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
