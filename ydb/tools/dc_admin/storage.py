"""Read the storage topology and the current DC policy from YDB monitoring."""

import json
import threading
import time
from http.client import HTTPException
from itertools import zip_longest
from urllib.parse import urljoin
from urllib.request import Request, urlopen


def _read_json(url, query=None):
    request = Request(url, data=json.dumps(query).encode() if query is not None else None)
    request.add_header("Accept", "application/json")
    if query is not None:
        request.add_header("Content-Type", "application/json")
    with urlopen(request, timeout=5) as response:
        return json.load(response)


def _groups(storage, config):
    locations = {n["NodeId"]: n.get("Location", {}).get("DataCenter") for n in config.get("AllNodes", [])}
    policy = {
        dc["Name"]: dc.get("State", "INCLUDED") for dc in config.get("DataCenterStates", {}).get("DataCenter", [])
    }
    static_groups = {
        (g["GroupID"], g["GroupGeneration"]): g
        for g in config.get("BlobStorageConfig", {}).get("ServiceSet", {}).get("Groups", [])
    }
    groups = []
    for pool in storage.get("StoragePools", []):
        for group in pool.get("Groups", []):
            realms = {}
            nodes = group.get("VDiskNodeIds", [])
            for expected_node, disk in zip_longest(nodes, group.get("VDisks", [])):
                disk = disk or {}
                vdisk = disk.get("VDiskId", {})
                ring = vdisk.get("Ring")
                node = disk.get("NodeId") or expected_node
                name = locations.get(node)
                key = name if name else ring
                if key not in realms:
                    realms[key] = {
                        "name": name or (f"Realm {ring}" if ring is not None else "—"),
                        "policy": policy.get(name, "INCLUDED") if name else None,
                        "disks": [],
                    }
                realms[key]["disks"].append(
                    {
                        "node": node,
                        "domain": vdisk.get("Domain"),
                        "state": disk.get("VDiskState"),
                        "replicated": disk.get("Replicated"),
                    }
                )
            group_id, generation = group["GroupID"], group.get("GroupGeneration")
            static = static_groups.get((group_id, generation))
            groups.append(
                {
                    "id": group_id,
                    "pool": pool.get("Name"),
                    "erasure": group.get("ErasureSpecies"),
                    "generation": generation,
                    "overall": group.get("Overall"),
                    "state": (
                        static.get("Mirror3dcState", "NORMAL")
                        if static and group.get("ErasureSpecies") == "mirror-3-dc"
                        else None
                    ),
                    "realms": list(realms.values()),
                }
            )
    return sorted(groups, key=lambda g: g["id"])


def _read_dynamic_group_states(url):
    # TQueryBaseConfig is a read-only BSC command. Only the group records are needed.
    query = {"QueryBaseConfig": {"SuppressPDisks": True, "SuppressVSlots": True, "SuppressNodes": True}}
    try:
        result = _read_json(urljoin(url, "/tablets/app?TabletID=72057594037932033&exec=1"), {"Command": [query]})
        if result["Success"] is not True or result["Status"][0]["Success"] is not True:
            return {}
        return {
            (group["GroupId"], group["GroupGeneration"]): group["Mirror3dcState"]
            for group in result["Status"][0]["BaseConfig"]["Group"]
            if "GroupId" in group and "GroupGeneration" in group and "Mirror3dcState" in group
        }
    except (OSError, HTTPException, ValueError, KeyError, TypeError, IndexError):
        # Viewer disk telemetry stays available while the controller is offline.
        return {}


class StorageViewer:
    def __init__(self, url, endpoints=None):
        self.url = url
        self.endpoints = endpoints or [url]
        self.lock = threading.Lock()
        self.snapshot = None
        self.valid_until = 0

    def read(self):
        if not self.url:
            raise ValueError("Для сервера не задан --monitoring-url.")
        with self.lock:
            if self.snapshot is not None and time.monotonic() < self.valid_until:
                return self.snapshot
            errors = []
            for url in self.endpoints:
                try:
                    snapshot = self._read_node(url)
                except (OSError, HTTPException, KeyError, ValueError) as error:
                    errors.append(str(error))
                    continue
                self.snapshot = snapshot
                self.valid_until = time.monotonic() + 3
                return snapshot
            raise ValueError("; ".join(errors))

    def _read_node(self, url):
        storage = _read_json(urljoin(url, "/viewer/json/storage?groups=true"))
        result = _read_json(urljoin(url, "/actors/nodewarden?page=distconf"), {"QueryConfig": {}})
        config = result["QueryConfig"]["Config"]
        groups = _groups(storage, config)
        # IsDynamicGroup in blobstorage_common.h uses the high bit of the group id.
        dynamic = [
            group
            for group in groups
            if group["id"] & (1 << 31) and group["erasure"] == "mirror-3-dc" and group["state"] is None
        ]
        if dynamic:
            states = _read_dynamic_group_states(url)
            for group in dynamic:
                group["state"] = states.get((group["id"], group["generation"]))
        return {"groups": groups, "monitoringUrl": self.url}
