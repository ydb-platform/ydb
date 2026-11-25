"""Cluster-owned topology and endpoint discovery from a bootstrap viewer URL."""
from dataclasses import dataclass, replace
import logging
import threading
import time
from urllib.parse import urlsplit

import requests

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.cluster_inventory import ClusterInventory, NodeInfo, SlotInfo
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import ClusterTopologyModel, FailureModelGuard
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.fault_engine import RecoveryUnavailable

logger = logging.getLogger(__name__)


def endpoint(host, address, scheme):
    if address.startswith(":"):
        address = f"[{host}]{address}" if ":" in host else host + address
    value = urlsplit(f"{scheme}://{address}")
    if not value.hostname or not value.port or value.username or value.password:
        raise ValueError("Invalid advertised endpoint")
    return value.geturl()


@dataclass(frozen=True)
class DiscoverySnapshot:
    topology: object
    inventory: object
    specs: tuple
    http: tuple
    grpc: tuple
    node_piles: dict


class ClusterDiscovery:
    def __init__(self, bootstrap, *, get=requests.get, max_age=90):
        parsed = urlsplit(bootstrap)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError("cluster_endpoint must be an HTTP(S) viewer URL")
        self.http = (bootstrap.rstrip("/"),)
        self.grpc = ()
        self.node_piles = {}
        self._get = get
        self.max_age = max_age
        self.last_success = None
        self.last_observation = None
        self.error = None
        self.generation = 0
        self.deferred = False
        self._stop = threading.Event()
        self._thread = None
        self._known = {}

    def _json(self, base, path):
        response = self._get(base + path, timeout=10)
        response.raise_for_status()
        return response.json()

    def fetch(self, specs):
        errors = []
        for base in self.http:
            try:
                config = self._json(base, "/viewer/config")
                view = self._json(base, "/viewer/json/nodes?fields_required=SystemState")
                return self._parse(config, view, specs, base)
            except (requests.RequestException, ValueError, KeyError, TypeError) as error:
                errors.append(f"{base}: {error}")
        raise ValueError("Discovery failed: " + "; ".join(errors))

    def _parse(self, config, view, specs, base):
        static = {int(n["NodeId"]): n for n in config["NameserviceConfig"]["Node"]}
        piles = {p["Name"]: i for i, p in enumerate(config.get("BridgeConfig", {}).get("Piles", []))}
        erasure = config.get("SelfManagementConfig", {}).get("ErasureSpecies")
        if not erasure:
            groups = config.get("BlobStorageConfig", {}).get("ServiceSet", {}).get("Groups", [])
            modes = {int(g["ErasureSpecies"]) for g in groups}
            erasure = {0: "none", 4: "block-4-2", 9: "mirror-3-dc"}.get(next(iter(modes))) if len(modes) == 1 else None
        if not erasure:
            raise ValueError("Cluster did not report an unambiguous erasure mode")
        nodes = view.get("Nodes", [])
        if not nodes or int(view.get("TotalNodes", len(nodes))) != len(nodes):
            raise ValueError("Incomplete discovery response")
        records = dict(self._known)
        http, grpc, membership = [], [], {}
        by_port = {s.ic_port: s for s in specs}
        updated_specs = {}
        for row in nodes:
            nid = int(row["NodeId"])
            state = row.get("SystemState", {})
            advertised = {e["Name"]: e["Address"] for e in state.get("Endpoints", [])}
            info = static.get(nid, {})
            # Static nameservice addresses are authoritative; localhost processes
            # must not become remote agent destinations through SystemState.Host.
            host = info.get("Host") or state.get("Host")
            location = state.get("Location") or info.get("Location", {})
            if not host or "ic" not in advertised:
                if nid in records:
                    continue
                raise ValueError(f"Missing endpoint identity for node {nid}")
            port = urlsplit(endpoint(host, advertised["ic"], "tcp")).port
            spec = by_port.get(port)
            if spec is not None:
                host = spec.host
            pile = piles.get(location.get("BridgePileName"))
            if piles and pile is None:
                raise ValueError(f"Missing pile for node {nid}")
            rack, dc = location.get("Rack"), location.get("DataCenter")
            if rack is None:
                raise ValueError(f"Missing rack for node {nid}")
            kind = "node" if nid in static else "slot"
            record = (NodeInfo(nid, host, port, str(rack), dc, pile) if kind == "node" else
                      SlotInfo(port, host, port, nid, str(rack), dc, pile))
            # Dynamic node IDs can change; IC port identifies a local slot.
            records = {key: old for key, old in records.items()
                       if not (old.host == host and old.ic_port == port and key != nid)}
            records[nid] = record
            if spec is not None:
                updated_specs[port] = replace(
                    spec, kind=kind, node_id=nid, slot_idx=port if kind == "slot" else None,
                    rack=str(rack), datacenter=dc, bridge_pile_id=pile,
                )
            if pile is not None:
                membership[nid] = pile
            if "http-mon" in advertised:
                http.append(endpoint(host, advertised["http-mon"], urlsplit(base).scheme))
            if "grpc" in advertised:
                grpc.append(endpoint(host, advertised["grpc"], "grpc"))
        if not set(static).issubset(records) or not http or not grpc:
            raise ValueError("Missing static nodes or service endpoints")
        hosts = {}
        for record in records.values():
            hosts.setdefault(record.host, {"name": record.host, "location": {
                "rack": record.rack, "data_center": record.datacenter,
                "bridge_pile_name": next((name for name, pid in piles.items() if pid == record.bridge_pile_id), None),
            }})
        document = {"static_erasure": erasure, "hosts": list(hosts.values())}
        if piles:
            document["bridge_config"] = {"piles": [{"name": name} for name in piles]}
        topology = ClusterTopologyModel(document=document)
        inventory = ClusterInventory(topology, agent_hosts=hosts, process_specs=[])
        inventory.nodes = {n.node_id: n for n in records.values() if isinstance(n, NodeInfo)}
        inventory.slots = {n.slot_idx: n for n in records.values() if isinstance(n, SlotInfo)}
        return DiscoverySnapshot(topology, inventory,
                                 tuple(updated_specs.get(s.ic_port, s) for s in specs),
                                 tuple(dict.fromkeys(http)), tuple(dict.fromkeys(grpc)), membership)

    def publish(self, snapshot):
        self.http, self.grpc = snapshot.http, snapshot.grpc
        self.node_piles = snapshot.node_piles
        self._known = {n.node_id: n for n in [*snapshot.inventory.nodes.values(), *snapshot.inventory.slots.values()]}
        self.last_success = time.monotonic()
        self.last_observation = self.last_success
        self.error = None
        self.deferred = False
        self.generation += 1

    def check(self):
        if self.last_success is None or time.monotonic() - self.last_success > self.max_age:
            raise RecoveryUnavailable("Cluster discovery is stale; restore remains available")

    def status(self):
        return {"generation": self.generation, "http_endpoints": self.http, "grpc_endpoints": self.grpc,
                "age_sec": None if self.last_success is None else time.monotonic() - self.last_success,
                "error": self.error, "deferred": self.deferred}

    def fresh_node_piles(self):
        if self.last_observation is None or time.monotonic() - self.last_observation > self.max_age:
            return {}
        return dict(self.node_piles)

    def start(self, *, engine, guard, inventory, controller, service, probe, interval, specs_source=None):
        def refresh():
            while not self._stop.wait(interval):
                try:
                    snapshot = self.fetch(specs_source() if specs_source else controller.specs)
                    replacement = FailureModelGuard(snapshot.topology, inventory=snapshot.inventory,
                                                    total_slots=len(snapshot.inventory.slots))
                    with engine._lock:
                        if self._stop.is_set():
                            return
                        if probe.pending() or any(p["status"] == "running" for p in service.get_all()) or controller._active:
                            self.deferred = True
                            # Endpoint failover is independent of budget placement.
                            self.http, self.grpc = snapshot.http, snapshot.grpc
                            self.node_piles = snapshot.node_piles
                            self.last_observation = time.monotonic()
                            continue
                        guard.reconfigure(replacement)
                        inventory.__dict__ = snapshot.inventory.__dict__.copy()
                        controller.specs = snapshot.specs
                        self.publish(snapshot)
                except Exception as error:
                    self.error = str(error)
                    logger.warning("Cluster discovery refresh failed: %s", error)
        self._thread = threading.Thread(target=refresh, name="cluster-discovery", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()


class DiscoveredTabletRunner:
    """Resolve tablet membership anew; do not retain a client to a killed node."""
    def __init__(self, discovery, tablet_type):
        self.discovery = discovery
        self.tablet_type = tablet_type

    def inject_fault(self, payload=None):
        import random
        from ydb.tests.library.clients.kikimr_client import kikimr_client_factory
        failures = []
        for address in self.discovery.grpc:
            parsed = urlsplit(address)
            client = kikimr_client_factory(parsed.hostname, parsed.port, retry_count=1)
            try:
                tablets = client.tablet_state(self.tablet_type).TabletStateInfo
            except Exception as error:
                failures.append(str(error))
                continue
            if not tablets:
                raise RuntimeError("No tablets found for requested type")
            tablet = random.choice(tablets).TabletId
            client.tablet_kill(tablet, assert_success=True)
            logger.info("Killed tablet %s through %s", tablet, address)
            return
        raise RuntimeError("Cannot query tablets: " + "; ".join(failures))

    def extract_fault(self, payload=None):
        pass

    def on_failed_inject_fault(self):
        pass

    def on_failed_extract_fault(self):
        pass
