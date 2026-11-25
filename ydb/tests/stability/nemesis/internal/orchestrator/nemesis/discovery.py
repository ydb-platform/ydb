"""Cluster-owned topology and endpoint discovery from a bootstrap viewer URL."""
from dataclasses import dataclass, replace
import logging
import threading
import time
from urllib.parse import urlsplit

import requests

from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.cluster_inventory import ClusterInventory, NodeInfo, SlotInfo
from ydb.tests.stability.nemesis.internal.orchestrator.nemesis.failure_model import ClusterTopologyModel
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
    missing: frozenset = frozenset()


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
        self._pile_names = ()
        self.missing = frozenset()
        self._signature = None

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
        names = [p["Name"] for p in config.get("BridgeConfig", {}).get("Piles", [])]
        if len(names) != len(set(names)):
            raise ValueError("Duplicate pile names")
        # IDs are installation-local identities, never positions in a viewer response.
        if self.generation and set(names) != set(self._pile_names):
            raise ValueError("Pile membership changed; drain and restart Nemesis")
        names = self._pile_names or tuple(sorted(names))
        piles = {name: i for i, name in enumerate(names)}
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
        observed = set()
        by_port = {s.ic_port: s for s in specs}
        updated_specs = {}
        known_ids = {s.node_id for s in self._known.values()}
        known_slots = {(s.host, s.ic_port): s for s in self._known.values() if isinstance(s, SlotInfo)}
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
                if nid in known_ids:
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
            # Runtime IDs may be reused while another slot is still missing.
            key = self.resource_key(record)
            previous = self._known.get(key) if kind == "node" else known_slots.get((host, port))
            if previous is not None and previous.bridge_pile_id != pile:
                raise ValueError(f"Node {nid} changed pile; drain and restart Nemesis")
            records[key] = record
            observed.add(key)
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
        if not set(static).issubset(n.node_id for n in records.values() if isinstance(n, NodeInfo)) or not http or not grpc:
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
                                 tuple(dict.fromkeys(http)), tuple(dict.fromkeys(grpc)), membership,
                                 frozenset(records[key].node_id for key in records.keys() - observed))

    @staticmethod
    def resource_key(record):
        return ("node", record.node_id) if isinstance(record, NodeInfo) else ("slot", record.host, record.ic_port)

    @staticmethod
    def signature(snapshot):
        return (tuple(sorted(snapshot.inventory.nodes.items())), tuple(sorted(snapshot.inventory.slots.items())),
                snapshot.specs, tuple(snapshot.topology.pile_names.items()), snapshot.topology.tolerance.erasure)

    def matches(self, snapshot):
        return self.signature(snapshot) == self._signature

    def observe(self, snapshot):
        self.http, self.grpc = snapshot.http, snapshot.grpc
        self.node_piles = dict(snapshot.node_piles)
        self.missing = snapshot.missing
        self.last_observation = time.monotonic()
        self.error = None

    def publish(self, snapshot):
        self.observe(snapshot)
        signature = self.signature(snapshot)
        if signature != self._signature:
            self.generation += 1
        self._signature = signature
        self._pile_names = tuple(snapshot.topology.pile_names.values())
        self._known = {self.resource_key(n): n
                       for n in [*snapshot.inventory.nodes.values(), *snapshot.inventory.slots.values()]}
        self.last_success = time.monotonic()
        self.last_observation = self.last_success
        self.error = None
        self.deferred = False

    def check(self):
        if self.missing:
            raise RecoveryUnavailable("Discovered resources are missing; restore remains available")
        if self.deferred:
            raise RecoveryUnavailable("Topology update awaits recovery of active faults")
        if self.last_observation is None or time.monotonic() - self.last_observation > self.max_age:
            raise RecoveryUnavailable("Cluster discovery is stale; restore remains available")

    def status(self):
        return {"generation": self.generation, "http_endpoints": self.http, "grpc_endpoints": self.grpc,
                "age_sec": None if self.last_success is None else time.monotonic() - self.last_success,
                "error": self.error, "deferred": self.deferred, "missing_nodes": sorted(self.missing),
                "observation_age_sec": None if self.last_observation is None else time.monotonic() - self.last_observation}

    def fresh_node_piles(self):
        if self.last_observation is None or time.monotonic() - self.last_observation > self.max_age:
            return {}
        return dict(self.node_piles)

    def start(self, *, engine, controller, service, interval, specs_source=None):
        def refresh():
            while not self._stop.wait(interval):
                try:
                    snapshot = self.fetch(specs_source() if specs_source else controller.specs)
                    engine.refresh_topology(self, snapshot, controller=controller, service=service)
                except Exception as error:
                    self.node_piles = {}
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
