# -*- coding: utf-8 -*-
# Adapted from ydb/tests/tools/nemesis/library/bridge_pile.py
#
# All runners execute **locally** on the agent host.  The orchestrator's
# ``BridgePileFanoutPlanner`` dispatches inject/extract to every host in the
# selected bridge pile; each agent runs the fault against its own local
# kikimr / kikimr-multi@ services or local iptables / ip-route.
#
# Bridge state management (failover / rejoin) is done via the bridge gRPC API
# which is a cluster-wide API call — it is performed by the first agent that
# receives the inject (the one that completes first).

from __future__ import annotations

import collections
import socket
import subprocess
import time
from typing import Any

from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState
from ydb.tests.library.clients.kikimr_bridge_client import bridge_client_factory
from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import require_external_cluster
from ydb.tests.stability.nemesis.internal.nemesis.monitored_actor import MonitoredAgentActor


VALID_FAILOVER_STATES = {PileState.PRIMARY, PileState.SYNCHRONIZED, PileState.NOT_SYNCHRONIZED}
VALID_REJOIN_STATES = {PileState.DISCONNECTED}

PILE_STATE_NAMES = {
    PileState.UNSPECIFIED: "UNSPECIFIED",
    PileState.PRIMARY: "PRIMARY",
    PileState.PROMOTED: "PROMOTED",
    PileState.SYNCHRONIZED: "SYNCHRONIZED",
    PileState.NOT_SYNCHRONIZED: "NOT_SYNCHRONIZED",
    PileState.SUSPENDED: "SUSPENDED",
    PileState.DISCONNECTED: "DISCONNECTED",
}


def get_pile_state_name(state):
    return PILE_STATE_NAMES.get(state, f"UNKNOWN({state})")


# ---------------------------------------------------------------------------
# Local helpers
# ---------------------------------------------------------------------------


def _kikimr_slot_id(node: Any) -> str | None:
    """``None`` = single ``kikimr`` service; else ``kikimr-multi@`` instance id."""
    return getattr(node, "_KikimrExternalNode__slot_id", None)


def _resolve_local_host():
    """Return (fqdn, hostname) for the current machine."""
    fqdn = socket.getfqdn()
    hostname = socket.gethostname()
    return fqdn, hostname


def _is_local_host(host, fqdn, hostname):
    return host in (fqdn, hostname) or host.split(".")[0] == hostname.split(".")[0]


def _local_stop_kikimr(node: Any) -> None:
    """``systemctl stop`` for the local kikimr / kikimr-multi@ service."""
    slot_id = _kikimr_slot_id(node)
    if slot_id is None:
        subprocess.run("sudo service kikimr stop", shell=True, check=True)
    else:
        subprocess.run(
            ["sudo", "systemctl", "stop", "kikimr-multi@{}".format(slot_id)],
            check=True,
        )


def _local_start_kikimr(node: Any) -> None:
    """``systemctl start`` for the local kikimr / kikimr-multi@ service."""
    slot_id = _kikimr_slot_id(node)
    if slot_id is None:
        subprocess.run("sudo service kikimr start", shell=True, check=True)
        return
    slot_dir = "/Berkanavt/kikimr_{slot}".format(slot=slot_id)
    slot_cfg = slot_dir + "/slot_cfg"
    env_txt = slot_dir + "/env.txt"
    cfg = """\
tenant=/Root/db1
grpc={grpc}
mbus={mbus}
ic={ic}
mon={mon}""".format(
        mbus=node.mbus_port,
        grpc=node.grpc_port,
        mon=node.mon_port,
        ic=node.ic_port,
    )
    subprocess.run(["sudo", "mkdir", "-p", slot_dir], check=True)
    subprocess.run(["sudo", "touch", env_txt], check=True)
    p = subprocess.Popen(
        ["sudo", "tee", slot_cfg],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    _out, _err = p.communicate(cfg.encode("utf-8"))
    if p.returncode != 0:
        raise subprocess.CalledProcessError(p.returncode, "sudo tee slot_cfg", None)
    subprocess.run(
        ["sudo", "systemctl", "start", "kikimr-multi@{}".format(slot_id)],
        check=True,
    )


def _resolve_hostname_to_ipv6(hostname):
    """Resolve hostname to IPv6 address."""
    try:
        result = socket.getaddrinfo(hostname, None, socket.AF_INET6)
        if result:
            return result[0][4][0]
    except socket.gaierror:
        pass
    return None


# ---------------------------------------------------------------------------
# Bridge state helpers (cluster-wide gRPC API — not SSH)
# ---------------------------------------------------------------------------


def _build_bridge_topology(cluster):
    """Return (pile_to_nodes, pile_ids, pile_id_to_name)."""
    pile_to_nodes = collections.defaultdict(list)
    pile_id_to_name = {}
    for node_id, node in cluster.nodes.items():
        if node.bridge_pile_id is not None and node.bridge_pile_name is not None:
            pile_to_nodes[node.bridge_pile_id].append(node)
            pile_id_to_name[node.bridge_pile_id] = node.bridge_pile_name
    pile_ids = list(pile_to_nodes.keys())
    return pile_to_nodes, pile_ids, pile_id_to_name


def _create_bridge_clients(cluster, pile_to_nodes, pile_ids):
    """Create bridge gRPC clients for each pile (connects to first node in pile)."""
    clients = {}
    for pile_id in pile_ids:
        node = pile_to_nodes[pile_id][0]
        clients[pile_id] = bridge_client_factory(
            node.host, node.port, cluster=cluster, retry_count=3, timeout=5
        )
        clients[pile_id].set_auth_token('root@builtin')
    return clients


def _get_pile_state(pile_name, bridge_client):
    pile_states = bridge_client.per_pile_state
    if pile_states is None:
        return None
    for pile in pile_states:
        if pile.pile_name == pile_name:
            return pile.state
    return None


def _do_failover(pile_id, pile_id_to_name, pile_ids, bridge_clients, logger):
    """Perform bridge failover for the given pile (cluster-wide gRPC API)."""
    another_pile_id = None
    for pid in pile_ids:
        if pid != pile_id:
            another_pile_id = pid
            break
    if another_pile_id is None:
        logger.error("Could not find another pile for bridge operations")
        return

    pile_name = pile_id_to_name[pile_id]
    current_state = _get_pile_state(pile_name, bridge_clients[another_pile_id])
    if current_state is not None and current_state not in VALID_FAILOVER_STATES:
        logger.warning("Pile %s in state %s, not valid for failover",
                       pile_name, get_pile_state_name(current_state))
        return

    logger.info("OPERATION: failover (pile_id=%d, pile_name=%s)", pile_id, pile_name)
    result = bridge_clients[another_pile_id].failover(pile_name)
    if not result:
        logger.error("Failed to failover pile %d", pile_id)


def _do_rejoin(pile_id, pile_id_to_name, pile_ids, bridge_clients, logger):
    """Perform bridge rejoin for the given pile (cluster-wide gRPC API)."""
    another_pile_id = None
    for pid in pile_ids:
        if pid != pile_id:
            another_pile_id = pid
            break
    if another_pile_id is None:
        logger.error("Could not find another pile for bridge restoration")
        return

    pile_name = pile_id_to_name[pile_id]
    current_state = _get_pile_state(pile_name, bridge_clients[another_pile_id])
    if current_state is not None and current_state not in VALID_REJOIN_STATES:
        logger.warning("Pile %s in state %s, not valid for rejoin (may already be joined)",
                       pile_name, get_pile_state_name(current_state))
        return

    logger.info("OPERATION: rejoin (pile_id=%d, pile_name=%s)", pile_id, pile_name)
    result = bridge_clients[another_pile_id].rejoin(pile_name)
    if not result:
        logger.error("Failed to rejoin pile %d", pile_id)


# ---------------------------------------------------------------------------
# Runners
# ---------------------------------------------------------------------------


class ClusterBridgePileStopNodesNemesis(MonitoredAgentActor):
    """Stop local kikimr node and slots, then manage bridge state.

    The orchestrator's ``BridgePileFanoutPlanner`` dispatches this to every
    host in the selected bridge pile.  Each agent stops its own local services.
    Bridge failover/rejoin is done via cluster-wide gRPC API.
    """

    def __init__(self, duration=60):
        super().__init__(scope='bridge_pile')
        self._duration = duration
        self._stopped_nodes = []
        self._stopped_slots = []

    def inject_fault(self, payload=None):
        payload = payload if isinstance(payload, dict) else {}
        cluster = require_external_cluster()
        fqdn, hostname = _resolve_local_host()

        # Find local nodes and slots
        local_nodes = [n for n in cluster.nodes.values() if _is_local_host(n.host, fqdn, hostname)]
        local_slots = [s for s in cluster.slots.values() if _is_local_host(s.host, fqdn, hostname)]

        if not local_nodes and not local_slots:
            self._logger.warning("No local nodes/slots found for bridge pile stop")
            return

        # Determine local pile_id
        local_pile_id = None
        for node in local_nodes:
            if node.bridge_pile_id is not None:
                local_pile_id = node.bridge_pile_id
                break

        self.start_inject_fault()

        # Stop slots first
        for slot in local_slots:
            try:
                self._logger.info("Stopping local slot ic_port=%d", slot.ic_port)
                _local_stop_kikimr(slot)
                self._stopped_slots.append(slot)
            except Exception as e:
                self._logger.error("Failed to stop slot ic_port=%d: %s", slot.ic_port, e)

        # Stop nodes
        for node in local_nodes:
            try:
                self._logger.info("Stopping local node node_id=%d", node.node_id)
                _local_stop_kikimr(node)
                self._stopped_nodes.append(node)
            except Exception as e:
                self._logger.error("Failed to stop node node_id=%d: %s", node.node_id, e)

        self._logger.info("Stopped %d nodes and %d slots locally",
                          len(self._stopped_nodes), len(self._stopped_slots))

        # Bridge failover (cluster-wide gRPC API)
        if local_pile_id is not None:
            try:
                pile_to_nodes, pile_ids, pile_id_to_name = _build_bridge_topology(cluster)
                bridge_clients = _create_bridge_clients(cluster, pile_to_nodes, pile_ids)
                _do_failover(local_pile_id, pile_id_to_name, pile_ids, bridge_clients, self._logger)
            except Exception as e:
                self._logger.error("Bridge failover failed: %s", e)

        time.sleep(self._duration)

        # Extract (start everything back)
        self._start_all()

        # Bridge rejoin
        if local_pile_id is not None:
            try:
                pile_to_nodes, pile_ids, pile_id_to_name = _build_bridge_topology(cluster)
                bridge_clients = _create_bridge_clients(cluster, pile_to_nodes, pile_ids)
                _do_rejoin(local_pile_id, pile_id_to_name, pile_ids, bridge_clients, self._logger)
            except Exception as e:
                self._logger.error("Bridge rejoin failed: %s", e)

        self.on_success_inject_fault()

    def _start_all(self):
        # Start nodes first
        for node in self._stopped_nodes:
            try:
                self._logger.info("Starting local node node_id=%d", node.node_id)
                _local_start_kikimr(node)
            except Exception as e:
                self._logger.error("Failed to start node node_id=%d: %s", node.node_id, e)

        # Start slots
        for slot in self._stopped_slots:
            try:
                self._logger.info("Starting local slot ic_port=%d", slot.ic_port)
                _local_start_kikimr(slot)
            except Exception as e:
                self._logger.error("Failed to start slot ic_port=%d: %s", slot.ic_port, e)

        self._stopped_nodes = []
        self._stopped_slots = []

    def extract_fault(self, payload=None):
        del payload
        if self._stopped_nodes or self._stopped_slots:
            self._start_all()
        self.on_success_extract_fault()


class ClusterBridgePileIptablesBlockPortsNemesis(MonitoredAgentActor):
    """Block YDB ports on the local host using ip6tables, then manage bridge state.

    The orchestrator's ``BridgePileFanoutPlanner`` dispatches this to every
    host in the selected bridge pile.  Each agent blocks its own local ports
    and schedules automatic recovery.
    """

    def __init__(self, duration=60):
        super().__init__(scope='bridge_pile')
        self._duration = duration

        self._block_ports_cmd = (
            "sudo /sbin/ip6tables -w -A YDB_FW -p tcp -m multiport "
            "--ports 2135,2136,8765,19001,31000:32000 -j REJECT"
        )

    def inject_fault(self, payload=None):
        payload = payload if isinstance(payload, dict) else {}
        cluster = require_external_cluster()
        fqdn, hostname = _resolve_local_host()

        # Determine local pile_id
        local_pile_id = None
        for node in cluster.nodes.values():
            if _is_local_host(node.host, fqdn, hostname) and node.bridge_pile_id is not None:
                local_pile_id = node.bridge_pile_id
                break

        self.start_inject_fault()

        # Block ports and schedule automatic recovery
        try:
            cmd = (
                f"nohup bash -c '{self._block_ports_cmd} && "
                f"sleep {self._duration} && "
                f"sudo /sbin/ip6tables -w -F YDB_FW' > /dev/null 2>&1 &"
            )
            self._logger.info("Blocking local YDB ports with auto-recovery in %ds", self._duration)
            subprocess.run(cmd, shell=True, check=True)
        except Exception as e:
            self._logger.error("Failed to block YDB ports: %s", e)
            return

        # Bridge failover (cluster-wide gRPC API)
        if local_pile_id is not None:
            try:
                pile_to_nodes, pile_ids, pile_id_to_name = _build_bridge_topology(cluster)
                bridge_clients = _create_bridge_clients(cluster, pile_to_nodes, pile_ids)
                _do_failover(local_pile_id, pile_id_to_name, pile_ids, bridge_clients, self._logger)
            except Exception as e:
                self._logger.error("Bridge failover failed: %s", e)

        time.sleep(self._duration)

        # Bridge rejoin after ports are restored
        if local_pile_id is not None:
            try:
                pile_to_nodes, pile_ids, pile_id_to_name = _build_bridge_topology(cluster)
                bridge_clients = _create_bridge_clients(cluster, pile_to_nodes, pile_ids)
                _do_rejoin(local_pile_id, pile_id_to_name, pile_ids, bridge_clients, self._logger)
            except Exception as e:
                self._logger.error("Bridge rejoin failed: %s", e)

        self.on_success_inject_fault()

    def extract_fault(self, payload=None):
        del payload
        self._logger.info("Flushing YDB_FW iptables chain to restore ports immediately")
        try:
            subprocess.run(
                "sudo /sbin/ip6tables -w -F YDB_FW",
                shell=True, check=True,
            )
        except Exception as e:
            self._logger.error("Failed to flush YDB_FW chain: %s", e)
        self.on_success_extract_fault()


class ClusterBridgePileRouteUnreachableNemesis(MonitoredAgentActor):
    """Block network routes to other pile nodes from the local host.

    The orchestrator's ``BridgePileFanoutPlanner`` dispatches this to every
    host in the selected bridge pile.  Each agent adds ``ip -6 ro add unreach``
    rules for IPs of nodes in **other** piles and schedules automatic recovery.
    """

    def __init__(self, duration=60):
        super().__init__(scope='bridge_pile')
        self._duration = duration

    def inject_fault(self, payload=None):
        payload = payload if isinstance(payload, dict) else {}
        cluster = require_external_cluster()
        fqdn, hostname = _resolve_local_host()

        # Determine local pile_id
        local_pile_id = None
        for node in cluster.nodes.values():
            if _is_local_host(node.host, fqdn, hostname) and node.bridge_pile_id is not None:
                local_pile_id = node.bridge_pile_id
                break

        if local_pile_id is None:
            self._logger.warning("Cannot determine local bridge pile — skipping")
            return

        # Collect IPs of nodes in OTHER piles
        pile_to_nodes, pile_ids, pile_id_to_name = _build_bridge_topology(cluster)
        other_ips = set()
        for pid, nodes in pile_to_nodes.items():
            if pid != local_pile_id:
                for node in nodes:
                    ip = _resolve_hostname_to_ipv6(node.host)
                    if ip:
                        other_ips.add(ip)
                    else:
                        self._logger.warning("Failed to resolve %s to IPv6", node.host)

        if not other_ips:
            self._logger.warning("No other-pile IPs to block")
            return

        self.start_inject_fault()
        for ip in other_ips:
            try:
                cmd = (
                    f"nohup bash -c 'sudo /usr/bin/ip -6 ro add unreach {ip} && "
                    f"sleep {self._duration} && "
                    f"sudo /usr/bin/ip -6 ro del unreach {ip}' > /dev/null 2>&1 &"
                )
                self._logger.info("Blocking route to %s with auto-recovery in %ds", ip, self._duration)
                subprocess.run(cmd, shell=True, check=True)
            except Exception as e:
                self._logger.error("Failed to block route to %s: %s", ip, e)

        # Bridge failover (cluster-wide gRPC API)
        try:
            bridge_clients = _create_bridge_clients(cluster, pile_to_nodes, pile_ids)
            _do_failover(local_pile_id, pile_id_to_name, pile_ids, bridge_clients, self._logger)
        except Exception as e:
            self._logger.error("Bridge failover failed: %s", e)

        time.sleep(self._duration)

        # Bridge rejoin after routes are restored
        try:
            bridge_clients = _create_bridge_clients(cluster, pile_to_nodes, pile_ids)
            _do_rejoin(local_pile_id, pile_id_to_name, pile_ids, bridge_clients, self._logger)
        except Exception as e:
            self._logger.error("Bridge rejoin failed: %s", e)

        self.on_success_inject_fault()

    def extract_fault(self, payload=None):
        del payload
        self._logger.info("Removing unreachable routes to restore connectivity immediately")
        cluster = require_external_cluster()
        fqdn, hostname = _resolve_local_host()

        local_pile_id = None
        for node in cluster.nodes.values():
            if _is_local_host(node.host, fqdn, hostname) and node.bridge_pile_id is not None:
                local_pile_id = node.bridge_pile_id
                break

        if local_pile_id is not None:
            pile_to_nodes, _, _ = _build_bridge_topology(cluster)
            for pid, nodes in pile_to_nodes.items():
                if pid != local_pile_id:
                    for node in nodes:
                        ip = _resolve_hostname_to_ipv6(node.host)
                        if ip:
                            try:
                                subprocess.run(
                                    f"sudo /usr/bin/ip -6 ro del unreach {ip}",
                                    shell=True, check=True,
                                )
                            except Exception as e:
                                self._logger.warning("Failed to remove unreach route for %s: %s", ip, e)
        self.on_success_extract_fault()
