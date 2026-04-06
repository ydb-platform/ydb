# -*- coding: utf-8 -*-
# Adapted from ydb/tests/tools/nemesis/library/datacenter.py
#
# All runners execute **locally** on the agent host.  The orchestrator's
# ``DataCenterFanoutPlanner`` dispatches inject/extract to every host in the
# selected datacenter; each agent runs the fault against its own local
# kikimr / kikimr-multi@ services or local iptables / ip-route.

from __future__ import annotations

import socket
import subprocess
import time

from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import require_external_cluster
from ydb.tests.stability.nemesis.internal.nemesis.monitored_actor import MonitoredAgentActor


# ---------------------------------------------------------------------------
# Local helpers
# ---------------------------------------------------------------------------


def _kikimr_slot_id(node):
    """``None`` = single ``kikimr`` service; else ``kikimr-multi@`` instance id."""
    return getattr(node, "_KikimrExternalNode__slot_id", None)


def _resolve_local_host():
    """Return (fqdn, hostname) for the current machine."""
    fqdn = socket.getfqdn()
    hostname = socket.gethostname()
    return fqdn, hostname


def _is_local_host(host, fqdn, hostname):
    return host in (fqdn, hostname) or host.split(".")[0] == hostname.split(".")[0]


def _local_stop_kikimr(node):
    """``systemctl stop`` for the local kikimr / kikimr-multi@ service."""
    slot_id = _kikimr_slot_id(node)
    if slot_id is None:
        subprocess.run("sudo service kikimr stop", shell=True, check=True)
    else:
        subprocess.run(
            ["sudo", "systemctl", "stop", "kikimr-multi@{}".format(slot_id)],
            check=True,
        )


def _local_start_kikimr(node):
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
# Runners
# ---------------------------------------------------------------------------


class ClusterDataCenterStopNodesNemesis(MonitoredAgentActor):
    """Stop local kikimr node and slots.

    The orchestrator's ``DataCenterFanoutPlanner`` dispatches this to every
    host in the selected datacenter.  Each agent stops its own local services.
    """

    def __init__(self, duration=60):
        super().__init__(scope='datacenter')
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
            self._logger.warning("No local nodes/slots found for datacenter stop")
            return

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

        time.sleep(self._duration)

        # Extract (start everything back)
        self._start_all()
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


class ClusterDataCenterRouteUnreachableNemesis(MonitoredAgentActor):
    """Block YDB ports on the local host using ip6tables.

    The orchestrator's ``DataCenterFanoutPlanner`` dispatches this to every
    host in the selected datacenter.  Each agent blocks its own local ports
    and schedules automatic recovery.
    """

    def __init__(self, duration=60):
        super().__init__(scope='datacenter')
        self._duration = duration

        self._block_ports_cmd = (
            "sudo /sbin/ip6tables -w -A YDB_FW -p tcp -m multiport "
            "--ports 2135,2136,8765,19001,31000:32000 -j REJECT"
        )
        self._restore_ports_cmd = (
            "sudo /sbin/ip6tables -w -F YDB_FW"
        )

    def inject_fault(self, payload=None):
        del payload
        self.start_inject_fault()
        try:
            # Block ports and schedule automatic recovery
            cmd = (
                f"nohup bash -c '{self._block_ports_cmd} && "
                f"sleep {self._duration} && "
                f"sudo /sbin/ip6tables -w -F YDB_FW' > /dev/null 2>&1 &"
            )
            self._logger.info("Blocking local YDB ports with auto-recovery in %ds", self._duration)
            subprocess.run(cmd, shell=True, check=True)
            self.on_success_inject_fault()
        except Exception as e:
            self._logger.error("Failed to block YDB ports: %s", e)

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


class ClusterDataCenterIptablesBlockPortsNemesis(MonitoredAgentActor):
    """Block network routes to other datacenter nodes from the local host.

    The orchestrator's ``DataCenterFanoutPlanner`` dispatches this to every
    host in the selected datacenter.  Each agent adds ``ip -6 ro add unreach``
    rules for IPs of nodes in **other** datacenters and schedules automatic
    recovery.
    """

    def __init__(self, duration=60):
        super().__init__(scope='datacenter')
        self._duration = duration

    def inject_fault(self, payload=None):
        payload = payload if isinstance(payload, dict) else {}
        cluster = require_external_cluster()
        fqdn, hostname = _resolve_local_host()

        # Determine which datacenter this host belongs to
        local_dc = None
        for node in cluster.nodes.values():
            if _is_local_host(node.host, fqdn, hostname) and node.datacenter is not None:
                local_dc = node.datacenter
                break

        if local_dc is None:
            self._logger.warning("Cannot determine local datacenter — skipping")
            return

        # Collect IPs of nodes in OTHER datacenters
        other_ips = set()
        for node in cluster.nodes.values():
            if node.datacenter is not None and node.datacenter != local_dc:
                ip = _resolve_hostname_to_ipv6(node.host)
                if ip:
                    other_ips.add(ip)
                else:
                    self._logger.warning("Failed to resolve %s to IPv6", node.host)

        if not other_ips:
            self._logger.warning("No other-DC IPs to block")
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

        self.on_success_inject_fault()

    def extract_fault(self, payload=None):
        del payload
        self._logger.info("Removing unreachable routes to restore connectivity immediately")
        cluster = require_external_cluster()
        fqdn, hostname = _resolve_local_host()

        local_dc = None
        for node in cluster.nodes.values():
            if _is_local_host(node.host, fqdn, hostname) and node.datacenter is not None:
                local_dc = node.datacenter
                break

        if local_dc is not None:
            for node in cluster.nodes.values():
                if node.datacenter is not None and node.datacenter != local_dc:
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
