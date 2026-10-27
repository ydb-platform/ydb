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
from ydb.tests.stability.nemesis.internal.nemesis.runners.local_kikimr import (
    active_local_kikimr,
    start_local_kikimr,
    stop_local_kikimr,
)


# ---------------------------------------------------------------------------
# Local helpers
# ---------------------------------------------------------------------------


def _resolve_local_host():
    """Return (fqdn, hostname) for the current machine."""
    fqdn = socket.getfqdn()
    hostname = socket.gethostname()
    return fqdn, hostname


def _is_local_host(host, fqdn, hostname):
    return host in (fqdn, hostname) or host.split(".")[0] == hostname.split(".")[0]


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
    """Stop the local kikimr units that are actually active.

    The orchestrator's ``DataCenterFanoutPlanner`` dispatches this to every
    host in the selected datacenter.  Each agent snapshots active units and
    starts that same set back.  ``cluster.slots`` is not walked, and extract
    does not discover a new set.
    """

    def __init__(self, duration=60):
        super().__init__(scope='datacenter')
        self._duration = duration
        # None until inject snapshots systemd.  Extract must not list units again.
        self._stopped_static = False
        self._stopped_slots = None

    def inject_fault(self, payload=None):
        payload = payload if isinstance(payload, dict) else {}
        require_external_cluster()

        self.start_inject_fault()
        self._snapshot_and_stop()
        time.sleep(self._duration)
        self._start_remembered()
        self.on_success_inject_fault()

    def _snapshot_and_stop(self):
        try:
            static, slots = active_local_kikimr()
        except Exception as e:
            self._logger.error("Failed to list active local kikimr: %s", e)
            return
        self._stopped_static = static
        self._stopped_slots = list(slots)
        self._logger.info("Stopping active local kikimr static=%s slots=%s", static, slots)
        try:
            stop_local_kikimr(static, slots)
        except Exception as e:
            self._logger.error("Failed to stop active local kikimr: %s", e)

    def _start_remembered(self):
        if self._stopped_slots is None:
            return
        static = self._stopped_static
        slots = list(self._stopped_slots)
        try:
            self._logger.info("Starting remembered local kikimr static=%s slots=%s", static, slots)
            start_local_kikimr(static, slots)
        except Exception as e:
            self._logger.error("Failed to start remembered local kikimr: %s", e)
            return
        self._stopped_static = False
        self._stopped_slots = None

    def extract_fault(self, payload=None):
        del payload
        self._start_remembered()
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
            raise

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
