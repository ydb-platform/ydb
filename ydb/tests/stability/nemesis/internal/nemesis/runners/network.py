"""Local network / DNS / time-skew nemesis actors (single-host harness)."""

from __future__ import annotations

import subprocess

from ydb.tests.stability.nemesis.internal.nemesis.local_network import LocalNetworkClient
from ydb.tests.stability.nemesis.internal.nemesis.monitored_actor import MonitoredAgentActor


class NetworkNemesis(MonitoredAgentActor):
    """Isolates localhost from the network or restores connectivity (payload is unused)."""

    def __init__(self) -> None:
        super().__init__(scope="node")

    def inject_fault(self, payload=None):
        del payload
        self._logger.info("=== INJECT_FAULT START: NetworkNemesis ===")
        client = LocalNetworkClient(port=19001)
        self._logger.info("Isolating node...")
        client.isolate_node()
        self.on_success_inject_fault()
        self._logger.info("=== INJECT_FAULT SUCCESS: NetworkNemesis ===")

    def extract_fault(self, payload=None):
        del payload
        self._logger.info("Extracting fault (network)")
        client = LocalNetworkClient(port=19001)
        self._logger.info("Restoring node...")
        client.clear_all_drops()
        self.on_success_extract_fault()


class DnsNemesis(MonitoredAgentActor):
    """iptables DNS isolation on localhost (same idea as ``ydb.tests.library.nemesis.nemesis_network.DnsNemesis``)."""

    def __init__(self) -> None:
        super().__init__(scope="node")

    def inject_fault(self, payload=None):
        del payload
        self._logger.info("=== INJECT_FAULT START: DnsNemesis ===")
        client = LocalNetworkClient(port=19001)
        client.isolate_dns()
        self.on_success_inject_fault()
        self._logger.info("=== INJECT_FAULT SUCCESS: DnsNemesis ===")

    def extract_fault(self, payload=None):
        del payload
        self._logger.info("Extracting DNS isolation")
        client = LocalNetworkClient(port=19001)
        client.clear_all_drops()
        self.on_success_extract_fault()


class TimeSkewNemesis(MonitoredAgentActor):
    """Step local system time forward; extract re-enables NTP sync (best-effort)."""

    def __init__(self) -> None:
        super().__init__(scope="node")

    def inject_fault(self, payload=None):
        payload = payload or {}
        delta = int(payload.get("delta_sec", 300))
        self._logger.info("=== INJECT_FAULT START: TimeSkewNemesis delta_sec=%s ===", delta)
        cmd = (
            "sudo timedatectl set-ntp false 2>/dev/null; "
            f"sudo date -s @$(($(date +%s)+{delta}))"
        )
        subprocess.run(cmd, shell=True, check=False)
        self.on_success_inject_fault()

    def extract_fault(self, payload=None):
        del payload
        self._logger.info("Restoring time sync (TimeSkewNemesis)")
        subprocess.run(
            "sudo timedatectl set-ntp true 2>/dev/null; "
            "(command -v chronyc >/dev/null && sudo chronyc -a makestep 2>/dev/null) || true",
            shell=True,
            check=False,
        )
        self.on_success_extract_fault()
