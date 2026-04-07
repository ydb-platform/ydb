# Adapted from ydb/tests/tools/nemesis/library/host.py
#
# Hard-reboot a remote host via sysrq-trigger.
# This runner executes on the agent host and reboots a **different** cluster
# node (never itself) via SSH, mirroring the original HardRebootHostNemesis.

from __future__ import annotations

import random
import socket
import subprocess

from ydb.tests.stability.nemesis.internal.nemesis.cluster_context import require_external_cluster
from ydb.tests.stability.nemesis.internal.nemesis.monitored_actor import MonitoredAgentActor


class ClusterHardRebootHostNemesis(MonitoredAgentActor):
    """Hard-reboot a random cluster host (excluding self) via ``echo b > /proc/sysrq-trigger``.

    The orchestrator dispatches this to one agent; the agent picks a random
    *other* host from cluster.yaml and sends the sysrq reboot command over SSH.
    """

    def __init__(self) -> None:
        super().__init__(scope="host")

    def inject_fault(self, payload=None) -> None:
        del payload
        cluster = require_external_cluster()
        self_hostname = socket.gethostname()

        target_nodes = [
            node for node in cluster.nodes.values()
            if node.host != self_hostname
        ]

        if not target_nodes:
            self._logger.warning("HardRebootHostNemesis: no target nodes available (excluded self), skipping")
            return

        node = random.choice(target_nodes)
        self._logger.info("HardRebootHostNemesis: hard-rebooting host %s", node.host)
        try:
            cmd = (
                f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
                f"{node.host} "
                f"\"nohup sudo sh -c 'echo b > /proc/sysrq-trigger' > /dev/null 2>&1 &\""
            )
            subprocess.run(cmd, shell=True, check=False, timeout=30)
            self.on_success_inject_fault()
            self._logger.info("HardRebootHostNemesis: successfully sent reboot command to %s", node.host)
        except Exception as e:
            self._logger.error("HardRebootHostNemesis: failed to reboot host %s: %s", node.host, e)

    def extract_fault(self, payload=None) -> None:
        del payload
        self._logger.info("HardRebootHostNemesis: extract_fault (no-op, reboot is irreversible)")
        self.on_success_extract_fault()
