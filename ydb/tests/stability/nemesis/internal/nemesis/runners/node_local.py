"""Single-host local process kill nemesis (SIGKILL ic-port)."""

from __future__ import annotations

import signal
import subprocess

from ydb.tests.stability.nemesis.internal.nemesis.monitored_actor import MonitoredAgentActor
from ydb.tests.stability.nemesis.internal.nemesis.runners.target_payload import target_from_payload


class KillNodeNemesis(MonitoredAgentActor):
    """SIGKILL one local YDB ic-port process; requires ChaosTarget.ic_port / node_ic_port."""

    def __init__(self) -> None:
        super().__init__(scope="node")

    def inject_fault(self, payload=None):
        payload = payload or {}
        sig_name = payload.get("signal", "SIGKILL")
        sig = getattr(signal, sig_name, signal.SIGKILL)
        self._logger.info("=== INJECT_FAULT START: KillNodeNemesis ===")
        target = target_from_payload(payload)
        ic_port = None
        if target is not None and target.ic_port is not None:
            ic_port = int(target.ic_port)
        elif payload.get("node_ic_port") is not None:
            ic_port = int(payload["node_ic_port"])

        if ic_port is None:
            self._logger.error(
                "KillNodeNemesis: ChaosTarget must include ic_port (or payload.node_ic_port)"
            )
            return

        cmd = (
            "ps aux | grep '\\--ic-port %d' | grep -v grep | awk '{ print $2 }' | "
            "xargs -r sudo kill -%d" % (ic_port, int(sig))
        )
        self._logger.info("Executing: %s", cmd)
        subprocess.run(cmd, shell=True, check=False)
        self.on_success_inject_fault()
        self._logger.info("=== INJECT_FAULT SUCCESS: KillNodeNemesis ===")

    def extract_fault(self, payload=None):
        del payload
        self._logger.info("Extract noop (kill node has no reversible extract)")
        self.on_success_extract_fault()
