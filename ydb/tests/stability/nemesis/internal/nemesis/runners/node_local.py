"""Single-host local process kill nemesis (SIGKILL ic-port)."""

from __future__ import annotations

import signal
import subprocess

from ydb.tests.stability.nemesis.internal.nemesis.monitored_actor import MonitoredAgentActor


class KillNodeNemesis(MonitoredAgentActor):
    """SIGKILL one local YDB ic-port process; extract is a monitoring noop."""

    supports_local_mode = True

    def __init__(self) -> None:
        super().__init__(scope="node")

    def inject_fault(self, payload=None):
        payload = payload or {}
        sig_name = payload.get("signal", "SIGKILL")
        sig = getattr(signal, sig_name, signal.SIGKILL)
        self._logger.info("=== INJECT_FAULT START: KillNodeNemesis ===")
        cmd = (
            "ps aux | grep '\\--ic-port' | grep -v grep | awk '{ print $2 }' | shuf -n 1 | xargs -r sudo kill -%d"
            % (int(sig),)
        )
        self._logger.info("Executing: %s", cmd)
        subprocess.check_call(cmd, shell=True)
        self.on_success_inject_fault()
        self._logger.info("=== INJECT_FAULT SUCCESS: KillNodeNemesis ===")

    def extract_fault(self, payload=None):
        del payload
        self._logger.info("Extract noop (kill node has no reversible extract)")
        self.on_success_extract_fault()
