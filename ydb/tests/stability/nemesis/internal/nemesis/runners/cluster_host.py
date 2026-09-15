# Adapted from ydb/tests/tools/nemesis/library/host.py
#
# Hard-reboot the local host via sysrq-trigger.
# This runner executes on the agent host and reboots the **local** machine
# by writing to /proc/sysrq-trigger, mirroring the original HardRebootHostNemesis.

from __future__ import annotations

import subprocess

from ydb.tests.stability.nemesis.internal.nemesis.monitored_actor import MonitoredAgentActor


class ClusterHardRebootHostNemesis(MonitoredAgentActor):
    """Hard-reboot the local host via ``echo b > /proc/sysrq-trigger``.

    The orchestrator dispatches this to one agent; the agent reboots the
    local machine by writing to ``/proc/sysrq-trigger`` via subprocess.
    """

    def __init__(self) -> None:
        super().__init__(scope="host")

    def inject_fault(self, payload=None) -> None:
        del payload

        self._logger.info("HardRebootHostNemesis: hard-rebooting host")
        try:
            cmd = "nohup sudo sh -c 'echo b > /proc/sysrq-trigger' > /dev/null 2>&1 &"
            subprocess.run(cmd, shell=True, check=False, timeout=30)
            self.on_success_inject_fault()
        except Exception as e:
            self._logger.error("HardRebootHostNemesis: failed to reboot host: %s", e)
            raise

    def extract_fault(self, payload=None) -> None:
        del payload
        self._logger.info("HardRebootHostNemesis: extract_fault (no-op, reboot is irreversible)")
        self.on_success_extract_fault()
