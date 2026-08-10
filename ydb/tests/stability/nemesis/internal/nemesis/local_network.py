"""Subclass :class:`NetworkClient` with local execution (no SSH) for stability nemesis agent."""

from __future__ import annotations

import shlex
import subprocess

from ydb.tests.library.nemesis.network.client import NetworkClient


class LocalNetworkClient(NetworkClient):
    """Same iptables logic as :class:`NetworkClient`, but commands run on this host via ``subprocess``."""

    def __init__(self, port: int = 19001, *, ipv6: bool = True) -> None:
        super().__init__("localhost", port=port, ssh_username=None, ipv6=ipv6)

    def clear_all_drops(self, match=None):
        ib = self._iptables_bin
        save = self._iptables_save_bin
        match_pipe = f" | grep -F -- {shlex.quote(str(match))}" if match else ""
        script = (
            f"sudo {save} | grep -e statistic -e probability{match_pipe} | "
            f"sed -e 's/-A/-D/g' | while read line; do sudo {ib} $line; done"
        )
        r = subprocess.run(script, shell=True, check=False)
        return int(r.returncode)

    def _exec_command(self, command):
        r = subprocess.run(command, check=False)
        return int(r.returncode)
