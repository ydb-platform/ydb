"""Subclass :class:`NetworkClient` with local execution (no SSH) for stability nemesis agent."""

from __future__ import annotations

import subprocess

from ydb.tests.library.nemesis.network.client import NetworkClient


class LocalNetworkClient(NetworkClient):
    """Same iptables logic as :class:`NetworkClient`, but commands run on this host via ``subprocess``."""

    def __init__(self, port: int = 19001, *, ipv6: bool = True) -> None:
        super().__init__("localhost", port=port, ssh_username=None, ipv6=ipv6)

    def _exec_command(self, command):
        if "|" in command:
            ib = self._iptables_bin
            save = self._iptables_save_bin
            script = (
                f"sudo {save} | grep -e statistic -e probability | sed -e 's/-A/-D/g' | "
                f"while read line; do sudo {ib} $line; done"
            )
            r = subprocess.run(script, shell=True, check=False)
            return int(r.returncode)
        r = subprocess.run(command, check=False)
        return int(r.returncode)
