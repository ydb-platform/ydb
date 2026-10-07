"""Subclass :class:`NetworkClient` with local execution (no SSH) for stability nemesis agent."""

from __future__ import annotations

import logging
import subprocess

from ydb.tests.library.nemesis.network.client import NetworkClient

logger = logging.getLogger(__name__)


class LocalNetworkClient(NetworkClient):
    """Same iptables logic as :class:`NetworkClient`, but commands run on this host via ``subprocess``."""

    def __init__(self, port: int = 19001, *, ipv6: bool = True) -> None:
        super().__init__("localhost", port=port, ssh_username=None, ipv6=ipv6)

    def _run(self, command):
        r = subprocess.run(command, check=False, capture_output=True, text=True)
        if r.returncode and r.stderr:
            logger.error("%s", r.stderr.strip())
        elif r.stderr:
            logger.warning("%s", r.stderr.strip())
        return int(r.returncode), r.stdout or ''
