# -*- coding: utf-8 -*-
import random
import socket
import logging

from ydb.tests.library.nemesis.nemesis_core import Nemesis
from ydb.tests.tools.nemesis.library import base

logger = logging.getLogger("host")
logger.info("=== HOST.PY LOADED ===")


class HardRebootHostNemesis(Nemesis, base.AbstractMonitoredNemesis):
    def __init__(self, cluster, schedule=(600, 900)):
        super(HardRebootHostNemesis, self).__init__(schedule=schedule)
        base.AbstractMonitoredNemesis.__init__(self, scope='host')
        self._cluster = cluster
        self._target_nodes = []

    def prepare_state(self):
        self_hostname = socket.gethostname()
        self._target_nodes = [
            node for node in self._cluster.nodes.values()
            if node.host != self_hostname
        ]
        logger.info(
            "HardRebootHostNemesis: self_hostname=%s, target_nodes=%d (excluded self)",
            self_hostname, len(self._target_nodes),
        )

    def inject_fault(self):
        if not self._target_nodes:
            logger.warning("HardRebootHostNemesis: no target nodes available, skipping")
            return

        node = random.choice(self._target_nodes)
        logger.info("HardRebootHostNemesis: hard-rebooting host %s", node.host)
        try:
            node.ssh_command(
                "nohup sudo sh -c 'echo b > /proc/sysrq-trigger' > /dev/null 2>&1 &",
                raise_on_error=True,
            )
            self.on_success_inject_fault()
            logger.info("HardRebootHostNemesis: successfully sent reboot command to %s", node.host)
        except Exception as e:
            logger.error("HardRebootHostNemesis: failed to reboot host %s: %s", node.host, e)

    def extract_fault(self):
        pass


def host_nemesis_list(cluster):
    return [
        HardRebootHostNemesis(cluster),
    ]
