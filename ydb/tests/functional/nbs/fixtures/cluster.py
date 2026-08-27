# -*- coding: utf-8 -*-
"""Session-scoped NBS cluster used by one functional suite.

One PY3TEST target is one pytest session. This fixture boots the 9-node
cluster, the ``/Root/NBS`` tenant and the DDisk pool once, and tears them
down when the suite ends. Individual cases must not start their own
cluster.

F2/F3 bind ``nbs_cluster_file_pdisks`` so ``node.stop()`` does not wipe
PDisk contents (in-memory SectorMap would).
"""

import logging

import pytest
import requests

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.functional.nbs.lib.helpers import execute_ydbd

logger = logging.getLogger(__name__)

NBS_DATABASE_NAME = '/Root/NBS'
DDISK_POOL_NAME = 'ddp1'


class NbsCluster:
    """Running KiKiMR cluster with NBS enabled and one DDisk pool defined."""

    def __init__(self, use_in_memory_pdisks=True):
        self.cluster = None
        self.slots = []
        self.ddisk_pool_name = DDISK_POOL_NAME
        self.nbs_database_name = NBS_DATABASE_NAME
        # File-backed PDisks survive node.stop(); in-memory SectorMap does not.
        self.use_in_memory_pdisks = use_in_memory_pdisks

    def start(self):
        """Boot the cluster, create the NBS database and define the DDisk pool."""
        self.cluster = KiKiMR(
            KikimrConfigGenerator(
                erasure=Erasure.MIRROR_3_DC,
                enable_nbs=True,
                nbs_database_name=self.nbs_database_name,
                use_in_memory_pdisks=self.use_in_memory_pdisks,
                additional_log_configs={
                    'NBS_PARTITION': LogLevels.INFO,
                    'NBS2_LOAD_TEST': LogLevels.DEBUG,
                    'NBS_VOLUME': LogLevels.DEBUG,
                    'NBS_SS_PROXY': LogLevels.DEBUG,
                },
            )
        )
        self.cluster.start()
        self.slots = self._start_nbs()
        self._create_ddisk_pool()

    def stop(self):
        """Stop every node and slot."""
        if self.cluster is None:
            return
        try:
            self.cluster.stop()
        except Exception as e:
            logger.warning('cluster.stop() raised %s (daemons may already be dead)', e)
        self.cluster = None

    def recover(self):
        """Restart any dead static node or NBS slot left by a previous case."""
        restarted = False
        for node_id, node in self.cluster.nodes.items():
            if not node.is_alive():
                logger.warning('restarting dead node %s', node_id)
                node.start()
                restarted = True
        for slot_id, slot in self.cluster.slots.items():
            if not slot.is_alive():
                logger.warning('restarting dead slot %s', slot_id)
                slot.start()
                restarted = True
        if restarted:
            self.cluster.wait_tenant_up(self.nbs_database_name)

    def assert_healthy(self):
        """Fail fast if a previous case left a node or slot dead."""
        dead = []
        for node_id, node in self.cluster.nodes.items():
            if not node.is_alive():
                dead.append('node {}'.format(node_id))
        for slot_id, slot in self.cluster.slots.items():
            if not slot.is_alive():
                dead.append('slot {}'.format(slot_id))
        assert not dead, 'cluster not healthy: {}'.format(dead)

        node = self.cluster.nodes[1]
        url = 'http://{}:{}/counters'.format(node.host, node.mon_port)
        response = requests.get(url, timeout=15)
        assert response.status_code == 200, (
            'mon is not responding: {} status={}'.format(url, response.status_code)
        )

    def wait_healthy(self, timeout_seconds=60):
        """Restart dead processes, then wait until ``assert_healthy`` succeeds."""
        import time

        self.recover()
        deadline = time.time() + timeout_seconds
        last_error = None
        while time.time() < deadline:
            try:
                self.assert_healthy()
                return
            except AssertionError as e:
                last_error = e
            time.sleep(1)
        raise AssertionError(
            'cluster did not become healthy within {}s: {}'.format(
                timeout_seconds, last_error
            )
        )

    def _start_nbs(self):
        logger.info('Creating NBS database: %s', self.nbs_database_name)
        self.cluster.create_database(
            self.nbs_database_name, storage_pool_units_count={'hdd': 9}
        )
        logger.info('Registering and starting NBS dynamic slots...')
        slots = self.cluster.register_and_start_slots(self.nbs_database_name, count=1)
        logger.info('Waiting for NBS tenant to be up...')
        self.cluster.wait_tenant_up(self.nbs_database_name)
        logger.info('NBS tenant is ready')
        return slots

    def _create_ddisk_pool(self):
        define_ddisk_pool = '''
            Command {{
                DefineDDiskPool {{
                    BoxId: 1
                    Name: "{ddisk_pool_name}"
                    Geometry {{
                        NumFailRealms: 1
                        NumFailDomainsPerFailRealm: 5
                        NumVDisksPerFailDomain: 1
                        RealmLevelBegin: 10
                        RealmLevelEnd: 10
                        DomainLevelBegin: 10
                        DomainLevelEnd: 40
                    }}
                    PDiskFilter {{
                        Property {{
                            Type: ROT
                        }}
                    }}
                    NumDDiskGroups: 10
                }}
            }}
        '''.format(ddisk_pool_name=self.ddisk_pool_name)
        execute_ydbd(
            self.cluster,
            'token',
            ['admin', 'bs', 'config', 'invoke', '--proto', define_ddisk_pool],
        )


def _run_nbs_cluster(use_in_memory_pdisks):
    handle = NbsCluster(use_in_memory_pdisks=use_in_memory_pdisks)
    handle.start()
    try:
        yield handle
    finally:
        handle.stop()


@pytest.fixture(scope='session')
def nbs_cluster():
    """One in-memory-PDisk cluster for the whole suite."""
    yield from _run_nbs_cluster(use_in_memory_pdisks=True)


@pytest.fixture(scope='session')
def nbs_cluster_file_pdisks():
    """One file-backed-PDisk cluster so node.stop/start keeps disk contents.

    F2/F3 host-loss cases share this cluster; in-memory SectorMap would
    wipe a stopped node's PDisk and poison later cases.
    """
    yield from _run_nbs_cluster(use_in_memory_pdisks=False)
