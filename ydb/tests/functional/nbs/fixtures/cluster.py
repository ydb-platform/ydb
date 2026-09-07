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

from ydb.tests.library.common.types import Erasure, FailDomainType
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator, PDISK_SIZE
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.functional.nbs.lib.helpers import execute_ydbd

logger = logging.getLogger(__name__)

NBS_DATABASE_NAME = '/Root/NBS'
DDISK_POOL_NAME = 'ddp1'
# Kind passed to create_database. The harness default for this pool is
# erasure "none": one VDisk per group on one node. YAML ErasureNames has
# no "mirror-3"; mirror-3-dc is the 3-copy species that parses.
TENANT_POOL_KIND = 'hdd'
TENANT_ERASURE_SPECIES = 'mirror-3-dc'
TENANT_NODE_COUNT = 9

# Mirrors NPDisk::SmallDiskSizeBoundary / SmallDiskMaximumChunkSize.
# FormatPDisk falls back to a 32 MB chunk on any disk below the boundary.
SMALL_DISK_SIZE_BOUNDARY = 800 * (1 << 30)
SMALL_DISK_MAX_CHUNK_SIZE = 32 * (1 << 20)

# EPDiskType.SSD from ydb/core/protos/blobstorage_base3.proto.
# KiKiMR.__add_bs_box forwards pdisk_type to DefineHostConfig.Drive.Type.
EPDISK_TYPE_SSD = 1


class NbsCluster:
    """Running KiKiMR cluster with NBS enabled and one DDisk pool defined."""

    def __init__(self, use_in_memory_pdisks=True):
        self.cluster = None
        self.slots = []
        self.ddisk_pool_name = DDISK_POOL_NAME
        self.nbs_database_name = NBS_DATABASE_NAME
        # File-backed PDisks survive node.stop(); in-memory SectorMap does not.
        self.use_in_memory_pdisks = use_in_memory_pdisks
        # Set when a node or slot is restarted so recover() waits for the tenant.
        self._restarted = False

    def start(self):
        """Boot the cluster, create the NBS database and define the DDisk pool."""
        configurator = KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            enable_nbs=True,
            nbs_database_name=self.nbs_database_name,
            use_in_memory_pdisks=self.use_in_memory_pdisks,
            # Extra PDisk per node so the DDisk pool does not share the tenant's
            # ROT disk. node.stop / pdisk stop on a DBG host must not take
            # MIRROR_3_DC groups down with it.
            dynamic_pdisks=[{}],
            # --data-center so BSC can form 3 fail realms. Matches the static
            # group's DC cycle (nodes 1,4,7 / 2,5,8 / 3,6,9).
            dc_mapping={
                node_id: str(((node_id - 1) % 3) + 1)
                for node_id in range(1, TENANT_NODE_COUNT + 1)
            },
            additional_log_configs={
                'NBS_PARTITION': LogLevels.INFO,
                'NBS2_LOAD_TEST': LogLevels.DEBUG,
                'NBS_VOLUME': LogLevels.DEBUG,
                'NBS_SS_PROXY': LogLevels.DEBUG,
            },
        )
        last_pdisk_id = max(p['pdisk_id'] for p in configurator.pdisks_info)
        for pdisk in configurator.pdisks_info:
            if pdisk['pdisk_id'] == last_pdisk_id:
                pdisk['pdisk_type'] = EPDISK_TYPE_SSD
                logger.info(
                    'DDisk-pool PDisk node=%s pdisk=%s type=SSD path=%s',
                    pdisk['node_id'],
                    pdisk['pdisk_id'],
                    pdisk['pdisk_path'],
                )
        # TOracleConfig defaults (10s / 10 errors) make every host-loss case wait
        # ~15s for a state the mon page only reports after Think fires.
        # min_errors=3: a single transient error must not demote a healthy host.
        configurator.yaml_config['nbs_config']['nbs_storage_config']['oracle_config'] = {
            'thinking_interval': 200,
            'max_duration_before_going_temporary_offline': 1000,
            'max_duration_before_going_offline': 2000,
            'min_errors_count_before_going_offline': 3,
        }
        # KikimrConfigGenerator picks vchunk_size from the PDisk backing, but the
        # chunk size follows the PDisk size: FormatPDisk falls back to a 32 MB chunk
        # on any disk below SmallDiskSizeBoundary, so the file-backed branch's 128 MB
        # is wrong at the harness default of 64 GB. A vchunk larger than the chunk
        # makes TDDiskDataCopier read past the chunk end and retry forever.
        if PDISK_SIZE < SMALL_DISK_SIZE_BOUNDARY:
            configurator.yaml_config['nbs_config']['nbs_storage_config']['vchunk_size'] = (
                SMALL_DISK_MAX_CHUNK_SIZE
            )
        # The harness default for every dynamic pool kind is erasure "none": one
        # VDisk per group on one node. Stopping a static node then kills whichever
        # tenant groups live there, and any NBS tablet whose channels landed in one
        # gets cancelled with DSPE4 and reboot-loops until the node is back. F2/F3
        # stop nodes on purpose, so the tenant must survive it.
        # DomainLevelEnd includes the PDisk id so the 3 nodes in one DC are
        # distinct fail domains (default levels 10/20/10/40 would collapse them).
        for domain in configurator.yaml_config['domains_config']['domain']:
            for pool_type in domain['storage_pool_types']:
                if pool_type['kind'] == TENANT_POOL_KIND:
                    pool_type['pool_config']['erasure_species'] = TENANT_ERASURE_SPECIES
                    pool_type['pool_config']['geometry'] = {
                        'realm_level_begin': int(FailDomainType.DC),
                        'realm_level_end': int(FailDomainType.Room),
                        'domain_level_begin': int(FailDomainType.DC),
                        'domain_level_end': int(FailDomainType.Disk) + 1,
                    }
        self.cluster = KiKiMR(configurator)
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
                self._restart_dead_process(node, 'node {}'.format(node_id))
                restarted = True
        for slot_id, slot in self.cluster.slots.items():
            if not slot.is_alive():
                self._restart_dead_process(slot, 'slot {}'.format(slot_id))
                restarted = True
        if restarted:
            self._restarted = True
        if self._restarted:
            self.wait_tenant_ready()
            self._restarted = False

    def mark_restarted(self):
        """A node or slot was restarted; recover() must wait for the tenant."""
        self._restarted = True

    def recycle_nbs_slot(self):
        """Stop and start the NBS slot to clear a wedged tenant or tablet."""
        slots = list(self.cluster.slots.values())
        if not slots:
            return
        slot = slots[0]
        try:
            if slot.is_alive():
                logger.info('recycle: stop slot %s', slot.node_id)
                slot.stop()
        except Exception as e:
            logger.warning('recycle: stop slot failed: %s', e)
        if not slot.is_alive():
            self._restart_dead_process(slot, 'recycle slot {}'.format(slot.node_id))
            self.mark_restarted()
        self.wait_tenant_ready(timeout_seconds=20)

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

    def wait_tenant_ready(self, timeout_seconds=15):
        """Wait until the NBS tenant accepts gRPC. Best-effort, time-bounded.

        ``wait_tenant_up`` hides a 240s CMS poll that would eat the chunk after
        a host-loss case. ``_wait_tenant_usable`` is the gRPC check we need.
        """
        try:
            self.cluster._wait_tenant_usable(
                self.nbs_database_name, timeout_seconds=timeout_seconds
            )
        except AssertionError as e:
            logger.warning('tenant not usable in %ss: %s', timeout_seconds, e)

    def wait_healthy(self, timeout_seconds=60):
        """Restart dead processes, then wait until ``assert_healthy`` succeeds."""
        import time

        self.recover()
        # Always settle: undo may have started nodes that recover() saw as
        # already alive, so DDisk allocation is not ready for the next create.
        self.wait_tenant_ready()
        deadline = time.time() + timeout_seconds
        last_error = None
        while time.time() < deadline:
            try:
                self.assert_healthy()
                return
            except AssertionError as e:
                last_error = e
            self.recover()
            time.sleep(1)
        raise AssertionError(
            'cluster did not become healthy within {}s: {}'.format(
                timeout_seconds, last_error
            )
        )

    def _restart_dead_process(self, process, label):
        """Start a dead node or slot without truncating its previous ydbd log."""
        daemon = getattr(process, 'daemon', None)
        exit_code = getattr(daemon, 'exit_code', None) if daemon is not None else None
        logger.warning('restarting dead %s exit_code=%s', label, exit_code)
        process.set_log_file_prefix('logfile_restart_')
        process.start()

    def _start_nbs(self):
        logger.info('Creating NBS database: %s', self.nbs_database_name)
        self.cluster.create_database(
            self.nbs_database_name, storage_pool_units_count={TENANT_POOL_KIND: 3}
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
                            Type: SSD
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
