from collections import Counter
import logging
import time
import unittest

from ydb.core.protos import blobstorage_base_pb2, blobstorage_config_pb2, msgbus_pb2
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.common.wait_for import retry_assertions
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.functional.mirror3dc.kv_workload import run_kv_workload
from ydb.tests.functional.mirror3dc.write2x2 import scan_write2x2


logger = logging.getLogger(__name__)


class Mirror3dcTestBase(unittest.TestCase):
    USE_IN_MEMORY_PDISKS = True
    ADDITIONAL_LOG_CONFIGS = None
    WRITERS = 3
    PARTITIONS = 3
    WRITES_PER_THREAD = 100
    INITIAL_WRITES = WRITERS * WRITES_PER_THREAD
    REALM_STATES = blobstorage_base_pb2.TGroupInfo.TFailRealm
    DC_STATES = blobstorage_config_pb2.TDefineDataCenterStates.TDataCenter

    def setUp(self):
        super().setUp()
        config = KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            nodes=9,
            additional_log_configs=self.ADDITIONAL_LOG_CONFIGS,
            use_in_memory_pdisks=self.USE_IN_MEMORY_PDISKS,
            dynamic_storage_pools=[dict(
                name='mirror3dc', kind='hdd', pdisk_user_kind=0, num_groups=1,
            )],
        )
        self.cluster = KiKiMR(config)
        self.addCleanup(self.cluster.stop)
        self.cluster.start()
        self.pool, = self.cluster.client.read_storage_pools()
        self.assertEqual(self.pool.Name, 'mirror3dc')
        self.assertEqual(self.pool.ErasureSpecies, 'mirror-3-dc')
        group = retry_assertions(self.check_group_ready)
        self.group_id = group.GroupId
        self.data_centers = tuple(sorted(realm.DataCenter for realm in group.FailRealms))
        base = self.bsc_client.query_base_config().BaseConfig
        self.node_data_centers = {node.NodeId: node.Location.DataCenter for node in base.Node}
        self.excluded_dcs = set()
        self.path = '/Root/blobs'
        response = self.cluster.kv_client.create_tablets(self.PARTITIONS, self.path, binded_channels=['hdd'] * 3)
        self.assertEqual(response.operation.status, StatusIds.SUCCESS, str(response))

    @property
    def bsc_client(self):
        return self.cluster.client

    def get_group(self):
        base = self.bsc_client.query_base_config().BaseConfig
        group, = [g for g in base.Group if g.StoragePoolId == self.pool.StoragePoolId]
        return group

    def check_group_ready(self):
        base = self.bsc_client.query_base_config().BaseConfig
        group, = [g for g in base.Group if g.StoragePoolId == self.pool.StoragePoolId]
        self.assertEqual(group.ErasureSpecies, 'mirror-3-dc')
        self.assertEqual(len(group.VSlotId), 9)
        node_ids = {slot.NodeId for slot in group.VSlotId}
        self.assertEqual(len(node_ids), 9)
        locations = {node.NodeId: node.Location.DataCenter for node in base.Node}
        dc_counts = Counter(locations[node_id] for node_id in node_ids)
        self.assertEqual(sorted(dc_counts.values()), [3, 3, 3])
        slots = [slot for slot in base.VSlot if slot.GroupId == group.GroupId]
        self.assertEqual(len(slots), 9)
        self.assertTrue(all(slot.Status == 'READY' for slot in slots), str(slots))
        self.assertEqual(len(group.FailRealms), 3, str(group))
        self.assertEqual(len({realm.DataCenter for realm in group.FailRealms}), 3, str(group))
        self.assertTrue(all(realm.DataCenter for realm in group.FailRealms), str(group))
        return group

    @property
    def recovery_node_ids(self):
        return [node_id for node_id, dc in self.node_data_centers.items() if dc not in self.excluded_dcs]

    @property
    def viewer_url(self):
        # Use a node outside the first DC even for the pre-exclusion snapshot.
        node_id = next(node_id for node_id in self.recovery_node_ids
                       if self.node_data_centers[node_id] != self.data_centers[0])
        return f'http://localhost:{self.cluster.nodes[node_id].mon_port}'

    def define_dc_state(self, dc, state, *, check_generation=False):
        before = self.get_group() if check_generation else None
        excluded = self.excluded_dcs.copy()
        if state == self.DC_STATES.EXCLUDED:
            excluded.add(dc)
        elif state == self.DC_STATES.INCLUDED:
            excluded.discard(dc)
        else:
            raise ValueError(f'Unsupported DC state: {state}')
        request = msgbus_pb2.TBlobStorageConfigRequest(Domain=1)
        command = request.Request.Command.add().DefineDataCenterStates
        # BSC replaces the entire policy. Preserve the excluded DCs; omitted
        # DCs are included by default. Send INCLUDED explicitly for a returning DC.
        for name in sorted(excluded | {dc}):
            command.DataCenter.add(
                Name=name, State=self.DC_STATES.EXCLUDED if name in excluded else self.DC_STATES.INCLUDED,
            )
        logger.info('DefineDataCenterStates: %s', command)
        response = self.bsc_client.send(request, 'BlobStorageConfig').BlobStorageConfigResponse
        self.assertTrue(response.Success, str(response))
        self.assertEqual(len(response.Status), 1, str(response))
        self.assertTrue(response.Status[0].Success, str(response))
        self.excluded_dcs = excluded
        if check_generation:
            self.assertEqual(self.get_group().GroupGeneration, before.GroupGeneration + 1)

    def recovered_realm_states(self):
        return {dc: self.REALM_STATES.F if dc in self.excluded_dcs else self.REALM_STATES.N2
                for dc in self.data_centers}

    def check_realm_states(self, expected):
        group = self.get_group()
        realms = group.FailRealms
        logger.info('Group %s generation %s FailRealms: %s', self.group_id, group.GroupGeneration,
                    [(realm.DataCenter, self.REALM_STATES.EState.Name(realm.State)) for realm in realms])
        self.assertEqual(len(realms), 3, str(group))
        self.assertEqual({realm.DataCenter for realm in realms}, set(self.data_centers), str(group))
        self.assertEqual({realm.DataCenter: realm.State for realm in realms}, expected, str(group))

    def wait_for_realm_states(self, expected):
        retry_assertions(lambda: self.check_realm_states(expected), timeout_seconds=240, step_seconds=1)

    def run_workload(self, before_action, background_action):
        return run_kv_workload(
            endpoints=[node.endpoint for node in self.cluster.nodes.values()],
            path=self.path,
            writers=self.WRITERS,
            writes_per_thread=self.WRITES_PER_THREAD,
            before_background_action=before_action,
            background_action=background_action,
        )

    def snapshot(self):
        return scan_write2x2(self.viewer_url, group_ids=[self.group_id], timeout=30)

    @staticmethod
    def kv_blobs(layouts):
        return {blob: layout for blob, layout in layouts.items() if blob[1].split(':')[3] == '2'}

    @staticmethod
    def copy_counts(layout):
        return tuple(sum(realm == r for realm, _ in layout) for r in range(3))

    def check_blob_layouts(self, blobs, *, copies_per_dc=2, dc_count=2):
        for blob, layout in blobs.items():
            self.assertGreaterEqual(sum(n >= copies_per_dc for n in self.copy_counts(layout)), dc_count,
                                    f'Blob {blob} has no {copies_per_dc} copies in {dc_count} DCs: {layout}')

    def wait_for_automatic_recovery(self, _pause_writes=None):
        time.sleep(1)
        self.wait_for_realm_states(self.recovered_realm_states())

    def run_automatic_recovery(self, dc):
        # Stop the workload after N2/N2/F, then inspect a snapshot without issuing
        # any external restoration requests.
        written = self.run_workload(
            lambda: self.define_dc_state(dc, self.DC_STATES.EXCLUDED), self.wait_for_automatic_recovery,
        )
        self.check_realm_states(self.recovered_realm_states())
        _, layouts = self.snapshot()
        blobs = self.kv_blobs(layouts)
        self.assertGreater(written, self.INITIAL_WRITES, 'No writes completed while the DC was excluded')
        self.assertEqual(len(blobs), written, 'Snapshot must contain every acknowledged KV write')
        self.check_blob_layouts(blobs)
        distribution = Counter(tuple(sorted(self.copy_counts(layout))) for layout in blobs.values())
        logger.info('Automatic recovery: %d KV blobs (%d old, %d new), layouts %s',
                    len(blobs), self.INITIAL_WRITES, written - self.INITIAL_WRITES, dict(distribution))

    def check_blob_layouts_under_load(self):
        # VDisks take snapshots independently. A live snapshot may contain
        # in-flight writes, so check its fixed set of BlobIds in a fresh snapshot
        # while allowing the writers to continue producing new blobs.
        _, layouts = self.snapshot()
        blobs = set(self.kv_blobs(layouts))
        self.assertGreater(len(blobs), self.INITIAL_WRITES, 'No new KV blobs after DC exclusion')

        def check_snapshot():
            _, current = self.snapshot()
            self.assertFalse(blobs - current.keys(), 'KV blobs disappeared between snapshots')
            selected = {blob: current[blob] for blob in blobs}
            self.check_blob_layouts(selected)
            return selected

        checked = retry_assertions(check_snapshot, timeout_seconds=120, step_seconds=1)
        distribution = Counter(tuple(sorted(self.copy_counts(layout))) for layout in checked.values())
        logger.info('Under continuous load: %d KV blobs checked for 2+2, layouts %s',
                    len(checked), dict(distribution))
