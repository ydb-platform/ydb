from collections import Counter
import logging
import time
import unittest

from ydb.core.protos import blobstorage_config_pb2, msgbus_pb2
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.common.wait_for import retry_assertions
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.functional.mirror3dc.kv_workload import run_kv_workload
from ydb.tests.functional.mirror3dc.write2x2 import restore_write2x2, scan_write2x2


logger = logging.getLogger(__name__)


class TestMirror3dc(unittest.TestCase):
    def setUp(self):
        config = KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            nodes=9,
            dynamic_storage_pools=[dict(
                name='mirror3dc', kind='hdd', pdisk_user_kind=0, num_groups=1,
            )],
        )
        self.cluster = KiKiMR(config)
        self.addCleanup(self.cluster.stop)
        self.cluster.start()

    def test_exclude_one_dc(self):
        pool, = self.cluster.client.read_storage_pools()
        self.assertEqual(pool.Name, 'mirror3dc')
        self.assertEqual(pool.ErasureSpecies, 'mirror-3-dc')

        def check_group():
            base = self.cluster.client.query_base_config().BaseConfig
            group, = [g for g in base.Group if g.StoragePoolId == pool.StoragePoolId]
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

        retry_assertions(check_group)

        path = '/Root/blobs'
        response = self.cluster.kv_client.create_tablets(3, path, binded_channels=['hdd'] * 3)
        self.assertEqual(response.operation.status, StatusIds.SUCCESS, str(response))

        base = self.cluster.client.query_base_config().BaseConfig
        dc = min(node.Location.DataCenter for node in base.Node)
        self.assertEqual(sum(node.Location.DataCenter == dc for node in base.Node), 3)
        group, = [g for g in base.Group if g.StoragePoolId == pool.StoragePoolId]
        recovery_node_ids = [node.NodeId for node in base.Node if node.Location.DataCenter != dc]
        recovery_node = self.cluster.nodes[recovery_node_ids[0]]
        viewer_url = f'http://localhost:{recovery_node.mon_port}'

        baseline = {}

        def counts(layout):
            return tuple(sum(realm == r for realm, _ in layout) for r in range(3))

        def define_dc_state(state):
            before = self.cluster.client.query_base_config().BaseConfig
            group, = [g for g in before.Group if g.StoragePoolId == pool.StoragePoolId]
            request = msgbus_pb2.TBlobStorageConfigRequest(Domain=1)
            command = request.Request.Command.add().DefineDataCenterStates
            command.DataCenter.add(Name=dc, State=state)
            logger.info('DefineDataCenterStates: %s', command)
            response = self.cluster.client.send(request, 'BlobStorageConfig').BlobStorageConfigResponse
            self.assertTrue(response.Success, str(response))
            self.assertEqual(len(response.Status), 1, str(response))
            self.assertTrue(response.Status[0].Success, str(response))
            after = self.cluster.client.query_base_config().BaseConfig
            updated, = [g for g in after.Group if g.GroupId == group.GroupId]
            self.assertEqual(updated.GroupGeneration, group.GroupGeneration + 1)

        def snapshot_and_exclude_dc():
            # Writers have finished their initial batch and wait until this
            # callback returns. Readers may continue validating those writes.
            _, layouts = scan_write2x2(
                viewer_url,
                group_ids=[group.GroupId], timeout=30,
            )
            baseline.update(layouts)
            self.assertTrue(baseline)
            kv_blobs = {blob: layout for blob, layout in baseline.items() if blob[1].split(':')[3] == '2'}
            self.assertEqual(len(kv_blobs), 300)  # 3 writers * 100 writes.
            logger.info('Before DC exclusion: %d total blobs, %d KV blobs, layouts %s',
                        len(baseline), len(kv_blobs), dict(Counter(counts(v) for v in baseline.values())))
            logger.info('Before DC exclusion: KV layouts %s', dict(Counter(counts(v) for v in kv_blobs.values())))
            for blob, layout in kv_blobs.items():
                # Extra handoff copies are possible even before excluding a DC.
                self.assertTrue(all(n >= 1 for n in counts(layout)), f'No 1+1+1 coverage: {blob}: {layout}')
            states = blobstorage_config_pb2.TDefineDataCenterStates.TDataCenter
            try:
                define_dc_state(states.EXCLUDED)
            except Exception:
                define_dc_state(states.INCLUDED)
                raise

        def exclude_dc(pause_writes):
            states = blobstorage_config_pb2.TDefineDataCenterStates.TDataCenter
            try:
                logger.info('DC %s is excluded; keeping KV workload running for 5 seconds', dc)
                time.sleep(5)
                with pause_writes() as written:
                    candidates, excluded = scan_write2x2(
                        viewer_url,
                        group_ids=[group.GroupId], timeout=30,
                    )
                    new_blobs = {blob: layout for blob, layout in excluded.items()
                                 if blob not in baseline and blob[1].split(':')[3] == '2'}
                    self.assertGreater(len(new_blobs), 0, 'No new KV blobs after DC exclusion')
                    self.assertEqual(len(new_blobs), written - 300)
                    new_candidates = candidates - baseline.keys()
                    self.assertFalse(new_candidates,
                                     f'Recovery candidates absent before DC exclusion: {sorted(new_candidates)}')
                    self.assertFalse(candidates & new_blobs.keys(),
                                     'New KV blobs must not be recovery candidates')
                    logger.info('Recovery candidates before restore: %d old blobs, %d new blobs',
                                len(candidates), len(new_candidates))
                    logger.info('New KV blobs before restore: %d, layouts %s', len(new_blobs),
                                dict(Counter(counts(layout) for layout in new_blobs.values())))
                    for blob, layout in sorted(new_blobs.items()):
                        self.assertGreaterEqual(sum(n >= 2 for n in counts(layout)), 2,
                                                f'New KV blob has no 2+2 layout before restore: {blob}: {layout}')
                logger.info('Restoring write2x2 layout while DC %s is excluded', dc)
                restored = restore_write2x2(
                    endpoint=viewer_url,
                    viewer_url=viewer_url,
                    group_ids=[group.GroupId],
                    node_ids=recovery_node_ids,
                    attempts=3,
                    retry_interval=1,
                    timeout=30,
                )
                for blob, layout in sorted(baseline.items()):
                    logger.info('Baseline blob %s: before=%s excluded=%s restored=%s',
                                blob, sorted(layout), sorted(excluded.get(blob, set())),
                                sorted(restored.get(blob, set())))
                    if blob[1].split(':')[3] != '2':
                        continue  # Service blobs can be collected between snapshots.
                    self.assertIn(blob, excluded)
                    self.assertIn(blob, restored)
                    self.assertGreaterEqual(sum(n >= 2 for n in counts(restored[blob])), 2,
                                            f'Not restored to 2x2: {blob}: {restored[blob]}')
            finally:
                define_dc_state(states.INCLUDED)
                logger.info('DC %s is included again', dc)

        run_kv_workload(
            endpoints=[node.endpoint for node in self.cluster.nodes.values()],
            path=path,
            background_action=exclude_dc,
            before_background_action=snapshot_and_exclude_dc,
        )
        retry_assertions(check_group, timeout_seconds=120)
