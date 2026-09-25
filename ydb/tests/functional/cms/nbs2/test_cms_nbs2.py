"""Process-level CMS/NBS2 test; only partition -> DBSC map publication is bypassed."""

from collections import defaultdict
from itertools import combinations
import time

import grpc
import pytest

from ydb.core.nbs.cloud.blockstore.libs.storage.dbs_controller.protos import (
    dbs_controller_db_pb2,
    dbs_controller_pb2,
)
from ydb.public.api.grpc.draft import ydb_maintenance_v1_pb2_grpc
from ydb.public.api.protos import ydb_value_pb2
from ydb.public.api.protos.draft import ydb_maintenance_pb2 as maintenance
from ydb.public.api.protos.draft.ydb_tablet_pb2 import ExecuteTabletMiniKQLRequest
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.functional.nbs.lib.common import NbsTestBase
from ydb.tests.functional.nbs.lib.fixtures.mon import (
    merge_hosts_with_connections,
    parse_dbg_connections,
    parse_dbg_hosts,
)
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.util import LogLevels


# MakeDbsControllerID(): MakeTabletID(false, 0x2004), the default domain.
DBSC_TABLET_ID = (1 << 56) | 0x2004
# An explicit ticket supplies the UserToken CMS needs to identify the task owner.
ADMIN_METADATA = (('x-ydb-auth-ticket', 'root@builtin'), ('x-ydb-database', '/Root'))


class TestCmsNbs2Maintenance(NbsTestBase):
    @pytest.fixture(autouse=True)
    def setup(self):
        database = '/Root/NBS'
        self.cluster = KiKiMR(KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            enable_nbs=True,
            nbs_database_name=database,
            extra_feature_flags=['enable_cms_nbs2_maintenance_checks'],
            extra_grpc_services=['maintenance'],
            cms_config={
                'cluster_limits': {'disabled_nodes_ratio_limit': 0},
                'tenant_limits': {'disabled_nodes_ratio_limit': 0},
                'sentinel_config': {'enable': False},
            },
            additional_log_configs={
                'CMS': LogLevels.DEBUG,
                'DBS_CONTROLLER': LogLevels.DEBUG,
            },
        ))
        try:
            self.cluster.start()
            self.start_nbs(database)
            self.create_ddisk_pool()
            node = self.cluster.nodes[1]
            with grpc.insecure_channel(f'{node.host}:{node.grpc_port}') as channel:
                self.cms = ydb_maintenance_v1_pb2_grpc.MaintenanceServiceStub(channel)
                yield
        finally:
            self.cluster.stop()

    def _read_placement(self, tablet_id):
        """Snapshot actual DDisk/PBuffer ids, including every DBG of the partition."""
        groups = {}

        def ready():
            groups.clear()
            listing = self.fetch_partition_dbg_page(tablet_id, allow_missing=True)
            indexes = self.parse_dbg_indexes(listing)
            assert indexes, f'Partition {tablet_id} has no DBG links yet'
            for index in indexes:
                html = self.fetch_partition_dbg_page(tablet_id, index)
                hosts = merge_hosts_with_connections(parse_dbg_hosts(html), parse_dbg_connections(html))
                assert len(hosts) == 5, f'DBG {index}: expected five logical hosts, got {hosts}'
                group = dbs_controller_pb2.TDirectBlockGroupDDisks()
                for host in sorted(hosts, key=lambda host: host.index):
                    assert host.health == 'Online', f'DBG {index}: {host}'
                    assert host.ddisk_id and host.pbuffer_id, f'DBG {index}: {host}'
                    disks = group.DDiskIds.add(Health=dbs_controller_pb2.ONLINE)
                    for target, slot_id in ((disks.DDisk, host.ddisk_id),
                                            (disks.PersistentBuffer, host.pbuffer_id)):
                        target.NodeId, target.PDiskId, target.DDiskSlotId = map(int, slot_id.split(':'))
                groups[index] = group
            return True

        self.wait_until(ready, description='healthy partition placement')
        return groups

    def _seed_dbsc_map(self, tablet_id, groups):
        """Test-only seeding until partition publication is implemented.
        Writes both DBSC indexes, read directly by maintenance checks without caching.
        Bypasses TEvUpdateDDiskMapRequest, publication retries and health updates.
        """
        request = ExecuteTabletMiniKQLRequest(tablet_id=DBSC_TABLET_ID)
        updates = []
        inverse = defaultdict(set)

        def proto_parameter(name, message):
            value = request.parameters[name]
            value.type.type_id = ydb_value_pb2.Type.STRING
            value.value.bytes_value = message.SerializeToString()
            return f"(Parameter '{name} (DataType 'String))"

        for index, group in groups.items():
            data = proto_parameter(f'direct_{index}', group)
            updates.append(f"""
                (UpdateRow 'DirectMap
                    '('('TabletId (Uint64 '{tablet_id}))
                      '('DirectBlockGroupIndex (Uint64 '{index})))
                    '('('DDisks {data})
                      '('LogicalNodesCount (Uint64 '{len(group.DDiskIds)}))))
            """)
            for disks in group.DDiskIds:
                for slot in (disks.DDisk, disks.PersistentBuffer):
                    inverse[slot.NodeId, slot.PDiskId, slot.DDiskSlotId].add(index)

        for number, ((node, pdisk, slot), indexes) in enumerate(sorted(inverse.items())):
            record = dbs_controller_db_pb2.TDDiskDirectBlockGroups()
            record.PartitionDirectBlockGroups.add(
                PartitionTabletId=int(tablet_id), DirectBlockGroupIndex=sorted(indexes))
            data = proto_parameter(f'inverse_{number}', record)
            updates.append(f"""
                (UpdateRow 'InverseMap
                    '('('NodeId (Uint32 '{node}))
                      '('PDiskId (Uint32 '{pdisk}))
                      '('DDiskSlotId (Uint32 '{slot})))
                    '('('DirectBlockGroups {data})))
            """)
        # One transaction, so a check can never observe just one of the indexes.
        request.program = '(\n(return (AsList\n' + '\n'.join(updates) + '\n))\n)'

        def seed():
            response = self.cluster.client.tablet_service.ExecuteTabletMiniKQL(
                request, metadata=ADMIN_METADATA, timeout=30)
            assert response.status == StatusIds.SUCCESS, response
            return True

        self.wait_until(seed, description='DBSC schema ready and placement seeded')

    @staticmethod
    def _pick_nodes(groups):
        # DDisk/PBuffer may be on different nodes. Pick nodes affecting <=1 host per DBG
        # individually, but two distinct hosts in at least one DBG together.
        affected = defaultdict(set)
        for index, group in groups.items():
            for host, disks in enumerate(group.DDiskIds):
                for node in (disks.DDisk.NodeId, disks.PersistentBuffer.NodeId):
                    affected[node].add((index, host))
        eligible = [node for node, hosts in affected.items()
                    if len(hosts) == len({index for index, _ in hosts})]
        for first, second in combinations(sorted(eligible), 2):
            hosts = affected[first] | affected[second]
            if len(hosts) > len({index for index, _ in hosts}):
                return first, second
        raise AssertionError(f'No suitable pair of nodes in real placement: {groups}')

    @staticmethod
    def _result(response, result_type):
        operation = response.operation
        assert operation.ready and operation.status == StatusIds.SUCCESS, response
        result = result_type()
        assert operation.result.Unpack(result), response
        return result

    def _create(self, uid, nodes, dry_run=False, max_inflight=0):
        request = maintenance.CreateMaintenanceTaskRequest()
        request.task_options.task_uid = uid
        request.task_options.description = 'NBS2 functional test with seeded DBSC map'
        # FORCE bypasses VDisk/state-storage availability checks, but must still consult DBSC.
        request.task_options.availability_mode = maintenance.AVAILABILITY_MODE_FORCE
        request.task_options.dry_run = dry_run
        request.task_options.max_inflight_actions = max_inflight
        for node in nodes:
            lock = request.action_groups.add().actions.add().lock_action
            lock.scope.node_id = node
            lock.duration.seconds = 600
        # Return the full response: dry-run denial is reported in operation.issues.
        return self.cms.CreateMaintenanceTask(request, metadata=ADMIN_METADATA, timeout=60)

    def _refresh(self, uid):
        response = self.cms.RefreshMaintenanceTask(
            maintenance.RefreshMaintenanceTaskRequest(task_uid=uid), metadata=ADMIN_METADATA, timeout=60)
        return self._result(response, maintenance.MaintenanceTaskResult)

    @staticmethod
    def _check_actions(result, nodes, performed, partition_id=None):
        actions = [action for group in result.action_group_states for action in group.action_states]
        assert len(actions) == len(nodes), result
        assert sorted(action.action.lock_action.scope.node_id for action in actions) == sorted(nodes), result
        expected = (maintenance.ActionState.ACTION_STATUS_PERFORMED if performed
                    else maintenance.ActionState.ACTION_STATUS_PENDING)
        for action in actions:
            assert action.status == expected, result
            if performed:
                assert action.deadline.seconds > time.time(), result
            else:
                assert 'DBSController denied node maintenance' in action.details, result
                assert f'BlockingPartitionIds: {partition_id}' in action.details, result
        return actions

    def _complete(self, action):
        response = self.cms.CompleteAction(
            maintenance.CompleteActionRequest(action_uids=[action.action_uid]), metadata=ADMIN_METADATA, timeout=60)
        result = self._result(response, maintenance.ManageActionResult)
        assert len(result.action_statuses) == 1, result
        assert result.action_statuses[0].action_uid == action.action_uid, result
        assert result.action_statuses[0].status == StatusIds.SUCCESS, result

    def test_locks_and_refresh_with_seeded_dbsc_map(self):
        disk_id = self.generate_disk_id()
        tablet_id = self.create_disk(disk_id)
        try:
            actor = self.get_load_actor_adapter_actor_id(disk_id)
            payload = 'cms-nbs2-real-disk'
            self.write(actor, 0, payload)
            assert self.read(actor, 0).startswith(payload)

            groups = self._read_placement(tablet_id)
            first, second = self._pick_nodes(groups)
            self._seed_dbsc_map(tablet_id, groups)

            # An empty map or a disabled integration would grant at least one action.
            # Even a CMS quota of one must not narrow the batch sent to DBSC.
            dry_run_uid = 'nbs2-batch-dry-run'
            response = self._create(dry_run_uid, [first, second], dry_run=True, max_inflight=1)
            batch = self._result(response, maintenance.MaintenanceTaskResult)
            assert batch.task_uid == dry_run_uid, response
            assert not batch.action_group_states, response
            assert batch.HasField('retry_after'), response
            assert any(
                'DBSController denied node maintenance' in issue.message
                and f'BlockingPartitionIds: {tablet_id}' in issue.message
                for issue in response.operation.issues
            ), response
            stored = self.cms.GetMaintenanceTask(
                maintenance.GetMaintenanceTaskRequest(task_uid=dry_run_uid), metadata=ADMIN_METADATA, timeout=60)
            assert stored.operation.ready and stored.operation.status == StatusIds.BAD_REQUEST, stored

            granted = self._result(self._create('nbs2-first', [first]), maintenance.MaintenanceTaskResult)
            first_action, = self._check_actions(granted, [first], True)

            # Both nodes are physically UP: only the CMS lock makes the pair unsafe.
            pending = self._result(self._create('nbs2-second', [second]), maintenance.MaintenanceTaskResult)
            pending_action, = self._check_actions(pending, [second], False, tablet_id)
            self._check_actions(self._refresh('nbs2-second'), [second], False, tablet_id)

            self._complete(first_action)
            refreshed = self._refresh('nbs2-second')
            second_action, = self._check_actions(refreshed, [second], True)
            # PENDING actions have no UID; granting a permission assigns one.
            assert refreshed.task_uid == pending.task_uid == 'nbs2-second', refreshed
            assert second_action.action == pending_action.action, refreshed
            assert second_action.action_uid.task_uid == refreshed.task_uid, refreshed
            assert second_action.action_uid.action_id, refreshed

            assert self.read(actor, 0).startswith(payload)
            self._complete(second_action)
        finally:
            self.delete_disk(disk_id)
