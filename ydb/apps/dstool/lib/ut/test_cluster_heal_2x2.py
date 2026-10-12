from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from ydb.apps.dstool.lib import commands
from ydb.apps.dstool.lib import dstool_cmd_cluster_heal_2x2 as heal
from ydb.apps.dstool.lib.arg_parser import ArgumentParser


class TestHeal2x2(unittest.TestCase):
    def setUp(self):
        self.base = heal.common.kikimr_bsconfig.TBaseConfig()
        self.pool = heal.common.kikimr_bsconfig.TDefineStoragePool(
            BoxId=1, StoragePoolId=1, Name='/Root:ssd', ErasureSpecies='mirror-3-dc')
        self.pool.PDiskFilter.add()
        self.group = self.base.Group.add(
            GroupId=0x80000001, GroupGeneration=7, BoxId=1, StoragePoolId=1, ErasureSpecies='mirror-3-dc')
        for realm in range(3):
            for domain in range(3):
                node_id = 1 + realm * 3 + domain
                self.add_node(node_id, ('from' if realm == 0 else 'other') + str(node_id), str(realm), str(domain))
                slot = self.base.VSlot.add(
                    GroupId=self.group.GroupId, GroupGeneration=7, FailRealmIdx=realm, FailDomainIdx=domain,
                    Status='READY' if realm == 2 else 'ERROR')
                slot.VSlotId.NodeId, slot.VSlotId.PDiskId, slot.VSlotId.VSlotId = node_id, 1, 100
                self.group.VSlotId.add().CopyFrom(slot.VSlotId)
        for index in range(3):
            self.add_node(20 + index, 'to' + str(index), 'new', str(index))
        self.snapshot = {'BaseConfig': self.base, 'StoragePools': [self.pool]}
        self.args = SimpleNamespace(from_prefix='from', dry_run=False,
                                    allow_unusable_pdisks=False, move_only_to_operational_pdisks=False,
                                    max_iterations=1, max_replicating_pdisks=None, time_between_reassignings=0,
                                    min_vdisk_uptime=0)

    def add_node(self, node_id, host, dc, rack):
        node = self.base.Node.add(NodeId=node_id, Connected=True)
        node.HostKey.Fqdn = host
        node.Location.DataCenter, node.Location.Rack = dc, rack
        self.base.PDisk.add(NodeId=node_id, PDiskId=1, BoxId=1, DriveStatus=heal.common.kikimr_bs3.ACTIVE,
                            ExpectedSlotCount=10)

    def sources(self):
        return heal.select_sources(self.group, heal.common.build_vslot_map(self.base),
                                   heal.common.build_node_fqdn_map(self.base), 'from')

    def response(self, request, base=None):
        response = heal.common.kikimr_bsconfig.TConfigResponse(
            Success=not request.Rollback, RollbackSuccess=request.Rollback)
        for command in request.Command:
            cmd = command.ReassignGroupDisk
            slot = next(slot for slot in (self.base if base is None else base).VSlot
                        if slot.GroupId == cmd.GroupId
                        and heal.coordinates(slot) == (cmd.FailRealmIdx, cmd.FailDomainIdx, cmd.VDiskIdx))
            item = response.Status.add(Success=True).ReassignedItem.add()
            item.From.CopyFrom(slot.VSlotId)
            item.To.NodeId, item.To.PDiskId, item.To.VSlotId = 20 + cmd.FailDomainIdx, 1, 101
        return response

    def run_command(self, pool_names=None):
        with TemporaryDirectory() as directory:
            path = Path(directory) / 'pools.txt'
            path.write_text('\n'.join(pool_names or [self.pool.Name]) + '\n', encoding='utf-8')
            self.args.storage_pools_file = str(path)
            with redirect_stdout(StringIO()) as output, redirect_stderr(StringIO()) as errors:
                heal.do(self.args)
            self.errors = errors.getvalue()
            return output.getvalue()

    def relocated_snapshot(self, statuses):
        snapshot = deepcopy(self.snapshot)
        base = snapshot['BaseConfig']
        group = base.Group[0]
        group.GroupGeneration = 8
        for slot in base.VSlot:
            slot.GroupGeneration = 8
        for index, status in enumerate(statuses):
            slot = base.VSlot[index]
            slot.Donors.add().VSlotId.CopyFrom(slot.VSlotId)
            slot.VSlotId.NodeId, slot.VSlotId.PDiskId, slot.VSlotId.VSlotId = 20 + index, 1, 101
            slot.Status = status
            slot.Ready = status == 'READY'
            slot.VDiskMetrics.Replicated = status == 'READY'
            slot.VDiskMetrics.State = heal.common.whiteboard_disk_states.EVDiskState.OK
            group.VSlotId[index].CopyFrom(slot.VSlotId)
        return snapshot

    def test_command_registration(self):
        parser = ArgumentParser()
        commands.make_command_map_by_structure(parser.add_subparsers(dest='global_command'))
        args = parser.parse_args(['cluster', 'heal-2x2', '--from', 'a', '--storage_pools_file', 'pools'])
        self.assertEqual((args.from_prefix, args.storage_pools_file), ('a', 'pools'))
        self.assertFalse(hasattr(args, 'to_prefix'))
        self.assertFalse(args.allow_unusable_pdisks or args.move_only_to_operational_pdisks)
        self.assertEqual(args.max_iterations, 0)
        self.assertIsNone(args.max_replicating_pdisks)
        self.assertEqual(args.time_between_reassignings, 0)
        self.assertEqual(args.min_vdisk_uptime, 0)

    def test_one_iteration_branches_on_current_ready_count(self):
        for node in range(23, 26):
            self.add_node(node, 'to%d' % node, 'new', str(node))
        for alive in [3, 4, 5, 6]:
            for dry_run in [False, True]:
                with self.subTest(alive=alive, dry_run=dry_run):
                    snapshot = self.relocated_snapshot(['REPLICATING'] * (6 - alive) + ['READY'] * (alive - 3))
                    self.args.dry_run = dry_run
                    self.args.max_replicating_pdisks = 0
                    sent = []

                    def invoke(request):
                        sent.append(deepcopy(request))
                        return self.response(request, snapshot['BaseConfig'])

                    with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=snapshot) as fetch, \
                            patch.object(heal.common, 'invoke_bsc_request', side_effect=invoke), \
                            patch.object(heal.time, 'sleep') as sleep:
                        self.run_command()
                    self.assertEqual(fetch.call_count, 1)
                    sleep.assert_not_called()
                    self.assertEqual(len(sent), 0 if alive == 6 else (1 if dry_run else 2))
                    for request in sent:
                        self.assertEqual(len(request.Command), 6 - alive)
                        self.assertFalse(request.IgnoreGroupLayoutChecks)
                        for command in request.Command:
                            cmd = command.ReassignGroupDisk
                            self.assertEqual(cmd.GroupGeneration, 8)
                            self.assertEqual(cmd.FailRealmIdx, 0)
                            self.assertLess(cmd.FailDomainIdx, 6 - alive)
                            self.assertFalse(cmd.HasField('TargetPDiskId'))

    def test_iterations_recheck_same_group_until_six_ready(self):
        for node in range(23, 26):
            self.add_node(node, 'to%d' % node, 'new', str(node))
        states = [self.snapshot]
        for alive in [4, 5, 6]:
            state = self.relocated_snapshot(['REPLICATING'] * (6 - alive) + ['READY'] * (alive - 3))
            state['BaseConfig'].Group[0].GroupGeneration = alive + 4
            states.append(state)
        self.args.max_iterations = 0
        current = None
        sent = []

        def fetch(**kwargs):
            nonlocal current
            current = states.pop(0)
            return current

        def invoke(request):
            sent.append(deepcopy(request))
            return self.response(request, current['BaseConfig'])

        with patch.object(heal.common, 'fetch_base_config_and_storage_pools', side_effect=fetch) as fetch_mock, \
                patch.object(heal.common, 'invoke_bsc_request', side_effect=invoke), \
                patch.object(heal.time, 'sleep') as sleep:
            self.run_command()
        self.assertEqual(fetch_mock.call_count, 4)
        self.assertEqual([len(request.Command) for request in sent], [3, 3, 2, 2, 1, 1])
        self.assertEqual([request.Command[0].ReassignGroupDisk.GroupGeneration for request in sent], [7, 7, 8, 8, 9, 9])
        sleep.assert_not_called()

    def test_unchanged_three_ready_repeats_triple_with_iteration_limit(self):
        for node in range(23, 26):
            self.add_node(node, 'to%d' % node, 'new', str(node))
        state = self.relocated_snapshot(['REPLICATING'] * 3)
        self.args.max_iterations = 2
        self.args.max_replicating_pdisks = 0
        with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=state) as fetch, \
                patch.object(heal.common, 'invoke_bsc_request',
                             side_effect=lambda request: self.response(request, state['BaseConfig'])) as invoke:
            self.run_command()
        self.assertEqual(fetch.call_count, 2)
        self.assertEqual(invoke.call_count, 4)
        self.assertTrue(all(len(call.args[0].Command) == 3 for call in invoke.call_args_list))

    def test_dry_run_checks_each_group_once_without_waiting_for_healing(self):
        self.add_second_group()
        self.args.dry_run = True
        self.args.max_iterations = 0
        with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=self.snapshot), \
                patch.object(heal.common, 'invoke_bsc_request', side_effect=self.response) as invoke:
            self.run_command()
        self.assertEqual(invoke.call_count, 2)
        self.assertEqual(len({call.args[0].Command[0].ReassignGroupDisk.GroupId for call in invoke.call_args_list}), 2)

    def test_partly_moved_realm_selected_without_previous_state(self):
        self.base.VSlot[0].Status = 'READY'
        self.base.Node[0].HostKey.Fqdn = 'to0'
        self.assertEqual([slot.FailDomainIdx for slot in self.sources()], [1, 2])

    def test_repeat_eviction_follows_donors_to_arbitrary_hosts(self):
        for alive in (3, 4, 5):
            with self.subTest(alive=alive):
                snapshot = self.relocated_snapshot(['REPLICATING'] * (6 - alive) + ['READY'] * (alive - 3))
                base = snapshot['BaseConfig']
                for node in base.Node:
                    if node.NodeId >= 20:
                        node.HostKey.Fqdn = 'bsc-chosen-%d' % node.NodeId
                for slot in base.VSlot:
                    if slot.Status == 'READY':
                        del slot.Donors[:]
                sources = heal.select_sources(base.Group[0], heal.common.build_vslot_map(base),
                                              heal.common.build_node_fqdn_map(base), 'from')
                self.assertEqual([slot.FailDomainIdx for slot in sources], list(range(6 - alive)))
                del base.VSlot[0].Donors[:]
                self.assertEqual(heal.select_sources(base.Group[0], heal.common.build_vslot_map(base),
                                                     heal.common.build_node_fqdn_map(base), 'from'), [])

    def test_repeat_eviction_within_source_prefix_bypasses_replication_limit(self):
        snapshot = self.relocated_snapshot(['REPLICATING'] * 3)
        for node in snapshot['BaseConfig'].Node:
            if node.NodeId >= 20:
                node.HostKey.Fqdn = 'from-new-%d' % node.NodeId
        self.args.max_replicating_pdisks = 0
        with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=snapshot), \
                patch.object(heal.common, 'invoke_bsc_request',
                             side_effect=lambda request: self.response(request, snapshot['BaseConfig'])) as invoke, \
                patch.object(heal.time, 'sleep') as sleep:
            self.run_command()
        self.assertEqual(invoke.call_count, 2)
        sleep.assert_not_called()

    def test_unrelated_failure_shape_is_not_selected(self):
        self.base.VSlot[0].Status = 'READY'
        self.base.VSlot[3].Status = 'READY'
        self.base.VSlot[8].Status = 'ERROR'
        self.assertEqual(self.sources(), [])

    def test_iteration_options_parse(self):
        parser = ArgumentParser()
        heal.add_options(parser)
        args = parser.parse_args(['--from', 'a', '--storage_pools_file', 'pools',
                                 '--max-iterations', '5', '--max-replicating-pdisks', '0',
                                  '--time-between-reassignings', '7', '--min-vdisk-uptime', '60'])
        self.assertEqual(args.max_iterations, 5)
        self.assertEqual(args.max_replicating_pdisks, 0)
        self.assertEqual(args.time_between_reassignings, 7)
        self.assertEqual(args.min_vdisk_uptime, 60)

    def uptime_counters(self, uptime=60):
        return {'sensors': [
            {'labels': {'sensor': 'SkeletonFrontUptimeSeconds', 'subsystem': 'timer',
                        'group': str(self.group.GroupId), 'pdisk': '000000001', 'orderNumber': '%02d' % index},
             'value': uptime}
            for index in range(9)
        ]}

    def test_uptime_checks_all_disks_including_ready(self):
        for index in range(9):
            with self.subTest(index=index):
                data = self.uptime_counters()
                data['sensors'][index]['value'] = 59
                with patch.object(heal.common, 'fetch', return_value=data) as fetch:
                    self.assertEqual(heal.uptime_wait(self.base, self.group, 60), 1)
                self.assertEqual(fetch.call_count, 9)
                for node, call in enumerate(fetch.call_args_list, 1):
                    self.assertEqual(call.args, ('node/%d/counters/counters=vdisks/json' % node,))
                    self.assertEqual(call.kwargs, {'cache': False})

    def test_uptime_boundary_and_other_group_counters(self):
        data = self.uptime_counters()
        unrelated = self.uptime_counters(0)
        for sensor in unrelated['sensors']:
            sensor['labels']['group'] = str(self.group.GroupId + 1)
        data['sensors'].extend(unrelated['sensors'])
        with patch.object(heal.common, 'fetch', return_value=data):
            self.assertEqual(heal.uptime_wait(self.base, self.group, 60), 0)

    def test_uptime_missing_for_failed_disks_and_disconnected_nodes(self):
        data = self.uptime_counters()
        data['sensors'] = data['sensors'][7:]
        self.base.Node[6].Connected = False
        with patch.object(heal.common, 'fetch', return_value=data) as fetch:
            self.assertEqual(heal.uptime_wait(self.base, self.group, 60), 0)
        self.assertEqual(fetch.call_count, 8)
        self.base.Node[6].Connected = True
        with patch.object(heal.common, 'fetch', return_value=data):
            self.assertEqual(heal.uptime_wait(self.base, self.group, 60), heal.WAITING_TIME)

    def test_young_group_does_not_delay_next_group_and_is_revisited(self):
        second = self.add_second_group()
        self.add_second_group()  # The untouched tail must not delay a repeat eviction.
        self.args.min_vdisk_uptime = 60
        self.args.max_iterations = 2
        sent = []

        def invoke(request):
            if not request.Rollback:
                sent.append(request.Command[0].ReassignGroupDisk.GroupId)
            return self.response(request)

        with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=self.snapshot) as fetch, \
                patch.object(heal, 'uptime_wait', side_effect=[5, 0, 0]) as uptime, \
                patch.object(heal.common, 'invoke_bsc_request', side_effect=invoke), \
                patch.object(heal.time, 'monotonic', return_value=100), \
                patch.object(heal.time, 'sleep') as sleep:
            self.run_command()
        self.assertEqual(sent, [second.GroupId, self.group.GroupId])
        self.assertEqual(fetch.call_count, 2)
        self.assertEqual([call.args[1].GroupId for call in uptime.call_args_list],
                         [self.group.GroupId, second.GroupId, self.group.GroupId])
        sleep.assert_not_called()

    def test_wait_for_uptime_only_when_no_group_can_be_reassigned(self):
        self.args.min_vdisk_uptime = 60
        self.args.max_iterations = 2
        with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=self.snapshot) as fetch, \
                patch.object(heal, 'uptime_wait', side_effect=[5, 0]), \
                patch.object(heal.common, 'invoke_bsc_request', side_effect=self.response) as invoke, \
                patch.object(heal.time, 'monotonic', return_value=100), \
                patch.object(heal.time, 'sleep') as sleep:
            self.run_command()
        self.assertEqual(fetch.call_count, 2)
        self.assertEqual(invoke.call_count, 2)
        sleep.assert_called_once_with(5)

    def test_young_group_dry_run_skips_without_waiting(self):
        self.args.min_vdisk_uptime = 60
        self.args.max_iterations = 0
        self.args.dry_run = True
        with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=self.snapshot), \
                patch.object(heal, 'uptime_wait', return_value=1), \
                patch.object(heal.common, 'invoke_bsc_request') as invoke, \
                patch.object(heal.time, 'sleep') as sleep:
            self.run_command()
        invoke.assert_not_called()
        sleep.assert_not_called()

    def test_uptime_fetch_failure_skips_group(self):
        self.args.min_vdisk_uptime = 60
        with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=self.snapshot), \
                patch.object(heal.common, 'fetch', side_effect=heal.common.ConnectionError('unreachable')), \
                patch.object(heal.common, 'invoke_bsc_request') as invoke:
            self.run_command()
        invoke.assert_not_called()
        self.assertIn('cannot read VDisk uptime', self.errors)

    def test_disabled_uptime_does_not_fetch_counters(self):
        with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=self.snapshot), \
                patch.object(heal.common, 'fetch') as fetch, \
                patch.object(heal.common, 'invoke_bsc_request', side_effect=self.response):
            self.run_command()
        fetch.assert_not_called()

    def add_second_group(self):
        group = self.base.Group.add()
        group.CopyFrom(self.group)
        group.GroupId = max(item.GroupId for item in self.base.Group) + 1
        del group.VSlotId[:]
        for original in [slot for slot in self.base.VSlot if slot.GroupId == self.group.GroupId]:
            slot = self.base.VSlot.add()
            slot.CopyFrom(original)
            slot.GroupId = group.GroupId
            slot.VSlotId.VSlotId += group.GroupId - self.group.GroupId
            group.VSlotId.add().CopyFrom(slot.VSlotId)
        return group

    def add_replicating_slot(self, base, node_id, status='REPLICATING'):
        # Replication outside the selected pool must also count towards the limit.
        slot = base.VSlot.add(GroupId=0x800000ff, Status=status)
        slot.VSlotId.NodeId, slot.VSlotId.PDiskId, slot.VSlotId.VSlotId = node_id, 1, 500 + len(base.VSlot)

    def test_pool_file_order_then_group_id_order(self):
        second_pool = deepcopy(self.pool)
        second_pool.StoragePoolId = 2
        second_pool.Name = '/Root:zpool'
        self.snapshot['StoragePools'].append(second_pool)
        second = self.add_second_group()
        second.StoragePoolId = 2
        third = self.add_second_group()
        third.StoragePoolId = 2
        excluded = self.add_second_group()
        excluded.StoragePoolId = 99
        self.args.max_iterations = 3

        def invoke_and_heal(request):
            response = self.response(request)
            if not request.Rollback:
                group_id = request.Command[0].ReassignGroupDisk.GroupId
                for slot in self.base.VSlot:
                    if slot.GroupId == group_id and slot.FailRealmIdx == 0:
                        slot.Status = 'READY'
            return response

        with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=self.snapshot), \
                patch.object(heal.common, 'invoke_bsc_request', side_effect=invoke_and_heal) as invoke:
            self.run_command([second_pool.Name, self.pool.Name, second_pool.Name])
        ids = [call.args[0].Command[0].ReassignGroupDisk.GroupId for call in invoke.call_args_list[::2]]
        self.assertEqual(ids, [second.GroupId, third.GroupId, self.group.GroupId])

    def test_each_iteration_starts_from_first_group(self):
        self.add_second_group()
        self.args.max_iterations = 3
        with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=self.snapshot), \
                patch.object(heal.common, 'invoke_bsc_request', side_effect=self.response) as invoke:
            self.run_command()
        ids = [call.args[0].Command[0].ReassignGroupDisk.GroupId for call in invoke.call_args_list[::2]]
        self.assertEqual(ids, [self.group.GroupId] * 3)

    def test_healed_group_skipped_before_next_candidate(self):
        second = self.add_second_group()
        for slot in self.base.VSlot[:3]:
            slot.Status = 'READY'
        with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=self.snapshot), \
                patch.object(heal.common, 'invoke_bsc_request', side_effect=self.response) as invoke:
            self.run_command()
        self.assertEqual(invoke.call_count, 2)
        self.assertEqual(invoke.call_args.args[0].Command[0].ReassignGroupDisk.GroupId, second.GroupId)

    def test_group_failures_do_not_block_next_group(self):
        second = self.add_second_group()
        self.args.max_iterations = 1
        self.args.time_between_reassignings = 3
        for stage in ['preflight', 'apply', 'preflight_transport', 'apply_transport', 'unexpected_moves']:
            with self.subTest(stage=stage):
                sent = []

                def invoke(request):
                    sent.append(deepcopy(request))
                    if request.Command[0].ReassignGroupDisk.GroupId == self.group.GroupId:
                        phase = 'preflight' if request.Rollback else 'apply'
                        if stage == phase:
                            return heal.common.kikimr_bsconfig.TConfigResponse(ErrorDescription='rejected')
                        if stage == phase + '_transport':
                            raise heal.common.ConnectionError('connection lost')
                        if stage == 'unexpected_moves':
                            result = self.response(request)
                            result.Status[0].ReassignedItem[0].From.NodeId = 99
                            return result
                    return self.response(request)

                with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=self.snapshot), \
                        patch.object(heal.common, 'invoke_bsc_request', side_effect=invoke), \
                        patch.object(heal.time, 'sleep') as sleep:
                    self.run_command()
                self.assertEqual([request.Rollback for request in sent[-2:]], [True, False])
                self.assertTrue(all(request.Command[0].ReassignGroupDisk.GroupId == second.GroupId for request in sent[-2:]))
                self.assertIn('Skipping group %d' % self.group.GroupId, self.errors)
                sleep.assert_called_once_with(3)

    def test_unsuccessful_pass_finishes_without_retry_loop(self):
        self.add_second_group()
        self.args.max_iterations = 0
        failure = heal.common.kikimr_bsconfig.TConfigResponse(ErrorDescription='no capacity')
        with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=self.snapshot), \
                patch.object(heal.common, 'invoke_bsc_request', return_value=failure) as invoke:
            self.run_command()
        self.assertEqual(invoke.call_count, 2)
        self.assertIn('2 unsuccessful group attempts skipped', self.errors)

    def test_delay_after_successful_requests(self):
        self.add_second_group()
        self.args.max_iterations = 2
        self.args.time_between_reassignings = 7
        for dry_run in [False, True]:
            with self.subTest(dry_run=dry_run):
                self.args.dry_run = dry_run
                events = []

                def invoke(request):
                    events.append('validate' if request.Rollback else 'apply')
                    return self.response(request)

                with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=self.snapshot), \
                        patch.object(heal.common, 'invoke_bsc_request', side_effect=invoke), \
                        patch.object(heal.time, 'sleep', side_effect=lambda seconds: events.append(seconds)):
                    self.run_command()
                self.assertEqual(events, (['validate', 7] if dry_run else ['validate', 'apply', 7]) * 2)

    def test_replication_threshold_counts_unique_pdisks_and_all_active_states(self):
        self.args.max_replicating_pdisks = 1
        for status in ['REPLICATING', 'INIT_PENDING', '']:
            self.add_replicating_slot(self.base, 20, status)
        self.add_replicating_slot(self.base, 21, 'READY')
        self.add_replicating_slot(self.base, 22, 'ERROR')
        with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=self.snapshot), \
                patch.object(heal.common, 'invoke_bsc_request', side_effect=self.response) as invoke, \
                patch.object(heal.time, 'sleep') as sleep:
            self.run_command()
        sleep.assert_not_called()
        self.assertEqual(invoke.call_count, 2)

    def test_wait_refreshes_snapshot_before_reassigning(self):
        for dry_run in [False, True]:
            with self.subTest(dry_run=dry_run):
                self.args.dry_run = dry_run
                self.args.max_replicating_pdisks = 0
                self.args.max_iterations = 2
                busy = deepcopy(self.snapshot)
                self.add_replicating_slot(busy['BaseConfig'], 20)
                with patch.object(heal.common, 'fetch_base_config_and_storage_pools',
                                  side_effect=[busy, self.snapshot]) as fetch, \
                        patch.object(heal.common, 'invoke_bsc_request', side_effect=self.response) as invoke, \
                        patch.object(heal.time, 'sleep') as sleep:
                    self.run_command()
                sleep.assert_called_once_with(heal.WAITING_TIME)
                self.assertEqual(fetch.call_count, 2)
                self.assertTrue(all(call.kwargs == {'cache': False} for call in fetch.call_args_list))
                self.assertEqual(invoke.call_count, 1 if dry_run else 2)

    def test_waits_count_towards_iteration_limit(self):
        self.args.max_iterations = 2
        self.args.max_replicating_pdisks = 0
        self.add_replicating_slot(self.base, 20)
        with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=self.snapshot) as fetch, \
                patch.object(heal.common, 'invoke_bsc_request') as invoke, \
                patch.object(heal.time, 'sleep') as sleep:
            self.run_command()
        invoke.assert_not_called()
        self.assertEqual(fetch.call_count, 2)
        self.assertEqual(sleep.call_count, 2)

    def test_negative_limits_fail_before_fetching(self):
        for option, default in [('max_iterations', 0), ('max_replicating_pdisks', None),
                                ('time_between_reassignings', 0), ('min_vdisk_uptime', 0)]:
            setattr(self.args, option, -1)
            with patch.object(heal.common, 'fetch_base_config_and_storage_pools') as fetch:
                with self.assertRaisesRegex(heal.common.QueryError, 'nonnegative'):
                    self.run_command()
            fetch.assert_not_called()
            setattr(self.args, option, default)

    def test_explicit_pdisk_options(self):
        for allow_unusable in [False, True]:
            for only_operational in [False, True]:
                with self.subTest(allow_unusable=allow_unusable, only_operational=only_operational):
                    parser = ArgumentParser()
                    heal.add_options(parser)
                    options = ['--from', 'a', '--storage_pools_file', 'pools']
                    if allow_unusable:
                        options.append('--allow-unusable-pdisks')
                    if only_operational:
                        options.append('--move-only-to-operational-pdisks')
                    args = parser.parse_args(options)
                    args.dry_run = False
                    request = heal.create_request(args, self.group, self.sources(), rollback=True)
                    self.assertEqual(request.AllowUnusableDisks, allow_unusable)
                    self.assertEqual(request.SettleOnlyOnOperationalDisks, only_operational)
                    self.assertFalse(request.IgnoreGroupLayoutChecks)

    def test_select_exact_failed_realm(self):
        self.assertEqual([heal.coordinates(slot) for slot in self.sources()], [(0, 0, 0), (0, 1, 0), (0, 2, 0)])
        self.base.VSlot[0].Status = 'READY'
        self.assertEqual([slot.FailDomainIdx for slot in self.sources()], [1, 2])

    def test_reject_other_states_and_geometry(self):
        for field, value in [('ErasureSpecies', 'block-4-2'), ('GroupId', 1)]:
            original = getattr(self.group, field)
            setattr(self.group, field, value)
            self.assertEqual(self.sources(), [])
            setattr(self.group, field, original)
        self.base.VSlot[8].Status = 'ERROR'
        self.assertEqual(self.sources(), [])

    def test_reject_sources_spanning_realms(self):
        self.base.Node[0].HostKey.Fqdn = 'other0'
        self.base.Node[3].HostKey.Fqdn = 'from3'
        self.assertEqual(self.sources(), [])

    def test_wide_prefix_still_selects_only_one_failed_realm(self):
        for node in self.base.Node[:9]:
            node.HostKey.Fqdn = 'from' + str(node.NodeId)
        self.assertEqual([heal.coordinates(slot) for slot in self.sources()], [(0, 0, 0), (0, 1, 0), (0, 2, 0)])

    def test_request_flags_and_generation(self):
        request = heal.create_request(self.args, self.group, self.sources(), rollback=True)
        self.assertEqual(len(request.Command), 3)
        self.assertTrue(request.Rollback and request.IgnoreGroupFailModelChecks)
        for field in ['IgnoreGroupLayoutChecks', 'AllowUnusableDisks', 'SettleOnlyOnOperationalDisks']:
            self.assertFalse(getattr(request, field))
        self.assertFalse(request.IgnoreGroupSanityChecks or request.IgnoreVSlotQuotaCheck)
        for command in request.Command:
            self.assertFalse(command.ReassignGroupDisk.HasField('TargetPDiskId'))
            self.assertEqual(command.ReassignGroupDisk.GroupGeneration, 7)
            self.assertFalse(command.ReassignGroupDisk.SuppressDonorMode)

    def test_reject_unexpected_controller_moves(self):
        moves = self.sources()
        response = self.response(heal.create_request(self.args, self.group, moves, rollback=True))
        response.Status[0].ReassignedItem[0].From.NodeId = 99
        with self.assertRaisesRegex(heal.common.QueryError, 'unexpected reassignments'):
            heal.check_response(response, moves, rollback=True)

    def test_controller_selects_different_targets_for_preflight_and_apply(self):
        def invoke(request):
            for command in request.Command:
                self.assertFalse(command.ReassignGroupDisk.HasField('TargetPDiskId'))
            response = self.response(request)
            for status in response.Status:
                status.ReassignedItem[0].To.NodeId += 100 if request.Rollback else 200
            return response

        with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=self.snapshot), \
                patch.object(heal.common, 'invoke_bsc_request', side_effect=invoke) as invoke_mock:
            output = self.run_command()
        self.assertEqual(invoke_mock.call_count, 2)
        self.assertNotIn('TargetPDiskId', output)

    def test_rollback_error_is_not_success(self):
        response = heal.common.kikimr_bsconfig.TConfigResponse(ErrorDescription='SelfHeal is disabled, transaction rollback')
        with self.assertRaisesRegex(heal.common.QueryError, 'failed'):
            heal.check_response(response, self.sources(), rollback=True)

    def test_dry_run_and_apply(self):
        for dry_run in [True, False]:
            self.args.dry_run = dry_run
            sent = []

            def invoke(request):
                copy = type(request)()
                copy.CopyFrom(request)
                sent.append(copy)
                return self.response(request)

            with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=self.snapshot), \
                    patch.object(heal.common, 'invoke_bsc_request', side_effect=invoke):
                output = self.run_command()
            self.assertEqual([request.Rollback for request in sent], [True] if dry_run else [True, False])
            self.assertIn('ReassignGroupDisk', output)

    def test_failed_preflight_never_applies(self):
        failure = heal.common.kikimr_bsconfig.TConfigResponse(ErrorDescription='no space')
        with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=self.snapshot), \
                patch.object(heal.common, 'invoke_bsc_request', return_value=failure) as invoke:
            self.run_command()
        self.assertIn('no space', self.errors)
        self.assertTrue(all(call.args[0].Rollback for call in invoke.call_args_list))

    def test_unknown_pool_fails_before_reassigning(self):
        with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=self.snapshot), \
                patch.object(heal, 'read_pool_names', return_value={'missing'}), \
                patch.object(heal.common, 'invoke_bsc_request') as invoke:
            with self.assertRaisesRegex(heal.common.QueryError, 'Unknown storage pools'):
                self.run_command()
        invoke.assert_not_called()

    def test_apply_error_reports_group_and_does_not_retry(self):
        preflight = self.response(heal.create_request(self.args, self.group, self.sources(), rollback=True))
        with patch.object(heal.common, 'fetch_base_config_and_storage_pools', return_value=self.snapshot), \
                patch.object(heal.common, 'invoke_bsc_request', side_effect=[
                    preflight, heal.common.QueryError('generation mismatch')]) as invoke:
            self.run_command()
        self.assertIn('inspect actual placement', self.errors)
        self.assertEqual(invoke.call_count, 2)

    def test_pool_file(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / 'pools'
            path.write_text('  /Root:ssd  \n\n/Root:ssd\n/Root:hdd\n', encoding='utf-8')
            self.assertEqual(heal.read_pool_names(path), ['/Root:ssd', '/Root:hdd'])
            path.write_text('\n', encoding='utf-8')
            with self.assertRaisesRegex(heal.common.QueryError, 'empty'):
                heal.read_pool_names(path)

    def test_empty_prefix_fails_before_fetch(self):
        self.args.from_prefix = ''
        with patch.object(heal.common, 'fetch_base_config_and_storage_pools') as fetch:
            with self.assertRaisesRegex(heal.common.QueryError, 'nonempty'):
                self.run_command()
        fetch.assert_not_called()
