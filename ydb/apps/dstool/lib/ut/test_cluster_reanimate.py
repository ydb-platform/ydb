import copy
import io
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from ydb.apps.dstool.lib import dstool_cmd_cluster_reanimate as reanimate
from ydb.apps.dstool.lib.arg_parser import ArgumentParser
from ydb.apps.dstool.lib import commands


GROUP_ID = 0x80000001
POOL = '/global/database:ssdencrypted'


class TestReanimate(unittest.TestCase):
    def setUp(self):
        self.args = SimpleNamespace(
            pool=POOL, dc='sas', inflight=1, timeout=10, poll_interval=3, state_dir=None,
            dry_run=False, ignore_degraded_group_check=False, ignore_failure_model_group_check=True,
        )
        self.base = reanimate.common.kikimr_bsconfig.TBaseConfig()
        self.group = self.base.Group.add(
            GroupId=GROUP_ID, GroupGeneration=10, BoxId=1, StoragePoolId=1,
        )
        for index in range(6):
            node = self.base.Node.add(NodeId=index + 1)
            node.Location.DataCenter = 'sas' if index < 5 else 'vla'
            disk = self.base.VSlot.add(
                GroupId=GROUP_ID, GroupGeneration=10,
                FailRealmIdx=index // 3, FailDomainIdx=index % 3,
                VDiskIdx=0, Status='READY' if index == 4 else 'ERROR',
            )
            disk.VSlotId.NodeId = index + 1
            disk.VSlotId.PDiskId = 1
            disk.VSlotId.VSlotId = 100 + index
            self.group.VSlotId.add().CopyFrom(disk.VSlotId)
        # A donor at the same logical position must not be selected as a current disk.
        donor = self.base.VSlot.add()
        donor.CopyFrom(self.base.VSlot[0])
        donor.GroupGeneration = 9
        donor.VSlotId.VSlotId = 99
        self.snapshot = dict(
            BaseConfig=self.base,
            StoragePools=[SimpleNamespace(BoxId=1, StoragePoolId=1, Name=POOL)],
        )
        self.fetch = self.enterContext(patch.object(
            reanimate.common, 'fetch_base_config_and_storage_pools', return_value=self.snapshot,
        ))
        self.invoke = self.enterContext(patch.object(reanimate.common, 'invoke_bsc_request'))
        self.poll_wait = Mock()
        self.enterContext(patch.object(reanimate, 'threading', SimpleNamespace(Event=self.make_stop)))
        self.stdout = self.enterContext(patch('sys.stdout', new_callable=io.StringIO))
        self.requests = []

    def make_stop(self):
        event = threading.Event()
        self.stop = event
        self.real_poll_wait = event.wait
        event.wait = self.poll_wait
        return event

    def move(self, request):
        self.requests.append(copy.deepcopy(request))
        current = [disk for disk in self.base.VSlot if disk.GroupGeneration == self.group.GroupGeneration]
        for item in request.Command:
            command = item.ReassignGroupDisk
            self.assertEqual(command.GroupGeneration, self.group.GroupGeneration)
            wanted = (command.FailRealmIdx, command.FailDomainIdx, command.VDiskIdx)
            disk = next(disk for disk in current
                        if (disk.FailRealmIdx, disk.FailDomainIdx, disk.VDiskIdx) == wanted)
            slot_id = next(slot_id for slot_id in self.group.VSlotId if slot_id == disk.VSlotId)
            disk.VSlotId.VSlotId += 10
            disk.VSlotId.NodeId = 6
            slot_id.CopyFrom(disk.VSlotId)
            disk.Status = 'READY'
        # The controller commits all commands with one generation change.
        self.group.GroupGeneration += 1
        for disk in current:
            disk.GroupGeneration = self.group.GroupGeneration
        response = reanimate.common.kikimr_bsconfig.TConfigResponse(Success=True)
        for _ in request.Command:
            response.Status.add(Success=True)
        return response

    def test_plan_filters_dc_status_and_donors_and_sorts_positions(self):
        rows, dcs = reanimate.fetch_disks()
        selected, order = reanimate.make_plan(list(reversed(rows)), dcs, GROUP_ID, 'sas')
        self.assertEqual([row['NodeId'] for row in selected], [1, 2, 3, 4])
        self.assertEqual(order, [[0, 1, 2], [0, 1, 2], [3]])
        self.fetch.assert_called_once_with(cache=False)
        selected, order = reanimate.make_plan(rows, dcs, GROUP_ID, 'vla')
        self.assertEqual(len(selected), 1)
        self.assertEqual(order, [])

    def test_three_disks_repeat_the_entire_batch(self):
        self.base.VSlot[3].Status = 'READY'
        rows, dcs = reanimate.fetch_disks()
        self.assertEqual(reanimate.make_plan(rows, dcs, GROUP_ID, 'sas')[1], [[0, 1, 2], [0, 1, 2]])

    def test_duplicate_position_and_missing_dc_stop_planning(self):
        rows, dcs = reanimate.fetch_disks()
        with self.assertRaisesRegex(reanimate.common.QueryError, 'Multiple current'):
            reanimate.make_plan(rows + [rows[0]], dcs, GROUP_ID, 'sas')
        del dcs[1]
        with self.assertRaisesRegex(reanimate.common.QueryError, 'Unknown DC'):
            reanimate.make_plan(rows, dcs, GROUP_ID, 'sas')

    def test_batched_eviction_refreshes_generation_and_keeps_plan(self):
        self.invoke.side_effect = self.move
        reanimate.do(self.args)
        commands = [item.ReassignGroupDisk for request in self.requests for item in request.Command]
        self.assertEqual([len(request.Command) for request in self.requests], [3, 3, 1])
        self.assertEqual([(cmd.FailRealmIdx, cmd.FailDomainIdx) for cmd in commands],
                         [(0, 0), (0, 1), (0, 2), (0, 0), (0, 1), (0, 2), (1, 0)])
        self.assertEqual([cmd.GroupGeneration for cmd in commands], [10, 10, 10, 11, 11, 11, 12])
        self.assertEqual(self.invoke.call_count, 3)
        self.assertEqual(reanimate.common.get_vslot_id(self.base.VSlot[3].VSlotId), (6, 1, 113))
        self.assertEqual(self.base.VSlot[3].Status, 'READY')
        self.assertTrue(all(request.IgnoreGroupFailModelChecks for request in self.requests))
        self.assertTrue(all(not request.IgnoreDegradedGroupsChecks for request in self.requests))
        self.assertTrue(all(not request.Rollback for request in self.requests))
        self.assertTrue(all(not request.Command[0].ReassignGroupDisk.SuppressDonorMode
                            for request in self.requests))
        self.assertIn('Completed: 1; skipped: 0; total: 1', self.stdout.getvalue())
        self.assertTrue(all(call.kwargs == {'cache': False} for call in self.fetch.call_args_list))
        self.assertEqual(self.fetch.call_count, 5)

    def test_dry_run_has_no_mutations_or_waits(self):
        self.args.dry_run = True
        with tempfile.TemporaryDirectory() as directory:
            self.args.state_dir = Path(directory) / 'evidence'
            reanimate.do(self.args)
            self.assertFalse(self.args.state_dir.exists())
        self.invoke.assert_not_called()
        self.poll_wait.assert_not_called()
        self.fetch.assert_called_once_with(cache=False)
        self.assertIn('Order: 1,2,3 -> 1,2,3 -> 4\n', self.stdout.getvalue())
        self.assertIn('Batch 3: evict [80000001:_:1:0:0]', self.stdout.getvalue())
        self.assertIn('Planned: 1; skipped: 0; inflight: 1', self.stdout.getvalue())

    def test_evidence_records_replacement_slots(self):
        self.invoke.side_effect = self.move
        self.args.ignore_degraded_group_check = True
        with tempfile.TemporaryDirectory() as directory:
            self.args.state_dir = Path(directory) / 'evidence'
            reanimate.do(self.args)
            group_dir = self.args.state_dir / ('group-%d' % GROUP_ID)
            plan = json.loads((group_dir / 'plan.json').read_text())
            before = json.loads((group_dir / '2-before.json').read_text())
            after = json.loads((group_dir / '2-after.json').read_text())
            self.assertEqual(plan['order'], [[0, 1, 2], [0, 1, 2], [3]])
            self.assertEqual(len(plan['selected']), 4)
            self.assertEqual(before[0]['VSlotId'], 110)
            self.assertEqual(after[0]['VSlotId'], 120)
            self.assertEqual(before[0]['GroupGeneration'], 11)
            self.assertEqual(after[0]['GroupGeneration'], 12)
            self.assertEqual(len(list(group_dir.iterdir())), 7)
        self.assertTrue(all(request.IgnoreDegradedGroupsChecks for request in self.requests))

    def test_existing_evidence_directory_stops_before_eviction(self):
        with tempfile.TemporaryDirectory() as directory:
            self.args.state_dir = Path(directory)
            with self.assertRaisesRegex(reanimate.common.QueryError, 'Cannot write'):
                reanimate.do(self.args)
        self.invoke.assert_not_called()

    def test_next_batch_waits_for_every_slot_to_change(self):
        original = copy.deepcopy(self.snapshot)
        partial = None

        def move(request):
            nonlocal partial
            if self.requests:
                self.assertGreaterEqual(self.fetch.call_count, 4)
            response = self.move(request)
            if len(self.requests) == 1:
                partial = copy.deepcopy(self.snapshot)
                old_slot = original['BaseConfig'].VSlot[2].VSlotId
                partial['BaseConfig'].VSlot[2].VSlotId.CopyFrom(old_slot)
                partial['BaseConfig'].Group[0].VSlotId[2].CopyFrom(old_slot)
            return response

        self.invoke.side_effect = move
        self.fetch.side_effect = lambda **kwargs: partial if self.fetch.call_count == 3 else self.snapshot
        reanimate.do(self.args)
        self.assertEqual(self.invoke.call_count, 3)
        self.assertEqual(self.fetch.call_count, 6)
        self.poll_wait.assert_called_once_with(3)

    def parallel_disks(self, count):
        rows, dcs = reanimate.fetch_disks()
        rows = [dict(row, GroupId=GROUP_ID + index) for index in range(count) for row in rows]
        lock = threading.Lock()

        def fetch():
            with lock:
                return copy.deepcopy(rows), dcs

        self.enterContext(patch.object(reanimate, 'fetch_disks', side_effect=fetch))
        return rows, lock

    def test_inflight_bounds_concurrent_groups_and_schedules_remaining_groups(self):
        self.args.inflight = 2
        rows, lock = self.parallel_disks(4)
        barrier = threading.Barrier(2, timeout=5)
        batches = {}
        active = set()
        max_active = 0

        def evict(args, before):
            nonlocal max_active
            group_id = before[0]['GroupId']
            with lock:
                batches[group_id] = batches.get(group_id, 0) + 1
                batch = batches[group_id]
                active.add(group_id)
                max_active = max(max_active, len(active))
            if batch == 1 and group_id < GROUP_ID + 2:
                barrier.wait()
            with lock:
                wanted = {reanimate.position(row) for row in before}
                for row in rows:
                    if row['GroupId'] == group_id:
                        row['GroupGeneration'] += 1
                        if reanimate.position(row) in wanted:
                            row['VSlotId'] += 10
                if batch == 3:
                    active.remove(group_id)

        with patch.object(reanimate, 'evict', side_effect=evict):
            reanimate.do(self.args)
        self.assertEqual(max_active, 2)
        self.assertEqual(batches, {GROUP_ID + index: 3 for index in range(4)})
        self.assertIn('Completed: 4; skipped: 0; total: 4', self.stdout.getvalue())

    def test_failure_interrupts_other_group_polling_and_stops_pending_groups(self):
        self.args.inflight = 2
        self.parallel_disks(3)
        polling = threading.Event()
        groups = []

        def evict(args, before):
            group_id = before[0]['GroupId']
            groups.append(group_id)
            if group_id == GROUP_ID:
                self.assertTrue(polling.wait(5))
                raise common_error

        def poll_wait(timeout):
            polling.set()
            return self.real_poll_wait(timeout)

        common_error = reanimate.common.QueryError('first group failed')
        self.poll_wait.side_effect = poll_wait
        with patch.object(reanimate, 'evict', side_effect=evict):
            with self.assertRaisesRegex(reanimate.common.QueryError, 'first group failed'):
                reanimate.do(self.args)
        self.assertEqual(sorted(groups), [GROUP_ID, GROUP_ID + 1])
        self.assertTrue(self.stop.is_set())
        self.poll_wait.assert_called_once_with(3)

    def test_eviction_failure_stops_without_retry_or_next_step(self):
        for failure in (reanimate.common.ConnectionError('uncertain result'),
                        reanimate.common.kikimr_bsconfig.TConfigResponse(Success=False)):
            with self.subTest(failure=failure):
                self.invoke.reset_mock()
                self.invoke.side_effect = [failure]
                with self.assertRaises((reanimate.common.ConnectionError, reanimate.common.QueryError)):
                    reanimate.do(self.args)
                self.invoke.assert_called_once()
        self.poll_wait.assert_not_called()

    def test_placement_timeout_stops_sequence(self):
        self.invoke.return_value = reanimate.common.kikimr_bsconfig.TConfigResponse(Success=True)
        self.invoke.return_value.Status.add(Success=True)
        with patch.object(reanimate.time, 'monotonic', side_effect=[0, 4, 10]):
            with self.assertRaisesRegex(reanimate.common.QueryError, 'relocations may still be in progress'):
                reanimate.do(self.args)
        self.invoke.assert_called_once()
        self.poll_wait.assert_called_once_with(3)

    def test_pool_change_between_batches_is_rejected(self):
        changed = copy.deepcopy(self.snapshot)
        changed['StoragePools'][0].Name = 'another-pool'
        self.fetch.side_effect = [self.snapshot, self.snapshot, changed]
        self.invoke.side_effect = self.move
        with self.assertRaisesRegex(reanimate.common.QueryError, 'no longer belongs'):
            reanimate.do(self.args)
        self.invoke.assert_called_once()

    def test_missing_current_disk_is_rejected(self):
        with self.assertRaisesRegex(reanimate.common.QueryError, 'found 0'):
            reanimate.current_disk([], GROUP_ID, (0, 0, 0), POOL)

    def test_pool_filter_and_small_groups_are_skipped(self):
        self.args.pool = 'another-pool'
        reanimate.do(self.args)
        self.args.pool = POOL
        self.args.dc = 'vla'
        reanimate.do(self.args)
        self.invoke.assert_not_called()
        self.assertIn('Completed: 0; skipped: 1; total: 1', self.stdout.getvalue())

    def test_command_registration_and_options(self):
        for before, after in ((['--dry-run'], []), ([], ['--dry-run']),
                              (['--dry-run'], ['--dry-run']), (['-n'], []), ([], ['-n'])):
            with self.subTest(before=before, after=after):
                parser = ArgumentParser()
                parser.add_argument('--dry-run', '-n', action='store_true')
                command_map = commands.make_command_map_by_structure(
                    parser.add_subparsers(dest='global_command', required=True),
                )
                command_args = ['cluster', 'reanimate', '--pool', POOL,
                                '--dc', 'sas', '--inflight', '2', '--ignore-failure-model-group-check']
                args = parser.parse_args(before + command_args + after)
                command_map['cluster'](args)
                self.assertTrue(args.dry_run)
                self.assertEqual(args.inflight, 2)
                self.assertTrue(args.ignore_failure_model_group_check)
        self.invoke.assert_not_called()
        self.poll_wait.assert_not_called()

    def test_positive_intervals(self):
        for value in ('0', '-1', 'invalid'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                reanimate.positive(value)

    def test_interrupt_reports_accepted_evictions(self):
        self.invoke.side_effect = KeyboardInterrupt
        with patch('sys.stderr', new_callable=io.StringIO) as stderr:
            with self.assertRaises(SystemExit) as error:
                reanimate.do(self.args)
            self.assertEqual(error.exception.code, 130)
            self.assertIn('accepted evicts may still be in progress', stderr.getvalue())
