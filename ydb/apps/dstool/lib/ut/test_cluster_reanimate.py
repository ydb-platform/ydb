import copy
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from ydb.apps.dstool.lib import dstool_cmd_cluster_reanimate as reanimate
from ydb.apps.dstool.lib.arg_parser import ArgumentParser
from ydb.apps.dstool.lib import commands


GROUP_ID = 0x80000001
POOL = '/global/database:ssdencrypted'


class TestReanimate(unittest.TestCase):
    def setUp(self):
        self.args = SimpleNamespace(
            pool=POOL, dc='sas', timeout=10, poll_interval=3, state_dir=None,
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
        self.sleep = self.enterContext(patch.object(reanimate.time, 'sleep'))
        self.stdout = self.enterContext(patch('sys.stdout', new_callable=io.StringIO))
        self.requests = []

    def move(self, request):
        self.requests.append(copy.deepcopy(request))
        command = request.Command[0].ReassignGroupDisk
        self.assertEqual(command.GroupGeneration, self.group.GroupGeneration)
        wanted = (command.FailRealmIdx, command.FailDomainIdx, command.VDiskIdx)
        current = [disk for disk in self.base.VSlot if disk.GroupGeneration == self.group.GroupGeneration]
        disk = next(disk for disk in current
                    if (disk.FailRealmIdx, disk.FailDomainIdx, disk.VDiskIdx) == wanted)
        slot_id = next(slot_id for slot_id in self.group.VSlotId if slot_id == disk.VSlotId)
        disk.VSlotId.VSlotId += 10
        disk.VSlotId.NodeId = 6
        slot_id.CopyFrom(disk.VSlotId)
        disk.Status = 'READY'
        self.group.GroupGeneration += 1
        for disk in current:
            disk.GroupGeneration = self.group.GroupGeneration
        response = reanimate.common.kikimr_bsconfig.TConfigResponse(Success=True)
        response.Status.add(Success=True)
        return response

    def test_plan_filters_dc_status_and_donors_and_sorts_positions(self):
        rows, dcs = reanimate.fetch_disks()
        selected, order = reanimate.make_plan(list(reversed(rows)), dcs, GROUP_ID, 'sas')
        self.assertEqual([row['NodeId'] for row in selected], [1, 2, 3, 4])
        self.assertEqual(order, [0, 1, 2, 0, 1, 3])
        self.fetch.assert_called_once_with(cache=False)
        selected, order = reanimate.make_plan(rows, dcs, GROUP_ID, 'vla')
        self.assertEqual(len(selected), 1)
        self.assertEqual(order, [])

    def test_three_disks_repeat_the_first_two(self):
        self.base.VSlot[3].Status = 'READY'
        rows, dcs = reanimate.fetch_disks()
        self.assertEqual(reanimate.make_plan(rows, dcs, GROUP_ID, 'sas')[1], [0, 1, 2, 0, 1])

    def test_duplicate_position_and_missing_dc_stop_planning(self):
        rows, dcs = reanimate.fetch_disks()
        with self.assertRaisesRegex(reanimate.common.QueryError, 'Multiple current'):
            reanimate.make_plan(rows + [rows[0]], dcs, GROUP_ID, 'sas')
        del dcs[1]
        with self.assertRaisesRegex(reanimate.common.QueryError, 'Unknown DC'):
            reanimate.make_plan(rows, dcs, GROUP_ID, 'sas')

    def test_sequential_eviction_refreshes_generation_and_keeps_plan(self):
        self.invoke.side_effect = self.move
        reanimate.do(self.args)
        commands = [request.Command[0].ReassignGroupDisk for request in self.requests]
        self.assertEqual([(cmd.FailRealmIdx, cmd.FailDomainIdx) for cmd in commands],
                         [(0, 0), (0, 1), (0, 2), (0, 0), (0, 1), (1, 0)])
        self.assertEqual([cmd.GroupGeneration for cmd in commands], list(range(10, 16)))
        self.assertEqual(self.invoke.call_count, 6)
        self.assertEqual(reanimate.common.get_vslot_id(self.base.VSlot[3].VSlotId), (6, 1, 113))
        self.assertEqual(self.base.VSlot[3].Status, 'READY')
        self.assertTrue(all(request.IgnoreGroupFailModelChecks for request in self.requests))
        self.assertTrue(all(not request.IgnoreDegradedGroupsChecks for request in self.requests))
        self.assertTrue(all(not request.Rollback for request in self.requests))
        self.assertTrue(all(not request.Command[0].ReassignGroupDisk.SuppressDonorMode
                            for request in self.requests))
        self.assertIn('Completed: 1; skipped: 0; total: 1', self.stdout.getvalue())
        self.assertTrue(all(call.kwargs == {'cache': False} for call in self.fetch.call_args_list))

    def test_dry_run_has_no_mutations_or_waits(self):
        self.args.dry_run = True
        with tempfile.TemporaryDirectory() as directory:
            self.args.state_dir = Path(directory) / 'evidence'
            reanimate.do(self.args)
            self.assertFalse(self.args.state_dir.exists())
        self.invoke.assert_not_called()
        self.sleep.assert_not_called()
        self.fetch.assert_called_once_with(cache=False)
        self.assertIn('Order: 1 -> 2 -> 3 -> 1 -> 2 -> 4\n', self.stdout.getvalue())
        self.assertIn('Step 6, VDisk 4:', self.stdout.getvalue())

    def test_evidence_records_replacement_slots(self):
        self.invoke.side_effect = self.move
        self.args.ignore_degraded_group_check = True
        with tempfile.TemporaryDirectory() as directory:
            self.args.state_dir = Path(directory) / 'evidence'
            reanimate.do(self.args)
            group_dir = self.args.state_dir / ('group-%d' % GROUP_ID)
            plan = json.loads((group_dir / 'plan.json').read_text())
            before = json.loads((group_dir / '4-before.json').read_text())
            after = json.loads((group_dir / '4-after.json').read_text())
            self.assertEqual(plan['order'], [0, 1, 2, 0, 1, 3])
            self.assertEqual(len(plan['selected']), 4)
            self.assertEqual(before['VSlotId'], 110)
            self.assertEqual(after['VSlotId'], 120)
            self.assertEqual(before['GroupGeneration'], 13)
            self.assertEqual(after['GroupGeneration'], 14)
            self.assertEqual(len(list(group_dir.iterdir())), 13)
        self.assertTrue(all(request.IgnoreDegradedGroupsChecks for request in self.requests))

    def test_existing_evidence_directory_stops_before_eviction(self):
        with tempfile.TemporaryDirectory() as directory:
            self.args.state_dir = Path(directory)
            with self.assertRaisesRegex(reanimate.common.QueryError, 'Cannot write'):
                reanimate.do(self.args)
        self.invoke.assert_not_called()

    def test_eviction_failure_stops_without_retry_or_next_step(self):
        for failure in (reanimate.common.ConnectionError('uncertain result'),
                        reanimate.common.kikimr_bsconfig.TConfigResponse(Success=False)):
            with self.subTest(failure=failure):
                self.invoke.reset_mock()
                self.invoke.side_effect = [failure]
                with self.assertRaises((reanimate.common.ConnectionError, reanimate.common.QueryError)):
                    reanimate.do(self.args)
                self.invoke.assert_called_once()
        self.sleep.assert_not_called()

    def test_placement_timeout_stops_sequence(self):
        self.invoke.return_value = reanimate.common.kikimr_bsconfig.TConfigResponse(Success=True)
        self.invoke.return_value.Status.add(Success=True)
        with patch.object(reanimate.time, 'monotonic', side_effect=[0, 4, 10]):
            with self.assertRaisesRegex(reanimate.common.QueryError, 'relocation may still be in progress'):
                reanimate.do(self.args)
        self.invoke.assert_called_once()
        self.sleep.assert_called_once_with(3)

    def test_pool_change_before_eviction_is_rejected(self):
        changed = copy.deepcopy(self.snapshot)
        changed['StoragePools'][0].Name = 'another-pool'
        self.fetch.side_effect = [self.snapshot, changed]
        with self.assertRaisesRegex(reanimate.common.QueryError, 'no longer belongs'):
            reanimate.do(self.args)
        self.invoke.assert_not_called()

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
        parser = ArgumentParser()
        parser.add_argument('--dry-run', action='store_true')
        command_map = commands.make_command_map_by_structure(
            parser.add_subparsers(dest='global_command', required=True),
        )
        args = parser.parse_args(['--dry-run', 'cluster', 'reanimate', '--pool', POOL,
                                  '--dc', 'sas', '--ignore-failure-model-group-check'])
        command_map['cluster'](args)
        self.assertTrue(args.dry_run)
        self.assertTrue(args.ignore_failure_model_group_check)
        self.invoke.assert_not_called()

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
