import contextlib
import io
import json
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

from ydb.apps.dstool.lib import dstool_cmd_pdisk_populate as populate


def make_args(**overrides):
    values = {
        'destination_pdisk': None,
        'dry_run': False,
        'format': 'pretty',
        'quiet': False,
        'snapshot_file': None,
        'snapshot_from_pdisk': None,
        'suppress_donor_mode': False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def make_vslot(
    node_id,
    pdisk_id,
    vslot_id,
    group_id,
    group_generation=1,
    fail_realm_idx=0,
    fail_domain_idx=0,
    vdisk_idx=0,
):
    return SimpleNamespace(
        VSlotId=SimpleNamespace(NodeId=node_id, PDiskId=pdisk_id, VSlotId=vslot_id),
        GroupId=group_id,
        GroupGeneration=group_generation,
        FailRealmIdx=fail_realm_idx,
        FailDomainIdx=fail_domain_idx,
        VDiskIdx=vdisk_idx,
        Donors=[],
    )


class PDiskIdTest(unittest.TestCase):
    def test_parse_accepts_bracketed_and_plain_ids(self):
        self.assertEqual(populate.parse_pdisk_id('[12:34]'), (12, 34))
        self.assertEqual(populate.parse_pdisk_id(' 12:34 '), (12, 34))

    def test_parse_rejects_invalid_format(self):
        with self.assertRaisesRegex(Exception, 'must be in format'):
            populate.parse_pdisk_id('12')

        with self.assertRaisesRegex(Exception, 'must contain integer'):
            populate.parse_pdisk_id('[node:34]')


class SnapshotFileTest(unittest.TestCase):
    def read_snapshot(self, contents):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, 'snapshot')
            with open(path, 'w', encoding='utf-8') as snapshot_file:
                snapshot_file.write(contents)
            return populate.read_snapshot(path)

    def test_reads_supported_snapshot_formats(self):
        cases = (
            ('{"vdisk_ids": ["id-1", "id-2"]}', ['id-1', 'id-2']),
            ('{"VDiskIds": "id-1 id-2"}', ['id-1', 'id-2']),
            ('["id-1 id-2", "id-3"]', ['id-1', 'id-2', 'id-3']),
            ('id-1 id-2\nid-3', ['id-1', 'id-2', 'id-3']),
        )

        for contents, expected in cases:
            with self.subTest(contents=contents):
                self.assertEqual(self.read_snapshot(contents), expected)

    def test_rejects_unsupported_json_value(self):
        with self.assertRaisesRegex(Exception, 'Failed to parse VDisk ids'):
            self.read_snapshot('{"unexpected": ["id-1"]}')

    def test_populate_requires_nonempty_snapshot_file(self):
        with self.assertRaisesRegex(Exception, 'requires --snapshot-file'):
            populate.make_vdisk_ids_for_populate(make_args())

        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, 'snapshot')
            with open(path, 'w', encoding='utf-8'):
                pass

            with self.assertRaisesRegex(Exception, 'VDisk list is empty'):
                populate.make_vdisk_ids_for_populate(make_args(snapshot_file=path))


class VSlotsOnPDiskTest(unittest.TestCase):
    def test_separates_active_vslots_from_donors(self):
        active = make_vslot(1, 2, 10, 0x80000001, fail_domain_idx=1)
        donor = make_vslot(1, 2, 11, 0x80000002, fail_domain_idx=2)
        foreign = make_vslot(3, 4, 12, 0x80000003, fail_domain_idx=3)
        foreign.Donors.append(SimpleNamespace(VSlotId=donor.VSlotId))
        base_config = SimpleNamespace(VSlot=[active, donor, foreign])

        result = populate.VSlotsOnPDisk.from_base_config(base_config, (1, 2))

        self.assertEqual(result.active, [active])
        self.assertEqual(result.donors, [donor])
        self.assertEqual(result.active_vdisk_ids(), ['[80000001:_:0:1:0]'])
        self.assertEqual(result.donor_vdisk_ids(), ['[80000002:_:0:2:0]'])

    def test_donor_warning_respects_quiet_and_limits_preview(self):
        vdisk_ids = ['vdisk-%u' % index for index in range(4)]
        stderr = io.StringIO()

        with contextlib.redirect_stderr(stderr):
            populate.warn_skipped_donors(make_args(), (1, 2), vdisk_ids)

        self.assertIn('skipped 4 donor VDisks on PDisk [1:2]', stderr.getvalue())
        self.assertIn('vdisk-0, vdisk-1, vdisk-2...', stderr.getvalue())
        self.assertNotIn('vdisk-3', stderr.getvalue())

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            populate.warn_skipped_donors(make_args(quiet=True), (1, 2), vdisk_ids)
        self.assertEqual(stderr.getvalue(), '')


class PopulateRequestTest(unittest.TestCase):
    def test_builds_request_and_skips_zero_group_vdisks(self):
        zero_group = make_vslot(1, 2, 10, 0, fail_domain_idx=1)
        migratable = make_vslot(
            1,
            2,
            11,
            0x80000001,
            group_generation=7,
            fail_realm_idx=2,
            fail_domain_idx=3,
            vdisk_idx=4,
        )
        base_config = SimpleNamespace(VSlot=[zero_group, migratable])
        args = make_args(dry_run=True, suppress_donor_mode=True)
        stderr = io.StringIO()

        with contextlib.redirect_stderr(stderr):
            request = populate.create_populate_request(
                args,
                base_config,
                (10, 20),
                ['[00000000:_:0:1:0]', '[80000001:_:2:3:4]'],
            )

        self.assertTrue(request.Rollback)
        self.assertEqual(len(request.Command), 1)
        command = request.Command[0].PopulatePDisk
        self.assertEqual(command.DestinationPDisk.TargetPDiskId.NodeId, 10)
        self.assertEqual(command.DestinationPDisk.TargetPDiskId.PDiskId, 20)
        self.assertTrue(command.SuppressDonorMode)
        self.assertEqual(len(command.VDiskId), 1)
        self.assertEqual(command.VDiskId[0].GroupID, 0x80000001)
        self.assertEqual(command.VDiskId[0].GroupGeneration, 7)
        self.assertEqual(command.VDiskId[0].Ring, 2)
        self.assertEqual(command.VDiskId[0].Domain, 3)
        self.assertEqual(command.VDiskId[0].VDisk, 4)
        self.assertIn('skipped 1 VDisks with GroupId=0', stderr.getvalue())

    def test_rejects_request_when_all_vdisks_have_zero_group(self):
        zero_group = make_vslot(1, 2, 10, 0)
        base_config = SimpleNamespace(VSlot=[zero_group])

        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaisesRegex(Exception, 'all VDisks have GroupId=0'):
                populate.create_populate_request(
                    make_args(),
                    base_config,
                    (10, 20),
                    ['[00000000:_:0:0:0]'],
                )


class SnapshotModeTest(unittest.TestCase):
    def test_writes_snapshot_and_excludes_donors(self):
        active = make_vslot(1, 2, 10, 0x80000001, fail_domain_idx=1)
        donor = make_vslot(1, 2, 11, 0x80000002, fail_domain_idx=2)
        owner = make_vslot(3, 4, 12, 0x80000003)
        owner.Donors.append(SimpleNamespace(VSlotId=donor.VSlotId))
        base_config = SimpleNamespace(VSlot=[active, donor, owner])

        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, 'snapshot.json')
            args = make_args(snapshot_from_pdisk='[1:2]', snapshot_file=path)
            stdout = io.StringIO()
            stderr = io.StringIO()

            with mock.patch.object(populate.common, 'fetch_base_config', return_value=base_config):
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    populate.run_snapshot_mode(args)

            with open(path, 'r', encoding='utf-8') as snapshot_file:
                snapshot = json.load(snapshot_file)

        expected = {'pdisk_id': '[1:2]', 'vdisk_ids': ['[80000001:_:0:1:0]']}
        self.assertEqual(snapshot, expected)
        self.assertEqual(stdout.getvalue(), '[80000001:_:0:1:0]\n')
        self.assertIn('skipped 1 donor VDisks', stderr.getvalue())

    def test_json_output_contains_snapshot(self):
        base_config = SimpleNamespace(VSlot=[])
        args = make_args(snapshot_from_pdisk='1:2', format='json')
        stdout = io.StringIO()

        with mock.patch.object(populate.common, 'fetch_base_config', return_value=base_config):
            with contextlib.redirect_stdout(stdout):
                populate.run_snapshot_mode(args)

        self.assertEqual(json.loads(stdout.getvalue()), {'pdisk_id': '1:2', 'vdisk_ids': []})

    def test_rejects_populate_only_options(self):
        with self.assertRaisesRegex(Exception, 'destination-pdisk'):
            populate.run_snapshot_mode(make_args(snapshot_from_pdisk='1:2', destination_pdisk='3:4'))

        with self.assertRaisesRegex(Exception, 'suppress-donor-mode'):
            populate.run_snapshot_mode(make_args(snapshot_from_pdisk='1:2', suppress_donor_mode=True))


class PopulateModeTest(unittest.TestCase):
    def test_invokes_request_and_prints_successful_result(self):
        args = make_args(destination_pdisk='3:4', snapshot_file='snapshot.json')
        base_config = object()
        request = object()
        response = object()

        with mock.patch.object(populate.common, 'fetch_base_config', return_value=base_config):
            with mock.patch.object(populate, 'make_vdisk_ids_for_populate', return_value=['vdisk']):
                with mock.patch.object(populate, 'create_populate_request', return_value=request) as create_request:
                    with mock.patch.object(populate.common, 'invoke_bsc_request', return_value=response):
                        with mock.patch.object(populate.common, 'print_request_result') as print_result:
                            with mock.patch.object(populate.common, 'is_successful_bsc_response', return_value=True):
                                populate.run_populate_mode(args)

        create_request.assert_called_once_with(args, base_config, (3, 4), ['vdisk'])
        print_result.assert_called_once_with(args, request, response)

    def test_reports_mapper_error_and_exits_on_failed_request(self):
        args = make_args(destination_pdisk='3:4', snapshot_file='snapshot.json')
        request = object()
        response = object()

        with mock.patch.object(populate.common, 'fetch_base_config'):
            with mock.patch.object(populate, 'make_vdisk_ids_for_populate', return_value=['vdisk']):
                with mock.patch.object(populate, 'create_populate_request', return_value=request):
                    with mock.patch.object(populate.common, 'invoke_bsc_request', return_value=response):
                        with mock.patch.object(populate.common, 'print_request_result'):
                            with mock.patch.object(populate.common, 'is_successful_bsc_response', return_value=False):
                                with mock.patch.object(populate.common, 'dump_group_mapper_error') as dump_error:
                                    with self.assertRaisesRegex(SystemExit, '1'):
                                        populate.run_populate_mode(args)

        dump_error.assert_called_once_with(response, args)


class EntryPointTest(unittest.TestCase):
    def test_requires_exactly_one_mode(self):
        for args in (
            make_args(),
            make_args(snapshot_from_pdisk='1:2', destination_pdisk='3:4'),
        ):
            with self.subTest(args=args):
                with mock.patch.object(populate.common, 'print_status') as print_status:
                    with self.assertRaisesRegex(SystemExit, '1'):
                        populate.do(args)

                print_status.assert_called_once_with(
                    args,
                    success=False,
                    error_reason='Specify exactly one mode: --snapshot-from-pdisk or --destination-pdisk',
                )

    def test_converts_mode_exception_to_status(self):
        args = make_args(destination_pdisk='3:4')
        error = Exception('bad snapshot')

        with mock.patch.object(populate, 'run_populate_mode', side_effect=error):
            with mock.patch.object(populate.common, 'print_status') as print_status:
                with self.assertRaisesRegex(SystemExit, '1'):
                    populate.do(args)

        print_status.assert_called_once_with(args, success=False, error_reason=error)


if __name__ == '__main__':
    unittest.main()
