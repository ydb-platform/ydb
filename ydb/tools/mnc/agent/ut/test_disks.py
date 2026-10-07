import re
import unittest
from unittest import mock

from ydb.tools.mnc.agent import config
from ydb.tools.mnc.agent.schemas.disk import (
    DiskCheckRequest,
    DiskDeviceSchema,
    DiskInfoRequest,
    DiskOperationRequest,
)
from ydb.tools.mnc.agent.services import disks
from ydb.tools.mnc.lib import common, device as device_lib, term


class DiskShellTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.old_mnc_home = config.mnc_home
        self.old_partlabels = config.managed_partlabels
        self.old_regexps = config.managed_partlabel_regexps
        config.mnc_home = "/mnc"
        config.managed_partlabels = {"label1", "label1_0", "label1_1"}
        config.managed_partlabel_regexps = []
        self.service = disks.DiskService()

    def tearDown(self):
        config.mnc_home = self.old_mnc_home
        config.managed_partlabels = self.old_partlabels
        config.managed_partlabel_regexps = self.old_regexps

    async def test_get_link_uses_sudo_readlink(self):
        calls = []

        async def run(args):
            calls.append(args)
            return term.Result(0, "/dev/sdb2\n", "")

        with mock.patch.object(disks.term, "run", run):
            self.assertEqual(await self.service.get_link("label1"), "/dev/sdb2")

        self.assertEqual(calls, [["sudo", "readlink", "-e", "/dev/disk/by-partlabel/label1"]])

    async def test_parted_info_uses_sudo_parted_on_device_path(self):
        calls = []
        expected_info = device_lib.DeviceInfo(path="/dev/sdb", size=common.Memory("100GB"), parts=[])

        async def get_link(partlabel):
            return "/dev/sdb2"

        async def run(args):
            calls.append(args)
            return term.Result(0, "parted output", "")

        with mock.patch.object(self.service, "get_link", get_link), \
                mock.patch.object(disks.term, "run", run), \
                mock.patch.object(disks.device_lib, "from_part_path_to_device_path", lambda path: "/dev/sdb"), \
                mock.patch.object(disks.device_lib, "make_device_info", lambda output: expected_info):
            self.assertIs(await self.service.parted_info(common.Device("label1", None)), expected_info)

        self.assertEqual(calls, [["sudo", "parted", "/dev/sdb", "-m", "-s", "print"]])

    async def test_parted_info_returns_error_on_parted_failure(self):
        async def get_link(partlabel):
            return "/dev/sdb2"

        async def run(args):
            return term.Result(1, "", "parted failed")

        with mock.patch.object(self.service, "get_link", get_link), \
                mock.patch.object(disks.term, "run", run), \
                mock.patch.object(disks.device_lib, "from_part_path_to_device_path", lambda path: "/dev/sdb"):
            info = await self.service.parted_info(common.Device("label1", None))

        self.assertEqual(info.path, "/dev/sdb")
        self.assertEqual(info.error, "parted failed")

    async def test_remove_part_refuses_first_partition_without_shelling_out(self):
        async def run(args):
            raise AssertionError("parted rm should not be called")

        with mock.patch.object(disks.term, "run", run):
            self.assertFalse(await self.service.remove_part("/dev/sdb", "1"))

    async def test_remove_part_uses_sudo_parted_rm(self):
        calls = []

        async def run(args):
            calls.append(args)
            return term.Result(0, "", "")

        with mock.patch.object(disks.term, "run", run):
            self.assertTrue(await self.service.remove_part("/dev/sdb", "2"))

        self.assertEqual(calls, [["sudo", "parted", "/dev/sdb", "-m", "-s", "rm", "2"]])

    async def test_make_part_uses_sudo_parted_mkpart(self):
        calls = []

        async def run(args):
            calls.append(args)
            return term.Result(0, "", "")

        with mock.patch.object(disks.term, "run", run):
            self.assertTrue(await self.service.make_part("/dev/sdb", "label1_0", "10GB", "20GB"))

        self.assertEqual(calls, [["sudo", "parted", "/dev/sdb", "-s", "mkpart", "label1_0", "10GB", "20GB"]])

    async def test_obliterate_uses_local_ydb_admin_command(self):
        part1 = device_lib.DevicePartInfo(id="1", from_mem=common.Memory(0), to_mem=common.Memory("1GB"), size=common.Memory("1GB"), label="skip")
        part2 = device_lib.DevicePartInfo(id="2", from_mem=common.Memory("1GB"), to_mem=common.Memory("2GB"), size=common.Memory("1GB"), label="label1_0")
        info = device_lib.DeviceInfo(path="/dev/sdb", size=common.Memory("2GB"), parts=[part1, part2])
        calls = []

        async def parted_info(device):
            return info

        async def run(args):
            calls.append(args)
            return term.Result(0, "", "")

        with mock.patch.object(self.service, "parted_info", parted_info), \
                mock.patch.object(disks.term, "run", run):
            response = await self.service.obliterate(DiskOperationRequest(devices=[DiskDeviceSchema(partlabel="label1")]))

        self.assertTrue(response.success)
        self.assertEqual(calls, [[
            "sudo",
            "/mnc/ydb/bin/ydb",
            "admin",
            "blobstorage",
            "disk",
            "obliterate",
            "/dev/disk/by-partlabel/label1_0",
        ]])

    async def test_obliterate_reports_ydb_command_failure(self):
        part = device_lib.DevicePartInfo(id="2", from_mem=common.Memory("1GB"), to_mem=common.Memory("2GB"), size=common.Memory("1GB"), label="label1_0")
        info = device_lib.DeviceInfo(path="/dev/sdb", size=common.Memory("2GB"), parts=[part])

        async def parted_info(device):
            return info

        async def run(args):
            return term.Result(1, "", "obliterate failed")

        with mock.patch.object(self.service, "parted_info", parted_info), \
                mock.patch.object(disks.term, "run", run):
            response = await self.service.obliterate(DiskOperationRequest(devices=[DiskDeviceSchema(partlabel="label1")]))

        self.assertFalse(response.success)
        self.assertIn("obliterate failed", response.operations[0].message)


class DiskValidationTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.old_mnc_home = config.mnc_home
        self.old_partlabels = config.managed_partlabels
        self.old_regexps = config.managed_partlabel_regexps
        config.mnc_home = "/mnc"
        config.managed_partlabels = {"label1"}
        config.managed_partlabel_regexps = []
        self.service = disks.DiskService()

    def tearDown(self):
        config.mnc_home = self.old_mnc_home
        config.managed_partlabels = self.old_partlabels
        config.managed_partlabel_regexps = self.old_regexps

    def test_invalid_partlabel_is_rejected(self):
        with self.assertRaises(ValueError):
            self.service._validate_partlabel("bad label!")

    def test_invalid_device_path_is_rejected(self):
        with self.assertRaises(ValueError):
            self.service._validate_device_path("/tmp/etc/passwd")

    def test_unmanaged_partlabel_is_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            self.service._validate_partlabel("other_label")
        self.assertIn("not managed", str(ctx.exception))

    def test_validate_partlabel_when_no_rules_configured(self):
        config.managed_partlabels = set()
        config.managed_partlabel_regexps = []
        with self.assertRaises(ValueError) as ctx:
            self.service._validate_partlabel("label1")
        self.assertIn("no managed disk partlabels configured", str(ctx.exception))

    def test_make_device_returns_common_device_for_valid_input(self):
        device = self.service._make_device(DiskDeviceSchema(partlabel="label1", device="/dev/sdb"))
        self.assertEqual(device.partlabel, "label1")
        self.assertEqual(device.path, "/dev/sdb")


class DiskGetPartlabelsTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.old_partlabels = config.managed_partlabels
        self.old_regexps = config.managed_partlabel_regexps
        config.managed_partlabels = {"label1", "label1_0", "label1_1"}
        config.managed_partlabel_regexps = []
        self.service = disks.DiskService()

    def tearDown(self):
        config.managed_partlabels = self.old_partlabels
        config.managed_partlabel_regexps = self.old_regexps

    async def test_returns_sorted_filtered_managed_labels(self):
        with mock.patch.object(disks.os, "listdir", return_value=["label1_1", "label1", "other", "label1_0"]):
            self.assertEqual(await self.service.get_partlabels(), ["label1", "label1_0", "label1_1"])

    async def test_pattern_filter_restricts_results(self):
        # Pattern itself must be a managed partlabel; "label1" is a substring of "label1_0"/"label1_1" too.
        with mock.patch.object(disks.os, "listdir", return_value=["label1", "label1_0", "label1_1", "other"]):
            self.assertEqual(await self.service.get_partlabels("label1"), ["label1", "label1_0", "label1_1"])

    async def test_pattern_filter_rejects_unmanaged_pattern(self):
        with mock.patch.object(disks.os, "listdir", return_value=["label1"]):
            with self.assertRaises(ValueError):
                await self.service.get_partlabels("label1_")

    async def test_missing_by_partlabel_directory_returns_empty(self):
        def listdir(path):
            raise FileNotFoundError("missing")

        with mock.patch.object(disks.os, "listdir", listdir):
            self.assertEqual(await self.service.get_partlabels(), [])

    async def test_unexpected_listdir_error_returns_none(self):
        def listdir(path):
            raise PermissionError("denied")

        with mock.patch.object(disks.os, "listdir", listdir):
            self.assertIsNone(await self.service.get_partlabels())


class DiskCheckTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.old_partlabels = config.managed_partlabels
        self.old_regexps = config.managed_partlabel_regexps
        config.managed_partlabels = {"label1", "label1_0", "label1_1"}
        config.managed_partlabel_regexps = []
        self.service = disks.DiskService()

    def tearDown(self):
        config.managed_partlabels = self.old_partlabels
        config.managed_partlabel_regexps = self.old_regexps

    async def test_disks_for_use_success(self):
        async def get_partlabels(pattern=None):
            return ["label1"]

        with mock.patch.object(self.service, "get_partlabels", get_partlabels):
            response = await self.service.check(
                DiskCheckRequest(disks_for_use=[DiskDeviceSchema(partlabel="label1")])
            )

        self.assertTrue(response.success)
        self.assertTrue(response.checks[0].success)

    async def test_disks_for_use_missing_label_fails(self):
        async def get_partlabels(pattern=None):
            return []

        with mock.patch.object(self.service, "get_partlabels", get_partlabels):
            response = await self.service.check(
                DiskCheckRequest(disks_for_use=[DiskDeviceSchema(partlabel="label1")])
            )

        self.assertFalse(response.success)
        self.assertIn("not found", response.checks[0].message)

    async def test_disks_for_split_missing_device_fails(self):
        response = await self.service.check(
            DiskCheckRequest(disks_for_split=[DiskDeviceSchema(partlabel="label1")])
        )

        self.assertFalse(response.success)
        self.assertIn("device is required", response.checks[0].message)

    async def test_disks_for_split_no_prefix_match(self):
        async def get_partlabels(pattern=None):
            return ["label1"]

        with mock.patch.object(self.service, "get_partlabels", get_partlabels):
            response = await self.service.check(
                DiskCheckRequest(
                    disks_for_split=[DiskDeviceSchema(partlabel="label1_0", device="/dev/sdb")],
                )
            )

        self.assertFalse(response.success)
        self.assertIn("prefix label1_0 not found", response.checks[0].message)

    async def test_disks_for_split_wrong_device_symlink(self):
        async def get_partlabels(pattern=None):
            return ["label1", "label1_0"]

        async def get_link(label):
            return "/dev/sdc2"  # not under /dev/sdb

        with mock.patch.object(self.service, "get_partlabels", get_partlabels), \
                mock.patch.object(self.service, "get_link", get_link):
            response = await self.service.check(
                DiskCheckRequest(
                    disks_for_split=[DiskDeviceSchema(partlabel="label1", device="/dev/sdb")],
                )
            )

        self.assertFalse(response.success)
        self.assertIn("expected prefix /dev/sdb", response.checks[0].message)

    async def test_one_failed_check_makes_response_fail(self):
        async def get_partlabels(pattern=None):
            return ["label1"]

        with mock.patch.object(self.service, "get_partlabels", get_partlabels):
            response = await self.service.check(
                DiskCheckRequest(
                    disks_for_use=[
                        DiskDeviceSchema(partlabel="label1"),
                        DiskDeviceSchema(partlabel="label1_0"),
                    ],
                )
            )

        self.assertFalse(response.success)
        self.assertEqual(len(response.checks), 2)
        self.assertTrue(any(c.success for c in response.checks))
        self.assertTrue(any(not c.success for c in response.checks))


class DiskInfoTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.old_partlabels = config.managed_partlabels
        self.old_regexps = config.managed_partlabel_regexps
        config.managed_partlabels = {"label1"}
        config.managed_partlabel_regexps = []
        self.service = disks.DiskService()

    def tearDown(self):
        config.managed_partlabels = self.old_partlabels
        config.managed_partlabel_regexps = self.old_regexps

    async def test_info_with_explicit_devices(self):
        info = device_lib.DeviceInfo(path="/dev/sdb", size=common.Memory("100GB"), parts=[])

        async def parted_info(device):
            return info

        with mock.patch.object(self.service, "parted_info", parted_info):
            response = await self.service.info(
                DiskInfoRequest(devices=[DiskDeviceSchema(partlabel="label1")])
            )

        self.assertEqual(len(response.disks), 1)
        self.assertEqual(response.disks[0].path, "/dev/sdb")

    async def test_info_discovers_partlabels_when_devices_empty(self):
        info = device_lib.DeviceInfo(path="/dev/sdb", size=common.Memory("100GB"), parts=[])

        async def get_partlabels(pattern=None):
            return ["label1"]

        async def parted_info(device):
            return info

        with mock.patch.object(self.service, "get_partlabels", get_partlabels), \
                mock.patch.object(self.service, "parted_info", parted_info):
            response = await self.service.info(DiskInfoRequest())

        self.assertEqual([d.partlabel for d in response.disks], ["label1"])

    async def test_info_discovery_failure_returns_error_schema(self):
        async def get_partlabels(pattern=None):
            return None

        with mock.patch.object(self.service, "get_partlabels", get_partlabels):
            response = await self.service.info(DiskInfoRequest())

        self.assertEqual(len(response.disks), 1)
        self.assertEqual(response.disks[0].error, "failed to list partlabels")

    async def test_info_per_device_validation_failure(self):
        response = await self.service.info(
            DiskInfoRequest(devices=[DiskDeviceSchema(partlabel="bad!")])
        )

        self.assertEqual(len(response.disks), 1)
        self.assertIn("invalid partlabel", response.disks[0].error)


class DiskPartitionTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.old_partlabels = config.managed_partlabels
        self.old_regexps = config.managed_partlabel_regexps
        config.managed_partlabels = {"label1"}
        config.managed_partlabel_regexps = [re.compile(r"label1(_\d+)?")]
        self.service = disks.DiskService()

    def tearDown(self):
        config.managed_partlabels = self.old_partlabels
        config.managed_partlabel_regexps = self.old_regexps

    def _make_info(self, parts):
        return device_lib.DeviceInfo(path="/dev/sdb", size=common.Memory("10GB"), parts=parts)

    def _first_part(self):
        return device_lib.DevicePartInfo(
            id="1",
            from_mem=common.Memory(0),
            to_mem=common.Memory(0),
            size=common.Memory(0),
            label="reserved",
        )

    async def test_parted_disk_splits_by_part_count(self):
        info = self._make_info([self._first_part()])
        infos = [
            self._make_info([
                self._first_part(),
                device_lib.DevicePartInfo("2", common.Memory(0), common.Memory("5GB"), common.Memory("5GB"), "old"),
            ]),
            info,
        ]

        async def parted_info(device):
            return infos.pop(0) if len(infos) > 1 else info

        remove_calls = []
        make_calls = []

        async def remove_part(path, part_id):
            remove_calls.append((path, part_id))
            return True

        async def make_part(path, label, last_end, part_end):
            make_calls.append((path, label, last_end, part_end))
            return True

        with mock.patch.object(self.service, "parted_info", parted_info), \
                mock.patch.object(self.service, "remove_part", remove_part), \
                mock.patch.object(self.service, "make_part", make_part):
            ok = await self.service.parted_disk(common.Device("label1", None), part_count=2)

        self.assertTrue(ok)
        self.assertEqual(remove_calls, [("/dev/sdb", "2")])
        self.assertEqual(len(make_calls), 2)
        self.assertEqual(make_calls[0][1], "label1_0")
        self.assertEqual(make_calls[1][1], "label1_1")
        # last partition should end at 100%
        self.assertEqual(make_calls[-1][3], "100%")

    async def test_parted_disk_splits_by_part_size(self):
        info = self._make_info([self._first_part()])

        async def parted_info(device):
            return info

        async def remove_part(*args):
            return True

        make_calls = []

        async def make_part(path, label, last_end, part_end):
            make_calls.append((path, label, last_end, part_end))
            return True

        with mock.patch.object(self.service, "parted_info", parted_info), \
                mock.patch.object(self.service, "remove_part", remove_part), \
                mock.patch.object(self.service, "make_part", make_part):
            ok = await self.service.parted_disk(
                common.Device("label1", None), part_size=common.Memory("5GB")
            )

        self.assertTrue(ok)
        self.assertEqual(len(make_calls), 2)

    async def test_parted_disk_with_both_part_size_and_count_is_not_full(self):
        info = self._make_info([self._first_part()])

        async def parted_info(device):
            return info

        async def remove_part(*args):
            return True

        make_calls = []

        async def make_part(path, label, last_end, part_end):
            make_calls.append((path, label, last_end, part_end))
            return True

        with mock.patch.object(self.service, "parted_info", parted_info), \
                mock.patch.object(self.service, "remove_part", remove_part), \
                mock.patch.object(self.service, "make_part", make_part):
            ok = await self.service.parted_disk(
                common.Device("label1", None), part_size=common.Memory("1GB"), part_count=2
            )

        self.assertTrue(ok)
        # When both are provided, the last partition end is not forced to 100%.
        self.assertNotEqual(make_calls[-1][3], "100%")

    async def test_unite_forces_single_partition(self):
        info = self._make_info([self._first_part()])

        async def parted_info(device):
            return info

        async def remove_part(*args):
            return True

        make_calls = []

        async def make_part(path, label, last_end, part_end):
            make_calls.append((path, label, last_end, part_end))
            return True

        with mock.patch.object(self.service, "parted_info", parted_info), \
                mock.patch.object(self.service, "remove_part", remove_part), \
                mock.patch.object(self.service, "make_part", make_part):
            response = await self.service.unite(
                DiskOperationRequest(devices=[DiskDeviceSchema(partlabel="label1")])
            )

        self.assertTrue(response.success)
        self.assertEqual(len(make_calls), 1)
        self.assertEqual(make_calls[0][1], "label1")
        self.assertEqual(make_calls[0][3], "100%")

    async def test_remove_part_failure_aborts(self):
        info = self._make_info([
            self._first_part(),
            device_lib.DevicePartInfo("2", common.Memory(0), common.Memory("5GB"), common.Memory("5GB"), "old"),
        ])

        async def parted_info(device):
            return info

        async def remove_part(*args):
            return False

        async def make_part(*args):
            raise AssertionError("should not make parts after failed remove")

        with mock.patch.object(self.service, "parted_info", parted_info), \
                mock.patch.object(self.service, "remove_part", remove_part), \
                mock.patch.object(self.service, "make_part", make_part):
            ok = await self.service.parted_disk(common.Device("label1", None), part_count=2)

        self.assertFalse(ok)

    async def test_post_remove_state_invalid_aborts(self):
        first_call = self._make_info([
            self._first_part(),
            device_lib.DevicePartInfo("2", common.Memory(0), common.Memory("5GB"), common.Memory("5GB"), "old"),
        ])
        # After removal, parted reports an error which should abort
        second_call = device_lib.DeviceInfo.make_error("/dev/sdb", "boom")
        sequence = [first_call, second_call]

        async def parted_info(device):
            return sequence.pop(0) if sequence else second_call

        async def remove_part(*args):
            return True

        async def make_part(*args):
            raise AssertionError("should not make parts after invalid post-remove state")

        with mock.patch.object(self.service, "parted_info", parted_info), \
                mock.patch.object(self.service, "remove_part", remove_part), \
                mock.patch.object(self.service, "make_part", make_part):
            ok = await self.service.parted_disk(common.Device("label1", None), part_count=1)

        self.assertFalse(ok)

    async def test_make_part_failure_aborts(self):
        info = self._make_info([self._first_part()])

        async def parted_info(device):
            return info

        async def remove_part(*args):
            return True

        async def make_part(*args):
            return False

        with mock.patch.object(self.service, "parted_info", parted_info), \
                mock.patch.object(self.service, "remove_part", remove_part), \
                mock.patch.object(self.service, "make_part", make_part):
            ok = await self.service.parted_disk(common.Device("label1", None), part_count=2)

        self.assertFalse(ok)


class DiskObliterateTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.old_mnc_home = config.mnc_home
        self.old_partlabels = config.managed_partlabels
        self.old_regexps = config.managed_partlabel_regexps
        config.mnc_home = "/mnc"
        config.managed_partlabels = {"label1", "label1_0", "label1_1"}
        config.managed_partlabel_regexps = []
        self.service = disks.DiskService()

    def tearDown(self):
        config.mnc_home = self.old_mnc_home
        config.managed_partlabels = self.old_partlabels
        config.managed_partlabel_regexps = self.old_regexps

    async def test_obliterate_skips_first_and_unlabeled_partitions(self):
        parts = [
            device_lib.DevicePartInfo("1", common.Memory(0), common.Memory("1GB"), common.Memory("1GB"), "label1_0"),
            device_lib.DevicePartInfo("2", common.Memory("1GB"), common.Memory("2GB"), common.Memory("1GB"), ""),
            device_lib.DevicePartInfo("3", common.Memory("2GB"), common.Memory("3GB"), common.Memory("1GB"), "label1_1"),
        ]
        info = device_lib.DeviceInfo(path="/dev/sdb", size=common.Memory("3GB"), parts=parts)
        calls = []

        async def parted_info(device):
            return info

        async def run(args):
            calls.append(args)
            return term.Result(0, "", "")

        with mock.patch.object(self.service, "parted_info", parted_info), \
                mock.patch.object(disks.term, "run", run):
            response = await self.service.obliterate(
                DiskOperationRequest(devices=[DiskDeviceSchema(partlabel="label1")])
            )

        self.assertTrue(response.success)
        # Only label1_1 (id=3) should be obliterated.
        self.assertEqual(len(calls), 1)
        self.assertIn("/dev/disk/by-partlabel/label1_1", calls[0])

    async def test_obliterate_rejects_unmanaged_label(self):
        parts = [
            device_lib.DevicePartInfo("1", common.Memory(0), common.Memory("1GB"), common.Memory("1GB"), "skip"),
            device_lib.DevicePartInfo("2", common.Memory("1GB"), common.Memory("2GB"), common.Memory("1GB"), "stranger"),
        ]
        info = device_lib.DeviceInfo(path="/dev/sdb", size=common.Memory("2GB"), parts=parts)

        async def parted_info(device):
            return info

        async def run(args):
            raise AssertionError("ydb obliterate must not run on unmanaged labels")

        with mock.patch.object(self.service, "parted_info", parted_info), \
                mock.patch.object(disks.term, "run", run):
            response = await self.service.obliterate(
                DiskOperationRequest(devices=[DiskDeviceSchema(partlabel="label1")])
            )

        self.assertFalse(response.success)
        self.assertIn("not managed", response.operations[0].message)

    async def test_obliterate_mixed_command_failures(self):
        parts = [
            device_lib.DevicePartInfo("1", common.Memory(0), common.Memory("1GB"), common.Memory("1GB"), "skip"),
            device_lib.DevicePartInfo("2", common.Memory("1GB"), common.Memory("2GB"), common.Memory("1GB"), "label1_0"),
            device_lib.DevicePartInfo("3", common.Memory("2GB"), common.Memory("3GB"), common.Memory("1GB"), "label1_1"),
        ]
        info = device_lib.DeviceInfo(path="/dev/sdb", size=common.Memory("3GB"), parts=parts)
        call_count = {"n": 0}

        async def parted_info(device):
            return info

        async def run(args):
            call_count["n"] += 1
            if "label1_0" in args[-1]:
                return term.Result(1, "", "boom")
            return term.Result(0, "", "")

        with mock.patch.object(self.service, "parted_info", parted_info), \
                mock.patch.object(disks.term, "run", run):
            response = await self.service.obliterate(
                DiskOperationRequest(devices=[DiskDeviceSchema(partlabel="label1")])
            )

        self.assertFalse(response.success)
        self.assertIn("label1_0", response.operations[0].message)
        # Still attempted the second partition (label1_1).
        self.assertEqual(call_count["n"], 2)

    async def test_obliterate_parted_error_short_circuits(self):
        info = device_lib.DeviceInfo.make_error("/dev/sdb", "parted busted")

        async def parted_info(device):
            return info

        async def run(args):
            raise AssertionError("run should not be called when parted_info has error")

        with mock.patch.object(self.service, "parted_info", parted_info), \
                mock.patch.object(disks.term, "run", run):
            response = await self.service.obliterate(
                DiskOperationRequest(devices=[DiskDeviceSchema(partlabel="label1")])
            )

        self.assertFalse(response.success)
        self.assertEqual(response.operations[0].message, "parted busted")
