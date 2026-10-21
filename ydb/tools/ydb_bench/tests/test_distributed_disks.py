import tempfile
from pathlib import Path
import unittest
from unittest import mock

from ydb.tools.ydb_bench.lib import distributed_disks
from ydb.tools.ydb_bench.lib.common import BenchmarkError
from ydb.tools.ydb_bench.lib.distributed_disks import DiskAdmission


class DiskAdmissionTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.admission = DiskAdmission(self.root)
        self.disk = {"source": "file", "name": "persistent.img", "size_gib": 1}

    def tearDown(self):
        self.admission.close()
        self.directory.cleanup()

    def test_new_file_and_explicit_reset(self):
        path, reset = self.admission.prepare(self.disk, "session", 1, 0, False)
        self.assertFalse(reset)
        self.assertEqual(Path(path).stat().st_size, 1024**3)
        self.admission.close()
        self.admission = DiskAdmission(self.root)
        with self.assertRaisesRegex(BenchmarkError, "already exists"):
            self.admission.prepare(self.disk, "session", 1, 0, False)
        self.assertTrue(self.admission.prepare(self.disk, "session", 1, 0, True)[1])
        self.admission.verify()

    def test_temporary_file_and_path_identity(self):
        disk = {"source": "file", "temporary": True, "size_gib": 1}
        path, reset = self.admission.prepare(disk, "session", 1, 0, False)
        self.assertFalse(reset)
        self.assertEqual(Path(path), self.root / "session" / "1-0.img")
        Path(path).rename(Path(path).with_suffix(".old"))
        Path(path).touch()
        with self.assertRaisesRegex(BenchmarkError, "changed"):
            self.admission.verify()

    def test_external_files_and_regular_files_as_devices_rejected(self):
        with self.assertRaisesRegex(BenchmarkError, "inside"):
            self.admission.prepare({**self.disk, "path": "/outside/disk"}, "session", 1, 0, True)
        path = self.root / "not-device"
        path.touch()
        with mock.patch.object(distributed_disks.os, "open") as open_disk:
            with self.assertRaisesRegex(BenchmarkError, "not a block"):
                self.admission.prepare({"source": "block_device", "path": str(path)}, "session", 1, 0, True)
            open_disk.assert_not_called()

    def test_device_permission_rejection_does_not_access_devices(self):
        # Device tests must not depend on /dev entries or privileged CI access.
        for disk in (
            {"source": "block_device", "path": "/dev/benchmark-test-does-not-exist"},
            {"source": "partlabel", "label": "benchmark-test-does-not-exist"},
        ):
            with self.subTest(source=disk["source"]):
                with (
                    mock.patch.object(Path, "resolve") as resolve,
                    mock.patch.object(distributed_disks.os, "open") as open_disk,
                    mock.patch.object(distributed_disks.fcntl, "ioctl") as ioctl,
                ):
                    with self.assertRaisesRegex(BenchmarkError, "permission"):
                        self.admission.prepare(disk, "session", 1, 0, False)
                    resolve.assert_not_called()
                    open_disk.assert_not_called()
                    ioctl.assert_not_called()

    def test_existing_size_and_symlink_rejected(self):
        path = self.root / self.disk["name"]
        path.write_bytes(b"keep")
        with self.assertRaisesRegex(BenchmarkError, "size differs"):
            self.admission.prepare(self.disk, "session", 1, 0, True)
        self.assertEqual(path.read_bytes(), b"keep")
        self.admission.close()
        path.unlink()
        path.symlink_to(self.root / "outside")
        with self.assertRaises(OSError):
            self.admission.prepare(self.disk, "session", 1, 0, True)
