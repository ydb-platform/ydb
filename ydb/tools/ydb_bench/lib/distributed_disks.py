"""Host-local disk admission. Never initialize an existing disk implicitly."""

import fcntl
import hashlib
import os
from pathlib import Path
import stat
import struct

from ydb.tools.ydb_bench.lib.common import BenchmarkError


class DiskAdmission:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.handles = []
        self.identities = set()
        self.devices = set()
        self.paths = []

    def close(self):
        for descriptor in reversed(self.handles):
            os.close(descriptor)
        self.handles.clear()

    def verify(self):
        for path, descriptor in self.paths:
            expected, actual = os.fstat(descriptor), os.stat(path)
            if (expected.st_dev, expected.st_ino, expected.st_rdev) != (actual.st_dev, actual.st_ino, actual.st_rdev):
                raise BenchmarkError("Disk path changed after admission: {}".format(path))

    def prepare(self, disk, session, node, index, reset):
        if disk["source"] == "sector_map":
            return "SectorMap:map_{}_{}:{}:NONE".format(node, index, disk["size_gib"]), False
        self.directory.mkdir(parents=True, exist_ok=True)
        if self.directory.is_symlink():
            raise BenchmarkError("Refusing symlinked file-disks directory")
        created = False
        if disk["source"] == "file":
            if disk.get("temporary"):
                parent = self.directory / session
                parent.mkdir(exist_ok=True)
                if parent.is_symlink():
                    raise BenchmarkError("Refusing symlinked temporary disk directory")
                path = parent / "{}-{}.img".format(node, index)
            else:
                path = self.directory / disk.get("name", Path(disk.get("path", "")).name)
                if "path" in disk and Path(disk["path"]) != path:
                    raise BenchmarkError("Persistent files must be inside {}".format(self.directory))
            try:
                descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                created = True
            except FileExistsError:
                if disk.get("temporary") or not reset:
                    raise BenchmarkError("Disk already exists; enable reset-disks explicitly: {}".format(path))
                descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK)
        else:
            if not reset:
                raise BenchmarkError("Block devices require explicit reset-disks permission")
            alias = (
                Path("/dev/disk/by-partlabel") / disk["label"] if disk["source"] == "partlabel" else Path(disk["path"])
            )
            path = alias.resolve(strict=True)
            if not stat.S_ISBLK(path.stat().st_mode):
                raise BenchmarkError("Disk is not a block device: {}".format(alias))
            # Linux exclusive block claims reject mounted devices and active holders.
            descriptor = os.open(path, os.O_RDWR | os.O_EXCL | os.O_NONBLOCK | os.O_NOFOLLOW)
        self.handles.append(descriptor)
        value = os.fstat(descriptor)
        block = stat.S_ISBLK(value.st_mode)
        if disk["source"] != "file" and not block:
            raise BenchmarkError("Disk changed or is not a block device")
        if disk["source"] == "file" and not stat.S_ISREG(value.st_mode):
            raise BenchmarkError("File disk must be a regular file")
        identity = ("block", value.st_rdev) if block else ("file", value.st_dev, value.st_ino)
        if identity in self.identities:
            raise BenchmarkError("The same disk is assigned more than once")
        self.identities.add(identity)
        if block:
            device = Path("/sys/dev/block/{}:{}".format(os.major(value.st_rdev), os.minor(value.st_rdev))).resolve(
                strict=True
            )
            family = device.parent if (device / "partition").exists() else device
            # Be conservative: do not share different partitions of one drive.
            if family in self.devices:
                raise BenchmarkError("Overlapping disk/partition assignments")
            self.devices.add(family)
            mounted = {line.split()[2] for line in Path("/proc/self/mountinfo").read_text().splitlines()}
            for related in [family, *[p for p in family.iterdir() if (p / "partition").exists()]]:
                if (related / "dev").read_text().strip() in mounted:
                    raise BenchmarkError("Disk or one of its partitions is mounted")
                if (related / "holders").exists() and any((related / "holders").iterdir()):
                    raise BenchmarkError("Disk has active holders")
            size = struct.unpack("Q", fcntl.ioctl(descriptor, 0x80081272, b"\0" * 8))[0]  # BLKGETSIZE64
            if size < 1024**3:
                raise BenchmarkError("Block device must be at least 1 GiB")
        locks = self.directory / ".locks"
        locks.mkdir(exist_ok=True)
        if locks.is_symlink():
            raise BenchmarkError("Refusing symlinked disk lock directory")
        key = hashlib.sha256(repr(identity).encode()).hexdigest()
        lock = os.open(locks / key, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        self.handles.append(lock)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Detect an existing formatter's lock without holding it across YDB's own formatting.
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        if disk["source"] == "file":
            size = disk["size_gib"] * 1024**3
            if not created and value.st_size != size:
                raise BenchmarkError("Existing file size differs from the configured disk size")
            if created:
                os.ftruncate(descriptor, size)
        self.paths.append((path, descriptor))
        return str(path), not created
