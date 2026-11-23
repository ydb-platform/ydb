"""Expose nine fixed-size PDisk slices on one or three dedicated GPT partitions."""

import argparse
import grp
import os
from pathlib import Path
import stat
import subprocess
from uuid import UUID


def read_mapping(name):
    table = subprocess.check_output(["dmsetup", "table", name], text=True).split()
    uuid = subprocess.check_output(["dmsetup", "info", "-c", "--noheadings", "-o", "uuid", name], text=True).strip()
    return table, uuid


def slices(partitions, sectors):
    per_partition = 9 // len(partitions)
    mounted = {line.split()[2] for line in Path("/proc/self/mountinfo").read_text().splitlines()}
    result = {}
    for index, part_uuid in enumerate(partitions):
        source = Path("/dev/disk/by-partuuid") / str(part_uuid)
        info = source.stat()
        device = f"{os.major(info.st_rdev)}:{os.minor(info.st_rdev)}"
        sysdir = Path("/sys/dev/block") / device
        if not stat.S_ISBLK(info.st_mode) or not (sysdir / "partition").is_file() or device in mounted:
            raise ValueError(f"{source} must be an unmounted partition")
        if int((sysdir / "size").read_text()) < sectors * per_partition:
            raise ValueError(f"{source} is too small for {per_partition} slices of {sectors} sectors")
        names = []
        for offset in range(per_partition):
            i = index * per_partition + offset
            name = f"ydb_dc{i // 3 + 1}_node{i % 3 + 1}_pdisk"
            names.append(name)
            table = ["0", str(sectors), "linear", device, str(offset * sectors)]
            result[name] = (table, f"YDBDC-{part_uuid}-{name}")
        for holder in (sysdir / "holders").iterdir():
            dm_name = holder / "dm/name"
            if not dm_name.is_file() or dm_name.read_text().strip() not in names:
                raise ValueError(f"{source} has an unexpected holder {holder.name}")
    return result


def ensure_mappings(mappings):
    group = grp.getgrnam("ydb").gr_gid
    missing = []
    for name, mapping in mappings.items():
        if subprocess.run(["dmsetup", "info", name], capture_output=True).returncode:
            missing.append(name)
        elif read_mapping(name) != mapping:
            raise ValueError(f"{name} already maps a different device or range")
    for name in missing:
        table, uuid = mappings[name]
        subprocess.run(["dmsetup", "create", name, "--uuid", uuid, "--table", " ".join(table)], check=True)
    subprocess.run(["udevadm", "settle", "--timeout=30"], check=True)
    for name, mapping in mappings.items():
        target = Path("/dev/mapper") / name
        info = target.stat()
        target_name = Path("/sys/dev/block") / f"{os.major(info.st_rdev)}:{os.minor(info.st_rdev)}" / "dm/name"
        if not stat.S_ISBLK(info.st_mode) or target_name.read_text().strip() != name or read_mapping(name) != mapping:
            raise ValueError(f"unexpected mapped device for {target}")
        os.chown(target, 0, group)
        os.chmod(target, 0o660)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sectors", type=int, help="512-byte sectors per PDisk, multiple of 2048")
    parser.add_argument("partuuid", type=UUID, nargs="+", help="one GPT partition UUID or one per DC in DC order")
    args = parser.parse_args()
    if args.sectors <= 0 or args.sectors % 2048:
        parser.error("PDisk size must be a positive multiple of 2048 sectors")
    if len(args.partuuid) not in (1, 3) or len(set(args.partuuid)) != len(args.partuuid):
        parser.error("specify one partition or three distinct partitions in DC order")
    try:
        ensure_mappings(slices(args.partuuid, args.sectors))
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
