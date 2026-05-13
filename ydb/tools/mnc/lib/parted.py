import logging
import os
import re

from . import common, term


logger = logging.getLogger(__name__)


sd_pattern = r'(sd[a-z]+)\d*'
sd_comp = re.compile(sd_pattern)
nvme_pattern = r'(nvme\d+n\d+)(p\d+)?'
nvme_comp = re.compile(nvme_pattern)


def from_part_path_to_device_path(path: str):
    if path is None:
        return None
    dirs = path.split('/')
    if len(dirs) != 3 or dirs[0] != '' or dirs[1] != 'dev':
        return None
    if not dirs[2].startswith('sd') and not dirs[2].startswith('nvme'):
        return None
    comp = sd_comp if dirs[2].startswith('sd') else nvme_comp
    match = comp.fullmatch(dirs[2])
    dirs[2] = match[1]
    return '/' + os.path.join(*dirs)


'''
BYT;
/dev/nvme1n1:3201GB:unknown:512:512:gpt:Unknown;
1:1049kB:15,7MB:14,7MB::primary:;
2:15,7MB:3201GB:3201GB::kikimr_nvme_02:;
'''


class DevicePartInfo:
    def __init__(self, id: str, from_mem: common.Memory, to_mem: common.Memory, size: common.Memory, label: str):
        self.id = id
        self.from_mem = from_mem
        self.to_mem = to_mem
        self.size = size
        self.label = label


class DeviceInfo:
    def __init__(self, path: str, size: common.Memory, parts: list[DevicePartInfo], error: str = None):
        self.path = path
        self.size = size
        self.parts = parts
        self.error = error

    @staticmethod
    def make_error(path: str, error: str):
        return DeviceInfo(path, None, None, error)


size_pattern = r'[1-9]\d*[\.,]?\d*[kMGT]?B?'

about_device_pattern = r'/dev/\w+:({size_pattern}):\w*:\d*:\d*:\w*:[ \-\w]*:?\w*;'.format(size_pattern=size_pattern)
about_device_comp = re.compile(about_device_pattern)
about_part_pattern = r'([1-9]\d*):({size_pattern}):({size_pattern}):({size_pattern}):\w*:(\w*):\w*;'.format(
    size_pattern=size_pattern
)
about_part_comp = re.compile(about_part_pattern)


async def get_link(host: str, partlabel: str):
    cmd = f"sudo readlink -e /dev/disk/by-partlabel/'{partlabel}'"
    result = await term.ssh_run(host, cmd)
    if result.returncode:
        return None
    return result.stdout.strip()


def make_device_info(output: str):
    lines = output.split('\n')
    if len(lines) < 3:
        return None
    match = about_device_comp.fullmatch(lines[1])
    if not match:
        return None
    about_device = lines[1][:-1].split(':')
    path = about_device[0]
    device_size = common.Memory(about_device[1])
    part_lines = lines[2:]
    parts = []
    for part_line in part_lines:
        match = about_part_comp.fullmatch(part_line)
        if not match:
            continue
        id = match[1]
        from_mem = common.Memory(match[2])
        to_mem = common.Memory(match[3])
        size = common.Memory(match[4])
        label = match[5]
        parts.append(DevicePartInfo(id, from_mem, to_mem, size, label))
    return DeviceInfo(path, device_size, parts)


async def parted_info(host: str, device: common.Device):
    path = device.path if device.path else await get_link(host, device.partlabel)
    good_path = from_part_path_to_device_path(path)
    if not good_path:
        logger.error(f"not correct path: '{path}'")
        return DeviceInfo.make_error(device.path, f"not correct path: '{path}'")
    cmd = f"sudo parted '{good_path}' -m -s print"
    result = await term.ssh_run(host, cmd)
    if result.returncode:
        return DeviceInfo.make_error(path, result.stderr)
    info = make_device_info(result.stdout)
    if not info:
        logger.error(f"can't parse output on {host} by cmd: '{cmd}' stdout: '{result.stdout}'")
    return info


async def remove_part(host: str, path: str, part_id: str):
    if str(part_id) == '1':
        logger.error('rm was forbiden, can\'t remove first part host: {host} device: {path} part_id: {part_id}')
        return False
    cmd = "sudo parted '{0}' -m -s rm {1}".format(path, part_id)
    result = await term.ssh_run(host, cmd)
    return not result.returncode


async def make_part(host: str, path: str, label: str, last_end: common.Memory, part_end: common.Memory):
    cmd = "sudo parted {path} -s mkpart {label} {last_end} {part_end}".format(
        path=path, label=label, last_end=last_end, part_end=part_end
    )
    result = await term.ssh_run(host, cmd)
    return not result.returncode


async def parted_disk(host: str, device: common.Device, part_size: common.Memory = None, part_count: int = None):
    info = await parted_info(host, device)

    if not info:
        logger.error('failed to receive parted info host: {host}'.format(host=host))
        return False

    full = True
    if not part_count and part_size:
        part_count = int(info.size / part_size)
        logger.info(
            "part_count calculated for host: {host} part_count: {part_count} part_size: {part_size} device_size: {device_size}'".format(
                host=host, part_count=part_count, part_size=part_size, device_size=info.size
            )
        )
    elif not part_size and part_count:
        part_size = info.size / part_count
        logger.info(
            "part_size calculated for host: {host} part_count: {part_count} part_size: {part_size} device_size: {device_size}'".format(
                host=host, part_count=part_count, part_size=part_size, device_size=info.size
            )
        )
    elif not part_count and not part_size:
        raise ValueError()
    else:
        full = False

    with_error = False
    for part in info.parts:
        if part.id == '1':
            continue
        path = device.path
        ok = await remove_part(host, path, part.id)
        if not ok:
            partlabel = device.partlabel
            logger.error(
                'removing parts was failed; host: {host} device: {device} partlabel: {label} part: {part}'.format(
                    host=host, device=path, label=partlabel, part=part.id
                )
            )
            with_error = True
    if with_error:
        return False

    info = await parted_info(host, device)
    if len(info.parts) != 1:
        logger.error('error during removing old parts')
        raise RuntimeError()

    last_end = info.parts[0].to_mem
    for i in range(2, part_count + 2):
        part_end = last_end + part_size
        if full and i == part_count + 1:
            part_end = '100%'
        path = device.path
        label = device.partlabel
        if part_count != 1:
            label = '{0}_{1}'.format(label, i - 2)
        ok = await make_part(host, path, label, last_end, part_end)
        if not ok:
            with_error = True
            logger.error(
                'making parts was failed; host: {host} device: {device} partlabel: {label} part: {part}'.format(
                    host=host, device=path, label=label, part=i
                )
            )
            break
        last_end = part_end
    return not with_error
