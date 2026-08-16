import os
import re

from . import common


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
    if match is None:
        return None
    dirs[2] = match[1]
    return '/' + os.path.join(*dirs)


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
