import multiprocessing
import os
import re


def _read_text(path):
    try:
        with open(path) as stream:
            return stream.read().strip()
    except (IOError, OSError):
        return ''


def _unescape_mount_path(path):
    return re.sub(r'\\([0-7]{3})', lambda match: chr(int(match.group(1), 8)), path)


def _cgroup_cpu_limits():
    groups = {}
    for line in _read_text('/proc/self/cgroup').splitlines():
        fields = line.split(':', 2)
        if len(fields) == 3:
            for controller in fields[1].split(','):
                groups[controller] = fields[2]

    for line in _read_text('/proc/self/mountinfo').splitlines():
        mount, separator, filesystem = line.partition(' - ')
        fields, fs = mount.split(), filesystem.split()
        if not separator or len(fields) < 6 or len(fs) < 3:
            continue
        if fs[0] == 'cgroup2':
            group = groups.get('')
        elif fs[0] == 'cgroup' and 'cpu' in fs[2].split(','):
            group = groups.get('cpu')
        else:
            continue
        if group is None:
            continue

        root, mountpoint = map(_unescape_mount_path, fields[3:5])
        mountpoint = os.path.normpath(mountpoint)
        relative = os.path.relpath(group, root)
        if relative == '..' or relative.startswith('../'):
            continue
        directory = os.path.normpath(os.path.join(mountpoint, relative))
        # A parent cgroup can impose a tighter limit than the process's own group.
        while True:
            if fs[0] == 'cgroup2':
                quota = _read_text(os.path.join(directory, 'cpu.max')).split()
            else:
                quota = [_read_text(os.path.join(directory, name)) for name in
                         ('cpu.cfs_quota_us', 'cpu.cfs_period_us')]
            try:
                maximum, period = map(int, quota)
                if maximum > 0 and period > 0:
                    yield (maximum + period - 1) // period
            except ValueError:
                pass  # Includes unlimited quotas (v2 "max") and missing files.
            if directory == mountpoint:
                break
            directory = os.path.dirname(directory)


def available_cpu_count():
    """Count usable CPUs, bounded by affinity and visible cgroup CPU quotas."""
    try:
        count = len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        try:
            count = multiprocessing.cpu_count()
        except NotImplementedError:
            count = 1
    return max(1, min([count] + list(_cgroup_cpu_limits())))
