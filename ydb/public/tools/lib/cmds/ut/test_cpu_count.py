import pytest

from ydb.public.tools.lib.cmds import cpu_count


@pytest.fixture
def cgroups(monkeypatch):
    files = {}
    monkeypatch.setattr(cpu_count, '_read_text', lambda path: files.get(path, ''))
    monkeypatch.setattr(cpu_count.os, 'sched_getaffinity', lambda pid: set(range(8)), raising=False)
    return files


@pytest.mark.parametrize('quota,expected', [
    ('100000 100000', 1), ('150000 100000', 2), ('50000 100000', 1),
    ('1600000 100000', 8), ('max 100000', 8), ('', 8), ('bad', 8), ('100000 0', 8),
])
def test_cgroup_v2_quota(cgroups, quota, expected):
    cgroups.update({
        '/proc/self/cgroup': '0::/',
        '/proc/self/mountinfo': '30 20 0:30 / /sys/fs/cgroup ro - cgroup2 cgroup rw',
        '/sys/fs/cgroup/cpu.max': quota,
    })
    assert cpu_count.available_cpu_count() == expected


@pytest.mark.parametrize('quota,expected', [('250000', 3), ('-1', 8), ('bad', 8)])
def test_cgroup_v1_quota(cgroups, quota, expected):
    cgroups.update({
        '/proc/self/cgroup': '3:cpu,cpuacct:/docker/container',
        '/proc/self/mountinfo': '30 20 0:30 /docker/container /sys/fs/cgroup/cpu,cpuacct ro - cgroup cgroup rw,cpu,cpuacct',
        '/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us': quota,
        '/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_period_us': '100000',
    })
    assert cpu_count.available_cpu_count() == expected


def test_parent_limit_and_non_root_membership(cgroups):
    cgroups.update({
        '/proc/self/cgroup': '0::/docker/parent/child',
        '/proc/self/mountinfo': '30 20 0:30 /docker /sys/fs/cgroup ro - cgroup2 cgroup rw',
        '/sys/fs/cgroup/parent/child/cpu.max': '400000 100000',
        '/sys/fs/cgroup/parent/cpu.max': '150000 100000',
        '/sys/fs/cgroup/cpu.max': 'max 100000',
    })
    assert cpu_count.available_cpu_count() == 2


def test_cpuset_is_tighter_than_quota(cgroups, monkeypatch):
    cgroups.update({
        '/proc/self/cgroup': '0::/',
        '/proc/self/mountinfo': '30 20 0:30 / /sys/fs/cgroup ro - cgroup2 cgroup rw',
        '/sys/fs/cgroup/cpu.max': '400000 100000',
    })
    monkeypatch.setattr(cpu_count.os, 'sched_getaffinity', lambda pid: {2})
    assert cpu_count.available_cpu_count() == 1


def test_affinity_without_cgroups(cgroups):
    assert cpu_count.available_cpu_count() == 8


def test_fallback_without_affinity(cgroups, monkeypatch):
    monkeypatch.delattr(cpu_count.os, 'sched_getaffinity')
    monkeypatch.setattr(cpu_count.multiprocessing, 'cpu_count', lambda: 3)
    assert cpu_count.available_cpu_count() == 3


def test_unavailable_cpu_count(cgroups, monkeypatch):
    monkeypatch.delattr(cpu_count.os, 'sched_getaffinity')

    def unavailable():
        raise NotImplementedError

    monkeypatch.setattr(cpu_count.multiprocessing, 'cpu_count', unavailable)
    assert cpu_count.available_cpu_count() == 1


def test_read_unavailable_cgroup_file(tmp_path):
    assert cpu_count._read_text(str(tmp_path / 'missing')) == ''
