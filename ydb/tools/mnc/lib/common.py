import asyncio
import collections
import logging
import re
import sys
import os
import requests
import time

from ydb.tools.mnc.lib.exceptions import CliError

import ydb.tools.mnc.scheme as scheme


logger = logging.getLogger(__name__)


class Device:
    def __init__(self, partlabel, path):
        self.partlabel = partlabel
        self.path = path
        self.type = self.get_device_type(partlabel)

    def get_device_type(self, partlabel):
        if "nvme" in partlabel:
            return "nvme"
        elif "ssd" in partlabel:
            return "ssd"
        else:
            return "rot"


size_pattern = r'([1-9]\d*[\.,]?\d*)([kMGT]B)?'
size_comp = re.compile(size_pattern)


class Memory:
    kB = 1000
    MB = 1000 * kB
    GB = 1000 * MB
    TB = 1000 * GB

    def __init__(self, value):
        if isinstance(value, int):
            self.value = value
        elif isinstance(value, str):
            match = size_comp.fullmatch(value)
            if not match:
                raise ValueError(
                    'value not matched to pattern; value: {value} pattern: {pattern}'.format(
                        value=value, pattern=size_pattern
                    )
                )
            num = float(match[1].replace(',', '.'))
            if match[2] == 'kB':
                self.value = int(num * Memory.kB)
            elif match[2] == 'MB':
                self.value = int(num * Memory.MB)
            elif match[2] == 'GB':
                self.value = int(num * Memory.GB)
            elif match[2] == 'TB':
                self.value = int(num * Memory.TB)
            else:
                self.value = int(num)
        else:
            raise ValueError('value isn\'t int or str')

    def __str__(self):
        if self.value >= 10 * Memory.TB:
            return '{:.2f}TB'.format(self.value / Memory.TB)
        elif self.value >= 10 * Memory.GB:
            return '{:.2f}GB'.format(self.value / Memory.GB)
        elif self.value >= 10 * Memory.MB:
            return '{:.2f}MB'.format(self.value / Memory.MB)
        elif self.value >= 10 * Memory.kB:
            return '{:.2f}kB'.format(self.value / Memory.kB)
        else:
            return str(self.value)

    def __add__(self, other):
        if not isinstance(other, Memory):
            raise TypeError('expected Memory given {0}'.format(type(other)))
        return Memory(self.value + other.value)

    def __sub__(self, other):
        if not isinstance(other, Memory):
            raise TypeError('expected Memory given {0}'.format(type(other)))
        return Memory(self.value - other.value)

    def __truediv__(self, other):
        if isinstance(other, int):
            return Memory(self.value // other)
        if isinstance(other, float):
            return Memory(int(self.value / other))
        if not isinstance(other, Memory):
            raise TypeError()
        return self.value / other.value

    def __mul__(self, other):
        if isinstance(other, int):
            return Memory(self.value * other)
        if isinstance(other, float):
            return Memory(int(self.value * other))
        raise TypeError()


async def get_machines(config: dict):
    hosts = config.get('hosts', None)
    exclude_hosts = config.get('exclude_hosts', None)

    if hosts is None:
        raise CliError("expected -H")
    if not hosts and hosts is not None:
        raise CliError("-H shouldn't be empty")

    if exclude_hosts:
        hosts = [x for x in hosts if x not in exclude_hosts]

    if not hosts:
        raise CliError("hosts weren't found")
    return hosts


async def for_each_host(hosts: list[str], expected_disks: list, action):
    expected_disks = {
        host: {
            'disks_for_split': d.get('disks_for_split', []),
            'disks_for_use': d.get('disks_for_use', []),
        } for d in expected_disks for host in d['hosts']
    }
    res = []
    for host in hosts:
        disks = expected_disks.get(host, {'disks_for_split': [], 'disks_for_use': []})
        res.append(action(host, disks))
    return await asyncio.gather(*res)


def calculate_node_count(hosts: list[str], nodes_per_host: int, freehost: str = None):
    node_count = nodes_per_host * len(hosts)
    if freehost and freehost in hosts:
        node_count -= nodes_per_host - 1
    elif freehost:
        node_count += 1
    return node_count


def get_node_location(node_id: int, hosts: list[str], nodes_per_host: int, freehost: str = None):
    hosts = [host for host in hosts if host != freehost]
    if freehost:
        hosts.append(freehost)
    host_idx = (node_id - 1) // nodes_per_host
    ydb_node_idx = (node_id - 1) % nodes_per_host + 1
    if host_idx >= len(hosts):
        logger.error('host_idx is greater than the count of hosts; {0} > {1}'.format(host_idx, len(hosts)))
        return None
    return hosts[host_idx], ydb_node_idx


def get_node_locations_by_host(
    hosts: list[str],
    nodes_per_host: int = 1,
    freehost: str = None,
    nodes: list[str] = None,
    exclude_nodes: list[str] = None,
):
    node_count = calculate_node_count(hosts, nodes_per_host, freehost)
    if nodes is None:
        nodes = list(range(1, node_count + 1))
    else:
        nodes = [int(x) for x in nodes]
    if exclude_nodes is not None:
        exclude_nodes = set((int(x) for x in exclude_nodes))
        nodes = [node for node in nodes if node not in exclude_nodes]
    locations_by_host = collections.defaultdict(list)
    for node in nodes:
        location = get_node_location(node, hosts, nodes_per_host, freehost)
        if location is None:
            logger.error('Failed on calculating a location for {0}'.format(node))
            return None
        host, id = location
        locations_by_host[host].append(id)
    return locations_by_host


def add_argument_breaker(parser):
    parser.add_argument('--', dest='__breaker__', action='count', default=0)


def add_common_options(parser):
    parser.add_argument('--config-path', '--config_path', dest='config_path', type=str, default=None)
    parser.add_argument('--config', dest='config_name', type=str, default=None)

    parser.add_argument('--exclude-hosts', '--exclude_hosts', '-X', dest='exclude_hosts', nargs='*', default=None)
    parser.add_argument('--freehost', dest='freehost', type=str, default=None)

    parser.add_argument('--wd', '--work-directory', '--work_directory', dest='work_directory', type=str)

    parser.add_argument('--deploy-flags', '--deploy_flags', nargs='*', help=f'Deploy flags: {scheme.common.deploy_flags}')

    add_argument_breaker(parser)


def get_host_disks(config):
    expected = {
        host: [Device(x['partlabel'], x.get('device')) for x in d.get('disks_for_use', []) + d.get('disks_for_split', [])]
        for d in config.get('disks', []) for host in d.get('hosts', [])
    }
    return {host: expected.get(host, []) for host in config['hosts']}


def get_host_disks_for_split(config):
    expected = {
        host: [Device(x['partlabel'], x.get('device')) for x in d.get('disks_for_split', [])]
        for d in config.get('disks', []) for host in d.get('hosts', [])
    }
    return {host: expected.get(host, []) for host in config['hosts']}


def validate_servers(hosts, config, repeat_count=0):
    expected_user = os.environ.get('USER')
    port = config['port']
    success = True
    repeated = -1
    for host in hosts:
        while repeated < repeat_count:
            try:
                response = requests.get(f'http://{host}:{port}/user')

            except requests.exceptions.ConnectionError:
                repeated += 1
                if repeated < repeat_count:
                    time.sleep(5)
                continue
            break
        if repeated >= repeat_count:
            print(f"Can't connect; {host}", file=sys.stderr)
            success = False
            continue
        if response.status_code != 200:
            print(f"Response wasn't 200 for /user; {host}", file=sys.stderr)
            success = False
            continue
        if response.json()['user'] != expected_user:
            print(f"Host has different owner; {host} {response.json()['user']}", file=sys.stderr)
            success = False
            continue
        response = requests.post(f'http://{host}:{port}/info', json={'guid': config['guid']})
        if response.status_code != 200:
            print(f"Response wasn't 200 for /info; {host}", file=sys.stderr)
            success = False
            continue
        data = response.json()
        if data['config'] != config:
            print(f"Host config doesn't same; {host}")
            success = False
            continue
        if data['is_leader'] and not data.get('connected'):
            print(f"Leader wasn't connected; {host}")
            success = False
            continue
    return success
