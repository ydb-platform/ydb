import logging
import itertools
import random
import sys
import collections
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable

from ydb.tools.mnc.lib import common, deploy_ctx, parted, templates, term, tools, ydb_config, structure, progress
from ydb.tools.mnc.scheme import multinode


logger = logging.getLogger(__name__)

expected_config = multinode.scheme


inited_from_config = False
enable_ic_ports = defaultdict(lambda: iter(range(19001, 20000)))
enable_grpc_ports = defaultdict(lambda: itertools.chain([2135], range(20000, 21000)))
enable_mon_ports = defaultdict(lambda: itertools.chain([8765], range(31000, 32000)))
first_static_grpc_port = 2135


re_range = re.compile(r'(\d+)-(\d+)|(\d+)')


def get_sub_range(str_range: str):
    match = re_range.match(str_range)
    if not match:
        raise ValueError(f'invalid range: {str_range}')
    if match.group(1):
        return range(int(match.group(1)), int(match.group(2)) + 1)
    return [int(match.group(3))]


def init_range(range_of_ports: list[str], default_range_factory: Callable[[], range]):
    if not range_of_ports:
        return default_range_factory()
    return itertools.chain(*[get_sub_range(r) for r in range_of_ports])


def get_default_ic_range():
    return itertools.chain([19001, 19000], range(19002, 20000))


def get_default_grpc_range():
    return itertools.chain([2135], range(20000, 21000))


def get_default_mon_range():
    return itertools.chain([8765], range(31000, 32000))


def get_default_port_range(protocol: str):
    if protocol == 'ic':
        return get_default_ic_range
    elif protocol == 'http':
        return get_default_mon_range
    elif protocol == 'grpc':
        return get_default_grpc_range


def get_port_factory(config: dict, protocol: str):
    def factory():
        ports_config = config.get('ports') or {}
        protocol_ports = ports_config.get(protocol, [])
        return iter(init_range(protocol_ports, get_default_port_range(protocol)))

    return factory


def init_ports(config: dict):
    assert config is not None
    global inited_from_config, enable_ic_ports, enable_grpc_ports, enable_mon_ports, first_static_grpc_port

    enable_ic_ports = defaultdict(get_port_factory(config, 'ic'))
    enable_grpc_ports = defaultdict(get_port_factory(config, 'grpc'))
    enable_mon_ports = defaultdict(get_port_factory(config, 'http'))
    first_static_grpc_port = next(get_port_factory(config, 'grpc')())


def get_current_and_shift(it):
    cur = it.current
    next(it)
    return cur


class NodeCountByKind:
    def __init__(self, config, hosts: list[str]):
        self.static_node_count = 0
        self.dynamic_node_count = 0
        self.nbs_node_count = 0
        self.freehost = config.get('freehost', None)
        self.nodes_per_host = config['nodes_per_host']
        self.hosts = []
        self.databases = config['domain']['databases'] if 'domain' in config else []
        self.with_nbs = config['with_nbs']
        self.next_dynamic_nodes = itertools.cycle([host for host in hosts if host != self.freehost])
        self.count(hosts)

    def count(self, hosts: list[str]):
        self.hosts = hosts
        self.static_node_count = common.calculate_node_count(hosts, self.nodes_per_host, self.freehost)
        for db in self.databases:
            if db['name'] == 'NBS' and self.with_nbs:
                self.nbs_node_count = db.get('compute_unit_count', 0)
            else:
                self.dynamic_node_count += db.get('compute_unit_count', 0)

    def count_node_count_by_host(self, node_count: int):
        result = collections.defaultdict(list)
        for idx in range(1, node_count + 1):
            host = next(self.next_dynamic_nodes)
            result[host].append(idx)
        return result

    def dynamic_node_count_by_host(self):
        return self.count_node_count_by_host(self.dynamic_node_count)

    def nbs_node_count_by_host(self):
        return self.count_node_count_by_host(self.nbs_node_count)


async def get_parts(
    host: str, devices: common.Device, npm: int, sector_map_use: str, disk_size: int, sector_map_profile: str
):
    if sector_map_use != 'always':
        disks = [await parted.parted_info(host, device) for device in devices]
        error_disks = [disk for disk in disks if disk.error]

        if error_disks:
            print('Has problems with disks:')
            for disk in error_disks:
                print(f'- {disk.path}')
                print(f'    "{disk.error}"')
            sys.exit(1)

        parts = [x for info in disks for x in info.parts[1:]]
        part_paths = ['/dev/disk/by-partlabel/{0}'.format(part.label) for part in parts]
    else:
        part_paths = []

    if sector_map_use == 'never' or len(part_paths) >= npm:
        return part_paths
    else:
        sector_map_paths = (
            "SectorMap:map_{idx}:{size}:{profile}".format(idx=idx, size=disk_size, profile=sector_map_profile)
            for idx in range(npm - len(part_paths))
        )
        return list(itertools.chain(part_paths, sector_map_paths))


def add_storage_pools(cluster, config):
    if not config.get('storage_pools', None):
        return
    for sp in config['storage_pools']:
        cluster.add_storage_pool(sp['name'], sp['storage_group_count'], config['device_type'], config['erasure'])


def add_domains(cluster, config):
    if config['domain'] is None:
        return
    dm = config['domain']
    domain = cluster.new_domain(dm['name'])
    domain.add_storage_pool_kind(config['device_type'].lower(), config['erasure'], config['device_type'])
    for db in dm['databases']:
        database = domain.new_database(db['name'])
        database.add_storage_unit(config['device_type'].lower(), db['storage_group_count'])
        database.add_compute_unit('slot', db['compute_unit_count'])
        if db.get('overridden_configs'):
            database.set_overridden_configs(db.get('overridden_configs'))


def assign_locations(num_nodes: int, nodes_per_dc: int, nodes_per_rack: int, shuffle_locations: bool):
    location_list = []
    dc_idx = 0
    rack_idx = 0
    body_idx = 0
    for location_idx in range(num_nodes):
        if location_idx % nodes_per_dc == 0:
            rack_idx += 1
            dc_idx += 1
            body_idx = 0
        elif location_idx % nodes_per_rack == 0:
            rack_idx += 1
            body_idx = 0
        else:
            body_idx += 1
        location_list.append((dc_idx - 1, rack_idx - 1, body_idx))

    if shuffle_locations:
        random.shuffle(location_list)

    return location_list


def add_tenants_configs(builder: ydb_config.YdbConfigBuilder, config: dict):
    dm = config.get('domain', None)
    if not dm:
        return
    for db in dm['databases']:
        if 'overridden_configs' in db:
            builder.add_tenant_selector(f'/{config['domain']['name']}/{db['name']}', db['overridden_configs'])


async def gen_yaml_legacy(
    filename: str,
    hosts: dict[str, list[common.Device]],
    config: dict,
    num_datacenters: int,
    node_per_rack: int,
    shuffle_locations: bool,
):
    npm = config['nodes_per_host']
    disk_size = config['disk_size']
    sector_map = config['sector_map']
    freehost = config.get('freehost', None)
    logger.debug('start to generate yaml')

    cluster = structure.Cluster(config['erasure'], config['device_type'])

    if config['use_nw_cache']:
        cluster.set_node_warden_cache_file_path(f'{deploy_ctx.deploy_path}/kikimr/cache/nodewarden_%h_%p_%n.txt')

    cluster.set_fake_secret_path(f'{deploy_ctx.deploy_path}/kikimr/cfg/fake-secret.txt')

    log_config = config['log']
    if log_config:
        if 'global' in log_config:
            cluster.set_global_log_level(log_config['global'])
        if log_config.get('entries', None):
            for entry_config in log_config['entries']:
                cluster.set_entry_log_level(entry_config['name'], entry_config['level'])

    node_count = NodeCountByKind(config, list(hosts)).static_node_count
    node_per_dc = max(1, (node_count + num_datacenters - 1) // num_datacenters)
    rack_count = 0

    dc_list = []
    rack_list = []

    dc_count = 0

    for location_idx in range(node_count):
        if location_idx % node_per_dc == 0:
            dc_count += 1
            dc_list.append(cluster.new_datacenter('DC' + str(dc_count)))
            rack_list.append(dc_list[dc_count - 1].new_rack())
            rack_count += 1
        elif location_idx % node_per_rack == 0:
            rack_list.append(dc_list[dc_count - 1].new_rack())
            rack_count += 1

    locations = assign_locations(node_count, node_per_dc, node_per_rack, shuffle_locations)

    device_to_host_config = {}

    def get_host_config_id(paths: list[str]):
        key = tuple(sorted(paths))
        if key in device_to_host_config:
            return device_to_host_config[key]
        device_to_host_config[key] = len(device_to_host_config) + 1
        drives = [structure.Drive(path, config['device_type'], 9) for path in paths]
        cluster.add_host_config(device_to_host_config[key], drives)
        return device_to_host_config[key]

    added_nodes = 0
    for fqdn, devices in hosts.items():
        if fqdn == freehost:
            continue
        part_paths = await get_parts(fqdn, devices, npm, sector_map['use'], disk_size, sector_map['profile'])
        drives_per_node = len(part_paths) // npm

        if drives_per_node == 0:
            return False

        ic_ports = get_port_factory(config, 'ic')()

        for idx in range(npm):
            dc_idx, rack_idx, body_idx = locations[added_nodes]
            dc = dc_list[dc_idx]
            rack = rack_list[rack_idx]

            ic_port = next(ic_ports)
            host = rack.new_host(fqdn, ic_port)

            paths = [part_paths[idx * drives_per_node + i] for i in range(0, drives_per_node)]
            host_config_id = get_host_config_id(paths)
            host.host_config_id = host_config_id

            added_nodes += 1

    if freehost:
        devices = hosts.get(freehost, [])
        part_paths = await get_parts(freehost, devices, 1, sector_map['use'], disk_size, sector_map['profile'])
        rack = dc.new_rack()
        ic_ports = get_port_factory(config, 'ic')()
        ic_port = next(ic_ports)
        host = rack.new_host(freehost, ic_port)
        host_config_id = get_host_config_id(part_paths)
        host.host_config_id = host_config_id
        cluster.set_nodes_for_system_tablets([node_count])
    else:
        cluster.set_nodes_for_system_tablets([1 + node_per_rack * idx for idx in range(rack_count)])

    if config.get('overridden_configs'):
        cluster.set_overridden_configs(config.get('overridden_configs'))

    add_storage_pools(cluster, config)
    add_domains(cluster, config)

    with open(f'{deploy_ctx.work_directory}/{filename}', 'w') as file:
        print(cluster.generate_config_for_kikimr_configure(), file=file)
    return True


async def gen_yaml(
    filename: str,
    hosts: dict[str, list[common.Device]],
    config: dict,
    piles: int,
    datacenters_per_pile: int,
    nodes_per_rack: int,
    shuffle_locations: bool,
):
    if config['ydb_config_type'] == 'v1':
        if piles != 1:
            raise ValueError('piles must be 1 for ydb_config_type v1')
        return await gen_yaml_legacy(filename, hosts, config, datacenters_per_pile, nodes_per_rack, shuffle_locations)

    npm = config['nodes_per_host']
    disk_size = config['disk_size']
    sector_map = config['sector_map']
    freehost = config.get('freehost', None)
    logger.debug('start to generate yaml')

    base_config = ydb_config.YdbBaseConfig(
        erasure=config['erasure'],
        default_disk_type=config['device_type'],
        fail_domain_type="rack",
    )
    builder = ydb_config.YdbConfigBuilder(base_config)

    if config['domain'] is not None:
        builder.domain_name = config['domain']['name']

    if config['use_nw_cache']:
        cache_path = f'{deploy_ctx.deploy_path}/kikimr/cache/nodewarden_%h_%p_%n.txt'
        blob_storage_config = {
            'cache_file_path': cache_path,
        }
        builder.add_manual_config_field('blob_storage_config', blob_storage_config)

    log_config = config['log']
    if log_config:
        ydbd_log_config = {}
        ydbd_log_config['default_level'] = structure.log_levels_map[log_config.get('global', 'notice')]
        if log_config.get('entries', None):
            ydbd_log_config['entry'] = []
            for entry_config in log_config['entries']:
                ydbd_log_config['entry'].append({
                    'component': entry_config['name'],
                    'level': structure.log_levels_map[entry_config['level']]
                })
        builder.add_manual_config_field('log_config', ydbd_log_config)

    num_datacenters = piles * datacenters_per_pile
    node_count = NodeCountByKind(config, list(hosts)).static_node_count
    nodes_per_dc = max(1, (node_count + num_datacenters - 1) // num_datacenters)
    nodes_per_pile = max(1, (node_count + piles - 1) // piles)

    pile_list = []
    dc_list = []
    rack_list = []
    bodies_list = []

    for location_idx in range(node_count):
        new_pile = (location_idx % nodes_per_pile) == 0
        new_dc = new_pile or ((location_idx % nodes_per_dc) == 0)
        new_rack = new_dc or ((location_idx % nodes_per_rack) == 0)

        if new_pile:
            pile = builder.add_pile()
            pile_list.append(pile)
        if new_dc:
            dc = pile.add_data_center()
            dc_list.append(dc)
        if new_rack:
            rack = dc.add_rack()
            rack_list.append(rack)
        body = rack.add_body()
        bodies_list.append(body)

    if shuffle_locations:
        random.shuffle(bodies_list)

    added_nodes = 0
    for fqdn, devices in hosts.items():
        if fqdn == freehost:
            continue
        part_paths = await get_parts(fqdn, devices, npm, sector_map['use'], disk_size, sector_map['profile'])
        drives_per_node = len(part_paths) // npm

        ic_ports = get_port_factory(config, 'ic')()

        if drives_per_node == 0:
            return False

        for idx in range(npm):
            body = bodies_list[added_nodes]

            ic_port = next(ic_ports)
            host = body.add_host(fqdn=fqdn, port=ic_port)

            paths = [part_paths[idx * drives_per_node + i] for i in range(0, drives_per_node)]
            for path in paths:
                host.add_device(path)

            added_nodes += 1

    if freehost:
        devices = hosts.get(freehost, [])
        part_paths = await get_parts(freehost, devices, 1, sector_map['use'], disk_size, sector_map['profile'])
        rack = dc.add_rack()
        ic_ports = get_port_factory(config, 'ic')()
        ic_port = next(ic_ports)
        host = rack.add_host(fqdn=freehost, port=ic_port)
        for path in part_paths:
            host.add_device(path)
        builder.system_tablets_node_ids = [node_count]
    else:
        if node_count > 20:
            builder.system_tablets_node_ids = [1 + nodes_per_rack * idx for idx in range(min(8, len(rack_list)))]

    if config.get('overridden_configs'):
        for field_name, config_value in config.get('overridden_configs').items():
            builder.add_manual_config_field(field_name, config_value)

    add_tenants_configs(builder, config)

    with open(deploy_ctx.work_directory + '/' + filename, 'w') as file:
        print(builder.generate_yaml_config(), file=file)
    return True


async def gen_yaml_mirror3dc(filename: str, hosts: dict[str, list[common.Device]], config: dict):
    node_count = NodeCountByKind(config, list(hosts)).static_node_count
    pile_count = config['pile_count']
    dc_count = 3 * pile_count
    node_per_dc = max(1, (node_count + dc_count - 1) // dc_count)
    node_per_rack = max(1, min(3, node_per_dc // 3))
    return await gen_yaml(filename, hosts, config, pile_count, 3, node_per_rack, False)


async def gen_yaml_one_dc(filename: str, hosts: dict[str, list[common.Device]], config: dict):
    node_count = NodeCountByKind(config, list(hosts)).static_node_count
    pile_count = config['pile_count']
    dc_count = 1 * pile_count
    node_per_dc = max(1, (node_count + dc_count - 1) // dc_count)
    node_per_rack = max(1, min(8, node_per_dc // 8))
    return await gen_yaml(filename, hosts, config, pile_count, 1, node_per_rack, False)


class GenerateStaticNodeServiceFiles(term.ParallelledGroupOfShellCommands):
    def __init__(self, nodes_per_host: int):
        term.ParallelledGroupOfShellCommands.__init__(self, 'generating static node service files')
        self._commands = []
        if not deploy_ctx.use_services:
            return
        for idx in range(1, nodes_per_host + 1):
            self._commands.append(self.command_make_service_file(idx))
            self._commands.append(self.command_make_upstart_service_file(idx))

    def command_make_service_file(self, idx: int):
        replace_args = {'%NODE_IDX%': idx}
        src_path = f'{deploy_ctx.work_directory}/init/test_kikimr_systemd.template'
        dest_path = f'{deploy_ctx.work_directory}/init/test_kikimr_static_{idx}.service'
        return tools.sed_command(src_path, dest_path, replace_args)

    def command_make_upstart_service_file(self, idx: int):
        replace_args = {'%NODE_IDX%': idx}
        src_path = f'{deploy_ctx.work_directory}/init/test_kikimr.template'
        dest_path = f'{deploy_ctx.work_directory}/init/test_kikimr_static_{idx}.conf'
        return tools.sed_command(src_path, dest_path, replace_args)


@dataclass
class TenantsForDynamicNodes:
    tenant: str
    host: str
    count: int
    pile_name: str


class GenerateDynamicNodeServiceFiles(term.ParallelledGroupOfShellCommands):
    def __init__(self, tenants: list[TenantsForDynamicNodes]):
        term.ParallelledGroupOfShellCommands.__init__(self, 'generating dynamic node service files')
        self._commands = []
        if not deploy_ctx.use_services:
            return
        current_node_idx = 0
        for cfg in tenants:
            for idx in range(current_node_idx + 1, current_node_idx + cfg.count + 1):
                self._commands.append(self.command_make_service_file(idx))
                self._commands.append(self.command_make_upstart_service_file(idx))
            current_node_idx += cfg.count

    def command_make_service_file(self, idx: int):
        replace_args = {'%NODE_IDX%': idx}
        src_path = f'{deploy_ctx.work_directory}/init/test_kikimr_dynamic_systemd.template'
        dest_path = f'{deploy_ctx.work_directory}/init/test_kikimr_dynamic_{idx}.service'
        return tools.sed_command(src_path, dest_path, replace_args)

    def command_make_upstart_service_file(self, idx: int):
        replace_args = {'%NODE_IDX%': idx}
        src_path = f'{deploy_ctx.work_directory}/init/test_kikimr_dynamic.template'
        dest_path = f'{deploy_ctx.work_directory}/init/test_kikimr_dynamic_{idx}.conf'
        return tools.sed_command(src_path, dest_path, replace_args)


class GenerateStaticNodeConfigsCommands(term.ParallelledGroupOfShellCommands):
    def __init__(self, hosts: list[str], nodes_per_host: int):
        term.ParallelledGroupOfShellCommands.__init__(self, 'generating static node configs')
        self._hosts = hosts
        self._commands = []
        for idx in range(1, nodes_per_host + 1):
            self._commands.append(self.command_make_cfg(idx))
            self._commands.append(self.commands_make_log_cfg(idx))

    def command_make_cfg(self, idx):
        grpc_port = next(enable_grpc_ports[self._hosts[0]])
        ic_port = next(enable_ic_ports[self._hosts[0]])
        mon_port = next(enable_mon_ports[self._hosts[0]])
        for host in self._hosts[1:]:
            next(enable_grpc_ports[host])
            next(enable_ic_ports[host])
            next(enable_mon_ports[host])

        replace_args = {
            '%MON_PORT%': mon_port,
            '%GRPC_PORT%': grpc_port,
            '%GRPCS_PORT%': (grpc_port if deploy_ctx.secure else ''),
            '%IC_PORT%': ic_port,
            '%NODE_IDX%': idx,
            '%DEPLOY_PATH%': deploy_ctx.deploy_path,
            '%CA%': ('/opt/ydb/certs/ca.crt' if deploy_ctx.secure else ''),
            '%CERT%': ('/opt/ydb/certs/node.crt' if deploy_ctx.secure else ''),
            '%KEY%': ('/opt/ydb/certs/node.key' if deploy_ctx.secure else ''),
            '%MON_CERT%': ('/opt/ydb/certs/web.pem' if deploy_ctx.secure else ''),
        }
        src_path = f'{deploy_ctx.work_directory}/replaced/kikimr.template'
        dest_path = f'{deploy_ctx.work_directory}/replaced/kikimr-{idx}.cfg'
        return tools.sed_command(src_path, dest_path, replace_args)

    def commands_make_log_cfg(self, idx):
        log_path = f'{deploy_ctx.deploy_path}/test_kikimr_static_{idx}/logs/kikimr.log'
        replace_args = {'%LOG_FILE_PATH%': log_path}
        src_path = f'{deploy_ctx.work_directory}/replaced/log.template'
        dest_path = f'{deploy_ctx.work_directory}/replaced/log-static-{idx}'
        return tools.sed_command(src_path, dest_path, replace_args)


class GenerateDynamicNodeConfigsCommands(term.ParallelledGroupOfShellCommands):
    def __init__(self, tenants: list[TenantsForDynamicNodes]):
        term.ParallelledGroupOfShellCommands.__init__(self, 'generating dynamic node configs')
        self._commands = []
        current_node_idx = 0
        for cfg in tenants:
            for idx in range(current_node_idx + 1, current_node_idx + cfg.count + 1):
                self._commands.append(self.command_make_cfg(cfg, idx))
                self._commands.append(self.commands_make_log_cfg(idx))
            current_node_idx += cfg.count

    def command_make_cfg(self, cfg: TenantsForDynamicNodes, idx: int):
        try:
            grpc_port_value = next(enable_grpc_ports[cfg.host])
            replace_args = {
                '%MON_PORT%': next(enable_mon_ports[cfg.host]),
                '%GRPC_PORT%': grpc_port_value,
                '%GRPCS_PORT%': (grpc_port_value if deploy_ctx.secure else ''),
                '%IC_PORT%': next(enable_ic_ports[cfg.host]),
                '%TENANT%': cfg.tenant,
                '%NODE_IDX%': idx,
                '%DEPLOY_PATH%': deploy_ctx.deploy_path,
                '%PILE_NAME%': cfg.pile_name,
                '%NODE_BROKER_PORT%': first_static_grpc_port,
                '%CA%': ('/opt/ydb/certs/ca.crt' if deploy_ctx.secure else ''),
                '%CERT%': ('/opt/ydb/certs/node.crt' if deploy_ctx.secure else ''),
                '%KEY%': ('/opt/ydb/certs/node.key' if deploy_ctx.secure else ''),
                '%MON_CERT%': ('/opt/ydb/certs/web.pem' if deploy_ctx.secure else ''),
            }
        except StopIteration:
            raise Exception(f'No more ports for host {cfg.host}, tenant {cfg.tenant}, idx {idx}')
        src_path = f'{deploy_ctx.work_directory}/replaced/dynamic_server.template'
        dest_path = f'{deploy_ctx.work_directory}/replaced/dynamic_server_{idx}.cfg'
        return tools.sed_command(src_path, dest_path, replace_args)

    def commands_make_log_cfg(self, idx):
        log_path = f'{deploy_ctx.deploy_path}/test_kikimr_dynamic_{idx}/logs/kikimr.log'
        replace_args = {'%LOG_FILE_PATH%': log_path}
        src_path = f'{deploy_ctx.work_directory}/replaced/log.template'
        dest_path = f'{deploy_ctx.work_directory}/replaced/log-dynamic-{idx}'
        return tools.sed_command(src_path, dest_path, replace_args)


class PrepareWorkingDirCommands(term.ParallelledGroupOfShellCommands):
    def __init__(self):
        term.ParallelledGroupOfShellCommands.__init__(self, f"prepare working dir '{deploy_ctx.work_directory}'")
        self._commands = [
            f'mkdir -p {deploy_ctx.work_directory}/init',
            f'mkdir -p {deploy_ctx.work_directory}/replaced',
            f'mkdir -p {deploy_ctx.work_directory}/special_dynamic',
            f'mkdir -p {deploy_ctx.work_directory}/static',
            f'mkdir -p {deploy_ctx.work_directory}/dynamic',
        ]


def clear_configs():
    paths = [
        f'{deploy_ctx.work_directory}/init/test_kikimr_*.conf',
        f'{deploy_ctx.work_directory}/init/test_kikimr_*.service',
        f'{deploy_ctx.work_directory}/replaced/kikimr-*.cfg',
        f'{deploy_ctx.work_directory}/replaced/dynamic_server_*.cfg',
        f'{deploy_ctx.work_directory}/replaced/slot_cfg_*.cfg',
        f'{deploy_ctx.work_directory}/static/*',
        f'{deploy_ctx.work_directory}/dynamic/*',
    ]
    term.sync_shell(f'rm {" ".join(paths)}', silent_error=True)


def verify_config(config: dict):
    if config['with_nbs']:
        has_nbs_database = False
        if 'domain' in config or 'databases' in config['domain']:
            has_nbs_database = any((db['name'] == 'NBS' for db in config['domain']['databases']))
        if not has_nbs_database:
            print("excepeted database with name 'NBS'")
            return False
    return True


@progress.with_parent_task
async def act_generate(
    hosts,
    config: dict,
    parent_task: progress.TaskNode = None,
):
    nodes_per_host = config['nodes_per_host']

    if not verify_config(config):
        return False

    init_ports(config)

    term.sync_shell(PrepareWorkingDirCommands())
    templates.make_templates()
    clear_configs()

    subtasks = []

    yaml_task = await parent_task.add_subtask("[bold green]Generate config.yaml", total=1)
    subtasks.append(yaml_task)

    node_layout = config.get('node_layout', None)
    if node_layout is not None:
        piles = node_layout["piles"] or 1
        datacenters_per_pile = node_layout["num_datacenters"]
        if piles > 1:
            datacenters_per_pile = node_layout["datacenters_per_pile"] or 1
        else:
            datacenters_per_pile = node_layout["num_datacenters"] or 1

        success = await gen_yaml(
            'config.yaml',
            common.get_host_disks(config),
            config,
            piles,
            datacenters_per_pile,
            node_layout["nodes_per_rack"] or 8,
            node_layout["shuffle_locations"],
        )

    elif config['erasure'] == 'mirror-3-dc':
        success = await gen_yaml_mirror3dc('config.yaml', common.get_host_disks(config), config)
    else:
        success = await gen_yaml_one_dc('config.yaml', common.get_host_disks(config), config)

    await yaml_task.update(advance=1)

    if not success:
        print('Failed to generate config.yaml', file=sys.stderr)
        return False

    static_service_groups = GenerateStaticNodeServiceFiles(nodes_per_host).subgroups(50)
    static_service_task = await parent_task.add_subtask("[bold green]Generate static node service files", total=len(static_service_groups))
    subtasks.append(static_service_task)

    static_cfg_groups = GenerateStaticNodeConfigsCommands(list(hosts), nodes_per_host).subgroups(50)
    static_cfg_task = await parent_task.add_subtask("[bold green]Generate static node configs", total=len(static_cfg_groups))
    subtasks.append(static_cfg_task)

    static_tasks = [
        term.parallel_shell(*static_service_groups, task=static_service_task),
        term.parallel_shell(*static_cfg_groups, task=static_cfg_task),
    ]

    tenants_tasks = []
    next_host_for_dynnodes = itertools.cycle(list(hosts))
    pile_count = config['pile_count']
    if config['domain'] is not None:
        tenants = []
        for db in config['domain']['databases']:
            dynnodes_per_pile = (db['compute_unit_count'] + pile_count - 1) // pile_count
            for idx in range(db['compute_unit_count']):
                tenants.append(TenantsForDynamicNodes(
                    tenant='/{}/{}'.format(config['domain']['name'], db['name']),
                    host=next(next_host_for_dynnodes),
                    count=1,
                    pile_name='' if pile_count == 1 else f'pile_{idx // dynnodes_per_pile}',
                ))

        dynamic_service_groups = GenerateDynamicNodeServiceFiles(tenants).subgroups(50)
        dynamic_service_task = await parent_task.add_subtask("[bold green]Generate dynamic node service files", total=len(dynamic_service_groups))
        subtasks.append(dynamic_service_task)

        dynamic_cfg_groups = GenerateDynamicNodeConfigsCommands(tenants).subgroups(50)
        dynamic_cfg_task = await parent_task.add_subtask("[bold green]Generate dynamic node configs", total=len(dynamic_cfg_groups))
        subtasks.append(dynamic_cfg_task)

        tenants_tasks = [
            term.parallel_shell(*dynamic_service_groups, task=dynamic_service_task),
            term.parallel_shell(*dynamic_cfg_groups, task=dynamic_cfg_task),
        ]

    await tools.chain_async(
        *static_tasks,
        *tenants_tasks,
    )

    if config['ydb_config_type'] == 'v1':
        freehost = config.get('freehost', None)
        generate_v1_task = await parent_task.add_subtask("[bold green]generate v1 configs", total=1)
        generate_dynamic_task = await parent_task.add_subtask("[bold green]generate dynamic configs", total=1)
        subtasks.append(generate_v1_task)
        subtasks.append(generate_dynamic_task)
        if config['with_nbs']:
            generate_nbs_task = await parent_task.add_subtask("[bold green]generate nbs configs", total=1)
            subtasks.append(generate_nbs_task)

        success = tools.generate_static(f'{deploy_ctx.work_directory}/config.yaml', config['build_args'])
        await generate_v1_task.update(advance=1)
        if success:
            success = tools.generate_dynamic(f'{deploy_ctx.work_directory}/config.yaml', config['build_args'], grpc_endpoint=freehost)
            await generate_dynamic_task.update(advance=1)
        if success and config['with_nbs']:
            success = tools.generate_nbs(f'{deploy_ctx.work_directory}/config.yaml', config['build_args'], grpc_endpoint=freehost)
            await generate_nbs_task.update(advance=1)
    else:
        success = await term.shell(f'mkdir -p {deploy_ctx.deploy_path}/static && cp {deploy_ctx.work_directory}/config.yaml {deploy_ctx.work_directory}/static/config.yaml')

    for subtask in subtasks:
        await subtask.update(visible=False)

    return success

'''
async def act_reassign_locations(
    config: dict,
    config_yaml_path='static/config.yaml',
    names_txt_path='static/names.txt',
):
    config_yaml = None
    names_txt = None

    with open(config_yaml_path, 'r') as yaml_file:
        config_yaml = yaml.safe_load(yaml_file)

    with open(names_txt_path, 'r') as proto_file:
        names_txt = config_pb2.TStaticNameserviceConfig()
        text_format.Parse(proto_file.read(), names_txt)

    node_count = len(config_yaml["hosts"])

    node_layout = config.get('node_layout', None)
    assert node_layout is not None

    num_datacenters = node_layout["num_datacenters"]
    node_per_dc = max(1, (node_count + num_datacenters - 1) // num_datacenters)
    nodes_per_rack = node_layout["nodes_per_rack"]
    shuffle_locations = node_layout["shuffle_locations"]

    locations = assign_locations(node_count, node_per_dc, nodes_per_rack, shuffle_locations)

    for node_idx in range(node_count):
        dc_idx, rack_idx, body_idx = locations[node_idx]

        dc_name = "DC" + str(dc_idx + 1)
        rack_name = str(1000000 * (dc_idx + 1) + rack_idx + 1)
        body_name = body_idx

        config_yaml["hosts"][node_idx]["walle_location"]["body"] = body_name
        config_yaml["hosts"][node_idx]["walle_location"]["rack"] = rack_name
        config_yaml["hosts"][node_idx]["walle_location"]["data_center"] = dc_name

        names_txt.Node[node_idx].WalleLocation.DataCenter = dc_name
        names_txt.Node[node_idx].WalleLocation.Rack = rack_name
        names_txt.Node[node_idx].WalleLocation.Body = body_name

    with open(config_yaml_path, 'w') as yaml_file:
        yaml.dump(config_yaml, yaml_file, default_flow_style=False)

    with open(names_txt_path, 'w') as proto_file:
        proto_file.write(text_format.MessageToString(names_txt))
    return True
'''


def add_arguments(parser):
    subparsers = parser.add_subparsers(help='Commands', dest='cmd', required=True)

    generate_parser = subparsers.add_parser('generate')
    common.add_common_options(generate_parser)


'''
    reassign_locations_parser = subparsers.add_parser('reassign_locations')
    reassign_locations_parser.add_argument('--config-yaml', '--config_yaml', dest='config_yaml', type=str, default='static/config.yaml')
    reassign_locations_parser.add_argument('--names-txt', '--names_txt', dest='names_txt', type=str, default='static/names.txt')
    common.add_common_options(reassign_locations_parser)
'''


async def do_generate(args):
    hosts = await common.get_machines(args.config)
    success = await act_generate(
        hosts,
        args.config,
    )
    print('success' if success else 'fail')

'''
async def do_reassign_locations(args):
    success = await act_reassign_locations(
        args.config,
        config_yaml_path=args.config_yaml,
        names_txt_path=args.names_txt,
    )
    print('success' if success else 'fail')
'''


async def do(args):
    if args.cmd == 'generate':
        await do_generate(args)
'''
    if args.cmd == 'reassign_locations':
        await do_reassign_locations(args)
'''
