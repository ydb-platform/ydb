import collections
import itertools
import logging
import random
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable

from ydb.tools.mnc.lib import agent_client, common, deploy_ctx, progress, templates, term, tools, ydb_config
from ydb.tools.mnc.lib.exceptions import CliError


logger = logging.getLogger(__name__)


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
    if protocol == 'http':
        return get_default_mon_range
    if protocol == 'grpc':
        return get_default_grpc_range
    raise ValueError(f'unknown protocol: {protocol}')


def get_port_factory(config: dict, protocol: str):
    def factory():
        ports_config = config.get('ports') or {}
        protocol_ports = ports_config.get(protocol, [])
        return iter(init_range(protocol_ports, get_default_port_range(protocol)))

    return factory


def init_ports(config: dict):
    assert config is not None
    global enable_ic_ports, enable_grpc_ports, enable_mon_ports, first_static_grpc_port

    enable_ic_ports = defaultdict(get_port_factory(config, 'ic'))
    enable_grpc_ports = defaultdict(get_port_factory(config, 'grpc'))
    enable_mon_ports = defaultdict(get_port_factory(config, 'http'))
    first_static_grpc_port = next(get_port_factory(config, 'grpc')())


class NodeCountByKind:
    def __init__(self, config, hosts: list[str]):
        self.static_node_count = 0
        self.dynamic_node_count = 0
        self.nbs_node_count = 0
        self.freehost = config.get('freehost', None)
        self.nodes_per_host = config['nodes_per_host']
        self.hosts = []
        self.databases = config['domain']['databases'] if config.get('domain') is not None else []
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


def _device_to_json(device: common.Device):
    return {
        "partlabel": device.partlabel,
        "device": device.path,
    }


async def get_parts(
    host: str, devices: list[common.Device], npm: int, sector_map_use: str, disk_size: int, sector_map_profile: str
):
    if sector_map_use != 'always':
        if not devices:
            part_paths = []
        else:
            response = await agent_client.post_json(
                host,
                "/disks/info",
                {"devices": [_device_to_json(device) for device in devices]},
            )
            if response is None:
                raise CliError(f'Failed to receive disk info from agent on {host}')

            disks = response.get("disks", [])
            error_disks = [disk for disk in disks if disk.get("error")]

            if error_disks:
                errors = ['Has problems with disks:']
                for disk in error_disks:
                    errors.append(f'- {disk.get("path") or disk.get("device") or disk.get("partlabel")}')
                    errors.append(f'    "{disk.get("error")}"')
                raise CliError('\n'.join(errors))

            parts = [part for disk in disks for part in disk.get("parts", [])[1:]]
            part_paths = ['/dev/disk/by-partlabel/{0}'.format(part["label"]) for part in parts]
    else:
        part_paths = []

    if sector_map_use == 'never' or len(part_paths) >= npm:
        return part_paths

    sector_map_paths = (
        "SectorMap:map_{idx}:{size}:{profile}".format(idx=idx, size=disk_size, profile=sector_map_profile)
        for idx in range(npm - len(part_paths))
    )
    return list(itertools.chain(part_paths, sector_map_paths))


def add_tenants_configs(builder: ydb_config.YdbConfigBuilder, config: dict):
    dm = config.get('domain', None)
    if not dm:
        return
    for db in dm['databases']:
        if 'overridden_configs' in db:
            builder.add_tenant_selector(f"/{config['domain']['name']}/{db['name']}", db['overridden_configs'])


async def gen_yaml(
    filename: str,
    hosts: dict[str, list[common.Device]],
    config: dict,
    piles: int,
    datacenters_per_pile: int,
    nodes_per_rack: int,
    shuffle_locations: bool,
):
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
        cache_path = f'{deploy_ctx.deploy_path}/ydb/cache/nodewarden_%h_%p_%n.txt'
        blob_storage_config = {
            'cache_file_path': cache_path,
        }
        builder.add_manual_config_field('blob_storage_config', blob_storage_config)

    log_config = config['log']
    if log_config:
        ydbd_log_config = {}
        ydbd_log_config['default_level'] = ydb_config.log_levels_map[log_config.get('global_level', 'notice')]
        if log_config.get('entries', None):
            ydbd_log_config['entry'] = []
            for entry_config in log_config['entries']:
                ydbd_log_config['entry'].append({
                    'component': entry_config['name'],
                    'level': ydb_config.log_levels_map[entry_config['level']],
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


@dataclass
class TenantsForDynamicNodes:
    tenant: str
    host: str
    count: int
    pile_name: str


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
        src_path = f'{deploy_ctx.work_directory}/replaced/ydb.template'
        dest_path = f'{deploy_ctx.work_directory}/replaced/ydb-{idx}.cfg'
        return tools.sed_command(src_path, dest_path, replace_args)

    def commands_make_log_cfg(self, idx):
        log_path = f'{deploy_ctx.deploy_path}/ydb_node_static_{idx}/logs/ydb.log'
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
        log_path = f'{deploy_ctx.deploy_path}/ydb_node_dynamic_{idx}/logs/ydb.log'
        replace_args = {'%LOG_FILE_PATH%': log_path}
        src_path = f'{deploy_ctx.work_directory}/replaced/log.template'
        dest_path = f'{deploy_ctx.work_directory}/replaced/log-dynamic-{idx}'
        return tools.sed_command(src_path, dest_path, replace_args)


class PrepareWorkingDirCommands(term.ParallelledGroupOfShellCommands):
    def __init__(self):
        term.ParallelledGroupOfShellCommands.__init__(self, f"prepare working dir '{deploy_ctx.work_directory}'")
        self._commands = [
            f'mkdir -p {deploy_ctx.work_directory}/replaced',
            f'mkdir -p {deploy_ctx.work_directory}/static',
            f'mkdir -p {deploy_ctx.work_directory}/dynamic',
        ]


def clear_configs():
    paths = [
        f'{deploy_ctx.work_directory}/replaced/ydb-*.cfg',
        f'{deploy_ctx.work_directory}/replaced/dynamic_server_*.cfg',
        f'{deploy_ctx.work_directory}/replaced/slot_cfg_*.cfg',
        f'{deploy_ctx.work_directory}/static/*',
        f'{deploy_ctx.work_directory}/dynamic/*',
    ]
    term.sync_shell(f'rm {" ".join(paths)}')


def verify_config(config: dict):
    if config['with_nbs']:
        has_nbs_database = False
        if config.get('domain') is not None and 'databases' in config['domain']:
            has_nbs_database = any((db['name'] == 'NBS' for db in config['domain']['databases']))
        if not has_nbs_database:
            print("expected database with name 'NBS'")
            return False
    return True


@progress.with_parent_task
async def act_generate(
    hosts,
    config: dict,
    parent_task: progress.TaskNode = None,
):
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

    static_cfg_groups = GenerateStaticNodeConfigsCommands(list(hosts), config['nodes_per_host']).subgroups(50)

    static_tasks = [
        term.make_parallel_shell_step(
            *static_cfg_groups,
            title="[bold green]Generate static node configs",
        ).run(parent_task, subtasks),
    ]

    tenants_tasks = []
    dynamic_hosts = [host for host in hosts if host != config.get('freehost')]
    next_host_for_dynnodes = itertools.cycle(dynamic_hosts)
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

        dynamic_cfg_groups = GenerateDynamicNodeConfigsCommands(tenants).subgroups(50)

        tenants_tasks = [
            term.make_parallel_shell_step(
                *dynamic_cfg_groups,
                title="[bold green]Generate dynamic node configs",
            ).run(parent_task, subtasks),
        ]

    success = await tools.chain_async(
        *static_tasks,
        *tenants_tasks,
        term.shell(f'mkdir -p {deploy_ctx.deploy_path}/static && cp {deploy_ctx.work_directory}/config.yaml {deploy_ctx.work_directory}/static/config.yaml'),
    )

    for subtask in subtasks:
        await subtask.update(visible=False)

    return success
