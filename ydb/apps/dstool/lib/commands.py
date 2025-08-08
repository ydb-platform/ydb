import ydb.apps.dstool.lib.dstool_cmd_device_list as device_list

import ydb.apps.dstool.lib.dstool_cmd_pdisk_add_by_serial as pdisk_add_by_serial
import ydb.apps.dstool.lib.dstool_cmd_pdisk_remove_by_serial as pdisk_remove_by_serial
import ydb.apps.dstool.lib.dstool_cmd_pdisk_set as pdisk_set
import ydb.apps.dstool.lib.dstool_cmd_pdisk_list as pdisk_list
import ydb.apps.dstool.lib.dstool_cmd_pdisk_stop as pdisk_stop
import ydb.apps.dstool.lib.dstool_cmd_pdisk_restart as pdisk_restart
import ydb.apps.dstool.lib.dstool_cmd_pdisk_readonly as pdisk_readonly
import ydb.apps.dstool.lib.dstool_cmd_pdisk_move as pdisk_move

import ydb.apps.dstool.lib.dstool_cmd_vdisk_evict as vdisk_evict
import ydb.apps.dstool.lib.dstool_cmd_vdisk_list as vdisk_list
import ydb.apps.dstool.lib.dstool_cmd_vdisk_set_read_only as vdisk_set_read_only
import ydb.apps.dstool.lib.dstool_cmd_vdisk_remove_donor as vdisk_remove_donor
import ydb.apps.dstool.lib.dstool_cmd_vdisk_wipe as vdisk_wipe

import ydb.apps.dstool.lib.dstool_cmd_group_add as group_add
import ydb.apps.dstool.lib.dstool_cmd_group_check as group_check
import ydb.apps.dstool.lib.dstool_cmd_group_decommit as group_decommit
import ydb.apps.dstool.lib.dstool_cmd_group_list as group_list
import ydb.apps.dstool.lib.dstool_cmd_group_show_blob_info as group_show_blob_info
import ydb.apps.dstool.lib.dstool_cmd_group_show_storage_efficiency as group_show_storage_efficiency
import ydb.apps.dstool.lib.dstool_cmd_group_show_usage_by_tablets as group_show_usage_by_tablets
import ydb.apps.dstool.lib.dstool_cmd_group_state as group_state
import ydb.apps.dstool.lib.dstool_cmd_group_take_snapshot as group_take_snapshot
import ydb.apps.dstool.lib.dstool_cmd_group_virtual_create as group_virtual_create
import ydb.apps.dstool.lib.dstool_cmd_group_virtual_cancel as group_virtual_cancel

import ydb.apps.dstool.lib.dstool_cmd_pool_create_virtual as pool_create_virtual
import ydb.apps.dstool.lib.dstool_cmd_pool_list as pool_list

import ydb.apps.dstool.lib.dstool_cmd_box_list as box_list

import ydb.apps.dstool.lib.dstool_cmd_node_list as node_list

import ydb.apps.dstool.lib.dstool_cmd_cluster_balance as cluster_balance
import ydb.apps.dstool.lib.dstool_cmd_cluster_list as cluster_list
import ydb.apps.dstool.lib.dstool_cmd_cluster_get as cluster_get
import ydb.apps.dstool.lib.dstool_cmd_cluster_set as cluster_set
import ydb.apps.dstool.lib.dstool_cmd_cluster_workload_run as cluster_workload_run

import sys
import ydb.apps.dstool.lib.common as common

MODULE_PREFIX = 'dstool_cmd_'

modules = [
    cluster_balance, cluster_get, cluster_set, cluster_list, cluster_workload_run,
    node_list,
    box_list,
    pool_list, pool_create_virtual,
    group_check, group_decommit, group_show_blob_info, group_show_storage_efficiency, group_show_usage_by_tablets,
    group_state, group_take_snapshot, group_add, group_list, group_virtual_create, group_virtual_cancel,
    pdisk_add_by_serial, pdisk_remove_by_serial, pdisk_set, pdisk_list, pdisk_stop, pdisk_restart, pdisk_readonly, pdisk_move,
    vdisk_evict, vdisk_list, vdisk_set_read_only, vdisk_remove_donor, vdisk_wipe,
    device_list,
]

default_structure = [
    ('device', ['list']),
    ('pdisk', ['add-by-serial', 'remove-by-serial', 'set', 'list', 'stop', 'restart', 'readonly', 'move']),
    ('vdisk', ['evict', 'list', 'set-read-only', 'remove-donor', 'wipe']),
    ('group', ['add', 'check', 'decommit', ('show', ['blob-info', 'storage-efficiency', 'usage-by-tablets']), 'state', 'take-snapshot', 'list', ('virtual', ['create', 'cancel'])]),
    ('pool', ['list', ('create', ['virtual'])]),
    ('box', ['list']),
    ('node', ['list']),
    ('cluster', ['balance', 'get', 'set', ('workload', ['run']), 'list']),
]


def make_command_map_by_structure(subparsers, modules=modules, structure=default_structure):
    module_map = {}
    for module in modules:
        last_name = module.__name__.split('.')[-1]
        module_map[last_name[len(MODULE_PREFIX):].replace('_', '-')] = module

    command_map = {}

    already_added = set()

    def add_commands_by_structue(struct, subparsers, prefix=''):
        nonlocal command_map
        for el in struct:
            if isinstance(el, tuple):
                def namespace_barrier():
                    parser = subparsers.add_parser(el[0])
                    group_name = prefix + el[0]
                    command_group_name = group_name.replace('_', '-')
                    new_prefix = group_name + '_'
                    new_prefix_for_command = new_prefix.replace('_', '-')
                    command_dest = new_prefix + 'command'
                    new_subparsers = parser.add_subparsers(dest=command_dest, required=True)
                    add_commands_by_structue(el[1], new_subparsers, new_prefix)
                    command_map[command_group_name] = lambda args: command_map[new_prefix_for_command + getattr(args, command_dest)](args)
                namespace_barrier()
            elif isinstance(el, str):
                def namespace_barrier():
                    command_name = prefix + el
                    key = command_name.replace('_', '-')
                    already_added.add(key)
                    if key in module_map:
                        module = module_map[key]
                        if 'help' in module.__dict__:
                            help = module.help
                        else:
                            help = module.description
                        module.add_options(subparsers.add_parser(el, help=help, description=module.description))
                        command_map[key] = module.do
                    else:
                        subparsers.add_parser(el, help='UNIMPLEMETED')
                namespace_barrier()
    add_commands_by_structue(structure, subparsers)

    for cmd_name in sorted(set(module_map) - already_added):
        module = module_map[cmd_name]
        module.add_options(subparsers.add_parser(cmd_name, help=module.description))
        command_map[cmd_name] = module.do

    return command_map


class TerminationOutput:
    def __init__(self):
        self._lines = []
        self._hint_lines = []

    def add_line(self, line):
        self._lines.append(line)

    def add_hint_line(self, line):
        self._hint_lines.append(line)

    def print_json(self):
        common.print_json_result('error', '\n'.join(self._lines))

    def print_pretty(self):
        for line in self._lines:
            print(line, file=sys.stderr)
        for line in self._hint_lines:
            print(line, file=sys.stderr)

    def print(self, args):
        if getattr(args, 'format', None) == 'json':
            self.print_json()
        else:
            self.print_pretty()


def run_command(command_map, args):
    output = TerminationOutput()
    try:
        command_map.get(args.global_command)(args)
    except common.ConnectionError as ex:
        output.add_line('Connection Error: {}'.format(ex))
        if common.connection_params.quiet:
            output.add_hint_line("Remove '--quiet' to see more information")
        output.print(args)
        sys.exit(1)
    except common.QueryError as ex:
        output.add_line('Query Error: {}'.format(ex))
        if common.connection_params.quiet:
            output.add_hint_line("Use '--verbose' to see more information")
        output.print(args)
        sys.exit(1)
    except common.GroupSelectionError as ex:
        output.add_line('Group Selection Error: {}'.format(ex))
        output.print(args)
        sys.exit(1)
    except Exception as ex:
        output.add_line('Unexpected Error: {}'.format(ex))
        output.print(args)
        raise
        sys.exit(1)
