import ydb.apps.dstool.lib.common as common
import sys

description = 'Change group size in units'


def add_options(p):
    common.add_group_ids_option(p, required=True)
    p.add_argument('--size-in-units', type=int, required=True, help='New size in units for the groups')
    common.add_basic_format_options(p)


def do(args):
    base_config_and_storage_pools = common.fetch_base_config_and_storage_pools()
    base_config = base_config_and_storage_pools['BaseConfig']
    group_map = common.build_group_map(base_config)
    storage_pools = base_config_and_storage_pools['StoragePools']
    storage_pools_map = common.build_storage_pools_map(storage_pools)

    request = common.create_bsc_request(args)
    commands_map = {}
    for group_id in args.group_ids:
        if group_id not in group_map:
            common.print_status(args, success=False, error_reason=f'Group {group_id} not found')
            sys.exit(1)

        group = group_map[group_id]
        pool_key = (group.BoxId, group.StoragePoolId)

        if pool_key not in storage_pools_map:
            common.print_status(args, success=False, error_reason=f'Storage pool not found for group {group_id}')
            sys.exit(1)

        if pool_key not in commands_map:
            storage_pool = storage_pools_map[pool_key]
            cmd = request.Command.add().ChangeGroupSizeInUnits
            cmd.BoxId = storage_pool.BoxId
            cmd.StoragePoolId = storage_pool.StoragePoolId
            cmd.SizeInUnits = args.size_in_units
            cmd.ItemConfigGeneration = storage_pool.ItemConfigGeneration
            commands_map[pool_key] = cmd
        else:
            cmd = commands_map[pool_key]

        cmd.GroupId.append(group_id)

    response = common.invoke_bsc_request(request)
    common.print_request_result(args, request, response)
