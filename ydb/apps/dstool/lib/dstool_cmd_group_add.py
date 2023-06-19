import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.table as table
import sys
from collections import defaultdict

description = 'Add groups to the pool'


def add_options(p):
    p.add_argument('--pool-name', type=str, required=True, help='Storage pool to add to')
    p.add_argument('--groups', type=int, required=True, help='Number of groups to add')
    table.TableOutput([]).add_options(p)


def create_request(args, storage_pool):
    request = common.kikimr_bsconfig.TConfigRequest(Rollback=args.dry_run)
    cmd = request.Command.add()
    cmd.DefineStoragePool.CopyFrom(storage_pool)
    cmd.DefineStoragePool.NumGroups += args.groups
    cmd = request.Command.add()
    cmd.QueryBaseConfig.CopyFrom(common.kikimr_bsconfig.TQueryBaseConfig())
    return request


def perform_request(request):
    return common.invoke_bsc_request(request)


def is_successful_response(response):
    return common.is_successful_bsc_response(response)


def do(args):
    columns = [
        'BoxId:PoolId',
        'PoolName',
        'Affected PDisks',
        'Static slots on PDisk',
        'VSlots on PDisk before',
        'VSlots on PDisk after',
        'Result',
    ]

    base_config_and_storage_pools = common.fetch_base_config_and_storage_pools()
    base_config = base_config_and_storage_pools['BaseConfig']
    storage_pools = base_config_and_storage_pools['StoragePools']

    sp = None
    for p in storage_pools:
        if p.Name == args.pool_name:
            if sp is not None:
                common.print_status(args, success=False, error_reason='Storage pool name %s is not unique' % args.pool_name)
                sys.exit(1)
            sp = p

    if sp is None:
        common.print_status(args, success=False, error_reason="Couldn't find storage pool with name %s" % args.pool_name)
        sys.exit(1)

    request = create_request(args, sp)
    response = perform_request(request)
    if not is_successful_response(response):
        common.print_request_result(args, request, response)
        sys.exit(1)

    pdisk_usage_before = common.build_pdisk_usage_map(base_config, count_donors=False, storage_pool=sp)
    pdisk_usage_after = common.build_pdisk_usage_map(response.Status[1].BaseConfig, count_donors=False, storage_pool=sp)

    keys = set(pdisk_usage_before) | set(pdisk_usage_after)
    changes = defaultdict(int)

    pdisk_static_slots_map = common.build_pdisk_static_slots_map(base_config)

    for pdisk_id in keys:
        changes[pdisk_usage_before.get(pdisk_id, 0), pdisk_usage_after.get(pdisk_id, 0), pdisk_static_slots_map.get(pdisk_id, 0)] += 1

    table_output = table.TableOutput(columns, default_visible_columns=columns)

    rows = []
    for (before, after, static), num in sorted(changes.items(), key=lambda x: changes[x[0]], reverse=True):
        row = {}
        row['BoxId:PoolId'] = '[%u:%u]' % (sp.BoxId, sp.StoragePoolId)
        row['PoolName'] = sp.Name
        row['Affected PDisks'] = num
        row['Static slots on PDisk'] = static
        row['VSlots on PDisk before'] = before
        row['VSlots on PDisk after'] = after
        row['Result'] = 'unchanged' if before == after else 'increased'
        rows.append(row)

    table_output.dump(rows, args)
