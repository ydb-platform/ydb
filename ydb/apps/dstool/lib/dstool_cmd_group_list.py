import ydb.core.protos.blobstorage_config_pb2 as kikimr_bsconfig
import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.table as table
import sys
from collections import defaultdict

description = 'List groups'


def add_options(p):
    p.add_argument('--show-vdisk-status', action='store_true', help='Show columns with VDisk status')
    p.add_argument('--show-vdisk-usage', action='store_true', help='Show columns with VDisk usage')
    p.add_argument('--virtual-groups-only', action='store_true', help='Show only virtual groups')
    table.TableOutput([], col_units=[]).add_options(p)


def do(args):
    base_config_and_storage_pools = common.fetch_base_config_and_storage_pools(virtualGroupsOnly=args.virtual_groups_only)

    base_config = base_config_and_storage_pools['BaseConfig']
    group_map = common.build_group_map(base_config)
    vslot_map = common.build_vslot_map(base_config)

    storage_pools = base_config_and_storage_pools['StoragePools']
    sp_name = common.build_storage_pool_names_map(storage_pools)

    all_columns = [
        'GroupId',
        'BoxId:PoolId',
        'PoolName',
        'BoxId',
        'PoolId',
        'Generation',
        'ErasureSpecies',
        'ExpectedStatus',
        'OperatingStatus',
        'SeenOperational',
        'VDisks_TOTAL',
        'VDisks_READY',
        'VDisks_ERROR',
        'VDisks_REPLICATING',
        'VDisks_INIT_PENDING',
        'Usage',
        'UsedSize',
        'AvailableSize',
        'TotalSize',
        'VirtualGroupState',
        'VirtualGroupName',
        'BlobDepotId',
        'ErrorReason',
        'DecommitStatus',
    ]
    visible_columns = [
        'GroupId',
        'BoxId:PoolId',
        'PoolName',
        'Generation',
        'ErasureSpecies',
        'OperatingStatus',
        'VDisks_TOTAL',
    ]
    col_units = {
        'Usage': '%',
        'UsedSize': 'bytes',
        'AvailableSize': 'bytes',
        'TotalSize': 'bytes',
    }

    if args.show_vdisk_status or args.all_columns:
        visible_columns.extend(['VDisks_READY', 'VDisks_ERROR', 'VDisks_REPLICATING', 'VDisks_INIT_PENDING'])

    if args.show_vdisk_usage or args.all_columns:
        visible_columns.extend(['Usage', 'UsedSize', 'AvailableSize', 'TotalSize'])

    if args.virtual_groups_only:
        visible_columns.extend(['VirtualGroupState', 'VirtualGroupName', 'BlobDepotId', 'ErrorReason', 'DecommitStatus'])

    table_output = table.TableOutput(all_columns, col_units=col_units, default_visible_columns=visible_columns)

    group_stat_map = defaultdict(lambda: defaultdict(int))

    for group_id, group in group_map.items():
        group_stat = group_stat_map[group_id]
        group_stat['BoxId:PoolId'] = '[%d:%d]' % (group.BoxId, group.StoragePoolId)
        group_stat['PoolName'] = sp_name[(group.BoxId, group.StoragePoolId)]
        group_stat['GroupId'] = group.GroupId
        group_stat['Generation'] = group.GroupGeneration
        group_stat['ErasureSpecies'] = group.ErasureSpecies
        group_stat['ExpectedStatus'] = kikimr_bsconfig.TGroupStatus.E.Name(group.ExpectedStatus)
        group_stat['OperatingStatus'] = kikimr_bsconfig.TGroupStatus.E.Name(group.OperatingStatus)
        group_stat['SeenOperational'] = group.SeenOperational

        if group.VirtualGroupInfo:
            group_stat['VirtualGroupState'] = common.EVirtualGroupState.Name(group.VirtualGroupInfo.State)
            group_stat['VirtualGroupName'] = group.VirtualGroupInfo.Name
            group_stat['BlobDepotId'] = group.VirtualGroupInfo.BlobDepotId
            group_stat['ErrorReason'] = group.VirtualGroupInfo.ErrorReason
            group_stat['DecommitStatus'] = common.TGroupDecommitStatus.E.Name(group.VirtualGroupInfo.DecommitStatus)

        group_stat['UsedSize'] = 0
        group_stat['TotalSize'] = 0
        group_stat['AvailableSize'] = 0

    for vslot_id, vslot in vslot_map.items():
        group_id = vslot.GroupId
        if not common.is_dynamic_group(group_id):
            common.print_if_verbose(args, 'Skipping non dynamic group %d of vslot %s' % (vslot.GroupId, vslot), file=sys.stderr)
            continue

        if group_id not in group_map:
            common.print_if_not_quiet(args, 'Unknown group id %d of vslot %s' % (vslot.GroupId, vslot), file=sys.stderr)
            continue

        group = group_map[group_id]
        group_stat = group_stat_map[group_id]

        group_stat['UsedSize'] += vslot.VDiskMetrics.AllocatedSize
        group_stat['TotalSize'] += vslot.VDiskMetrics.AllocatedSize
        group_stat['AvailableSize'] += vslot.VDiskMetrics.AvailableSize
        group_stat['TotalSize'] += vslot.VDiskMetrics.AvailableSize

        for key in ['VDisks_TOTAL', 'VDisks_' + vslot.Status]:
            group_stat[key] += 1

    rows = []
    for group_stat in group_stat_map.values():
        # set missing columns to 0
        for column in visible_columns:
            if column not in group_stat:
                group_stat[column] = 0

        # calculate usage at the end
        group_stat['Usage'] = group_stat['UsedSize'] / group_stat['TotalSize'] if group_stat['TotalSize'] != 0 else 0.0

        rows.append(group_stat)

    table_output.dump(rows, args)
