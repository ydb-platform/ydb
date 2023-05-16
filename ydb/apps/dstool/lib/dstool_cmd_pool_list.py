import ydb.core.protos.blobstorage_config_pb2 as kikimr_bsconfig
import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.table as table
import math
from collections import defaultdict

description = 'List pools'


def add_options(p):
    p.add_argument('--show-group-status', action='store_true', help='Show columns with Group status')
    p.add_argument('--show-vdisk-status', action='store_true', help='Show columns with VDisk status')
    p.add_argument('--show-vdisk-usage', action='store_true', help='Show columns with VDisk usage')
    p.add_argument('--show-vdisk-estimated-usage', action='store_true', help='Show columns with VDisk estimated usage')
    table.TableOutput([], col_units=[]).add_options(p)


def apply_func(func, arg):
    if len(arg) > 0:
        return func(arg)
    else:
        return 0


def calculate_estimated_usage(pdisk_map, pdisk_slot_usage_map, vslot_map, groups):
    max_used_size = 0
    min_fair_size = 0
    vslot_fair_usages = []
    for group in groups:
        vslot_fair_sizes = []
        vslot_used_sizes = []

        for vslot in common.vslots_of_group(group, vslot_map):
            pdisk_id = common.get_pdisk_id(vslot.VSlotId)
            if pdisk_id not in pdisk_map:
                continue

            vslot_used_sizes.append(vslot.VDiskMetrics.AllocatedSize)
            vslot_fair_size = pdisk_map[pdisk_id].PDiskMetrics.TotalSize / pdisk_slot_usage_map[pdisk_id]
            vslot_fair_sizes.append(vslot_fair_size)

        min_vslot_fair_size = apply_func(min, vslot_fair_sizes)
        max_vslot_used_size = apply_func(max, vslot_used_sizes)
        min_vslot_used_size = apply_func(min, vslot_used_sizes)
        group_size = len(group.VSlotId)

        min_fair_size += min_vslot_fair_size * group_size
        max_used_size += max_vslot_used_size * group_size

        if min_vslot_fair_size > 0:
            vslot_fair_usages.append(min_vslot_used_size / min_vslot_fair_size)
        else:
            vslot_fair_usages.append(0.0)

    estimated_usage = max_used_size / min_fair_size if min_fair_size else 0.0
    max_vslot_fair_usage = apply_func(max, vslot_fair_usages)
    mean_vslot_fair_usage = apply_func(sum, vslot_fair_usages) / len(vslot_fair_usages) if len(vslot_fair_usages) > 0 else 0.0
    std_dev_vslot_fair_usage = math.sqrt(sum((x - mean_vslot_fair_usage)**2 for x in vslot_fair_usages) / len(vslot_fair_usages)) if len(vslot_fair_usages) > 0 else 0.0
    groups_fair_count = math.ceil(len(vslot_fair_usages) * estimated_usage / 0.85)

    res = {}
    res['EstimatedUsage'] = estimated_usage
    res['MaxVDiskEstimatedUsage'] = max_vslot_fair_usage
    res['MeanVDiskEstimatedUsage'] = mean_vslot_fair_usage
    res['StdDevVDiskEstimatedUsage'] = std_dev_vslot_fair_usage
    res['GroupsForEstimatedUsage@85'] = groups_fair_count
    return res


def do(args):
    all_columns = [
        'BoxId:PoolId',
        'PoolName',
        'BoxId',
        'PoolId',
        'ErasureSpecies',
        'Kind',
        'VDiskKind',
        'Groups_TOTAL',
        'Groups_UNKNOWN',
        'Groups_FULL',
        'Groups_PARTIAL',
        'Groups_DEGRADED',
        'Groups_DISINTEGRATED',
        'GroupsForEstimatedUsage@85',
        'VDisks_TOTAL',
        'VDisks_READY',
        'VDisks_ERROR',
        'VDisks_REPLICATING',
        'VDisks_INIT_PENDING',
        'Usage',
        'AvailableSize',
        'UsedSize',
        'TotalSize',
        'EstimatedUsage',
        'MaxVDiskEstimatedUsage',
        'MeanVDiskEstimatedUsage',
        'StdDevVDiskEstimatedUsage',
        'ItemConfigGeneration',
    ]
    visible_columns = [
        'BoxId:PoolId',
        'PoolName',
        'ErasureSpecies',
        'Kind',
        'Groups_TOTAL',
        'VDisks_TOTAL',
    ]
    col_units = {
        'Usage': '%',
        'AvailableSize': 'bytes',
        'UsedSize': 'bytes',
        'TotalSize': 'bytes',
        'EstimatedUsage': '%',
        'MaxVDiskEstimatedUsage': '%',
        'MeanVDiskEstimatedUsage': '%',
        'StdDevVDiskEstimatedUsage': '%',
    }

    if args.show_vdisk_status or args.all_columns:
        visible_columns.extend(['VDisks_READY', 'VDisks_ERROR', 'VDisks_REPLICATING', 'VDisks_INIT_PENDING'])

    if args.show_vdisk_usage or args.all_columns:
        visible_columns.extend(['Usage', 'AvailableSize', 'UsedSize', 'TotalSize'])

    if args.show_vdisk_estimated_usage or args.all_columns:
        visible_columns.extend(['GroupsForEstimatedUsage@85', 'EstimatedUsage', 'MaxVDiskEstimatedUsage', 'MeanVDiskEstimatedUsage', 'StdDevVDiskEstimatedUsage'])

    if args.show_group_status or args.all_columns:
        visible_columns.extend(['Groups_UNKNOWN', 'Groups_FULL', 'Groups_PARTIAL', 'Groups_DEGRADED', 'Groups_DISINTEGRATED'])

    table_output = table.TableOutput(all_columns, col_units=col_units, default_visible_columns=visible_columns)

    base_config_and_storage_pools = common.fetch_base_config_and_storage_pools()
    base_config = base_config_and_storage_pools['BaseConfig']

    box_pool_map = defaultdict(dict)

    group_map = common.build_group_map(base_config)
    for group_id, group in group_map.items():
        pool = box_pool_map[group.BoxId, group.StoragePoolId]

        if 'groups' not in pool:
            pool['groups'] = defaultdict(int)
        groups = pool['groups']

        for key in ['Groups_TOTAL', 'Groups_' + kikimr_bsconfig.TGroupStatus.E.Name(group.OperatingStatus)]:
            groups[key] += 1

        if 'groups_list' not in pool:
            pool['groups_list'] = []
        pool['groups_list'].append(group)

    vslot_map = common.build_vslot_map(base_config)
    for vslot_id, vslot in vslot_map.items():
        if vslot.GroupId not in group_map:
            continue

        group = group_map[vslot.GroupId]
        pool = box_pool_map[group.BoxId, group.StoragePoolId]

        if 'vslots' not in pool:
            pool['vslots'] = defaultdict(int)
        vslots = pool['vslots']

        vslots['UsedSize'] += vslot.VDiskMetrics.AllocatedSize
        vslots['TotalSize'] += vslot.VDiskMetrics.AllocatedSize
        vslots['AvailableSize'] += vslot.VDiskMetrics.AvailableSize
        vslots['TotalSize'] += vslot.VDiskMetrics.AvailableSize

        for key in ['VDisks_TOTAL', 'VDisks_' + vslot.Status]:
            vslots[key] += 1

    rows = []
    for sp in base_config_and_storage_pools['StoragePools']:
        row = {}
        row['BoxId:PoolId'] = '[%u:%u]' % (sp.BoxId, sp.StoragePoolId)
        row['BoxId'] = sp.BoxId
        row['PoolId'] = sp.StoragePoolId
        row['PoolName'] = sp.Name
        row['ErasureSpecies'] = sp.ErasureSpecies
        row['Kind'] = sp.Kind
        row['VDiskKind'] = sp.VDiskKind
        row['ItemConfigGeneration'] = sp.ItemConfigGeneration

        pool = box_pool_map[sp.BoxId, sp.StoragePoolId]

        # fill in groups data
        if 'groups' not in pool:
            pool['groups'] = defaultdict(int)
        groups = pool['groups']
        for key, value in groups.items():
            row[key] = value

        # fill in vslots data
        if 'vslots' not in pool:
            pool['vslots'] = defaultdict(int)
        vslots = pool['vslots']
        for key, value in vslots.items():
            row[key] = value

        # fill in per group stat
        if 'groups_list' not in pool:
            pool['groups_list'] = []

        # fill in usage estimations
        pdisk_map = common.build_pdisk_map(base_config)
        pdisk_slot_usage_map = common.build_pdisk_usage_map(base_config, count_donors=True)
        usage_map = calculate_estimated_usage(pdisk_map, pdisk_slot_usage_map, vslot_map, pool['groups_list'])

        for key, value in usage_map.items():
            row[key] = value

        # set missing columns to 0
        for column in visible_columns:
            if column not in row:
                row[column] = 0

        # fill usage at the end
        if 'UsedSize' in row and 'TotalSize' in row:
            row['Usage'] = row['UsedSize'] / row['TotalSize'] if row['TotalSize'] != 0 else 0.0
        else:
            row['Usage'] = 0.0

        rows.append(row)

    table_output.dump(rows, args)
