from collections import defaultdict

import ydb.core.protos.blobstorage_base3_pb2 as kikimr_bs3
import ydb.core.protos.blobstorage_disk_color_pb2 as kikimr_disk_color
import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.table as table
import ydb.public.api.protos.draft.ydb_distributed_storage_pb2 as ydb_distributed_storage

description = 'List groups'


def _enum_name(enum, value, prefix):
    return enum.Name(value)[len(prefix):]


def _group_status_name(value):
    name = _enum_name(ydb_distributed_storage.GroupStatus, value, 'GROUP_STATUS_')
    return 'UNKNOWN' if name == 'UNSPECIFIED' else name


def _prefixed_enum_value(enum, prefix, name):
    try:
        return enum.Value(prefix + name)
    except ValueError:
        return 0


def _copy_legacy_vdisk_id(source, target):
    target.group_id = source.GroupId
    target.group_generation = source.GroupGeneration
    target.fail_realm_idx = source.FailRealmIdx
    target.fail_domain_idx = source.FailDomainIdx
    target.vdisk_idx = source.VDiskIdx


def _copy_legacy_vslot_id(source, target):
    target.node_id = source.NodeId
    target.pdisk_id = source.PDiskId
    target.vslot_id = source.VSlotId


def _convert_legacy_storage_state(data):
    result = ydb_distributed_storage.StorageStateResult()
    base_config = data['BaseConfig']

    for source in data['StoragePools']:
        storage_pool = result.storage_pools.add()
        storage_pool.box_id = source.BoxId
        storage_pool.storage_pool_id = source.StoragePoolId
        storage_pool.name = source.Name

    for source in base_config.Group:
        group = result.groups.add()
        group.group_id = source.GroupId
        group.generation = source.GroupGeneration
        group.erasure_species = source.ErasureSpecies
        group.box_id = source.BoxId
        group.storage_pool_id = source.StoragePoolId
        group.size_in_units = source.GroupSizeInUnits
        group.seen_operational = source.SeenOperational

        expected_status = kikimr_bs3.TGroupStatus.E.Name(source.ExpectedStatus)
        operating_status = kikimr_bs3.TGroupStatus.E.Name(source.OperatingStatus)
        group.expected_status = _prefixed_enum_value(
            ydb_distributed_storage.GroupStatus, 'GROUP_STATUS_',
            'UNSPECIFIED' if expected_status == 'UNKNOWN' else expected_status)
        group.operating_status = _prefixed_enum_value(
            ydb_distributed_storage.GroupStatus, 'GROUP_STATUS_',
            'UNSPECIFIED' if operating_status == 'UNKNOWN' else operating_status)

        if source.HasField('VirtualGroupInfo'):
            source_virtual_group = source.VirtualGroupInfo
            virtual_group = group.virtual_group
            virtual_group.state = _prefixed_enum_value(
                ydb_distributed_storage.VirtualGroupState, 'VIRTUAL_GROUP_STATE_',
                common.EVirtualGroupState.Name(source_virtual_group.State))
            virtual_group.name = source_virtual_group.Name
            virtual_group.blob_depot_id = source_virtual_group.BlobDepotId
            virtual_group.error_reason = source_virtual_group.ErrorReason
            virtual_group.decommit_status = _prefixed_enum_value(
                ydb_distributed_storage.GroupDecommitStatus, 'GROUP_DECOMMIT_STATUS_',
                common.TGroupDecommitStatus.E.Name(source_virtual_group.DecommitStatus))

    for source in base_config.VSlot:
        vdisk = result.vdisks.add()
        _copy_legacy_vdisk_id(source, vdisk.id)
        _copy_legacy_vslot_id(source.VSlotId, vdisk.slot_id)
        vdisk.status = _prefixed_enum_value(
            ydb_distributed_storage.VDiskStatus, 'VDISK_STATUS_', source.Status)

        metrics = source.VDiskMetrics
        vdisk.allocated_size = metrics.AllocatedSize if metrics.HasField('AllocatedSize') else source.AllocatedSize
        vdisk.available_size = metrics.AvailableSize
        if metrics.HasField('VDiskSlotUsage'):
            vdisk.slot_usage = metrics.VDiskSlotUsage / 100
        if metrics.HasField('VDiskRawUsage'):
            vdisk.raw_usage = metrics.VDiskRawUsage / 100
        if metrics.HasField('NormalizedOccupancy'):
            vdisk.normalized_occupancy = metrics.NormalizedOccupancy
        if metrics.HasField('CapacityAlert'):
            color = kikimr_disk_color.TPDiskSpaceColor.E.Name(metrics.CapacityAlert)
            vdisk.space_color = _prefixed_enum_value(
                ydb_distributed_storage.SpaceColor, 'SPACE_COLOR_', color)

    for source in base_config.PDisk:
        pdisk = result.pdisks.add()
        pdisk.id.node_id = source.NodeId
        pdisk.id.pdisk_id = source.PDiskId
        _, pdisk.slot_size_in_units = common.get_pdisk_inferred_settings(source)
        pdisk.enforced_dynamic_slot_size = source.PDiskMetrics.EnforcedDynamicSlotSize

    return result


def _fetch_storage(args):
    try:
        return common.fetch_storage_state(storage_pools=True, groups=True, vdisks=True, pdisks=True)
    except common.DistributedStorageUnavailable as error:
        common.print_if_verbose(args, 'INFO: %s; falling back to BlobStorageConfig' % error)
        data = common.fetch_base_config_and_storage_pools(virtualGroupsOnly=args.virtual_groups_only)
        return _convert_legacy_storage_state(data)


def add_options(p):
    p.add_argument('--show-vdisk-status', action='store_true', help='Show columns with VDisk status')
    p.add_argument('--show-vdisk-usage', action='store_true', help='Show columns with VDisk usage')
    p.add_argument('--virtual-groups-only', action='store_true', help='Show only virtual groups')
    table.TableOutput([], col_units=[]).add_options(p)


def do(args):
    storage = _fetch_storage(args)
    groups = [group for group in storage.groups if common.is_dynamic_group(group.group_id)]
    if args.virtual_groups_only:
        groups = [group for group in groups if group.HasField('virtual_group')]

    group_map = {group.group_id: group for group in groups}
    pdisk_map = {(pdisk.id.node_id, pdisk.id.pdisk_id): pdisk for pdisk in storage.pdisks}
    sp_name = {
        (storage_pool.box_id, storage_pool.storage_pool_id): storage_pool.name
        for storage_pool in storage.storage_pools
    }

    all_columns = [
        'GroupId',
        'BoxId:PoolId',
        'PoolName',
        'BoxId',
        'PoolId',
        'Generation',
        'ErasureSpecies',
        'SizeInUnits',
        'ExpectedStatus',
        'OperatingStatus',
        'SeenOperational',
        'VDisks_TOTAL',
        'VDisks_READY',
        'VDisks_ERROR',
        'VDisks_REPLICATING',
        'VDisks_INIT_PENDING',
        'UsedSize',
        'AvailableSize',
        'Limit',
        'TotalSize',  # legacy
        'VDiskSlotUsage',
        'VDiskRawUsage',
        'NormalizedOccupancy',
        'CapacityAlert',
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
        'SizeInUnits',
        'OperatingStatus',
        'CapacityAlert',
        'VDisks_TOTAL',
    ]
    col_units = {
        'UsedSize': 'bytes',
        'AvailableSize': 'bytes',
        'Limit': 'bytes',
        'TotalSize': 'bytes',
        'VDiskSlotUsage': '%',
        'VDiskRawUsage': '%',
    }

    if args.show_vdisk_status or args.all_columns:
        visible_columns.extend(['VDisks_READY', 'VDisks_ERROR', 'VDisks_REPLICATING', 'VDisks_INIT_PENDING'])

    if args.show_vdisk_usage or args.all_columns:
        visible_columns.extend(['UsedSize', 'AvailableSize', 'Limit', 'TotalSize', 'VDiskSlotUsage'])

    if args.virtual_groups_only:
        visible_columns.extend(['VirtualGroupState', 'VirtualGroupName', 'BlobDepotId', 'ErrorReason', 'DecommitStatus'])

    table_output = table.TableOutput(all_columns, col_units=col_units, default_visible_columns=visible_columns)

    group_stat_map = defaultdict(lambda: defaultdict(int))

    for group_id, group in group_map.items():
        group_stat = group_stat_map[group_id]
        group_stat['BoxId:PoolId'] = '[%d:%d]' % (group.box_id, group.storage_pool_id)
        group_stat['PoolName'] = sp_name[(group.box_id, group.storage_pool_id)]
        group_stat['GroupId'] = group.group_id
        group_stat['Generation'] = group.generation
        group_stat['ErasureSpecies'] = group.erasure_species
        group_stat['SizeInUnits'] = group.size_in_units
        group_stat['ExpectedStatus'] = _group_status_name(group.expected_status)
        group_stat['OperatingStatus'] = _group_status_name(group.operating_status)
        group_stat['SeenOperational'] = group.seen_operational

        if group.HasField('virtual_group'):
            virtual_group = group.virtual_group
            group_stat['VirtualGroupState'] = _enum_name(ydb_distributed_storage.VirtualGroupState,
                                                         virtual_group.state, 'VIRTUAL_GROUP_STATE_')
            group_stat['VirtualGroupName'] = virtual_group.name
            group_stat['BlobDepotId'] = virtual_group.blob_depot_id
            group_stat['ErrorReason'] = virtual_group.error_reason
            group_stat['DecommitStatus'] = _enum_name(ydb_distributed_storage.GroupDecommitStatus,
                                                      virtual_group.decommit_status,
                                                      'GROUP_DECOMMIT_STATUS_')
        else:
            group_stat['VirtualGroupState'] = 'NEW'
            group_stat['VirtualGroupName'] = ''
            group_stat['BlobDepotId'] = 0
            group_stat['ErrorReason'] = ''
            group_stat['DecommitStatus'] = 'NONE'

        group_stat['UsedSize'] = 0
        group_stat['Limit'] = 0
        group_stat['TotalSize'] = 0
        group_stat['AvailableSize'] = 0
        group_stat['VDiskSlotUsage'] = None
        group_stat['VDiskRawUsage'] = None
        group_stat['NormalizedOccupancy'] = None
        group_stat['CapacityAlert'] = None

    for vdisk in storage.vdisks:
        group_id = vdisk.id.group_id
        if group_id not in group_map:
            continue

        group = group_map[group_id]
        group_stat = group_stat_map[group_id]

        group_stat['UsedSize'] += vdisk.allocated_size
        group_stat['TotalSize'] += vdisk.allocated_size
        group_stat['AvailableSize'] += vdisk.available_size
        group_stat['TotalSize'] += vdisk.available_size

        pdisk = pdisk_map.get((vdisk.slot_id.node_id, vdisk.slot_id.pdisk_id))
        vdisk_slot_size = 0
        if pdisk is not None and pdisk.enforced_dynamic_slot_size > 0:
            weight = common.get_vslot_owner_weight(group.size_in_units, pdisk.slot_size_in_units)
            vdisk_slot_size = pdisk.enforced_dynamic_slot_size * weight
            group_stat['Limit'] += vdisk_slot_size

        # Aggregate capacity metrics - use max values
        if vdisk.HasField('slot_usage'):
            group_stat['VDiskSlotUsage'] = max(group_stat['VDiskSlotUsage'] or 0, vdisk.slot_usage)

        if vdisk.HasField('raw_usage'):
            group_stat['VDiskRawUsage'] = max(group_stat['VDiskRawUsage'] or 0, vdisk.raw_usage)
        elif vdisk_slot_size > 0:
            # VDiskRawUsage metric was added in 26.1.1
            # For older versions we calculate it on client side
            #
            # Formula matches blobstorage_pdisk_keeper.h GetVDiskRawUsage()
            #   VDiskRawUsage = 100.0 * (used / hardLimit)
            # Per blobstorage_pdisk_impl.cpp TPDisk::WhiteboardReport(), EnforcedDynamicSlotSize is calculated as:
            #   EnforcedDynamicSlotSize = min(HardLimit / Weight) across all owners
            #
            vdisk_raw_usage = vdisk.allocated_size / vdisk_slot_size
            group_stat['VDiskRawUsage'] = max(group_stat['VDiskRawUsage'] or 0, vdisk_raw_usage)

        if vdisk.HasField('normalized_occupancy'):
            group_stat['NormalizedOccupancy'] = max(group_stat['NormalizedOccupancy'] or 0,
                                                    vdisk.normalized_occupancy)

        if vdisk.HasField('space_color'):
            # Take the worst (maximum) alert level across all VDisks
            group_stat['CapacityAlert'] = max(group_stat['CapacityAlert'] or 0, vdisk.space_color)

        status = _enum_name(ydb_distributed_storage.VDiskStatus, vdisk.status, 'VDISK_STATUS_')
        for key in ['VDisks_TOTAL', 'VDisks_' + status]:
            group_stat[key] += 1

    rows = []
    for group_stat in group_stat_map.values():
        # set missing columns to 0
        for column in visible_columns:
            if column not in group_stat:
                group_stat[column] = 0

        # Convert CapacityAlert from enum to string name
        if isinstance(group_stat['CapacityAlert'], int):
            group_stat['CapacityAlert'] = _enum_name(ydb_distributed_storage.SpaceColor,
                                                     group_stat['CapacityAlert'], 'SPACE_COLOR_')

        rows.append(group_stat)

    table_output.dump(rows, args)
