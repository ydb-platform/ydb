import ydb.core.protos.blobstorage_config_pb2 as kikimr_bsconfig
import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.table as table

description = 'List vdisks'


def add_options(p):
    p.add_argument('--show-pdisk-status', action='store_true', help='Show columns with PDisk statuses')
    p.add_argument('--show-vdisk-usage', action='store_true', help='Show columns with VDisk usage')
    table.TableOutput([], col_units=[]).add_options(p)


def do(args):
    base_config_and_storage_pools = common.fetch_base_config_and_storage_pools()
    base_config = base_config_and_storage_pools['BaseConfig']
    storage_pools = base_config_and_storage_pools['StoragePools']

    group_map = common.build_group_map(base_config)
    node_fqdn_map = common.build_node_fqdn_map(base_config)
    pdisk_map = common.build_pdisk_map(base_config)
    vslot_map = common.build_vslot_map(base_config)

    sp_name = {
        (sp.BoxId, sp.StoragePoolId): sp.Name
        for sp in storage_pools
    }

    group_to_sp_name = {
        group_id: sp_name[group.BoxId, group.StoragePoolId]
        for group_id, group in group_map.items()
    }

    all_columns = [
        'VDiskId',
        'GroupId',
        'GroupGeneration',
        'NodeId:PDiskId',
        'FQDN',
        'NodeId',
        'PDiskId',
        'PDiskDriveStatus',
        'PDiskDecommitStatus',
        'PDiskPath',
        'VSlotId',
        'VSlotStatus',
        'IsDonor',
        'FailRealmIdx',
        'FailDomainIdx',
        'VDiskIdx',
        'VDiskKind',
        'Usage',
        'UsedSize',
        'AvailableSize',
        'TotalSize',
        'SatisfactionRank',
        'PoolName',
        'BoxId',
        'PDiskPage',
        'VDiskPage',
    ]
    visible_columns = [
        'VDiskId',
        'GroupId',
        'NodeId:PDiskId',
        'VSlotId',
        'VSlotStatus',
        'IsDonor',
        'ReadOnly',
    ]
    col_units = {
        'Usage': '%',
        'UsedSize': 'bytes',
        'AvailableSize': 'bytes',
        'TotalSize': 'bytes'
    }

    if args.show_pdisk_status:
        visible_columns.extend(['PDiskDriveStatus', 'PDiskDecommitStatus'])

    if args.show_vdisk_usage:
        visible_columns.extend(['Usage', 'UsedSize', 'AvailableSize', 'TotalSize'])

    table_output = table.TableOutput(all_columns, col_units=col_units, default_visible_columns=visible_columns)

    rows = []
    for group in group_map:
        for vslot_data in group_map[group].VSlotId:
            pdisk = pdisk_map[vslot_data.NodeId, vslot_data.PDiskId]
            vslot = vslot_map[common.get_vslot_id(vslot_data)]
            row = {}
            row['BoxId'] = pdisk.BoxId
            row['PoolName'] = group_to_sp_name[group]
            row['VDiskId'] = '[%08x:%u:%u:%u:%u]' % (vslot.GroupId, vslot.GroupGeneration, vslot.FailRealmIdx, vslot.FailDomainIdx, vslot.VDiskIdx)
            row['GroupId'] = group
            row['GroupGeneration'] = vslot.GroupGeneration
            row['NodeId:PDiskId'] = '[%u:%u]' % (vslot_data.NodeId, vslot_data.PDiskId)
            row['FQDN'] = node_fqdn_map[vslot_data.NodeId]
            row['NodeId'] = vslot_data.NodeId
            row['PDiskId'] = vslot_data.PDiskId
            row['PDiskDriveStatus'] = kikimr_bsconfig.EDriveStatus.Name(pdisk.DriveStatus)
            row['PDiskDecommitStatus'] = kikimr_bsconfig.EDecommitStatus.Name(pdisk.DecommitStatus)
            row['PDiskPath'] = pdisk.Path
            row['VSlotId'] = vslot_data.VSlotId
            row['VSlotStatus'] = vslot.Status
            row['IsDonor'] = group_map[group].GroupGeneration != vslot.GroupGeneration
            row['ReadOnly'] = vslot.ReadOnly
            row['FailRealmIdx'] = vslot.FailRealmIdx
            row['FailDomainIdx'] = vslot.FailDomainIdx
            row['VDiskIdx'] = vslot.VDiskIdx
            row['VDiskKind'] = vslot.VDiskKind
            row['UsedSize'] = vslot.VDiskMetrics.AllocatedSize
            row['AvailableSize'] = vslot.VDiskMetrics.AvailableSize
            row['TotalSize'] = row['UsedSize'] + row['AvailableSize']
            row['Usage'] = row['UsedSize'] / row['TotalSize'] if row['TotalSize'] > 0 else 0.0
            row['PDiskPage'] = 'actors/pdisks/pdisk%09u' % (vslot_data.PDiskId)
            row['VDiskPage'] = 'actors/vdisks/vdisk%09u_%09u' % (vslot_data.PDiskId, vslot_data.VSlotId)
            rows.append(row)

    table_output.dump(rows, args)
