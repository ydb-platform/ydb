import ydb.core.protos.blobstorage_base3_pb2 as kikimr_bs3
import ydb.core.protos.blobstorage_disk_color_pb2 as kikimr_disk_color
import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.table as table

description = 'List vdisks'


def add_options(p):
    p.add_argument('--show-pdisk-status', action='store_true', help='Show columns with PDisk statuses')
    p.add_argument('--show-vdisk-usage', action='store_true', help='Show columns with VDisk usage')
    p.add_argument('--show-donors', action='store_true', help='Show donor vdisks')
    table.TableOutput([], col_units=[]).add_options(p)


def make_row(group, group_to_sp_name, node_fqdn_map, pdisk_map, vslot_data, group_generation, fail_realm_idx,
             fail_domain_idx, vdisk_idx, vdisk_metrics, vslot_status, vdisk_kind, read_only, is_donor):
    pdisk = pdisk_map[vslot_data.NodeId, vslot_data.PDiskId]
    row = {}
    row['BoxId'] = pdisk.BoxId
    row['PoolName'] = group_to_sp_name[group.GroupId]
    row['VDiskId'] = '[%08x:%u:%u:%u:%u]' % (group.GroupId, group_generation, fail_realm_idx, fail_domain_idx, vdisk_idx)
    row['GroupId'] = group.GroupId
    row['GroupGeneration'] = group_generation
    row['NodeId:PDiskId'] = '[%u:%u]' % (vslot_data.NodeId, vslot_data.PDiskId)
    row['FQDN'] = node_fqdn_map[vslot_data.NodeId]
    row['NodeId'] = vslot_data.NodeId
    row['PDiskId'] = vslot_data.PDiskId
    row['PDiskDriveStatus'] = kikimr_bs3.EDriveStatus.Name(pdisk.DriveStatus)
    row['PDiskDecommitStatus'] = kikimr_bs3.EDecommitStatus.Name(pdisk.DecommitStatus)
    row['PDiskPath'] = pdisk.Path
    row['VSlotId'] = vslot_data.VSlotId
    row['VSlotStatus'] = vslot_status
    row['IsDonor'] = is_donor
    row['ReadOnly'] = read_only
    row['FailRealmIdx'] = fail_realm_idx
    row['FailDomainIdx'] = fail_domain_idx
    row['VDiskIdx'] = vdisk_idx
    row['VDiskKind'] = vdisk_kind
    row['GroupSizeInUnits'] = group.GroupSizeInUnits
    _, row['PDiskSlotSizeInUnits'] = common.get_pdisk_inferred_settings(pdisk)
    row['UsedSize'] = vdisk_metrics.AllocatedSize
    row['AvailableSize'] = vdisk_metrics.AvailableSize
    row['SlotSize'] = common.get_vslot_quota_from_pdisk(row['GroupSizeInUnits'], pdisk)
    row['TotalSize'] = row['UsedSize'] + row['AvailableSize']
    row['VDiskSlotUsage'] = None
    row['VDiskRawUsage'] = None
    row['NormalizedOccupancy'] = None
    row['CapacityAlert'] = None

    if vdisk_metrics.HasField('VDiskSlotUsage'):
        row['VDiskSlotUsage'] = vdisk_metrics.VDiskSlotUsage / 100

    if vdisk_metrics.HasField('VDiskRawUsage'):
        row['VDiskRawUsage'] = vdisk_metrics.VDiskRawUsage / 100
    elif row['SlotSize'] > 0:
        # VDiskRawUsage metric was added in 26.1.1
        # For older versions we calculate it on client side
        #
        # Formula matches blobstorage_pdisk_keeper.h GetVDiskRawUsage()
        #   VDiskRawUsage = 100.0 * (used / hardLimit)
        # For slot-weight-based quotas, TPDisk::WhiteboardReport() uses:
        #   EnforcedDynamicSlotSize = min(HardLimit / Weight) across all owners
        # Fixed quotas instead scale by group units and are capped by the user chunk pool.
        #
        row['VDiskRawUsage'] = row['UsedSize'] / row['SlotSize']

    if vdisk_metrics.HasField('NormalizedOccupancy'):
        row['NormalizedOccupancy'] = vdisk_metrics.NormalizedOccupancy

    if vdisk_metrics.HasField('CapacityAlert'):
        row['CapacityAlert'] = kikimr_disk_color.TPDiskSpaceColor.E.Name(vdisk_metrics.CapacityAlert)

    row['PDiskPage'] = 'actors/pdisks/pdisk%09u' % (vslot_data.PDiskId)
    row['VDiskPage'] = 'actors/vdisks/vdisk%09u_%09u' % (vslot_data.PDiskId, vslot_data.VSlotId)
    return row


def build_rows(base_config, storage_pools, show_donors=False):
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

    rows = []
    for group in group_map:
        for vslot_data in group_map[group].VSlotId:
            vslot = vslot_map[common.get_vslot_id(vslot_data)]
            rows.append(make_row(group, group_to_sp_name, node_fqdn_map, pdisk_map, vslot_data,
                                 vslot.GroupGeneration, vslot.FailRealmIdx, vslot.FailDomainIdx, vslot.VDiskIdx,
                                 vslot.VDiskMetrics, vslot.Status, vslot.VDiskKind, vslot.ReadOnly, False))
            if show_donors:
                for donor in vslot.Donors:
                    rows.append(make_row(group, group_to_sp_name, node_fqdn_map, pdisk_map, donor.VSlotId,
                                         donor.VDiskId.GroupGeneration, donor.VDiskId.Ring,
                                         donor.VDiskId.Domain, donor.VDiskId.VDisk,
                                         donor.VDiskMetrics, None, None, None, True))
    return rows


def do(args):
    base_config_and_storage_pools = common.fetch_base_config_and_storage_pools()
    base_config = base_config_and_storage_pools['BaseConfig']
    storage_pools = base_config_and_storage_pools['StoragePools']

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
        'GroupSizeInUnits',
        'PDiskSlotSizeInUnits',
        'UsedSize',
        'AvailableSize',
        'SlotSize',
        'TotalSize',  # legacy
        'VDiskSlotUsage',
        'VDiskRawUsage',
        'NormalizedOccupancy',
        'CapacityAlert',
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
        'GroupSizeInUnits',
        'CapacityAlert',
        'IsDonor',
        'ReadOnly',
    ]
    col_units = {
        'UsedSize': 'bytes',
        'AvailableSize': 'bytes',
        'SlotSize': 'bytes',
        'TotalSize': 'bytes',
        'VDiskSlotUsage': '%',
        'VDiskRawUsage': '%',
    }

    if args.show_pdisk_status:
        visible_columns.extend(['PDiskDriveStatus', 'PDiskDecommitStatus'])

    if args.show_vdisk_usage:
        visible_columns.extend(['UsedSize', 'AvailableSize', 'SlotSize', 'TotalSize', 'VDiskSlotUsage'])

    table_output = table.TableOutput(all_columns, col_units=col_units, default_visible_columns=visible_columns)

    rows = build_rows(base_config, storage_pools, show_donors=args.show_donors)

    table_output.dump(rows, args)
