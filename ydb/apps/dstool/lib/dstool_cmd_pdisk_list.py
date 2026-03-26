import ydb.core.protos.blobstorage_config_pb2 as kikimr_bsconfig
import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.table as table
from google.protobuf import text_format

description = 'List pdisks'


def add_options(p):
    p.add_argument('--show-pdisk-usage', action='store_true', help='Show columns with PDisk usage')
    p.add_argument('--check-leaked-slots', action='store_true', help='Perform additional check of the state consistency between BSController and PDisk')
    table.TableOutput([], col_units=[]).add_options(p)


def calculate_num_active_slots(base_config):
    group_size_map = {group.GroupId: group.GroupSizeInUnits for group in base_config.Group}

    slot_size_in_units_map = {}
    for pdisk in base_config.PDisk:
        pdisk_id = (pdisk.NodeId, pdisk.PDiskId)
        _, slot_size_in_units = common.get_pdisk_inferred_settings(pdisk)
        slot_size_in_units_map[pdisk_id] = slot_size_in_units

    num_active_slots_map = {}
    for vslot in base_config.VSlot:
        pdisk_id = (vslot.VSlotId.NodeId, vslot.VSlotId.PDiskId)
        slot_size_in_units = slot_size_in_units_map.get(pdisk_id, 0)
        group_size_in_units = group_size_map.get(vslot.GroupId, 0)
        weight = common.get_vslot_owner_weight(group_size_in_units, slot_size_in_units)
        num_active_slots_map[pdisk_id] = num_active_slots_map.get(pdisk_id, 0) + weight
        for donor in vslot.Donors:
            donor_pdisk_id = (donor.VSlotId.NodeId, donor.VSlotId.PDiskId)
            donor_slot_size_in_units = slot_size_in_units_map.get(donor_pdisk_id, 0)
            donor_group_size_in_units = group_size_map.get(donor.VDiskId.GroupID, 0)
            donor_weight = common.get_vslot_owner_weight(donor_group_size_in_units, donor_slot_size_in_units)
            num_active_slots_map[donor_pdisk_id] = num_active_slots_map.get(donor_pdisk_id, 0) + donor_weight

    return num_active_slots_map


def do(args):
    base_config = common.fetch_base_config()
    node_to_fqdn = common.fetch_node_to_fqdn_map()

    all_columns = [
        'NodeId:PDiskId',
        'NodeId',
        'PDiskId',
        'ExpectedSerial',
        'LastSeenSerial',
        'FQDN',
        'Path',
        'Type',
        'Status',
        'DecommitStatus',
        'MaintenanceStatus',
        'Kind',
        'BoxId',
        'Guid',
        'NumStaticSlots',
        'NumActiveSlots',
        'ExpectedSlotCount',
        'SlotSizeInUnits',
        'PDiskConfig',
        'Usage',
        'UsedSize',
        'AvailableSize',
        'TotalSize',
        'MaxReadThroughput',
        'MaxWriteThroughput',
        'MaxIOPS',
    ]
    visible_columns = [
        'NodeId:PDiskId',
        'ExpectedSerial',
        'FQDN',
        'Path',
        'Type',
        'Status',
        'DecommitStatus',
        'MaintenanceStatus',
    ]
    col_units = {
        'Usage': '%',
        'UsedSize': 'bytes',
        'AvailableSize': 'bytes',
        'TotalSize': 'bytes'
    }
    right_align = {
        'Usage',
        'UsedSize',
        'AvailableSize',
        'TotalSize',
    }

    if args.show_pdisk_usage:
        visible_columns.extend(['Usage', 'UsedSize', 'AvailableSize', 'TotalSize'])

    num_active_slots_map = None
    if args.all_columns or args.check_leaked_slots or (args.columns and 'NumActiveSlots' in args.columns):
        num_active_slots_map = calculate_num_active_slots(base_config)

    pdisk_whiteboard_info = {}
    if args.check_leaked_slots:
        pdisk_node_ids = sorted({pdisk.NodeId for pdisk in base_config.PDisk})
        pdisk_whiteboard_info = common.fetch_json_info('pdiskinfo', nodes=pdisk_node_ids)
        all_columns.insert(all_columns.index('NumActiveSlots')+1, 'LeakedSlots')
        visible_columns.extend(['NumActiveSlots', 'LeakedSlots'])

    table_output = table.TableOutput(
        all_columns,
        col_units=col_units,
        default_visible_columns=visible_columns,
        right_align=right_align)

    rows = []
    for pdisk in base_config.PDisk:
        row = {}
        row['ExpectedSerial'] = pdisk.ExpectedSerial
        row['LastSeenSerial'] = pdisk.LastSeenSerial
        row['NodeId:PDiskId'] = '[%u:%u]' % (pdisk.NodeId, pdisk.PDiskId)
        row['NodeId'] = pdisk.NodeId
        row['PDiskId'] = pdisk.PDiskId
        row['FQDN'] = node_to_fqdn[pdisk.NodeId]
        row['Path'] = pdisk.Path
        row['Status'] = kikimr_bsconfig.EDriveStatus.Name(pdisk.DriveStatus)
        row['DecommitStatus'] = kikimr_bsconfig.EDecommitStatus.Name(pdisk.DecommitStatus)
        row['MaintenanceStatus'] = kikimr_bsconfig.TMaintenanceStatus.E.Name(pdisk.MaintenanceStatus)
        row['Type'] = common.EPDiskType.Name(pdisk.Type)
        row['BoxId'] = pdisk.BoxId
        row['Kind'] = pdisk.Kind
        row['Guid'] = pdisk.Guid
        row['NumStaticSlots'] = pdisk.NumStaticSlots
        row['NumActiveSlots'] = num_active_slots_map.get((pdisk.NodeId, pdisk.PDiskId), 0) if num_active_slots_map else None
        if args.check_leaked_slots:
            wb_info = pdisk_whiteboard_info.get((pdisk.NodeId, pdisk.PDiskId), {})
            pdisk_reported = wb_info.get('NumActiveSlots')
            row['LeakedSlots'] = pdisk_reported - row['NumActiveSlots'] if pdisk_reported is not None else None
        row['ExpectedSlotCount'], row['SlotSizeInUnits'] = common.get_pdisk_inferred_settings(pdisk)
        row['PDiskConfig'] = text_format.MessageToString(pdisk.PDiskConfig, as_one_line=True)
        row['AvailableSize'] = pdisk.PDiskMetrics.AvailableSize
        row['TotalSize'] = pdisk.PDiskMetrics.TotalSize
        row['UsedSize'] = pdisk.PDiskMetrics.TotalSize - pdisk.PDiskMetrics.AvailableSize
        row['Usage'] = row['UsedSize'] / pdisk.PDiskMetrics.TotalSize if pdisk.PDiskMetrics.TotalSize > 0 else 0.0
        row['MaxReadThroughput'] = pdisk.PDiskMetrics.MaxReadThroughput
        row['MaxWriteThroughput'] = pdisk.PDiskMetrics.MaxWriteThroughput
        row['MaxIOPS'] = pdisk.PDiskMetrics.MaxIOPS
        rows.append(row)

    table_output.dump(rows, args)
