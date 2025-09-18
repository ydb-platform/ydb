import ydb.core.protos.blobstorage_config_pb2 as kikimr_bsconfig
import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.table as table
from google.protobuf import text_format

description = 'List pdisks'


def add_options(p):
    p.add_argument('--show-pdisk-usage', action='store_true', help='Show columns with PDisk usage')
    table.TableOutput([], col_units=[]).add_options(p)


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
        'ExpectedSlotCount',
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
        row['ExpectedSlotCount'] = pdisk.ExpectedSlotCount
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
