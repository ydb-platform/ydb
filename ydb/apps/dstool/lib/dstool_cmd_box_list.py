import ydb.core.protos.blobstorage_config_pb2 as kikimr_bsconfig
import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.table as table
from collections import defaultdict

description = 'List boxes'


def add_options(p):
    p.add_argument('--show-pdisk-status', action='store_true', help='Show columns with PDisk status')
    p.add_argument('--show-pdisk-usage', action='store_true', help='Show columns with PDisk usage')
    table.TableOutput([], col_units=[]).add_options(p)


def do(args):
    all_columns = [
        'BoxId',
        'DiskType',
        'PDisks_TOTAL',
        'PDisks_ACTIVE',
        'PDisks_INACTIVE',
        'PDisks_BROKEN',
        'PDisks_FAULTY',
        'PDisks_TO_BE_REMOVED',
        'Usage',
        'AvailableSize',
        'UsedSize',
        'TotalSize',
    ]
    visible_columns = [
        'BoxId',
        'DiskType',
        'PDisks_TOTAL',
    ]
    col_units = {
        'Usage': '%',
        'UsedSize': 'bytes',
        'AvailableSize': 'bytes',
        'TotalSize': 'bytes'
    }

    if args.show_pdisk_status or args.all_columns:
        visible_columns.extend(['PDisks_ACTIVE', 'PDisks_INACTIVE', 'PDisks_BROKEN', 'PDisks_FAULTY', 'PDisks_TO_BE_REMOVED'])

    if args.show_pdisk_usage or args.all_columns:
        visible_columns.extend(['Usage', 'AvailableSize', 'UsedSize', 'TotalSize'])

    table_output = table.TableOutput(all_columns, col_units=col_units, default_visible_columns=visible_columns)

    base_config = common.fetch_base_config()

    boxes = {}
    for pdisk in base_config.PDisk:
        if pdisk.BoxId not in boxes:
            boxes[pdisk.BoxId] = defaultdict(dict)

        if pdisk.Type not in boxes[pdisk.BoxId]:
            boxes[pdisk.BoxId][pdisk.Type] = defaultdict(int)

        box = boxes[pdisk.BoxId][pdisk.Type]

        for key in ['PDisks_TOTAL', 'PDisks_' + kikimr_bsconfig.EDriveStatus.Name(pdisk.DriveStatus)]:
            box[key] += 1

        box['TotalSize'] += pdisk.PDiskMetrics.TotalSize
        box['AvailableSize'] += pdisk.PDiskMetrics.AvailableSize
        box['UsedSize'] += (pdisk.PDiskMetrics.TotalSize - pdisk.PDiskMetrics.AvailableSize)

    rows = []
    for box_id, box_data in boxes.items():
        for disk_type, box in box_data.items():
            row = {}
            row['BoxId'] = box_id
            row['DiskType'] = common.EPDiskType.Name(disk_type)

            for key, value in box.items():
                row[key] = value

            # set missing columns to 0
            for column in visible_columns:
                if column not in row:
                    row[column] = 0

            if 'UsedSize' in row and 'TotalSize' in row:
                row['Usage'] = row['UsedSize'] / row['TotalSize'] if row['TotalSize'] != 0 else 0.0
            else:
                row['Usage'] = 0.0
            rows.append(row)

    table_output.dump(rows, args)
