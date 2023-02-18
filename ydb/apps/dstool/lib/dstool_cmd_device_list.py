import ydb.core.protos.blobstorage_config_pb2 as kikimr_bsconfig
import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.table as table

description = 'List available storage devices'


def add_options(p):
    table.TableOutput([], col_units=[]).add_options(p)


def do(args):
    base_config = common.fetch_base_config()
    node_to_fqdn = common.fetch_node_to_fqdn_map()

    all_columns = [
        'SerialNumber',
        'FQDN',
        'Path',
        'Type',
        'StorageStatus',
        'NodeId:PDiskId',
        'NodeId',
        'Guid',
        'BoxId',
    ]
    visible_columns = [
        'SerialNumber',
        'FQDN',
        'Path',
        'Type',
        'StorageStatus',
        'NodeId:PDiskId',
    ]

    table_output = table.TableOutput(
        all_columns,
        default_visible_columns=visible_columns)

    rows = []
    for device in base_config.Device:
        usedByPDisk = True if device.PDiskId > 0 else False

        row = {}
        row['SerialNumber'] = device.SerialNumber
        row['NodeId'] = device.NodeId
        row['FQDN'] = node_to_fqdn[device.NodeId]
        row['Path'] = device.Path
        row['Type'] = kikimr_bsconfig.EPDiskType.Name(device.Type)
        row['BoxId'] = device.BoxId
        row['Guid'] = device.Guid if device.Guid > 0 else 'NULL'

        if usedByPDisk:
            row['NodeId:PDiskId'] = '[%u:%u]' % (device.NodeId, device.PDiskId)
        else:
            row['NodeId:PDiskId'] = 'NULL'

        if usedByPDisk:
            if device.LifeStage == kikimr_bsconfig.TDriveLifeStage.E.ADDED_TO_BSC:
                row['StorageStatus'] = 'Configured by ydb-dstool'
            else:
                row['StorageStatus'] = 'Configured by DefineBox'
        else:
            row['StorageStatus'] = 'FREE'

        rows.append(row)

    table_output.dump(rows, args)
