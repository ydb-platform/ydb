import ydb.core.protos.blobstorage_config_pb2 as kikimr_bsconfig
import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.table as table

description = 'List storage devices'


def add_options(p):
    table.TableOutput([], col_units=[]).add_options(p)


def do(args):
    base_config = common.fetch_base_config()
    node_to_fqdn = common.fetch_node_to_fqdn_map()

    all_columns = [
        'SerialNumber',
        'NodeId',
        'FQDN',
        'Path',
        'Type',
        'StorageStatus',
        'BoxId',
    ]
    visible_columns = [
        'SerialNumber',
        'FQDN',
        'Path',
        'Type',
        'StorageStatus',
    ]

    table_output = table.TableOutput(
        all_columns,
        default_visible_columns=visible_columns)

    rows = []
    for device in base_config.Device:
        row = {}
        row['SerialNumber'] = device.SerialNumber
        row['NodeId'] = device.NodeId
        row['FQDN'] = node_to_fqdn[device.NodeId]
        row['Type'] = kikimr_bsconfig.EPDiskType.Name(device.Type)
        row['StorageStatus'] = kikimr_bsconfig.TDriveLifeStage.E.Name(device.LifeStage)
        row['Path'] = device.Path
        row['BoxId'] = device.BoxId
        rows.append(row)

    table_output.dump(rows, args)
