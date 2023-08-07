import ydb.core.protos.blobstorage_config_pb2 as kikimr_bsconfig
import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.table as table

description = 'List available storage devices'


def add_options(p):
    table.TableOutput([], col_units=[]).add_options(p)


def do(args):
    base_config = common.fetch_base_config(retrieveDevices=True)
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

    pdiskBySerialNumber = {}
    for pdisk in base_config.PDisk:
        if pdisk.ExpectedSerial:
            pdiskBySerialNumber[pdisk.ExpectedSerial] = pdisk

    rows = []
    for device in base_config.Device:
        row = {}
        row['SerialNumber'] = device.SerialNumber
        row['NodeId'] = device.NodeId
        row['FQDN'] = node_to_fqdn[device.NodeId]
        row['Path'] = device.Path
        row['Type'] = common.EPDiskType.Name(device.Type)
        row['BoxId'] = device.BoxId
        row['Guid'] = device.Guid if device.Guid > 0 else 'NULL'

        if device.SerialNumber in pdiskBySerialNumber:
            pdisk = pdiskBySerialNumber[device.SerialNumber]
            row['NodeId:PDiskId'] = '[%u:%u]' % (pdisk.NodeId, pdisk.PDiskId)

            if device.LifeStage == kikimr_bsconfig.TDriveLifeStage.E.ADDED_BY_DSTOOL:
                row['StorageStatus'] = 'PDISK_ADDED_BY_DSTOOL'
            else:
                row['StorageStatus'] = 'PDISK_ADDED_BY_DEFINE_BOX'
        else:
            row['NodeId:PDiskId'] = 'NULL'
            row['StorageStatus'] = 'FREE'

        rows.append(row)

    table_output.dump(rows, args)
