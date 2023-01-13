import ydb.core.protos.blobstorage_config_pb2 as kikimr_bsconfig
import ydb.core.protos.blobstorage_disk_color_pb2 as disk_color
import ydb.apps.dstool.lib.table as table
import ydb.apps.dstool.lib.common as common

description = 'Get cluster wide settings'


def add_options(p):
    table.TableOutput([]).add_options(p)


def fetch_settings(args):
    request = common.create_bsc_request(args)
    request.Command.add().ReadSettings.CopyFrom(kikimr_bsconfig.TReadSettings())
    response = common.invoke_bsc_request(request)
    assert len(response.Status) == 1
    assert response.Status[0].Success, 'ReadSettings failed with error: %s' % response.Status[0].ErrorDescription
    return response.Status[0].Settings


def do(args):
    columns = [
        'DefaultMaxSlots',
        'EnableSelfHeal',
        'EnableDonorMode',
        'ScrubPeriodicitySeconds',
        'PDiskSpaceMarginPromille',
        'GroupReserveMin',
        'GroupReservePartPPM',
        'MaxScrubbedDisksAtOnce',
        'EnableGroupLayoutSanitizer',
        'PDiskSpaceColorBorder',
        'SerialManagementStage',
    ]

    table_output = table.TableOutput(columns, default_visible_columns=columns)

    settings = fetch_settings(args)
    row = {}
    for attr in columns:
        if not hasattr(settings, attr):
            continue
        value = getattr(settings, attr)[0]
        if attr == 'PDiskSpaceColorBorder':
            row[attr] = disk_color.TPDiskSpaceColor.E.Name(value)
        elif attr == 'SerialManagementStage':
            row[attr] = kikimr_bsconfig.TSerialManagementStage.E.Name(value)
        else:
            row[attr] = value

    table_output.dump([row], args)
