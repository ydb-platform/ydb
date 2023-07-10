from google.protobuf import text_format
import ydb.apps.dstool.lib.common as common
import sys

description = 'Add disk to storage box by serial number'


def add_options(p):
    p.add_argument('--serial', type=str, required=True, help='Disk serial number')
    p.add_argument('--box', type=int, required=True, help='Box of PDisk')
    p.add_argument('--kind', type=int, help='Kind of PDisk')
    types = common.EPDiskType.keys()
    p.add_argument('--pdisk-type', type=str, choices=types, default='UNKNOWN_TYPE', help='Type of PDisk')
    p.add_argument('--pdisk-config', type=str, metavar='TEXT_PROTOBUF', help='Proto config for PDisk')
    common.add_basic_format_options(p)


def create_request(args):
    request = common.kikimr_bsconfig.TConfigRequest(Rollback=args.dry_run)
    cmd = request.Command.add().AddDriveSerial
    cmd.Serial = args.serial
    cmd.BoxId = args.box
    if args.kind:
        cmd.Kind = args.kind
    if args.pdisk_type:
        cmd.PDiskType = common.EPDiskType.Value(args.pdisk_type)
    if args.pdisk_config:
        text_format.Parse(args.pdisk_config, cmd.PDiskConfig)
    return request


def perform_request(request):
    return common.invoke_bsc_request(request)


def is_successful_response(response):
    return common.is_successful_bsc_response(response)


def do(args):
    request = create_request(args)
    response = perform_request(request)
    common.print_request_result(args, request, response)
    if not is_successful_response(response):
        fail_reasons = set([status.FailReason for status in response.Status])
        if len(fail_reasons) == 1:
            sys.exit(100 + list(fail_reasons)[0])
        else:
            sys.exit(1)
