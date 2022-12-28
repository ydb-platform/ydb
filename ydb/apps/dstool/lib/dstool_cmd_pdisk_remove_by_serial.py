import ydb.apps.dstool.lib.common as common
import sys

description = 'Remove disk from storage by serial number'


def add_options(p):
    p.add_argument('--serial', type=str, required=True, help="Disk serial number")
    common.add_basic_format_options(p)


def create_request(args):
    request = common.kikimr_bsconfig.TConfigRequest(Rollback=args.dry_run)
    cmd = request.Command.add().RemoveDriveSerial
    cmd.Serial = args.serial
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
