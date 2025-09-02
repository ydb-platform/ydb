import ydb.apps.dstool.lib.common as common
import sys

description = 'Move PDisk'


def add_options(p):
    common.add_pdisk_select_options(p)
    common.add_ignore_degraded_group_check_option(p)
    common.add_ignore_failure_model_group_check_option(p)
    common.add_ignore_vslot_quotas_option(p)
    common.add_basic_format_options(p)
    p.add_argument('--source-pdisk', '-s', type=str, required=True,
                   help='Source PDisk in the format nodeId:pdiskId (e.g. "1:1000")')
    p.add_argument('--destination-pdisk', '-d', type=str, required=True,
                   help='Destination PDisk in the format nodeId:pdiskId (e.g. "2:1000")')


def create_request(args, source_pdisk, target_pdisk):
    request = common.create_bsc_request(args)
    cmd = request.Command.add().MovePDisk

    cmd.SourcePDisk.TargetPDiskId.NodeId = int(source_pdisk.split(';')[0])
    cmd.SourcePDisk.TargetPDiskId.PDiskId = int(source_pdisk.split(';')[1])

    cmd.DestinationPDisk.TargetPDiskId.NodeId = int(target_pdisk.split(';')[0])
    cmd.DestinationPDisk.TargetPDiskId.PDiskId = int(target_pdisk.split(';')[1])

    return request


def perform_request(request):
    return common.invoke_bsc_request(request)


def is_successful_response(response):
    return common.is_successful_bsc_response(response)


def do(args):
    assert not args.dry_run, '--dry-run is not supported for this command'

    src = args.source_pdisk
    if src is None:
        common.print_status(args, success=False, error_reason='Source PDisk is not specified')
        sys.exit(1)

    dst = args.destination_pdisk
    if dst is None:
        common.print_status(args, success=False, error_reason='Destination PDisk is not specified')
        sys.exit(1)

    success = True
    error_reason = ''

    request = create_request(args, src, dst)
    response = perform_request(request)
    if not is_successful_response(response):
        success = False
        error_reason += 'Request has failed: \n{0}\n{1}\n'.format(request, response)

    common.print_status(args, success, error_reason)
    if not success:
        sys.exit(1)
