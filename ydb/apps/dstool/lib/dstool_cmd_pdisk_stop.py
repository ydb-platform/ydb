import ydb.apps.dstool.lib.common as common
import sys

description = 'Stop PDisk'


def add_options(p):
    common.add_pdisk_select_options(p)
    common.add_ignore_degraded_group_check_option(p)
    common.add_ignore_failure_model_group_check_option(p)
    common.add_ignore_vslot_quotas_option(p)
    common.add_basic_format_options(p)


def create_request(args, pdisk):
    request = common.create_bsc_request(args)
    cmd = request.Command.add().StopPDisk

    cmd.TargetPDiskId.NodeId = pdisk[0]
    cmd.TargetPDiskId.PDiskId = pdisk[1]

    return request


def perform_request(request):
    return common.invoke_bsc_request(request)


def is_successful_response(response):
    return common.is_successful_bsc_response(response)


def do(args):
    base_config = common.fetch_base_config()

    assert not args.dry_run, '--dry-run is not supported for this command'

    pdisks = common.get_selected_pdisks(args, base_config)

    if len(pdisks) != 1:
        common.print_status(args, success=False, error_reason='Only stop one PDisk at a time')
        sys.exit(1)

    success = True
    error_reason = ''

    request = create_request(args, list(pdisks)[0])
    response = perform_request(request)
    if not is_successful_response(response):
        success = False
        error_reason += 'Request has failed: \n{0}\n{1}\n'.format(request, response)

    common.print_status(args, success, error_reason)
    if not success:
        sys.exit(1)
