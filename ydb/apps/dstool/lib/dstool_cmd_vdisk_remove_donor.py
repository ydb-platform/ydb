import ydb.apps.dstool.lib.common as common
import sys

description = 'Remove vdisks that are in donor mode'


def add_options(p):
    common.add_vdisk_ids_option(p, required=True)
    common.add_basic_format_options(p)


def create_request(args):
    base_config = common.fetch_base_config()
    vslots = common.get_vslots_by_vdisk_ids(base_config, args.vdisk_ids)
    request = common.kikimr_bsconfig.TConfigRequest(Rollback=args.dry_run)
    for vslot in vslots:
        for donor in vslot.Donors:
            cmd = request.Command.add().DropDonorDisk
            cmd.VSlotId.CopyFrom(donor.VSlotId)
            cmd.VDiskId.CopyFrom(donor.VDiskId)

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
        sys.exit(1)
