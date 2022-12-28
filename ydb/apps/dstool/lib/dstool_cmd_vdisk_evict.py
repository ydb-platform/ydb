import ydb.apps.dstool.lib.common as common
import sys

description = 'Relocate vdisks to other pdisks'


def add_options(p):
    common.add_vdisk_ids_option(p, required=True)
    common.add_allow_unusable_pdisks_option(p)
    common.add_ignore_degraded_group_check_option(p)
    common.add_ignore_failure_model_group_check_option(p)
    common.add_ignore_vslot_quotas_option(p)
    p.add_argument('--move-only-to-operational-pdisks', action='store_true', help='Move VDisks only to operational PDisks')
    p.add_argument('--suppress-donor-mode', action='store_true', help='Do not leave the previous VDisk in donor mode after the moving and drop it')
    common.add_basic_format_options(p)


def create_request(args):
    base_config = common.fetch_base_config()
    vslots = common.get_vslots_by_vdisk_ids(base_config, args.vdisk_ids)

    vslot_ids = {common.get_vslot_id(vslot.VSlotId) for vslot in vslots if common.get_pdisk_id(vslot.VSlotId)}

    request = common.create_bsc_request(args)
    for vslot in base_config.VSlot:
        if common.get_vslot_id(vslot.VSlotId) not in vslot_ids:
            continue
        cmd = request.Command.add().ReassignGroupDisk
        cmd.GroupId = vslot.GroupId
        cmd.GroupGeneration = vslot.GroupGeneration
        cmd.FailRealmIdx = vslot.FailRealmIdx
        cmd.FailDomainIdx = vslot.FailDomainIdx
        cmd.VDiskIdx = vslot.VDiskIdx
        if args.suppress_donor_mode:
            cmd.SuppressDonorMode = True

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
