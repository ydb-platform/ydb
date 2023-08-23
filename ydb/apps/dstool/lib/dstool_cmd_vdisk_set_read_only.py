import ydb.apps.dstool.lib.common as common
import sys

description = 'Set vdisk read-only mode (experimental mode that might help restore data in certain situations)'


def add_options(p):
    p.add_argument('--vdisk-id', type=str, required=True, help='Vdisk id in format [GroupId:_:FailRealm:FailDomain:VDiskIdx]')
    p.add_argument('value', type=str, choices=('true', 'false'), help='Use one of the values to set or unset read-only mode')
    common.add_ignore_degraded_group_check_option(p)
    common.add_ignore_disintegrated_group_check_option(p)
    common.add_ignore_failure_model_group_check_option(p)
    common.add_basic_format_options(p)


def create_request(args, vslot):
    assert not args.dry_run, '--dry-run is not supported for this command'
    request = common.create_bsc_request(args)
    cmd = request.Command.add().SetVDiskReadOnly
    if args.value == "true":
        cmd.Value = True
    elif args.value == "false":
        cmd.Value = False
    else:
        raise Exception("invalid 'value' argument: {}".format(args.value))
    cmd.VSlotId.NodeId = vslot.VSlotId.NodeId
    cmd.VSlotId.PDiskId = vslot.VSlotId.PDiskId
    cmd.VSlotId.VSlotId = vslot.VSlotId.VSlotId
    cmd.VDiskId.GroupID = vslot.GroupId
    cmd.VDiskId.GroupGeneration = vslot.GroupGeneration
    cmd.VDiskId.Ring = vslot.FailRealmIdx
    cmd.VDiskId.Domain = vslot.FailDomainIdx
    cmd.VDiskId.VDisk = vslot.VDiskIdx
    return request


def perform_request(request):
    return common.invoke_bsc_request(request)


def is_successful_response(response):
    return common.is_successful_bsc_response(response)


def do(args):
    base_config = common.fetch_base_config()
    vslots = common.get_vslots_by_vdisk_ids(base_config, [args.vdisk_id])
    if len(vslots) != 1:
        common.print_status(args, success=False, error_reason='Unexpected number of found vslots: {}'.format(len(vslots)))
        sys.exit(1)

    success = True
    error_reason = ''
    vslot = vslots[0]

    request = create_request(args, vslot)
    response = perform_request(request)
    if not is_successful_response(response):
        success = False
        error_reason += 'Request has failed: \n{0}\n{1}\n'.format(request, response)

    common.print_status(args, success, error_reason)
    if not success:
        sys.exit(1)
