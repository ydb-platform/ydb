import ydb.apps.dstool.lib.common as common

description = 'Describe blobstorage components'


def add_options(p):
    common.add_vdisk_ids_option(p)


def create_request(args):
    base_config = common.fetch_base_config()
    vslots = common.get_vslots_by_vdisk_ids(base_config, args.vdisk_ids)

    vslot_ids = {common.get_vslot_id(vslot.VSlotId) for vslot in vslots if common.get_pdisk_id(vslot.VSlotId)}

    request = common.create_bsc_describe_request()
    for vslot in base_config.VSlot:
        if common.get_vslot_id(vslot.VSlotId) not in vslot_ids:
            continue
        vdisk_target = request.Targets.add().VDiskId
        vdisk_target.GroupID = vslot.GroupId
        vdisk_target.GroupGeneration = vslot.GroupGeneration
        vdisk_target.Ring = vslot.FailRealmIdx
        vdisk_target.Domain = vslot.FailDomainIdx
        vdisk_target.VDisk = vslot.VDiskIdx

    return request


def perform_request(request):
    return common.invoke_bsc_describe_request(request)


def is_successful_response(response):
    return common.is_successful_bsc_response(response)


def do(args):
    request = create_request(args)
    response = perform_request(request)

    for result in response.Results:
        match result.WhichOneof('Response'):
            case 'VDiskResponse':
                print(result.VDiskResponse.Result)
            case _:
                return "Error"
