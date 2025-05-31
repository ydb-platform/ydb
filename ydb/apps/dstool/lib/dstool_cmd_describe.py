import ydb.apps.dstool.lib.common as common
import ydb.public.api.protos.draft.ydb_bscontroller_pb2 as bsc_protos

description = 'Describe blobstorage components'


def add_options(p):
    common.add_vdisk_ids_option(p)


def create_request(args):
    base_config = common.fetch_base_config()
    vslots = common.get_vslots_by_vdisk_ids(base_config, args.vdisk_ids)

    vslot_ids = {common.get_vslot_id(vslot.VSlotId) for vslot in vslots if common.get_pdisk_id(vslot.VSlotId)}
    request = bsc_protos.DescribeRequest()
    for vslot in base_config.VSlot:
        if common.get_vslot_id(vslot.VSlotId) not in vslot_ids:
            continue
        vdisk_target = request.targets.add().vdisk_id
        vdisk_target.group_id = vslot.GroupId
        vdisk_target.group_generation = vslot.GroupGeneration
        vdisk_target.ring = vslot.FailRealmIdx
        vdisk_target.domain = vslot.FailDomainIdx
        vdisk_target.vdisk = vslot.VDiskIdx

    return request


def perform_request(request):
    return common.invoke_bsc_describe_request(request)


def is_successful_response(response):
    return common.is_successful_bsc_response(response)


def do(args):
    request = create_request(args)
    response = perform_request(request)

    for result in response.results:
        match result.WhichOneof('Response'):
            case 'vdisk_response':
                print(result.vdisk_response.result)
            case _:
                return "Error"
