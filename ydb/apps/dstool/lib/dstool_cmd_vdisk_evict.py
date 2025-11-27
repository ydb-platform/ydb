import ydb.apps.dstool.lib.common as common
import ydb.apps.dstool.lib.table as table
import ydb.core.protos.blobstorage_config_pb2 as kikimr_bsconfig
from typing import Iterable
from types import SimpleNamespace
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


def stats_to_dict(st):
    return {
        "PDiskId": f"{st.ExamplePDiskId.NodeId}:{st.ExamplePDiskId.PDiskId}",
        "AllSlotsAreOccupied": st.AllSlotsAreOccupied,
        "NotEnoughSpace": st.NotEnoughSpace,
        "NotAcceptingNewSlots": st.NotAcceptingNewSlots,
        "NotOperational": st.NotOperational,
        "Decommission": st.Decommission,
    }


def dump_group_mapper_error(err: kikimr_bsconfig.TGroupMapperError, args):
    table_args = SimpleNamespace(sort_by=None, columns=None, format=args.format, no_header=None)
    def table_generator(data: Iterable[kikimr_bsconfig.TGroupMapperError.TStats], print_pdisk_id: bool = True):
        all_columns = []
        if print_pdisk_id:
            all_columns += ['Example PDiskId from domain']
        all_columns += [
            'All slots are occupied',
            'Not enough space',
            'Not accepting new slots',
            'Not operational',
            'Decommission',
        ]
        table_output = table.TableOutput(all_columns)
        rows = []
        for st in data:
            row = {}
            if print_pdisk_id:
                row['Example PDiskId from domain'] = f"{st.ExamplePDiskId.NodeId}:{st.ExamplePDiskId.PDiskId}"
            row['All slots are occupied'] = str(st.AllSlotsAreOccupied)
            row['Not enough space'] = str(st.NotEnoughSpace)
            row['Not accepting new slots'] = str(st.NotAcceptingNewSlots)
            row['Not operational'] = str(st.NotOperational)
            row['Decommission'] = str(st.Decommission)
            rows.append(row)
        
        table_output.dump(rows, table_args)
    
    print("Total stats")
    table_generator([err.TotalStats], print_pdisk_id=False)
    if len(err.MatchingDomainsStats) > 0:
        print("Matching domains")
        table_generator(err.MatchingDomainsStats)
    else:
        print("No matching domains")


def do(args):
    request = create_request(args)
    response = perform_request(request)
    common.print_request_result(args, request, response)
    if not is_successful_response(response):
        verbose = getattr(args, 'verbose', False)
        if (len(response.Status) == 1) and verbose:
            for fail_param in response.Status[0].FailParam:
                if fail_param.HasField("GroupMapperError"):
                    group_mapper_error = fail_param.GroupMapperError
                    dump_group_mapper_error(group_mapper_error, args)
                    
        sys.exit(1)
