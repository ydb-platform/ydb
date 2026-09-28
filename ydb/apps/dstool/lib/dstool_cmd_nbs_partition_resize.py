import json
import ydb.apps.dstool.lib.common as common
import ydb.public.api.protos.draft.ydb_nbs_pb2 as nbs
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds


description = 'Grow an NBS 2.0 partition'


def add_options(p):
    p.add_argument('--disk-id', type=str, required=True, help='disk id')
    p.add_argument(
        '--blocks-count',
        type=int,
        required=True,
        help='New count of blocks in the partition (must not shrink)',
    )


def is_successful_response(response):
    return response.Success


def do(args):
    request = nbs.ResizePartitionRequest(
        DiskId=args.disk_id,
        BlocksCount=args.blocks_count,
    )
    response = common.invoke_nbs_request('ResizePartition', request)

    common.print_nbs_request_result(args, request, response)

    output = {
        'status': StatusIds.StatusCode.Name(response.operation.status),
    }
    if common.get_status(response):
        result = nbs.ResizePartitionResult()
        response.operation.result.Unpack(result)
        output['blocksCount'] = result.BlocksCount

    print(json.dumps(output))
