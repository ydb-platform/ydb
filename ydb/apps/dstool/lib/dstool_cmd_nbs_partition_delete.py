import ydb.apps.dstool.lib.common as common
import ydb.public.api.protos.draft.ydb_nbs_pb2 as nbs


description = 'Delete NBS 2.0 partition'


def add_options(p):
    p.add_argument('--disk-id', type=str, required=True, help='Disk id')


def is_successful_response(response):
    return response.Success


def do(args):
    request = nbs.DeletePartitionRequest(DiskId=args.disk_id)
    response = common.invoke_nbs_request('DeletePartition', request)

    common.print_nbs_request_result(args, request, response)

    result = nbs.DeletePartitionResult()
    response.operation.result.Unpack(result)
    print(result)
