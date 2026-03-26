import ydb.apps.dstool.lib.common as common
import ydb.public.api.protos.draft.ydb_nbs_pb2 as nbs

from ydb.public.api.grpc.draft import ydb_nbs_v1_pb2_grpc as nbs_grpc_server


description = 'Delete NBS 2.0 partition'


def add_options(p):
    p.add_argument('--id', type=str, required=True, help='Partition tablet id')


def is_successful_response(response):
    return response.Success


def do(args):
    request = nbs.DeletePartitionRequest(TabletId=args.id)
    response = invoke_nbs_request('DeletePartition', request)

    common.print_nbs_request_result(args, request, response)


def invoke_nbs_request(request_type, request):
    response = common.invoke_grpc(request_type, request, stub_factory=nbs_grpc_server.NbsServiceStub)

    return response
