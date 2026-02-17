import ydb.apps.dstool.lib.common as common
import ydb.public.api.protos.draft.ydb_nbs_pb2 as nbs


description = 'Get LoadActorAdapter actor id for NBS 2.0 partition'


def add_options(p):
    p.add_argument('--id', type=str, required=True, help='Partition tablet id')


def is_successful_response(response):
    return response.Success


def do(args):
    request = nbs.GetLoadActorAdapterActorIdRequest(TabletId=args.id)
    response = common.invoke_nbs_request('GetLoadActorAdapterActorId', request)

    common.print_nbs_request_result(args, request, response)

    result = nbs.GetLoadActorAdapterActorIdResult()
    response.operation.result.Unpack(result)
    print(result.ActorId or '')
