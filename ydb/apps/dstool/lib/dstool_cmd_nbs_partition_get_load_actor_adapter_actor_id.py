import json
import ydb.apps.dstool.lib.common as common
import ydb.public.api.protos.draft.ydb_nbs_pb2 as nbs


description = 'Get LoadActorAdapter actor id for NBS 2.0 partition'


def add_options(p):
    p.add_argument('--disk-id', type=str, required=True, help='Disk id')


def is_successful_response(response):
    return response.Success


def do(args):
    request = nbs.GetLoadActorAdapterActorIdRequest(DiskId=args.disk_id)
    response = common.invoke_nbs_request('GetLoadActorAdapterActorId', request)

    common.print_nbs_request_result(args, request, response)

    result = nbs.GetLoadActorAdapterActorIdResult()
    response.operation.result.Unpack(result)

    output = {'status': common.get_status(response), 'actorId': result.ActorId or ''}
    print(json.dumps(output))
