import ydb.apps.dstool.lib.common as common

description = 'Cancel virtual group creation/decommission'


def add_options(p):
    p.add_argument('--group-id', type=int, required=True, help='group id to cancel')


def do(args):
    request = common.create_bsc_request(args)
    cmd = request.Command.add().CancelVirtualGroup
    cmd.GroupId = args.group_id
    response = common.invoke_bsc_request(request)
    common.print_request_result(args, request, response)
