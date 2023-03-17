import ydb.apps.dstool.lib.common as common

description = 'Create pool suitable for virtual groups'


def add_options(p):
    p.add_argument('--box-id', type=int, required=True, help='Containing box id')
    p.add_argument('--name', type=str, metavar='POOL_NAME', required=True, help='Virtual group pool name')
    p.add_argument('--kind', type=str, help='Optional pool kind')


def do(args):
    request = common.create_bsc_request(args)
    cmd = request.Command.add().DefineStoragePool
    cmd.BoxId = args.box_id
    cmd.ErasureSpecies = 'none'
    cmd.VDiskKind = 'Default'
    cmd.Name = args.name
    if args.kind is not None:
        cmd.Kind = args.kind

    response = common.invoke_bsc_request(request)
    common.print_request_result(args, request, response)
