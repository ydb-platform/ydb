import ydb.apps.dstool.lib.common as common
import ydb.public.api.protos.draft.ydb_nbs_pb2 as nbs


description = 'Send IO to NBS 2.0 partition'


def add_options(p):
    p.add_argument('--id', type=str, required=True, help='Partition tablet id')
    p.add_argument('--start_index', type=int, default=0, help='Offset')
    p.add_argument('--blocks_count', type=int, default=1, help='Count of blocks (fore read operations)')
    p.add_argument('--data', type=str, default="test_data", help='Data (for write operations)')
    p.add_argument('--type', type=str, default="write", help='Operation type (write/read)')


def is_successful_response(response):
    return response.Success


def do(args):
    if args.type == "write":
        do_write(args)
    elif args.type == "read":
        do_read(args)
    else:
        raise ValueError("Unknown operation type")


def do_write(args):
    blocks = nbs.IOVector(Buffers=[args.data.encode()])
    request = nbs.WriteBlocksRequest(DiskId=args.id, StartIndex=args.start_index, Blocks=blocks)
    response = common.invoke_nbs_request('WriteBlocks', request)

    common.print_nbs_request_result(args, request, response)


def do_read(args):
    request = nbs.ReadBlocksRequest(DiskId=args.id, StartIndex=args.start_index, BlocksCount=args.blocks_count)
    response = common.invoke_nbs_request('ReadBlocks', request)

    common.print_nbs_request_result(args, request, response)

    print(response)
