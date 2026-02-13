import ydb.apps.dstool.lib.common as common
import ydb.public.api.protos.draft.ydb_nbs_pb2 as nbs


description = 'Create NBS 2.0 partition'


def add_options(p):
    p.add_argument('--pool', type=str, required=True, help='DDisk pool name')
    p.add_argument('--block-size', type=int, default=4096, help='Block size in bytes')
    p.add_argument('--blocks-count', type=int, default=262144, help='Count of blocks in partition')
    p.add_argument('--type', type=str, default="ssd", help='Type of partition: ssd/mem')


def is_successful_response(response):
    return response.Success


def do(args):
    if args.type == "mem":
        args.type = nbs.StorageMediaKind.STORAGE_MEDIA_MEMORY
    else:
        args.type = nbs.StorageMediaKind.STORAGE_MEDIA_DEFAULT
    request = nbs.CreatePartitionRequest(BlockSize=args.block_size,
                                         BlocksCount=args.blocks_count,
                                         StoragePoolName=args.pool,
                                         StorageMedia=args.type)
    response = common.invoke_nbs_request('CreatePartition', request)

    common.print_nbs_request_result(args, request, response)

    result = nbs.CreatePartitionResult()
    response.operation.result.Unpack(result)
    print(result)
