import ydb.apps.dstool.lib.common as common
import sys

description = 'Make storage pool of bridged cluster bridged or ordinary one; its groups keep their ids and data'


def add_options(p):
    p.add_argument('--pool-name', type=str, required=True, action='append', help='Storage pool to change (may be repeated)')
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument('--bridged', action='store_true',
                   help='Make groups of the pool bridged ones: they get their parts in every pile, synced in the background')
    g.add_argument('--ordinary', action='store_true',
                   help='Make the pool ordinary: its bridged groups keep their parts in a single pile, other parts are deleted')
    p.add_argument('--retain-pile', type=str, metavar='NAME',
                   help='With --ordinary, the pile which bridged groups keep their parts in (by default, the part each group '
                   'relies on, its inline pile, or its part in the primary pile)')
    common.add_basic_format_options(p)


def create_request(args, storage_pools):
    if args.retain_pile and not args.ordinary:
        raise Exception('--retain-pile requires --ordinary')
    request = common.create_bsc_request(args)
    for sp in storage_pools:
        cmd = request.Command.add().ChangeStoragePoolBridgeMode
        cmd.BoxId = sp.BoxId
        cmd.StoragePoolId = sp.StoragePoolId
        cmd.BridgeMode = args.bridged
        cmd.ItemConfigGeneration = sp.ItemConfigGeneration
        if args.retain_pile:
            cmd.RetainPileName = args.retain_pile
    return request


def perform_request(request):
    return common.invoke_bsc_request(request)


def is_successful_response(response):
    return common.is_successful_bsc_response(response)


def do(args):
    try:
        storage_pools = []
        all_pools = common.fetch_storage_pools()
        for name in args.pool_name:
            matching = [sp for sp in all_pools if sp.Name == name]
            if not matching:
                raise Exception("Couldn't find storage pool with name %s" % name)
            if len(matching) > 1:
                raise Exception('Storage pool name %s is not unique' % name)
            storage_pools.append(matching[0])

        request = create_request(args, storage_pools)
        response = perform_request(request)
        common.print_request_result(args, request, response)
        if not is_successful_response(response):
            sys.exit(1)
    except Exception as e:
        common.print_status(args, success=False, error_reason=e)
        sys.exit(1)
