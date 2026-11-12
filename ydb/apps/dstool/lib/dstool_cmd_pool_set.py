import ydb.apps.dstool.lib.common as common
import sys

description = 'Set storage pool settings that are not part of the pool definition'


def add_options(p):
    p.add_argument('--pool-name', type=str, required=True, action='append', help='Storage pool to change (may be repeated)')
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument('--vdisk-heap-allocator-num-leading-disks', type=int, metavar='N',
                   help='Let VDisks with order number 0 .. N-1 in every group of the pool use the stripe heap; '
                   'applied when a VDisk starts')
    g.add_argument('--reset-vdisk-heap-allocator-num-leading-disks', action='store_true',
                   help='Make the pool inherit blob_storage_config.vdisk_heap_allocator_num_leading_disks again')
    common.add_basic_format_options(p)


def create_request(args, storage_pools):
    request = common.create_bsc_request(args)
    for sp in storage_pools:
        cmd = request.Command.add().UpdateStoragePoolSettings
        cmd.BoxId = sp.BoxId
        cmd.StoragePoolId = sp.StoragePoolId
        if args.vdisk_heap_allocator_num_leading_disks is not None:
            if args.vdisk_heap_allocator_num_leading_disks < 0:
                raise Exception('VDisk heap allocator leading disk count must not be negative')
            cmd.Settings.VDiskHeapAllocatorNumLeadingDisks = args.vdisk_heap_allocator_num_leading_disks
        if args.reset_vdisk_heap_allocator_num_leading_disks:
            cmd.Reset.append('VDiskHeapAllocatorNumLeadingDisks')
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
