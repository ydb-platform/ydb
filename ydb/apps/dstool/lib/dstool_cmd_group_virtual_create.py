import ydb.apps.dstool.lib.common as common
import ydb.core.protos.blob_depot_config_pb2 as blob_depot_config
import sys
import time

description = 'Create virtual group backed by BlobDepot'


def add_options(p):
    p.add_argument('--name', type=str, required=True, nargs='+', help='cluster-unique name(s) of newly created virtual groups')
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument('--hive-id', type=int, help='tablet id of containing hive')
    g.add_argument('--database', type=str, help='database path of containing hive')
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument('--storage-pool-name', type=str, metavar='POOL_NAME', help='name of the containing storage pool')
    g.add_argument('--storage-pool-id', type=str, metavar='BOX:POOL', help='id of the cotaining storage pool')
    p.add_argument('--log-channel-sp', type=str, metavar='POOL_NAME', required=True, help='channel 0 specifier')
    p.add_argument('--snapshot-channel-sp', type=str, metavar='POOL_NAME', help='channel 1 specifier (defaults to channel 0)')
    p.add_argument('--data-channel-sp', type=str, metavar='POOL_NAME[*COUNT]', nargs='+', required=True, help='data channel specifier')
    p.add_argument('--wait', action='store_true', help='wait for operation to complete by polling')


def do(args):
    request = common.create_bsc_request(args)
    for name in args.name:
        cmd = request.Command.add().AllocateVirtualGroup

        cmd.Name = name
        if args.hive_id is not None:
            cmd.HiveId = args.hive_id
        if args.database is not None:
            cmd.Database = args.database

        if args.storage_pool_name is not None:
            cmd.StoragePoolName = args.storage_pool_name
        else:
            id_ = cmd.StoragePoolId
            try:
                id_.BoxId, id_.StoragePoolId = map(int, args.storage_pool_id.split(':'))
            except Exception:
                print(f'Invalid --storage-pool-id={args.storage_pool_id} format, <number>:<number> expected', file=sys.stderr)
                sys.exit(1)

        cmd.ChannelProfiles.add(StoragePoolName=args.log_channel_sp, ChannelKind=blob_depot_config.TChannelKind.System)
        chan1 = args.snapshot_channel_sp if args.snapshot_channel_sp is not None else args.log_channel_sp
        cmd.ChannelProfiles.add(StoragePoolName=chan1, ChannelKind=blob_depot_config.TChannelKind.System)
        for data_sp in args.data_channel_sp:
            pool_name, sep, count = data_sp.rpartition('*')
            if sep == '*':
                count = int(count)
            else:
                pool_name, count = count, 1
            cmd.ChannelProfiles.add(StoragePoolName=pool_name, ChannelKind=blob_depot_config.TChannelKind.Data, Count=count)

    response = common.invoke_bsc_request(request)
    common.print_request_result(args, request, response)

    if args.wait and not args.dry_run:
        while True:
            base_config = common.fetch_base_config(virtualGroupsOnly=True, cache=False)
            names_remaining = set(args.name)
            errors = []

            for group in base_config.Group:
                if group.VirtualGroupInfo.Name in names_remaining:
                    if group.VirtualGroupInfo.State == common.EVirtualGroupState.WORKING:
                        names_remaining.remove(group.VirtualGroupInfo.Name)
                    elif group.VirtualGroupInfo.State == common.EVirtualGroupState.CREATE_FAILED:
                        names_remaining.remove(group.VirtualGroupInfo.Name)
                        errors.append(f'{group.VirtualGroupInfo.Name}: {group.VirtualGroupInfo.ErrorReason}')

            if names_remaining:
                time.sleep(1)
                continue

            if errors:
                print('Some of groups were not created:', file=sys.stderr)
                for line in errors:
                    print(line, file=sys.stderr)
                sys.exit(1)
            else:
                break
