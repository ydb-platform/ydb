import ydb.apps.dstool.lib.common as common
import ydb.core.protos.blob_depot_config_pb2 as blob_depot_config
import sys

description = 'Decommit physical group'


def add_options(p):
    common.add_group_ids_option(p, required=True)
    p.add_argument('--hive-id', type=int, required=True, help='tablet id of containing hive')
    p.add_argument('--log-channel-sp', type=str, metavar='POOL_NAME', help='channel 0 specifier')
    p.add_argument('--snapshot-channel-sp', type=str, metavar='POOL_NAME', help='channel 1 specifier (defaults to channel 0)')
    p.add_argument('--data-channel-sp', type=str, metavar='POOL_NAME[*COUNT]', nargs='*', help='data channel specifier')


def do(args):
    request = common.create_bsc_request(args)
    cmd = request.Command.add().DecommitGroups
    cmd.GroupIds.extend(args.group_ids)
    cmd.HiveId = args.hive_id

    if args.log_channel_sp or args.snapshot_channel_sp or args.data_channel_sp:
        if args.log_channel_sp is None:
            print('--log-channel-sp must be specified', file=sys.stderr)
            sys.exit(1)
        elif args.data_channel_sp is None:
            print('--data-channel-sp must be specified', file=sys.stderr)
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
