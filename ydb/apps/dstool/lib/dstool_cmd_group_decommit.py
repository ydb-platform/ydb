import ydb.apps.dstool.lib.common as common
import ydb.core.protos.blob_depot_config_pb2 as blob_depot_config
import sys
import time

description = 'Decommit physical group'


def add_options(p):
    common.add_group_ids_option(p, required=True)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument('--database', type=str, required=True, help='database path for storage pools with groups being decommitted')
    p.add_argument('--log-channel-sp', type=str, metavar='POOL_NAME', help='channel 0 specifier')
    p.add_argument('--snapshot-channel-sp', type=str, metavar='POOL_NAME', help='channel 1 specifier (defaults to channel 0)')
    p.add_argument('--data-channel-sp', type=str, metavar='POOL_NAME[*COUNT]', nargs='*', help='data channel specifier')
    p.add_argument('--wait', action='store_true', help='wait until group decommission is started')


def do(args):
    request = common.create_bsc_request(args)
    cmd = request.Command.add().DecommitGroups
    cmd.GroupIds.extend(args.group_ids)
    cmd.Database = args.database

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

    if args.wait and response.Success:
        groups_of_interest = set(args.group_ids)

        while groups_of_interest:
            time.sleep(1)
            base_config_and_storage_pools = common.fetch_base_config_and_storage_pools(virtualGroupsOnly=True)
            base_config = base_config_and_storage_pools['BaseConfig']
            group_map = common.build_group_map(base_config)
            for group_id in list(groups_of_interest):
                if group_id not in group_map:
                    print(f'Group {group_id} is missing in group list', file=sys.stderr)
                    groups_of_interest.remove(group_id)
                    continue
                group = group_map[group_id]
                if group.VirtualGroupInfo is None:
                    print(f'Group {group_id} is not virtual group', file=sys.stderr)
                elif group.VirtualGroupInfo.State == common.EVirtualGroupState.WORKING:
                    pass
                elif group.VirtualGroupInfo.State == common.EVirtualGroupState.CREATE_FAILED:
                    print(f'Group {group_id} decommission failed to start: {group.VirtualGroupInfo.ErrorReason}, canceling', file=sys.stderr)
                    request = common.kikimr_bsconfig.TConfigRequest()
                    cmd = request.Command.add().CancelVirtualGroup
                    cmd.GroupId = group_id
                    response = common.invoke_bsc_request(request)
                    if not response.Success:
                        print(f'Failed to cancel decommission for group {group_id}', file=sys.stderr)
                elif group.VirtualGroupInfo.State == common.EVirtualGroupState.DELETING:
                    print(f'Group {group_id} is being deleted', file=sys.stderr)
                elif group.VirtualGroupInfo.State == common.EVirtualGroupState.NEW:
                    continue
                else:
                    print(f'Group {group_id} has unexpected virtual group state', file=sys.stderr)
                groups_of_interest.remove(group_id)
    else:
        common.print_request_result(args, request, response)
