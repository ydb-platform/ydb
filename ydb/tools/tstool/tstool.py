from argparse import ArgumentParser, FileType
from ydb.core.protos.grpc_pb2_grpc import TGRpcServerStub
from ydb.core.protos.msgbus_pb2 import THiveCreateTablet, TTestShardControlRequest
from ydb.core.protos.tablet_pb2 import TTabletTypes
from ydb.core.protos.base_pb2 import EReplyStatus
from google.protobuf import text_format
import grpc
import socket
import sys
import time
import multiprocessing
import random

grpc_host = None
grpc_port = None
domain_uid = None
default_tsserver_port = 35000
hive_domain_key = None
hive_id = None


def invoke_grpc(func, *params):
    with grpc.insecure_channel('%s:%d' % (grpc_host, grpc_port), []) as channel:
        stub = TGRpcServerStub(channel)
        return getattr(stub, func)(*params)


def create_tablet(owner_idx, channels, count):
    request = THiveCreateTablet(DomainUid=domain_uid)
    if hive_id is not None:
        request.HiveId = hive_id
    for i in range(count):
        cmd = request.CmdCreateTablet.add(OwnerId=0, OwnerIdx=owner_idx + i, TabletType=TTabletTypes.TestShard, ChannelsProfile=0)
        for channel in channels:
            cmd.BindedChannels.add(StoragePoolName=channel, PhysicalGroupsOnly=False)
        if hive_domain_key is not None:
            cmd.AllowedDomains.add(SchemeShard=hive_domain_key[0], PathId=hive_domain_key[1])

    for _ in range(10):
        res = invoke_grpc('HiveCreateTablet', request)
        if res.Status == 1:
            assert all(r.Status in [EReplyStatus.OK, EReplyStatus.ALREADY] for r in res.CreateTabletResult)
            return [r.TabletId for r in res.CreateTabletResult]
        else:
            print(res, file=sys.stderr)
            time.sleep(3)

    assert False


def init_tablet(args):
    tablet_id, cmd = args
    request = TTestShardControlRequest(TabletId=tablet_id, Initialize=cmd)

    for _ in range(100):
        res = invoke_grpc('TestShardControl', request)
        if res.Status == 1:
            return None
        else:
            time.sleep(random.uniform(1.0, 2.0))

    return str(tablet_id) + ': ' + str(res)


def main():
    parser = ArgumentParser(description='YDB TestShard control tool')
    parser.add_argument('--grpc-host', type=str, required=True, help='gRPC endpoint hostname')
    parser.add_argument('--grpc-port', type=int, default=2135, help='gRPC endpoint port')
    parser.add_argument('--domain-uid', type=int, default=1, help='domain UID')
    parser.add_argument('--owner-idx', type=int, required=True, help='unique instance id for tablet creation')
    parser.add_argument('--channels', type=str, nargs='+', required=True, help='channel storage pool names')
    parser.add_argument('--count', type=int, default=1, help='create desired amount of tablets at once')

    subparsers = parser.add_subparsers(help='Action', dest='action', required=True)

    p = subparsers.add_parser('initialize', help='initialize test shard state')
    p.add_argument('--proto-file', type=FileType(), required=True, help='path to protobuf containing TCmdInitialize')
    p.add_argument('--tsserver', type=str, help='FQDN:port pair for tsserver')
    p.add_argument('--subdomain', type=str, help='subdomain to create tablets in')
    p.add_argument('--hive-id', type=int, help='hive tablet id')

    args = parser.parse_args()

    global grpc_host, grpc_port, domain_uid, hive_domain_key, hive_id
    grpc_host = args.grpc_host
    grpc_port = args.grpc_port
    domain_uid = args.domain_uid
    hive_domain_key = tuple(map(int, args.subdomain.split(':'))) if args.subdomain is not None else None
    hive_id = args.hive_id

    tablet_ids = create_tablet(args.owner_idx, args.channels, args.count)
    print('TabletIds# %s' % ', '.join(map(str, tablet_ids)))

    try:
        if args.action == 'initialize':
            cmd = text_format.Parse(args.proto_file.read(), TTestShardControlRequest.TCmdInitialize())
            if args.tsserver is not None:
                host, sep, port = args.tsserver.partition(':')
                port = int(port) if sep else default_tsserver_port
                sockaddr = None
                for _, _, _, _, sockaddr in socket.getaddrinfo(host, port, socket.AF_INET6):
                    break
                if sockaddr is None:
                    print('Failed to resolve hostname %s' % host, file=sys.stderr)
                    sys.exit(1)
                cmd.StorageServerHost = sockaddr[0]
            with multiprocessing.Pool(None) as p:
                status = 0
                for r in p.imap_unordered(init_tablet, ((tablet_id, cmd) for tablet_id in tablet_ids), 1):
                    if r is not None:
                        sys.stderr.write(r)
                        status = 1
                sys.exit(status)
    finally:
        if args.proto_file:
            args.proto_file.close()
