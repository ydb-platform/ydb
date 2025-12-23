# -*- coding: utf-8 -*-
import argparse
import logging
from ydb.tests.stress.kv_volume.workload import YdbKeyValueVolumeWorkload

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Workload kv wrapper", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--endpoint', default='grpc://localhost:2135', help="YDB endpoint")
    parser.add_argument('--database', default=None, required=True, help='A database to connect')
    parser.add_argument('--duration', default=120, type=lambda x: int(x), help='A duration of workload in seconds')
    parser.add_argument('--load-type', default="read", choices=["read", "read-inline"], help='Load type for kv volumes')
    parser.add_argument('--in-flight', default=1, type=int, help='In flight')
    parser.add_argument('--volume-path', default='test_kv', help='Volume path')
    parser.add_argument('--log-file', default=None, help='Append log into specified file')
    parser.add_argument('--partitions', default=1, type=int, help='Append log into specified file')
    parser.add_argument('--storage-channels', default=['ssd']*3, nargs='*', help='Storage channels')
    parser.add_argument('--version', default='v1', choices=['v1', 'v2'], help='Keyvalue grpc api version')

    args = parser.parse_args()

    if args.log_file:
        logging.basicConfig(
            filename=args.log_file,
            filemode='a',
            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
            datefmt='%H:%M:%S',
            level=logging.INFO
        )

    workload = YdbKeyValueVolumeWorkload(
        args.endpoint,
        args.database,
        duration=args.duration,
        path='test_kv',
        partitions=args.partitions,
        storage_channels=args.storage_channels,
        kv_load_type=args.load_type,
        inflight=args.in_flight,
        version=args.version
    )
    workload.start(use_multiprocessing=True)
    workload.wait_stop()
