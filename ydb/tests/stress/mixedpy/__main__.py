# -*- coding: utf-8 -*-
import argparse
import logging

from ydb.tests.stress.mixedpy.workload import YdbMixedWorkload

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Workload mixed wrapper", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--endpoint', default='grpc://localhost:2135', help="YDB endpoint")
    parser.add_argument('--database', default=None, required=True, help='A database to connect')
    parser.add_argument('--duration', default=120, type=lambda x: int(x), help='A duration of workload in seconds')
    parser.add_argument('--mixed_prefix', default='mixed', help='Mixed path')
    parser.add_argument('--store_type', default='row', choices=['row', 'column'], help='Table type either row or column')
    parser.add_argument('--log_file', default=None, help='Append log into specified file')

    args = parser.parse_args()

    if args.log_file:
        logging.basicConfig(
            filename=args.log_file,
            filemode='a',
            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
            datefmt='%H:%M:%S',
            level=logging.INFO
        )

    workload = YdbMixedWorkload(args.endpoint, args.database, args.duration, args.store_type, args.mixed_prefix)
    workload.start()
    workload.join()
