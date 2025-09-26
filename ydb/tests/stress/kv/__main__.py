# -*- coding: utf-8 -*-
import argparse
import logging
from ydb.tests.stress.kv.workload import YdbKvWorkload

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Workload kv wrapper", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--endpoint', default='grpc://localhost:2135', help="YDB endpoint")
    parser.add_argument('--database', default=None, required=True, help='A database to connect')
    parser.add_argument('--duration', default=120, type=lambda x: int(x), help='A duration of workload in seconds')
    parser.add_argument('--store_type', default="row", choices=["row", "column"], help='STORE mode for CREATE TABLE')
    parser.add_argument('--kv_prefix', default='topic', help='Topic name')
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

    workload = YdbKvWorkload(args.endpoint, args.database, duration=args.duration, store_type=args.store_type, tables_prefix=args.kv_prefix)
    workload.start()
    workload.join()
