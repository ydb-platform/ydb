# -*- coding: utf-8 -*-
import argparse
import logging
from ydb.tests.stress.tpcc.workload import YdbTpccWorkload

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Workload tpcc wrapper", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--endpoint', default='grpc://localhost:2135', help="YDB endpoint")
    parser.add_argument('--database', default=None, required=True, help='A database to connect')
    parser.add_argument('--duration', default=120, type=int, help='A duration of workload in seconds')
    parser.add_argument('--warehouses', default=100, type=int, help='Number of warehouses')
    parser.add_argument('--path', default='tpcc_workload', help='Table path prefix')
    parser.add_argument('--log_file', default=None, help='Append log into specified file')
    parser.add_argument('--phase', choices=['prepare', 'run', 'clean'], default=None,
                        help='Phase to run: prepare (init+import), run, clean. If omitted, all phases run in sequence.')

    args = parser.parse_args()

    if args.log_file:
        logging.basicConfig(
            filename=args.log_file,
            filemode='a',
            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
            datefmt='%H:%M:%S',
            level=logging.INFO
        )

    workload = YdbTpccWorkload(args.endpoint, args.database, duration=args.duration, warehouses=args.warehouses, tables_prefix=args.path)
    if args.phase == 'prepare':
        workload.import_data()
    elif args.phase == 'run':
        workload.start()
        workload.join()
    elif args.phase == 'clean':
        workload.clean_data()
    else:
        workload.import_data()
        workload.start()
        workload.join()
        workload.clean_data()
