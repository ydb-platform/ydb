# -*- coding: utf-8 -*-
import argparse
import logging
from ydb.tests.stress.fulltext_workload.workload import YdbFulltextWorkload

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Workload fulltext wrapper", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--endpoint', default='grpc://localhost:2135', help="YDB endpoint")
    parser.add_argument('--database', default=None, required=True, help='A database to connect')
    parser.add_argument('--duration', default=120, type=lambda x: int(x), help='A duration of workload in seconds')
    parser.add_argument('--rows', default=100000, type=int, help='Number of rows in generated database (default: 100000)')
    parser.add_argument('--targets', default=1000, type=int, help='Number of rows to sample for query word set (default: 1000)')
    parser.add_argument('--threads', default=10, type=int, help='Number of threads for load testing (default: 10)')
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

    workload = YdbFulltextWorkload(args.endpoint, args.database, duration=args.duration,
                                    rows=args.rows, targets=args.targets, threads=args.threads)
    workload.start()
    workload.join()
