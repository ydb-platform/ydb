# -*- coding: utf-8 -*-
import argparse
import ydb
import logging

from ydb.tests.stress.statistics_workload.workload import Workload

ydb.interceptor.monkey_patch_event_handler()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="statistics stability workload", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--host', default='localhost', help="An host to be used")
    parser.add_argument('--port', default='2135', help="A port to be used")
    parser.add_argument('--database', default=None, required=True, help='A database to connect')
    parser.add_argument('--duration', default=120, type=lambda x: int(x), help='A duration of workload in seconds')
    parser.add_argument('--batch_size', default=1000, help='Batch size for bulk insert')
    parser.add_argument('--batch_count', default=3, help='The number of butches to be inserted')
    parser.add_argument('--prefix', default='test_table', help='Table prefix')
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

    with Workload(args.host, args.port, args.database, args.duration, args.batch_size, args.batch_count, args.prefix) as workload:
        workload.run()
