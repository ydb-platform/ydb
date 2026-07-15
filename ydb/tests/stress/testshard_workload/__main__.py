# -*- coding: utf-8 -*-
import argparse
import logging
from ydb.tests.stress.testshard_workload.workload import YdbTestShardWorkload

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="TestShard workload wrapper", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--endpoint', default='grpc://localhost:2135', help="YDB endpoint")
    parser.add_argument('--database', default=None, required=True, help='A database, where TestShard will be created')
    parser.add_argument('--duration', default=120, type=lambda x: int(x), help='A duration of workload in seconds')
    parser.add_argument('--owner-idx', default=1, type=lambda x: int(x), help='Owner index for tablet creation')
    parser.add_argument('--count', default=1, type=lambda x: int(x), help='Number of tablets to create')
    parser.add_argument('--config', default=None, help='Path to custom testshard config file')
    parser.add_argument('--channels', default=None, help='Comma-separated list of storage channels (e.g., /Root/db1:ssd,/Root/db1:ssd,/Root/db1:ssd). \
                                                          Required, if database is /Root')
    parser.add_argument('--tsserver-port', default=35000, type=lambda x: int(x), help='Port for TestShard validation server')
    parser.add_argument('--tsserver-host', default='localhost', help='Host of a TestShard validation server')
    parser.add_argument('--stats-interval', default=10, type=lambda x: int(x), help='Interval in seconds between stats output')
    parser.add_argument('--monitoring-port', default=8765, type=lambda x: int(x), help='YDB monitoring HTTP port for fetching tablet stats')
    parser.add_argument('--log_file', default=None, help='Append log into specified file')

    args = parser.parse_args()

    channels = None
    if args.channels:
        channels = [ch.strip() for ch in args.channels.split(',')]

    log_format = '%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s'
    log_datefmt = '%H:%M:%S'

    if args.log_file:
        logging.basicConfig(
            filename=args.log_file,
            filemode='a',
            format=log_format,
            datefmt=log_datefmt,
            level=logging.INFO
        )
    else:
        logging.basicConfig(
            format=log_format,
            datefmt=log_datefmt,
            level=logging.INFO
        )

    workload = YdbTestShardWorkload(
        args.endpoint,
        args.database,
        duration=args.duration,
        owner_idx=args.owner_idx,
        count=args.count,
        config_path=args.config,
        channels=channels,
        tsserver_port=args.tsserver_port,
        tsserver_host=args.tsserver_host,
        stats_interval=args.stats_interval,
        monitoring_port=args.monitoring_port,
    )
    workload.start()
    workload.join()
