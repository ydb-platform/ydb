# -*- coding: utf-8 -*-
import argparse
import logging
import dataclasses
from ydb.tests.stress.topic_kafka.workload import YdbTopicWorkload, WriteProfile, parse_write_profile

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Workload topic wrapper", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--endpoint', default='grpc://localhost:2135', help="YDB endpoint")
    parser.add_argument('--database', default=None, required=True, help='A database to connect')
    parser.add_argument('--duration', default=120, type=lambda x: int(x), help='A duration of workload in seconds')

    parser.add_argument('--consumers', default=50, type=lambda x: int(x), help='Consumers of the topic')
    parser.add_argument('--consumer-threads', default=1, type=int, help='Number of consumer threads')
    parser.add_argument(
        '--restart-interval', default=None, type=str, help='Reader restart interval in seconds (ex. "60s", "5m", "1h")'
    )

    parser.add_argument('--topic_prefix', default='topic', help='Topic name')
    parser.add_argument('--partitions', default=100, type=int, help='Number of partitions')

    parser.add_argument(
        '--write-workload',
        action='append',
        default=[],
        nargs=len(dataclasses.fields(WriteProfile)),
        help='Set of write workload parameters\n',
        metavar=tuple(f'{field.name.upper()}:{field.type.__name__}' for field in dataclasses.fields(WriteProfile)),
    )

    parser.add_argument('--no-cleanup-policy-compact', action='store_false', dest='cleanup_policy_compact', help='disable "compact" cleanup policy')

    parser.add_argument('--log_file', default=None, help='Append log into specified file')

    args = parser.parse_args()

    if args.log_file:
        logging.basicConfig(
            filename=args.log_file,
            filemode='a',
            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
            datefmt='%H:%M:%S',
            level=logging.INFO,
        )

    workload = YdbTopicWorkload(
        args.endpoint,
        args.database,
        duration=args.duration,
        consumers=args.consumers,
        consumer_threads=args.consumer_threads,
        restart_interval=args.restart_interval,
        tables_prefix=args.topic_prefix,
        partitions=args.partitions,
        write_profiles=[parse_write_profile(profile) for profile in args.write_workload],
        cleanup_policy_compact=args.cleanup_policy_compact,
    )
    workload.tear_up()
    try:
        workload.start()
        workload.join()
    finally:
        workload.tear_down()
