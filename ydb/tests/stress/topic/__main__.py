# -*- coding: utf-8 -*-
import argparse
import logging
from ydb.tests.stress.topic.workload.workload_topic import YdbTopicWorkload

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Workload topic wrapper", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--endpoint', default='grpc://localhost:2135', help="YDB endpoint")
    parser.add_argument('--database', default=None, required=True, help='A database to connect')
    parser.add_argument('--duration', default=120, type=lambda x: int(x), help='A duration of workload in seconds')
    parser.add_argument('--consumers', default=50, type=lambda x: int(x), help='Consumers of the topic')
    parser.add_argument('--producers', default=100, type=lambda x: int(x), help='Producers of the topic')
    parser.add_argument('--topic_prefix', default='topic', help='Topic name')
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

    workload = YdbTopicWorkload(args.endpoint, args.database, duration=args.duration, consumers=args.consumers, producers=args.producers, tables_prefix=args.topic_prefix)
    workload.start()
    workload.join()
