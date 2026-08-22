# -*- coding: utf-8 -*-
import argparse
import logging
from ydb.tests.stress.topic.workload import YdbTopicWorkload

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
    parser.add_argument('--limit-memory-usage', action='store_true', help='Try to use less memory for intermediate buffers')
    parser.add_argument('--chunk-index', default=None, type=lambda x: int(x), help='Test chunk index')
    parser.add_argument('--chunk-size', default=None, type=lambda x: int(x), help='Test chunk size')

    args = parser.parse_args()

    if args.log_file:
        logging.basicConfig(
            filename=args.log_file,
            filemode='a',
            format='%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s',
            datefmt='%H:%M:%S',
            level=logging.DEBUG
        )

    workload = YdbTopicWorkload(
        args.endpoint,
        args.database,
        duration=args.duration,
        consumers=args.consumers,
        producers=args.producers,
        tables_prefix=args.topic_prefix,
        limit_memory_usage=args.limit_memory_usage,
        chunk_index=args.chunk_index,
        chunk_size=args.chunk_size
    )
    workload.start()
    workload.join()
