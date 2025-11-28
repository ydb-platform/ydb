# -*- coding: utf-8 -*-
import argparse
import os
from ydb.tests.stress.kafka.workload import Workload

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""YDB topic workload with Kafka Streams.\n""",
    )
    parser.add_argument("-d", "--database", help="Name of the database to use")
    parser.add_argument("-b", "--bootstrap", help="Bootstrap server")
    parser.add_argument("-e", "--endpoint", help="Endpoint url to use")
    parser.add_argument("--source-path", help="Test topic name")
    parser.add_argument("--target-path", help="Target topic name")
    parser.add_argument("-c", "--consumer", help="Consumer name")
    parser.add_argument("-n", "--num-workers", help="Number of workers")
    parser.add_argument("--duration", help="Duration of waiting")
    args = parser.parse_args()

    os.environ["YDB_ANONYMOUS_CREDENTIALS"] = "1"

    with Workload(args.endpoint, args.database, bootstrap=args.bootstrap,
                  test_topic_path=args.source_path, target_topic_path=args.target_path,
                  workload_consumer_name=args.consumer, num_workers=int(args.num_workers),
                  duration=int(args.duration)) as workload:
        workload.loop()
