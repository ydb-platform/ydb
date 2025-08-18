# -*- coding: utf-8 -*-
import argparse
# import logging
import os
from ydb.tests.stress.kafka.workload import Workload

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""YDB topic workload with Kafka Streams.\n""",
    )
    parser.add_argument("-d", "--database", default="/Root/test", help="Name of the database to use")
    parser.add_argument("-b", "--bootstrap", default="lbk-dev-4.search.yandex.net:19092", help="Bootstrap server")
    parser.add_argument("-e", "--endpoint", default="grpc://lbk-dev-4.search.yandex.net:31105", help="Endpoint url to use")
    parser.add_argument("-source", "--sourcePath", default="test-topic", help="Test topic name")
    parser.add_argument("-target", "--targetPath", default="target-topic", help="Target topic name")
    parser.add_argument("-c", "--consumer", default="workload-consumer-0", help="Consumer name")
    parser.add_argument("-n", "--numWorkers", default=2, help="Number of workers")
    args = parser.parse_args()

    os.environ["YDB_ANONYMOUS_CREDENTIALS"] = "1"

    with Workload(args.endpoint, args.database, bootstrap=args.bootstrap, test_topic_path=args.sourcePath, target_topic_path=args.targetPath,
                  workload_consumer_name=args.consumer, num_workers=int(args.numWorkers)) as workload:
        workload.loop()
