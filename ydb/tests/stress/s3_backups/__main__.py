# -*- coding: utf-8 -*-
import argparse

from ydb.tests.stress.s3_backups.workload import WorkloadRunner
from ydb.tests.stress.common.common import YdbClient


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="oltp stability workload", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--endpoint", default="localhost:2135", help="An endpoint to be used")
    parser.add_argument("--database", default="Root/test", help="A database to connect")
    parser.add_argument("--duration", default=10 ** 9, type=lambda x: int(x), help="A duration of workload in seconds.")
    args = parser.parse_args()
    client = YdbClient(args.endpoint, args.database, True)
    client.wait_connection()
    with WorkloadRunner(client, args.endpoint, args.duration) as runner:
        runner.run()
