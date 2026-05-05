# -*- coding: utf-8 -*-
import argparse
import logging
import os

from ydb.tests.stress.common.instrumented_client import InstrumentedYdbClient
from ydb.tests.stress.oltp_workload.workload import WorkloadRunner

logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="oltp stability workload", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--endpoint", default="localhost:2135", help="An endpoint to be used")
    parser.add_argument("--database", default="Root/test", help="A database to connect")
    parser.add_argument("--path", default="oltp_workload", help="A path prefix for tables")
    parser.add_argument("--duration", default=10 ** 9, type=lambda x: int(x), help="A duration of workload in seconds.")
    args = parser.parse_args()
    client = InstrumentedYdbClient(args.endpoint, args.database, True, sessions=3000)
    client.wait_connection()
    try:
        with WorkloadRunner(client, args.path, args.duration) as runner:
            runner.run()
    finally:
        client.close()
