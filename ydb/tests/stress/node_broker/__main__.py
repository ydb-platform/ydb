# -*- coding: utf-8 -*-
import argparse

from ydb.tests.stress.node_broker.workload import WorkloadRunner
from ydb.tests.stress.common.instrumented_client import InstrumentedYdbClient


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="node broker & dynamic nameserver workload", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--endpoint", default="localhost:2135", help="An endpoint to be used")
    parser.add_argument("--mon-endpoint", default="localhost:8765", help="An monitoring endpoint to be used")
    parser.add_argument("--database", default="Root/test", help="A database to connect")
    parser.add_argument("--duration", default=10**9, type=lambda x: int(x), help="A duration of workload in seconds.")
    args = parser.parse_args()
    client = InstrumentedYdbClient(args.endpoint, args.database, True)
    client.wait_connection()
    try:
        with WorkloadRunner(client, args.mon_endpoint, args.duration) as runner:
            runner.run()
    finally:
        client.close()
