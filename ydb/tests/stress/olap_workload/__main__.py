# -*- coding: utf-8 -*-
import argparse
from ydb.tests.stress.common.instrumented_client import InstrumentedYdbClient
from ydb.tests.stress.olap_workload.workload import WorkloadRunner


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="olap stability workload", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--endpoint", default="localhost:2135", help="An endpoint to be used")
    parser.add_argument("--database", default="Root/test", help="A database to connect")
    parser.add_argument("--path", default="olap_workload", help="A path prefix for tables")
    parser.add_argument("--duration", default=10 ** 9, type=lambda x: int(x), help="A duration of workload in seconds.")
    parser.add_argument("--allow-nullables-in-pk", default=False, help="Allow nullable types for columns in a Primary Key.")
    args = parser.parse_args()
    client = InstrumentedYdbClient(args.endpoint, args.database, True)
    client.wait_connection()
    try:
        with WorkloadRunner(client, args.path, args.duration, args.allow_nullables_in_pk) as runner:
            runner.run()
    finally:
        client.close()
