# -*- coding: utf-8 -*-
import argparse

from ydb.tests.stress.system_tablet_backup.workload import WorkloadRunner
from ydb.tests.stress.common.instrumented_client import InstrumentedYdbClient


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="system tablet backup workload", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--endpoint", default="localhost:2135", help="An endpoint to be used")
    parser.add_argument("--mon-endpoint", default="localhost:8765", help="A monitoring endpoint to be used")
    parser.add_argument("--database", default="Root/test", help="A database to connect")
    parser.add_argument("--duration", default=10**9, type=lambda x: int(x), help="A duration of workload in seconds.")
    parser.add_argument("--backup-path", help="Path to system tablet backup directory")
    args = parser.parse_args()

    client = InstrumentedYdbClient(args.endpoint, args.database, True)
    client.wait_connection()
    with WorkloadRunner(
        client, args.duration, args.endpoint, args.mon_endpoint, args.backup_path,
    ) as runner:
        runner.run()
