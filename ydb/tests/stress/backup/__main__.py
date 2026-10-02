import argparse

from ydb.tests.stress.backup.workload import WorkloadRunnerBackup
from ydb.tests.stress.common.instrumented_client import InstrumentedYdbClient

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backup workload", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--endpoint", default="localhost:2135", help="An endpoint to be used")
    parser.add_argument("--database", default="/Root", help="A database to connect to")
    parser.add_argument("--duration", default=10 ** 9, type=lambda x: int(x), help="A duration of workload in seconds.")
    parser.add_argument("--backup-interval", default=10, type=lambda x: int(x), help="Seconds between triggering incremental backups")
    args = parser.parse_args()
    client = InstrumentedYdbClient(args.endpoint, args.database, True)
    client.wait_connection()
    try:
        with WorkloadRunnerBackup(client, args.duration, args.backup_interval) as runner:
            runner.run()
    finally:
        client.close()
