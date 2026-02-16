import argparse
import logging

from ydb.tests.stress.result_set_format.workload import WorkloadRunner
from ydb.tests.stress.common.common import YdbClient


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Result set format stability workload", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--endpoint", default="localhost:2135", help="An endpoint to be used")
    parser.add_argument("--database", default="Root/test", help="A database to connect")
    parser.add_argument("--path", default="result_set_format", help="A path prefix for tables")
    parser.add_argument("--format", default="value", choices=["value", "arrow"], help="A format of result sets")
    parser.add_argument("--duration", default=10**9, type=lambda x: int(x), help="A duration of workload in seconds.")
    parser.add_argument("--log-level", default="WARNING", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Logging level")

    args = parser.parse_args()

    logging.basicConfig(level=args.log_level)
    client = YdbClient(args.endpoint, args.database, True, sessions=3000)
    client.wait_connection()
    try:
        with WorkloadRunner(client, args.path, args.duration, args.format) as runner:
            runner.run()
    finally:
        client.close()
