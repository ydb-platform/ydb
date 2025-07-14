# -*- coding: utf-8 -*-
import argparse
import logging
import sys
from ydb.tests.stress.reconfig_state_storage_workload.workload import WorkloadRunner


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="state storage reconfiguration stability workload", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--grpc_endpoint", default="localhost:2135", help="An endpoint to be used")
    parser.add_argument("--http_endpoint", default="http://localhost:8765", help="An endpoint to be used")
    parser.add_argument("--database", default="/Root", help="A database to connect")
    parser.add_argument("--path", default="olap_workload", help="A path prefix for tables")
    parser.add_argument("--config_name", default="StateStorage", help="Can be StateStorage / StateStorageBoard / SchemeBoard")
    parser.add_argument("--duration", default=10 ** 9, type=lambda x: int(x), help="A duration of workload in seconds.")
    args = parser.parse_args()
    logger = logging.getLogger("reconfig_state_storage_workload")

    with WorkloadRunner(args.grpc_endpoint, args.http_endpoint, args.database, args.path, args.duration, args.config_name) as runner:
        if runner.run() < 2:
            logger.error("Test was not successful.")
            sys.exit(1)
