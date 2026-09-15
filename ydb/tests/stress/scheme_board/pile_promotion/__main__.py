# -*- coding: utf-8 -*-
import argparse
import logging
import sys
from ydb.tests.stress.scheme_board.pile_promotion.workload import WorkloadRunner


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="test scheme board stability during pile promotions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--grpc_endpoint", default="localhost:2135", help="GRPC endpoint to be used")
    parser.add_argument("--http_endpoint", default="http://localhost:8765", help="HTTP endpoint to be used")
    parser.add_argument("--database", default="/Root", help="Database to connect to")
    parser.add_argument("--path", default="olap_workload", help="Path prefix for tables")
    parser.add_argument(
        "--duration", default=180, type=lambda x: int(x), help="Duration of workload in seconds."
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(name)s:%(lineno)d - %(funcName)s: %(message)s"
    )
    logger = logging.getLogger("pile_promotion")

    with WorkloadRunner(args.grpc_endpoint, args.http_endpoint, args.database, args.path, args.duration) as runner:
        if runner.run() < 2:
            logger.error("Test was not successful.")
            sys.exit(1)
