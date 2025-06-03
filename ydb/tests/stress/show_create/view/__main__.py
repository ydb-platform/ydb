# -*- coding: utf-8 -*-
import argparse
import logging
import sys
from ydb.tests.stress.show_create.view.workload import ShowCreateViewWorkload

if __name__ == "__main__":
    text = "SHOW CREATE VIEW Workload Test"
    parser = argparse.ArgumentParser(description=text, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--endpoint", required=True, help="YDB endpoint (e.g., grpc://localhost:2135)")
    parser.add_argument("--database", required=True, help="YDB database path (e.g., /Root or /local)")
    parser.add_argument("--duration", type=int, default=60, help="Workload duration in seconds (default: 60)")
    parser.add_argument(
        "--path-prefix",
        default=None,
        help="Optional path prefix for tables/views within the database (e.g., my_tests/scv)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()), format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    logger = logging.getLogger("ShowCreateViewWorkload")
    logger.info(f"Starting SHOW CREATE VIEW workload with args: {args}")

    with ShowCreateViewWorkload(args.endpoint, args.database, args.duration, args.path_prefix) as workload:
        workload.loop()
        if workload.failed_cycles > 0:
            logger.error("Test completed with failures.")
            sys.exit(1)
        elif workload.successful_cycles == 0:
            logger.error("Test was not successful.")
            sys.exit(1)
        else:
            logger.info("Test completed successfully.")
