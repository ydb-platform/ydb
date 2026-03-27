# -*- coding: utf-8 -*-
import argparse
import logging
import sys

from ydb.tests.stress.nfs_backups.workload import WorkloadRunner
from ydb.tests.stress.common.instrumented_client import InstrumentedYdbClient

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    stream=sys.stderr,
)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="NFS export/import stress workload",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--endpoint", default="localhost:2135", help="An endpoint to be used")
    parser.add_argument("--database", default="Root/test", help="A database to connect")
    parser.add_argument("--duration", default=10 ** 9, type=lambda x: int(x), help="A duration of workload in seconds.")
    args = parser.parse_args()

    logger = logging.getLogger("nfs_backups")
    logger.info("Connecting to %s database=%s", args.endpoint, args.database)

    client = InstrumentedYdbClient(args.endpoint, args.database, True)
    client.wait_connection()
    logger.info("Connected successfully")

    try:
        with WorkloadRunner(client, args.duration) as runner:
            runner.run()
    except Exception:
        logger.exception("Workload failed")
        raise
    finally:
        logger.info("Closing client")
        client.close()
        logger.info("Done")
