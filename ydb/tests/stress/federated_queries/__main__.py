# -*- coding: utf-8 -*-
import argparse
import logging

from ydb.tests.stress.federated_queries.workload import Workload

logger = logging.getLogger("logger")

if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        level=logging.INFO,
    )

    text = """\033[92mFederated queries workload\x1b[0m"""
    parser = argparse.ArgumentParser(
        description=text, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--endpoint", default="localhost:2135", help="An endpoint to be used")
    parser.add_argument("--database", default=None, required=True, help="A database to connect")
    parser.add_argument("--duration", default=60, type=int, help="A duration of workload in seconds")
    parser.add_argument("--prefix", default="federated_queries_stress", help="External source name prefix")
    args = parser.parse_args()

    with Workload(
        args.endpoint,
        args.database,
        args.duration,
        args.prefix,
    ) as workload:
        workload.loop()
