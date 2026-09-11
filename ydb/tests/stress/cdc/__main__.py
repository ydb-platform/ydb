# -*- coding: utf-8 -*-
import argparse
import time

from ydb.tests.stress.cdc.workload import WorkloadRunner
from ydb.tests.stress.common.instrumented_client import InstrumentedYdbClient


def wait_ready(client, attempts=12, timeout=10):
    last = None
    for _ in range(attempts):
        try:
            client.wait_connection(timeout=timeout)
            return
        except Exception as e:
            last = e
            time.sleep(2)
    if last is None:
        raise RuntimeError("failed to connect to YDB")
    raise last


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Change Data Capture (CDC) workload", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--endpoint", default="localhost:2135", help="An endpoint to be used")
    parser.add_argument("--database", default="/Root", help="A database to connect to")
    parser.add_argument("--duration", default=10 ** 9, type=lambda x: int(x), help="A duration of workload in seconds.")
    parser.add_argument("--path", default=None, help="Table path (relative to database or absolute)")
    parser.add_argument("--phase", choices=['prepare', 'run', 'clean'], default=None,
                        help='Phase to run: prepare (create table+changefeed), run, clean. If omitted, all phases run in sequence.')
    args = parser.parse_args()
    if args.phase is not None and not args.path:
        parser.error("--path is required when --phase is set")
    client = InstrumentedYdbClient(args.endpoint, args.database, True)
    wait_ready(client)
    try:
        with WorkloadRunner(client, args.duration, path=args.path) as runner:
            if args.phase == 'prepare':
                runner.prepare()
            elif args.phase == 'run':
                runner.run_load()
            elif args.phase == 'clean':
                runner.clean()
            else:
                runner.run()
    finally:
        client.close()
