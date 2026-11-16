import argparse
import logging
import os

from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.stress.snapshot_retention.workload import SnapshotRetentionWorkload


def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    parser = argparse.ArgumentParser(description="Check retained row-table snapshots under concurrent writes")
    parser.add_argument("--endpoint", default="localhost:2135")
    parser.add_argument("--database", default="/Root/test")
    parser.add_argument("--path", default="snapshot_retention", help="Table name prefix")
    parser.add_argument("--duration", default=10 ** 9, type=int, help="Workload duration in seconds")
    parser.add_argument(
        "--hold-seconds", default=360, type=float,
        help="Must exceed the cluster's MVCC and registry safety windows",
    )
    parser.add_argument("--readers", default=4, type=int)
    parser.add_argument("--seed", default=1, type=int)
    args = parser.parse_args()
    with ydb.Driver(endpoint=args.endpoint, database=args.database) as driver:
        driver.wait(timeout=60)
        with SnapshotRetentionWorkload(driver, args.database, args.path) as workload:
            workload.create_tables()
            workload.write_generation(0)
            workload.run(args.duration, args.hold_seconds, args.readers, args.seed)


if __name__ == "__main__":
    main()
