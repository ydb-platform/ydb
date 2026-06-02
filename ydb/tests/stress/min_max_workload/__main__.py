# -*- coding: utf-8 -*-
import argparse
import logging

from ydb.tests.stress.min_max_workload.workload import MinMaxWorkload

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="min_max index stress workload",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--endpoint", default="localhost:2135", help="YDB endpoint")
    parser.add_argument("--database", default="/Root/test", help="YDB database")
    parser.add_argument("--path", default="min_max_workload", help="Path prefix for tables")
    parser.add_argument("--duration", default=100, type=int, help="Duration in seconds")
    parser.add_argument("--batch-size", default=10_000, type=int, help="Rows per insert batch")
    parser.add_argument("--insert-threads", default=4, type=int, help="Number of insert threads")
    parser.add_argument("--query-threads", default=2, type=int, help="Number of query threads")
    args = parser.parse_args()

    with MinMaxWorkload(
        args.endpoint,
        args.database,
        args.path,
        args.duration,
        args.batch_size,
        args.insert_threads,
        args.query_threads,
    ) as workload:
        workload.run()
