# -*- coding: utf-8 -*-
import argparse
import logging
from ydb.tests.stress.topic_set_offsets.workload import Workload

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Topic SetOffsets stress: write, read/commit and set_offsets in parallel",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--endpoint", default="grpc://localhost:2135", help="YDB endpoint")
    parser.add_argument("--database", default=None, required=True, help="A database to connect")
    parser.add_argument("--duration", default=60, type=int, help="A duration of workload in seconds")
    parser.add_argument("--writers", default=4, type=int, help="Number of writer sessions")
    parser.add_argument("--consumers", default=3, type=int, help="Number of consumers")
    parser.add_argument("--readers-per-consumer", default=2, type=int, help="Reader sessions per consumer")
    parser.add_argument("--log-file", default=None, help="Append log into specified file")
    args = parser.parse_args()

    log_kwargs = {
        "format": "%(asctime)s %(name)s %(levelname)s %(message)s",
        "level": logging.INFO,
    }
    if args.log_file:
        log_kwargs["filename"] = args.log_file
        log_kwargs["filemode"] = "a"
    logging.basicConfig(**log_kwargs)

    with Workload(
        args.endpoint,
        args.database,
        args.duration,
        writers=args.writers,
        consumers=args.consumers,
        readers_per_consumer=args.readers_per_consumer,
    ) as workload:
        workload.loop()
