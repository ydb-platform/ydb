# -*- coding: utf-8 -*-
import argparse
import logging
from ydb.tests.stress.topic_reset_offset.workload import MEGABYTE, Workload

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Topic ResetOffset stress: stagger per-partition write times, then rewind consumers independently",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--endpoint", default="grpc://localhost:2135", help="YDB endpoint")
    parser.add_argument("--database", default=None, required=True, help="A database to connect")
    parser.add_argument("--duration", default=30, type=int, help="Reset loop duration in seconds")
    parser.add_argument("--writers", default=1, type=int, help="Live small-message writers during the reset loop")
    parser.add_argument("--consumers", default=2, type=int, help="Number of consumers")
    parser.add_argument("--readers-per-consumer", default=1, type=int, help="Reader sessions per consumer")
    parser.add_argument("--partitions", default=10, type=int, help="Topic partition count")
    parser.add_argument("--messages-per-partition", default=1000, type=int, help="Messages to preload per partition")
    parser.add_argument(
        "--large-message-bytes",
        default=10 * MEGABYTE,
        type=int,
        help="Size of the large preloaded message in each partition",
    )
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
        partitions=args.partitions,
        messages_per_partition=args.messages_per_partition,
        large_message_bytes=args.large_message_bytes,
    ) as workload:
        workload.loop()
