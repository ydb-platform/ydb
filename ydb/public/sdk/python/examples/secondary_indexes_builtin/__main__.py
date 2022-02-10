# -*- coding: utf-8 -*-
import argparse
from . import secondary_indexes_builtin
import logging

INTERESTING_TARGETS = [
    "ydb.resolver.DiscoveryEndpointsResolver",
    "ydb.connection",
]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="""\033[92mYandex.Database examples binary.\x1b[0m\n""",
    )
    parser.add_argument(
        "-d", "--database", required=True, help="Name of the database to use"
    )
    parser.add_argument("-e", "--endpoint", required=True, help="Endpoint url to use")
    parser.add_argument("-p", "--path", default="")
    parser.add_argument("-v", "--verbose", default=False, action="store_true")

    args = parser.parse_args()

    if args.verbose:
        for target in INTERESTING_TARGETS:
            logger = logging.getLogger(target)
            logger.setLevel(logging.DEBUG)
            logger.addHandler(logging.StreamHandler())

    secondary_indexes_builtin.run(
        args.endpoint,
        args.database,
        args.path,
    )
