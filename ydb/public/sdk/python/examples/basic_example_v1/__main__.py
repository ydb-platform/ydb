# -*- coding: utf-8 -*-
import argparse
import basic_example
import logging


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
        logger = logging.getLogger("ydb.pool.Discovery")
        logger.setLevel(logging.INFO)
        logger.addHandler(logging.StreamHandler())

    basic_example.run(
        args.endpoint,
        args.database,
        args.path,
    )
