import os
import sys
import logging
import argparse

import fetch_from


def parse_args():
    parser = argparse.ArgumentParser()
    fetch_from.add_common_arguments(parser)

    parser.add_argument('--key', required=True)
    parser.add_argument('--entrypoint', required=False, default="storage.yandex-team.ru/get-devtools")
    parser.add_argument('--scheme', required=False, default="https")

    return parser.parse_args()


def fetch(args):
    parts = args.key.split("/")
    if len(parts) != 3:
        raise ValueError("Invalid MDS key '{}'".format(args.key))

    _, sha1, file_name = parts
    url = "{}://{}/{}".format(args.scheme, args.entrypoint, args.key)
    fetched_file = fetch_from.fetch_url(url, False, file_name, expected_sha1=sha1)

    return fetched_file, file_name


def main(args):
    fetched_file, resource_file_name = fetch(args)

    fetch_from.process(fetched_file, resource_file_name, args)


if __name__ == '__main__':
    args = parse_args()
    fetch_from.setup_logging(args, os.path.basename(__file__))

    try:
        main(args)
    except Exception as e:
        logging.exception(e)
        print >> sys.stderr, open(args.abs_log_path).read()
        sys.stderr.flush()

        import error

        sys.exit(error.ExitCodes.INFRASTRUCTURE_ERROR if fetch_from.is_temporary(e) else 1)
