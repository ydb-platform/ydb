from __future__ import print_function
import os
import sys
import logging
import argparse

# Explicitly enable local imports
# Don't forget to add imported scripts to inputs of the calling command!
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import fetch_from


def parse_args():
    parser = argparse.ArgumentParser()
    fetch_from.add_common_arguments(parser)
    parser.add_argument('--file-name', required=True)
    parser.add_argument('--archive', required=True)

    return parser.parse_args()


def main(args):
    archive = args.archive
    file_name = args.file_name.rstrip('-')

    fetch_from.process(archive, file_name, args, remove=False)


if __name__ == '__main__':
    args = parse_args()
    fetch_from.setup_logging(args, os.path.basename(__file__))

    try:
        main(args)
    except Exception as e:
        logging.exception(e)
        print(open(args.abs_log_path).read(), file=sys.stderr)
        sys.stderr.flush()

        import error

        sys.exit(error.ExitCodes.INFRASTRUCTURE_ERROR if fetch_from.is_temporary(e) else 1)
