#!/usr/bin/env python3

"""CLI: ``sync`` manual fast-unmute (YDB ``fast_unmute_active``, GitHub label/comments, project status).

Logic lives in ``mute.fast_unmute_*``; needs ``GITHUB_TOKEN``. Optional test env
``MANUAL_UNMUTE_SIMULATE_UNMUTED`` (comma-separated ``full_name``) — see ``mute.fast_unmute_ydb``.

Run: ``python3 .../manual_unmute.py sync`` (``-v`` = log enter skip reasons).
"""

import argparse
import logging
import os
import sys

_mutedir = os.path.dirname(os.path.abspath(__file__))
_tests_dir = os.path.dirname(_mutedir)
_scripts_dir = os.path.dirname(_tests_dir)
for _p in (_scripts_dir, os.path.join(_scripts_dir, 'analytics'), _tests_dir):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from mute.fast_unmute_pipeline import sync
from ydb_wrapper import YDBWrapper

_LOG = logging.getLogger('manual_unmute')


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    if not os.environ.get('GITHUB_TOKEN'):
        logging.error(
            'GITHUB_TOKEN is required for GitHub GraphQL (issue close, labels, timeline). '
            'Set it in workflow env or export it when running locally.'
        )
        return 1

    parser = argparse.ArgumentParser(description='Manual fast-unmute state machine')
    subparsers = parser.add_subparsers(dest='mode', required=True)
    sync_parser = subparsers.add_parser(
        'sync',
        help='Enter new rows and clean up stale/failed/unmuted rows',
    )
    sync_parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='Log enter-phase skip reasons (does not enable ydb/grpc DEBUG)',
    )
    args = parser.parse_args()
    if getattr(args, 'verbose', False):
        _LOG.setLevel(logging.DEBUG)

    with YDBWrapper() as ydb_wrapper:
        if not ydb_wrapper.check_credentials():
            return 1
        sync(ydb_wrapper)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
