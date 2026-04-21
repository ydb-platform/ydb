#!/usr/bin/env python3

"""Manual fast-unmute state machine (CLI entry).

Implementation is split across ``mute.fast_unmute_*`` modules; this file only
bootstraps ``sys.path`` and runs ``sync``.

When a user manually closes a mute issue as Completed, every listed test that
is still muted in CI gets a row in ``fast_unmute_active``. While a row exists,
``mute/create_new_muted_ya.py`` uses the short window from ``mute_config.json``.

**During TTL** (``manual_unmute_ttl_calendar_days``): if a test becomes unmuted
in monitor data, only that row is removed; automation may post a short
**progress** comment while other tests on the same issue are still tracked.

**Before TTL ends**, if every tracked test for the issue has left ``is_muted`` in
CI, all rows are gone → **success** comment, org project **Status → Unmuted**,
``manual-fast-unmute`` label removed (issue stays closed).

**After TTL**: if any tracked row is still muted, **all** rows for that issue are
removed, the issue is **reopened**, **Status → Muted**, one **deadline** comment
(lists who already unmuted vs who is still muted vs tracking cleared early),
label removed.

``create_new_muted_ya`` still decides short-window unmute; rows in
``fast_unmute_active`` are removed only when the test is unmuted in CI or when
the TTL path runs for the issue.

Entering fast-unmute does **not** reopen the issue (issue stays closed); tracking
uses YDB and the ``manual-fast-unmute`` label.

If someone **closes the issue again** as Completed while tests are still muted,
the next ``sync`` can insert new rows and start another fast-unmute cycle.

Usage:
    python3 .github/scripts/tests/mute/manual_unmute.py sync
    python3 .github/scripts/tests/mute/manual_unmute.py sync -v   # DEBUG: why each issue/test was skipped

Testing only — unset before real runs.

``MANUAL_UNMUTE_SIMULATE_UNMUTED``: comma-separated ``full_name`` values removed from the
"currently muted" set so cleanup behaves as if ``is_muted`` were already 0 (grace path).
Defined in ``mute.fast_unmute_ydb`` (same env var).

``export MANUAL_UNMUTE_SIMULATE_UNMUTED='suite/path/Test.one' && python3 .github/scripts/tests/mute/manual_unmute.py sync``
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

from mute.fast_unmute_pipeline import BOT_LOGINS, sync
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
