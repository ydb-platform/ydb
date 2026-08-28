# -*- coding: utf-8 -*-
"""Markers for product bugs found by functional tests.

A case that fails because of a real product bug is marked with
``known_bug`` (non-strict xfail, not executed). CI stays green.
Pass ``run=True`` to execute the test and wait for XPASS.
"""

import pytest

from ydb.tests.library.test_meta import link_test_case

SLOT_CRASH_ON_PRIMARY_DDISK_LOSS = (
    'NBS slot dies with SIGSEGV (exit -11) while applying the VChunk config '
    'after a stopped Primary DDisk host goes TemporaryOffline; the partition '
    'tablet and its vhost endpoint disappear with it'
)


def known_bug(reason, issue=None, run=False):
    """Mark a test as failing because of a known product bug.

    Args:
        reason: Short description of the bug (shown in the xfail reason).
        issue: Optional ticket id passed to ``link_test_case``.
        run: If true, the test still executes and XPASS appears when fixed.
    """
    mark = pytest.mark.xfail(
        reason='KNOWN BUG: {}'.format(reason), strict=False, run=run
    )
    if issue is None:
        return mark

    def _apply(obj):
        return mark(link_test_case(issue)(obj))

    return _apply
