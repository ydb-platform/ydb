# -*- coding: utf-8 -*-
"""Markers for product bugs found by functional tests.

A case that fails because of a real product bug is marked with
``known_bug`` (non-strict xfail). The test still runs; CI stays green;
XPASS appears when the bug is fixed.
"""

import pytest

from ydb.tests.library.test_meta import link_test_case


def known_bug(reason, issue=None):
    """Mark a test as failing because of a known product bug.

    Args:
        reason: Short description of the bug (shown in the xfail reason).
        issue: Optional ticket id passed to ``link_test_case``.
    """
    mark = pytest.mark.xfail(reason='KNOWN BUG: {}'.format(reason), strict=False)
    if issue is None:
        return mark

    def _apply(obj):
        return mark(link_test_case(issue)(obj))

    return _apply
