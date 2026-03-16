# coding: utf-8
from __future__ import unicode_literals, division, absolute_import, print_function

import re
import sys
from contextlib import contextmanager


if sys.version_info < (3,):
    str_cls = unicode  # noqa
else:
    str_cls = str


@contextmanager
def assert_exception(test_case, expected_class, expected_msg):
    """
    Look for a specific exception type and message, allowing the exception
    to be raised if it doesn't match
    """

    expected_re = re.compile(expected_msg)

    try:
        yield

    except Exception as e:
        should_raise = True
        if isinstance(e, expected_class):
            test_case.assertIsInstance(e, expected_class)
            msg = str_cls(e)
            if expected_re.search(msg):
                test_case.assertRegex(msg, expected_re)
                should_raise = False
        if should_raise:
            raise
