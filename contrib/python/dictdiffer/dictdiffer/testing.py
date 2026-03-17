# This file is part of Dictdiffer.
#
# Copyright (C) 2021 CERN.
#
# Dictdiffer is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more
# details.
"""Define helper functions for testing."""

from pprint import pformat

from . import diff


def assert_no_diff(*args, **kwargs):
    """Compare two dictionary/list/set objects and raise error on difference.

    When there is a difference, this will print a formatted diff.
    This is especially useful for pytest.

    Usage example:

        >>> from dictdiffer.testing import assert_no_diff
        >>> result = {'a': 'b'}
        >>> expected = {'a': 'c'}
        >>> assert_no_diff(result, expected)
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
          File "<stdin>", line 14, in assert_no_diff
        AssertionError: [('change', 'a', ('b', 'c'))]

    :param args: Positional arguments to the ``diff`` function.
    :param second: Named arguments to the ``diff`` function.
    """
    d = [d for d in diff(*args, **kwargs)]
    assert not d, pformat(d)
