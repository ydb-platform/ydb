# -*- coding: utf-8 -*-
"""
This example shows a function with an unused optional parameter. A warning
message should be emitted if `z` is used (as a positional or keyword parameter).
"""

import sys
import warnings

import pytest

from deprecated.params import deprecated_params

PY38 = sys.version_info[0:2] >= (3, 8)

if PY38:
    # Positional-Only Arguments are only available for Python >= 3
    # On other version, this code raises a SyntaxError exception.
    exec (
        """
@deprecated_params("z")
def pow2(x, y, z=None, /):
    return x ** y
        """
    )

else:

    @deprecated_params("z")
    def pow2(x, y, z=None):
        return x ** y


@pytest.mark.parametrize(
    "args, kwargs, expected",
    [
        pytest.param((5, 6), {}, [], id="'z' not used: no warnings"),
        pytest.param(
            (5, 6, 8),
            {},
            ["'z' parameter is deprecated"],
            id="'z' used in positional params",
        ),
        pytest.param(
            (5, 6),
            {"z": 8},
            ["'z' parameter is deprecated"],
            id="'z' used in keyword params",
            marks=pytest.mark.skipif(PY38, reason="'z' parameter is positional only"),
        ),
    ],
)
def test_pow2(args, kwargs, expected):
    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        pow2(*args, **kwargs)
    actual = [str(warn.message) for warn in warns]
    assert actual == expected
