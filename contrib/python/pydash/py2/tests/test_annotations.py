# -*- coding: utf-8 -*-

import pytest

import pydash as _
from pydash._compat import PY2


if PY2:
    typed_function = None
else:
    # Hack around the fact that we can't define this kind of function in Python 2 so have to rely on
    # conditionally creating it using exec() to avoid syntax errors while still having this test
    # case covered for Python 3.
    # fmt: off
    exec("def typed_function(row: int, index: int, col: list): return row + 1")
    # fmt: on


@pytest.mark.skipif(PY2, reason="test requires Python 3 annotations")
def test_annotated_iteratee():
    assert _.map_([1, 2], typed_function) == [2, 3]
