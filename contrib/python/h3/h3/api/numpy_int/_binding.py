"""
This API handles H3 Indexes of type `int` (specifically, `uint64`),
using `numpy.array` objects for collections.
`h3` will interpret these Indexes as unsigned 64-bit integers.

This API is **optional**, and will only work if the
user has `numpy` installed.

Input collections:

- `Iterable[int]`
    - works for `lists`, but not `sets`
    - will attempt to convert `int` to `uint64`
    - no memory copy is made if input dtype is `uint64`

Output collections:

- `np.ndarray[np.uint64]` for unordered
- `np.ndarray[np.uint64]` for ordered
"""

import numpy as np

from .._api_template import _API_FUNCTIONS


def _id(x):
    return x


def _in_collection(x):
    # array is copied only if dtype does not match
    # `list`s should work, but not `set`s of integers
    return np.asarray(x, dtype='uint64')


_binding = _API_FUNCTIONS(
    _in_scalar = _id,
    _out_scalar = _id,
    _in_collection = _in_collection,
    _out_unordered = np.asarray,
    _out_ordered = np.asarray,
)
