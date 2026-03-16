"""
This API handles H3 Indexes of type `int` (specifically, `uint64`),
using Python `memoryview` objects for collections.
`h3` will interpret these Indexes as unsigned 64-bit integers.

Input collections:

- `memoryview[uint64]`, i.e., anything that supports the buffer protocol
    - `dtype` must be `uint64`. for example, `long` will raise an error
    - `list` or `set` inputs will not be accepted

Output collections:

- `memoryview[uint64]` for unordered
- `memoryview[uint64]` for ordered
"""

from .._api_template import _API_FUNCTIONS


def _id(x):
    return x


_binding = _API_FUNCTIONS(
    _in_scalar = _id,
    _out_scalar = _id,
    _in_collection = _id,
    _out_unordered = _id,
    _out_ordered = _id,
)
