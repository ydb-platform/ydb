# encoding: utf-8

"""Provides Python 2/3 compatibility objects."""

import sys

try:
    from collections.abc import Container, Mapping, Sequence
except ImportError:
    from collections import Container, Mapping, Sequence

if sys.version_info >= (3, 0):
    from .python3 import (  # noqa
        BytesIO,
        Unicode,
        is_integer,
        is_string,
        is_unicode,
        to_unicode,
    )
else:
    from .python2 import (  # noqa
        BytesIO,
        Unicode,
        is_integer,
        is_string,
        is_unicode,
        to_unicode,
    )

__all__ = [
    "BytesIO",
    "Container",
    "Mapping",
    "Sequence",
    "Unicode",
    "is_integer",
    "is_string",
    "is_unicode",
    "to_unicode",
]
