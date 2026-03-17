"""Platform-dependent objects"""

import sys

if sys.flags.optimize == 2:
    raise RuntimeError("Cerberus can't be run with Python's optimization level 2.")


if sys.version_info < (3,):
    _int_types = (int, long)  # noqa: F821
    _str_type = basestring  # noqa: F821
else:
    _int_types = (int,)
    _str_type = str


if sys.version_info < (3, 3):
    from collections import (
        Callable,
        Container,
        Hashable,
        Iterable,
        Mapping,
        MutableMapping,
        Sequence,
        Set,
        Sized,
    )
else:
    from collections.abc import (
        Callable,
        Container,
        Hashable,
        Iterable,
        Mapping,
        MutableMapping,
        Sequence,
        Set,
        Sized,
    )

if sys.version_info < (3, 8):
    import importlib_metadata
else:
    import importlib.metadata as importlib_metadata


__all__ = (
    "_int_types",
    "_str_type",
    "importlib_metadata",
    Callable.__name__,
    Container.__name__,
    Hashable.__name__,
    Iterable.__name__,
    Mapping.__name__,
    MutableMapping.__name__,
    Sequence.__name__,
    Set.__name__,
    Sized.__name__,
)
