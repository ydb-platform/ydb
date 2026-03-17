"""Python 2/3 compatibility module."""
try:
    from collections.abc import Mapping, Sequence  # noqa: F401
except ImportError:  # Python 2
    from collections import Mapping, Sequence  # noqa: F401
