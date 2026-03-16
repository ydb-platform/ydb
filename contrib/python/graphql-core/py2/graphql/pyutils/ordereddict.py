import sys

if sys.version_info >= (3, 7):
    # As of Python 3.7, dictionaries are specified to preserve insertion order
    OrderedDict = dict

else:
    try:
        # Try to load the Cython performant OrderedDict (C)
        # as is more performant than collections.OrderedDict (Python)
        from cyordereddict import OrderedDict  # type: ignore
    except ImportError:
        from collections import OrderedDict


__all__ = ["OrderedDict"]
