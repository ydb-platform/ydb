from __future__ import absolute_import

try:
    from ._cuda import *  # noqa
    HAS_CUDA = True
except ImportError:
    HAS_CUDA = False
