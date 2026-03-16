"""Pathlib API extended to use fsspec backends."""

import sys

try:
    from upath._version import __version__
except ImportError:
    __version__ = "not-installed"

from upath.core import UPath

__all__ = ["UPath"]
