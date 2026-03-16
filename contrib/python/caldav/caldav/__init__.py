#!/usr/bin/env python
import logging

try:
    from ._version import __version__
except ModuleNotFoundError:
    __version__ = "(unknown)"
    import warnings

    warnings.warn(
        "You need to install the `build` package and do a `python -m build` to get caldav.__version__ set correctly"
    )
from .davclient import DAVClient
from .davclient import get_davclient
from .search import CalDAVSearcher

## TODO: this should go away in some future version of the library.
from .objects import *

## We should consider if the NullHandler-logic below is needed or not, and
## if there are better alternatives?
# Silence notification of no default logging handler
log = logging.getLogger("caldav")


class NullHandler(logging.Handler):
    def emit(self, record) -> None:
        pass


log.addHandler(NullHandler())

__all__ = ["__version__", "DAVClient", "get_davclient"]
