# -*- coding: utf-8 -*-
"""Pure Python backend for PyVISA.


:copyright: 2014-2024 by PyVISA-py Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

from importlib.metadata import PackageNotFoundError, version

# We need to import all attributes so that __init_subclass__() is executed once
# (hence the noqa)
from . import attributes  # noqa: F401
from .highlevel import PyVisaLibrary

__version__ = "unknown"
try:
    __version__ = version(__name__)
except PackageNotFoundError:
    # package is not installed
    pass

WRAPPER_CLASS = PyVisaLibrary
