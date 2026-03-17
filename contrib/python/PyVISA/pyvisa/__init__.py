# -*- coding: utf-8 -*-
"""Python wrapper of IVI Virtual Instruments Software Architecture library (VISA).

This file is part of PyVISA.

:copyright: 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import logging
from importlib.metadata import PackageNotFoundError, version

# Defined here since it is imported in other pyvisa modules
logger = logging.getLogger("pyvisa")
logger.addHandler(logging.NullHandler())

# Those import cannot at the top of the file
from .errors import (  # noqa: E402
    Error,
    InvalidBinaryFormat,
    InvalidSession,
    LibraryError,
    OSNotSupported,
    UnknownHandler,
    VisaIOError,
    VisaIOWarning,
    VisaTypeError,
)
from .highlevel import ResourceManager  # noqa: E402

# This is needed to register all resources (E402, F401).
from .resources import Resource  # noqa


def log_to_screen(level=logging.DEBUG) -> None:
    log_to_stream(None, level)  # sys.stderr by default


def log_to_stream(stream_output, level=logging.DEBUG) -> None:
    logger.setLevel(level)
    ch = logging.StreamHandler(stream_output)
    ch.setLevel(level)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    ch.setFormatter(formatter)

    logger.addHandler(ch)


__version__ = "unknown"
try:
    __version__ = version(__name__)
except PackageNotFoundError:
    # package is not installed
    pass


__all__ = [
    "Error",
    "InvalidBinaryFormat",
    "InvalidSession",
    "LibraryError",
    "OSNotSupported",
    "ResourceManager",
    "UnknownHandler",
    "VisaIOError",
    "VisaIOWarning",
    "VisaTypeError",
    "log_to_screen",
    "log_to_stream",
    "logger",
]
