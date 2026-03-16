#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
"""
This module provides debugging and logging utilities for the pysnmp library.

Classes:
    Printer: A class for controlling logging output.
    Debug: A helper class for managing debug flags and logging messages.

Functions:
    __getattr__(attr: str): Handles deprecated attribute access and raises AttributeError for unknown attributes.
    setLogger(value): Sets the global logger.
    hexdump(octets): Returns a hexdump of the given octets.

Constants:
    FLAG_NONE: No flags set.
    FLAG_IO: Input/Output flag.
    FLAG_DSP: Dispatcher flag.
    FLAG_MP: Message Processing flag.
    FLAG_SM: Security Model flag.
    FLAG_BLD: MIB Builder flag.
    FLAG_MIB: MIB flag.
    FLAG_INS: Instrumentation flag.
    FLAG_ACL: Access Control List flag.
    FLAG_PRX: Proxy flag.
    FLAG_APP: Application flag.
    FLAG_ALL: All flags set.
    FLAG_MAP: A dictionary mapping flag names to their corresponding constants.

Attributes:
    NullHandler: A logging handler that does nothing.
    logger: A global logger variable.
"""
import logging
import warnings


from pysnmp import __version__, error

# Compatibility API
deprecated_attributes = {
    "flagMap": "FLAG_MAP",
    "flagNone": "FLAG_NONE",
    "flagIO": "FLAG_IO",
    "flagDSP": "FLAG_DSP",
    "flagMP": "FLAG_MP",
    "flagSM": "FLAG_SM",
    "flagBLD": "FLAG_BLD",
    "flagMIB": "FLAG_MIB",
    "flagINS": "FLAG_INS",
    "flagACL": "FLAG_ACL",
    "flagPRX": "FLAG_PRX",
    "flagAPP": "FLAG_APP",
    "flagALL": "FLAG_ALL",
}


def __getattr__(attr: str):
    """Handle deprecated attributes."""
    if newAttr := deprecated_attributes.get(attr):
        warnings.warn(
            f"{attr} is deprecated. Please use {newAttr} instead.", DeprecationWarning
        )
        return globals()[newAttr]
    raise AttributeError(f"module {__name__} has no attribute {attr}")


FLAG_NONE = 0x0000
FLAG_IO = 0x0001
FLAG_DSP = 0x0002
FLAG_MP = 0x0004
FLAG_SM = 0x0008
FLAG_BLD = 0x0010
FLAG_MIB = 0x0020
FLAG_INS = 0x0040
FLAG_ACL = 0x0080
FLAG_PRX = 0x0100
FLAG_APP = 0x0200
FLAG_ALL = 0xFFFF

FLAG_MAP = {
    "io": FLAG_IO,
    "dsp": FLAG_DSP,
    "msgproc": FLAG_MP,
    "secmod": FLAG_SM,
    "mibbuild": FLAG_BLD,
    "mibview": FLAG_MIB,
    "mibinstrum": FLAG_INS,
    "acl": FLAG_ACL,
    "proxy": FLAG_PRX,
    "app": FLAG_APP,
    "all": FLAG_ALL,
}


class Printer:
    """Logging control."""

    def __init__(self, logger=None, handler=None, formatter=None):
        """Logging control."""
        if logger is None:
            logger = logging.getLogger("pysnmp")
        logger.setLevel(logging.DEBUG)
        if handler is None:
            handler = logging.StreamHandler()
        if formatter is None:
            formatter = logging.Formatter("%(asctime)s %(name)s: %(message)s")
        handler.setFormatter(formatter)
        handler.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        self.__logger = logger

    def __call__(self, msg):
        """Log a message."""
        self.__logger.debug(msg)

    def __str__(self):
        """Return a string representation of the object."""
        return "<python built-in logging>"


NullHandler = logging.NullHandler


class Debug:
    """Logging control."""

    DEFAULT_PRINTER = None

    def __init__(self, *flags, **options):
        """Helper class for logging control."""
        self._flags = FLAG_NONE
        if options.get("printer") is not None:
            self._printer = options.get("printer")
        elif self.DEFAULT_PRINTER is not None:
            self._printer = self.DEFAULT_PRINTER
        else:
            if "loggerName" in options:
                # route our logs to parent logger
                self._printer = Printer(
                    logger=logging.getLogger(options["loggerName"]),
                    handler=NullHandler(),
                )
            else:
                self._printer = Printer()
        self("running pysnmp version %s" % __version__)
        for f in flags:
            inverse = f and f[0] in ("!", "~")
            if inverse:
                f = f[1:]
            try:
                if inverse:
                    self._flags &= ~FLAG_MAP[f]
                else:
                    self._flags |= FLAG_MAP[f]
            except KeyError:
                raise error.PySnmpError("bad debug flag %s" % f)

            self(
                "debug category '{}' {}".format(f, inverse and "disabled" or "enabled")
            )

    def __str__(self):
        """Return a string representation of the object."""
        return f"logger {self._printer}, flags {self._flags:x}"

    def __call__(self, msg):
        """Log a message."""
        self._printer(msg)

    def __and__(self, flag):
        """Return a boolean value of the flag."""
        return self._flags & flag

    def __rand__(self, flag):
        """Return a boolean value of the flag."""
        return flag & self._flags


# This will yield false from bitwise and with a flag, and save
# on unnecessary calls
logger = 0


def set_logger(value):
    """Set the global logger."""
    global logger
    logger = value


def hexdump(octets):
    """Return a hexdump of the given octets."""
    return " ".join(
        [
            "{}{:02X}".format(n % 16 == 0 and ("\n%.5d: " % n) or "", x)
            for n, x in zip(range(len(octets)), octets)
        ]
    )
