#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
"""
This module defines the custom exception class for the PySNMP library.

Classes:
    PySnmpError: Base class for all PySNMP exceptions.

Usage:
    This module is used to handle exceptions specific to the PySNMP library.
"""
import sys


class PySnmpError(Exception):
    """Base class for all PySNMP exceptions."""

    def __init__(self, *args):
        """Python SNMP error."""
        msg = args and str(args[0]) or ""

        self.cause = sys.exc_info()

        if self.cause[0]:
            msg += f" caused by {self.cause[0]}: {self.cause[1]}"

        if msg:
            args = (msg,) + args[1:]

        Exception.__init__(self, *args)
