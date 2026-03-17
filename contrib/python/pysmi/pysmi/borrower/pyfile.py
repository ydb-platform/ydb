#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
import importlib.machinery

from pysmi.borrower.base import AbstractBorrower


SOURCE_SUFFIXES = importlib.machinery.EXTENSION_SUFFIXES


class PyFileBorrower(AbstractBorrower):
    """Create PySNMP MIB file borrowing object."""

    exts = SOURCE_SUFFIXES
