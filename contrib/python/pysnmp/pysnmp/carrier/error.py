#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from pysnmp import error


class CarrierError(error.PySnmpError):
    """Base class for all carrier-related exceptions."""

    pass
