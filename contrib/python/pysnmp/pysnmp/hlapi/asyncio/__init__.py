#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2019, Ilya Etingof <etingof@gmail.com>
# License: http://snmplabs.com/pysnmp/license.html
#
"""
This module initializes the asyncio-based high-level API for pysnmp.

It imports all components from the v3arch.asyncio module, which is the
default architecture for the asyncio-based implementation of pysnmp.

Attributes:
    *: All components from the pysnmp.hlapi.v3arch.asyncio module.
"""
from pysnmp.hlapi.v3arch.asyncio import *  # default is v3arch
