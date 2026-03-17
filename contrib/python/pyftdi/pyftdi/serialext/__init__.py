# Copyright (c) 2010-2024 Emmanuel Blot <emmanuel.blot@free.fr>
# Copyright (c) 2008-2015, Neotion
# All rights reserved.
#
# SPDX-License-Identifier: BSD-3-Clause

"""Serial modules compliant with pyserial APIs
"""

try:
    from serial.serialutil import SerialException
except ImportError as exc:
    raise ImportError("Python serial module not installed") from exc
try:
    from serial import VERSION, serial_for_url as serial4url
    version = tuple(int(x) for x in VERSION.split('.'))
    if version < (3, 0):
        raise ValueError
except (ValueError, IndexError, ImportError) as exc:
    raise ImportError("pyserial 3.0+ is required") from exc
try:
    from serial import protocol_handler_packages
    protocol_handler_packages.append('pyftdi.serialext')
except ImportError as exc:
    raise SerialException('Cannot register pyftdi extensions') from exc

serial_for_url = serial4url


def touch():
    """Do nothing, only for static checkers than do not like module import
       with no module references
    """
