"""Support for SSL socket timeouts.

Copyright (c) 1999-2003 Ng Pheng Siong. All rights reserved.

Copyright 2008 Heikki Toivonen. All rights reserved.
"""

__all__ = [
    'DEFAULT_TIMEOUT',
    'timeout',
    'struct_to_timeout',
    'struct_size',
]

import sys
import struct

from M2Crypto import m2

DEFAULT_TIMEOUT: int = 600


class timeout(object):
    sec: int
    microsec: int

    def __init__(
        self, sec: int = DEFAULT_TIMEOUT, microsec: int = 0
    ) -> None:
        self.sec = sec
        self.microsec = microsec

    def pack(self) -> bytes:
        if sys.platform == 'win32':
            millisec = int(
                self.sec * 1000 + round(float(self.microsec) / 1000)
            )
            binstr = struct.pack('l', millisec)
        else:
            if m2.time_t_bits() == 32:
                binstr = struct.pack('ii', self.sec, self.microsec)
            else:
                binstr = struct.pack('ll', self.sec, self.microsec)
        return binstr


def struct_to_timeout(binstr: bytes) -> timeout:
    if sys.platform == 'win32':
        millisec = struct.unpack('l', binstr)[0]
        # On py3, int/int performs exact division and returns float. We want
        # the whole number portion of the exact division result:
        sec = int(millisec / 1000)
        microsec = (millisec % 1000) * 1000
    else:
        (sec, microsec) = struct.unpack('ll', binstr)
    return timeout(sec, microsec)


def struct_size() -> int:
    if sys.platform == 'win32':
        return struct.calcsize('l')
    else:
        return struct.calcsize('ll')
