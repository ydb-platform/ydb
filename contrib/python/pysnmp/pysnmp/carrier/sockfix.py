#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import socket

from pysnmp import debug


SYMBOLS = {
    "IP_PKTINFO": 8,
    "IP_TRANSPARENT": 19,
    "SOL_IPV6": 41,
    "IPV6_RECVPKTINFO": 49,
    "IPV6_PKTINFO": 50,
    "IPV6_TRANSPARENT": 75,
}

for symbol, value in SYMBOLS.items():
    if not hasattr(socket, symbol):
        setattr(socket, symbol, value)

        debug.logger & debug.FLAG_IO and debug.logger(
            "WARNING: the socket module on this platform misses option %s. "
            "Assuming its value is %d." % (symbol, value)
        )
