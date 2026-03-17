# $Id: udp.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""User Datagram Protocol."""
from __future__ import absolute_import

from . import dpkt

UDP_HDR_LEN = 8
UDP_PORT_MAX = 65535


class UDP(dpkt.Packet):
    """User Datagram Protocol.

    User Datagram Protocol (UDP) is one of the core members of the Internet protocol suite.
    With UDP, computer applications can send messages, in this case referred to as datagrams,
    to other hosts on an Internet Protocol (IP) network. Prior communications are not required
    in order to set up communication channels or data paths.

    Attributes:
        __hdr__: Header fields of UDP.
            sport: (int): Source port. (2 bytes)
            dport: (int): Destination port. (2 bytes)
            ulen: (int): Length. (2 bytes)
            sum: (int): Checksum. (2 bytes)
    """

    __hdr__ = (
        ('sport', 'H', 0xdead),
        ('dport', 'H', 0),
        ('ulen', 'H', 8),
        ('sum', 'H', 0)
    )
