# $Id: ipx.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Internetwork Packet Exchange."""
from __future__ import absolute_import

from . import dpkt

IPX_HDR_LEN = 30


class IPX(dpkt.Packet):
    """Internetwork Packet Exchange.

    Internetwork Packet Exchange (IPX) is the network layer protocol in the IPX/SPX protocol suite.
    IPX is derived from Xerox Network Systems' IDP. It also has the ability to act as a transport layer protocol.

    Attributes:
        __hdr__: Header fields of IPX.
            sum: (int): Checksum (2 bytes).
            len: (int): Packet Length (including the IPX header / 2 bytes).
            tc: (int): Transport Control (hop count / 1 byte).
            pt: (int): Packet Type (1 byte).
            dst: (bytes): Destination address (12 bytes).
            src: (bytes): Source address (12 bytes).
    """

    __hdr__ = (
        ('sum', 'H', 0xffff),
        ('len', 'H', IPX_HDR_LEN),
        ('tc', 'B', 0),
        ('pt', 'B', 0),
        ('dst', '12s', b''),
        ('src', '12s', b'')
    )
