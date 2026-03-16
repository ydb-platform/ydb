# $Id: tpkt.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""ISO Transport Service on top of the TCP (TPKT)."""
from __future__ import absolute_import

from . import dpkt

# TPKT - RFC 1006 Section 6
# http://www.faqs.org/rfcs/rfc1006.html


class TPKT(dpkt.Packet):
    """ISO Transport Service on top of the TCP (TPKT).

    "Emulate" ISO transport services COTP on top of TCP. The two major points missing in TCP (compared to COTP)
    are the TSAP addressing and the detection of packet boundaries on the receiving host.

    Attributes:
        __hdr__: Header fields of TPKT.
            v: (int): Version (1 byte)
            rsvd: (int): Reserved (1 byte)
            len: (int): Packet Length (2 bytes)
    """

    __hdr__ = (
        ('v', 'B', 3),
        ('rsvd', 'B', 0),
        ('len', 'H', 0)
    )
