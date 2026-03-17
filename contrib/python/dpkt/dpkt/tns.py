# $Id: tns.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Transparent Network Substrate."""
from __future__ import print_function
from __future__ import absolute_import

from . import dpkt


class TNS(dpkt.Packet):
    """Transparent Network Substrate.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of TNS.
        TODO.
    """

    __hdr__ = (
        ('length', 'H', 0),
        ('pktsum', 'H', 0),
        ('type', 'B', 0),
        ('rsvd', 'B', 0),
        ('hdrsum', 'H', 0),
        ('msg', '0s', b''),
    )

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        n = self.length - self.__hdr_len__
        if n > len(self.data):
            raise dpkt.NeedData('short message (missing %d bytes)' %
                                (n - len(self.data)))
        self.msg = self.data[:n]
        self.data = self.data[n:]


def test_tns():
    s = (b'\x00\x23\x00\x00\x01\x00\x00\x00\x01\x34\x01\x2c\x00\x00\x08\x00\x7f'
         b'\xff\x4f\x98\x00\x00\x00\x01\x00\x01\x00\x22\x00\x00\x00\x00\x01\x01X')
    t = TNS(s)
    assert t.msg.startswith(b'\x01\x34')

    # test a truncated packet
    try:
        t = TNS(s[:-10])
    except dpkt.NeedData:
        pass
