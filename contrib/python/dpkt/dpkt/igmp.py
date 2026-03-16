# $Id: igmp.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Internet Group Management Protocol."""
from __future__ import absolute_import

from . import dpkt


class IGMP(dpkt.Packet):
    """Internet Group Management Protocol.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of IGMP.
        TODO.
    """

    __hdr__ = (
        ('type', 'B', 0),
        ('maxresp', 'B', 0),
        ('sum', 'H', 0),
        ('group', '4s', b'\x00' * 4)
    )

    def __bytes__(self):
        if not self.sum:
            self.sum = dpkt.in_cksum(dpkt.Packet.__bytes__(self))
        return dpkt.Packet.__bytes__(self)


def test_construction_no_sum():
    igmp = IGMP()
    assert igmp.type == 0
    assert igmp.maxresp == 0
    assert igmp.sum == 0
    assert igmp.group == b'\x00' * 4

    assert bytes(igmp) == b'\x00\x00' + b'\xff\xff' + b'\x00' * 4


def test_construction_sum_set():
    igmp = IGMP(sum=1)
    assert igmp.type == 0
    assert igmp.maxresp == 0
    assert igmp.sum == 1
    assert igmp.group == b'\x00' * 4

    assert bytes(igmp) == b'\x00\x00\x00\x01' + b'\x00' * 4
