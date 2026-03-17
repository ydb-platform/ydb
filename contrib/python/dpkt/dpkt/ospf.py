# $Id: ospf.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Open Shortest Path First."""
from __future__ import absolute_import

from . import dpkt

AUTH_NONE = 0
AUTH_PASSWORD = 1
AUTH_CRYPTO = 2


class OSPF(dpkt.Packet):
    """Open Shortest Path First.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of OSPF.
        TODO.
    """

    __hdr__ = (
        ('v', 'B', 0),
        ('type', 'B', 0),
        ('len', 'H', 0),
        ('router', 'I', 0),
        ('area', 'I', 0),
        ('sum', 'H', 0),
        ('atype', 'H', 0),
        ('auth', '8s', b'')
    )

    def __bytes__(self):
        if not self.sum:
            self.sum = dpkt.in_cksum(dpkt.Packet.__bytes__(self))
        return dpkt.Packet.__bytes__(self)


def test_creation():
    ospf = OSPF()
    assert ospf.v == 0
    assert ospf.type == 0
    assert ospf.len == 0
    assert ospf.router == 0
    assert ospf.area == 0
    assert ospf.sum == 0
    assert ospf.atype == 0
    assert ospf.auth == b''

    # sum is 0, so it will be recalculated
    assert bytes(ospf) == b''.join([
        b'\x00' * 12,
        b'\xff\xff',
        b'\x00' * 10
    ])

    ospf.sum = 0x1234
    # sum is not 0, so it will be used
    assert bytes(ospf) == b''.join([
        b'\x00' * 12,
        b'\x12\x34',
        b'\x00' * 10
    ])
