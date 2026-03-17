# $Id: vrrp.py 88 2013-03-05 19:43:17Z andrewflnr@gmail.com $
# -*- coding: utf-8 -*-
"""Virtual Router Redundancy Protocol."""
from __future__ import print_function
from __future__ import absolute_import

from . import dpkt


class VRRP(dpkt.Packet):
    """Virtual Router Redundancy Protocol.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of VRRP.
        TODO.
    """

    __hdr__ = (
        ('_v_type', 'B', 0x21),
        ('vrid', 'B', 0),
        ('priority', 'B', 0),
        ('count', 'B', 0),
        ('atype', 'B', 0),
        ('advtime', 'B', 0),
        ('sum', 'H', 0),
    )
    __bit_fields__ = {
        '_v_type': (
            ('v', 4),
            ('type', 4),
        )
    }

    addrs = ()
    auth = ''

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        l_ = []
        off = 0
        for off in range(0, 4 * self.count, 4):
            l_.append(self.data[off:off + 4])
        self.addrs = l_
        self.auth = self.data[off + 4:]
        self.data = ''

    def __len__(self):
        return self.__hdr_len__ + (4 * self.count) + len(self.auth)

    def __bytes__(self):
        data = b''.join(self.addrs) + self.auth
        if not self.sum:
            self.sum = dpkt.in_cksum(self.pack_hdr() + data)
        return self.pack_hdr() + data


def test_vrrp():
    # no addresses
    s = b'\x00\x00\x00\x00\x00\x00\xff\xff'
    v = VRRP(s)
    assert v.sum == 0xffff
    assert bytes(v) == s

    # have address
    s = b'\x21\x01\x64\x01\x00\x01\xba\x52\xc0\xa8\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00'
    v = VRRP(s)
    assert v.count == 1
    assert v.addrs == [b'\xc0\xa8\x00\x01']  # 192.168.0.1
    assert bytes(v) == s

    # test checksum generation
    v.sum = 0
    assert bytes(v) == s

    # test length
    assert len(v) == len(s)

    # test getters
    assert v.v == 2
    assert v.type == 1

    # test setters
    v.v = 3
    v.type = 2
    assert bytes(v)[0] == b'\x32'[0]
