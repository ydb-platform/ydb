# $Id: loopback.py 38 2007-03-17 03:33:16Z dugsong $
# -*- coding: utf-8 -*-
"""Platform-dependent loopback header."""

# https://wiki.wireshark.org/NullLoopback

from __future__ import absolute_import

from . import dpkt
from . import ethernet
from . import ip
from . import ip6


class Loopback(dpkt.Packet):
    """Platform-dependent loopback header.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of Loopback.
        TODO.
    """

    __hdr__ = (('family', 'I', 0), )
    __byte_order__ = '@'

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        if self.family in (0x02, 0x02000000):
            self.family = 2
            self.data = ip.IP(self.data)

        elif self.family in (0x18, 0x18000000):
            self.family = 24
            self.data = ip6.IP6(self.data)

        elif self.family in (0x1c, 0x1c000000):
            self.family = 28
            self.data = ip6.IP6(self.data)

        elif self.family in (0x1e, 0x1e000000):
            self.family = 30
            self.data = ip6.IP6(self.data)

        else:
            self.data = ethernet.Ethernet(self.data)


def test_ethernet_unpack():
    buf = b'\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x08\x00'
    hdr = b'\x00\x02\x00\x02'

    lo = Loopback(hdr + buf)
    assert lo.family in (0x02000200, 0x00020002)  # little endian, big endian
    assert isinstance(lo.data, ethernet.Ethernet)
    assert lo.data.src == b'\x07\x08\t\n\x0b\x0c'
    assert lo.data.dst == b'\x01\x02\x03\x04\x05\x06'


def test_ip_unpack():
    buf = b'E\x00\x004\xbd\x04@\x00@\x06\x7f\xbd\x7f\x00\x00\x02\x7f\x00\x00\x01'

    for hdr in (b'\x00\x00\x00\x02', b'\x02\x00\x00\x00'):
        lo = Loopback(hdr + buf)
        assert lo.family == 2
        assert isinstance(lo.data, ip.IP)
        assert lo.data.src == b'\x7f\x00\x00\x02'
        assert lo.data.dst == b'\x7f\x00\x00\x01'


def test_ip6_unpack():
    import struct
    buf = (b'\x60\x00\x00\x00\x00\x14\x06\x38\x26\x07\xf8\xb0\x40\x0c\x0c\x03\x00\x00\x00\x00\x00\x00'
           b'\x00\x1a\x20\x01\x04\x70\xe5\xbf\xde\xad\x49\x57\x21\x74\xe8\x2c\x48\x87')
    hdr_suffix = b'\x00' * 3

    for family in (24, 28, 30):
        hdr = struct.pack('B', family) + hdr_suffix

        lo = Loopback(hdr + buf)
        assert lo.family == family
        assert isinstance(lo.data, ip6.IP6)
        assert lo.data.src == b'&\x07\xf8\xb0@\x0c\x0c\x03\x00\x00\x00\x00\x00\x00\x00\x1a'
        assert lo.data.dst == b' \x01\x04p\xe5\xbf\xde\xadIW!t\xe8,H\x87'
