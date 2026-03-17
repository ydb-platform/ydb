# -*- coding: utf-8 -*-
"""Linux libpcap "cooked v2" capture encapsulation."""
from __future__ import absolute_import

from . import arp
from . import dpkt
from . import ethernet


class SLL2(dpkt.Packet):
    """Linux libpcap "cooked v2" capture encapsulation.

    See https://www.tcpdump.org/linktypes/LINKTYPE_LINUX_SLL2.html

    Attributes:
        __hdr__: Header fields of SLLv2.
    """

    __hdr__ = (
        ('ethtype', 'H', ethernet.ETH_TYPE_IP),
        ('mbz', 'H', 0),  # reserved
        ('intindex', 'i', 0),  # the 1-based index of the interface on which the packet was observed
        ('hrd', 'H', arp.ARP_HRD_ETH),
        ('type', 'B', 0),  # 0: to us, 1: bcast, 2: mcast, 3: other, 4: from us
        ('hlen', 'B', 6),  # hardware address length
        ('hdr', '8s', b''),  # first 8 bytes of link-layer header
    )
    _typesw = ethernet.Ethernet._typesw

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        try:
            self.data = self._typesw[self.ethtype](self.data)
            setattr(self, self.data.__class__.__name__.lower(), self.data)
        except (KeyError, dpkt.UnpackError):
            pass


def test_sll2():
    sll2data = (b'\x08\x00\x00\x00\x00\x00\x00\x03\x00\x01\x00\x06\x00\x0b\xdb\x52\x0e\x08\xf6\x7f'
                b'\x45\x00\x00\x34\xcc\x6c\x40\x00\x40\x06\x74\x08\x82\xd9\xfa\x8e\x82\xd9\xfa\x0d')
    sll2test = SLL2(sll2data)
    assert sll2test.type == 0
    assert sll2test.mbz == 0
    assert sll2test.intindex == 3
    assert sll2test.hrd == 1
    assert sll2test.hlen == 6
    assert sll2test.hdr == b'\x00\x0b\xdb\x52\x0e\x08\xf6\x7f'
    assert sll2test.ethtype == 0x0800

    # give invalid ethtype of 0x1234 to make sure error is handled
    sll2data2 = (b'\x12\x34\x00\x00\x00\x00\x00\x03\x00\x01\x00\x06\x00\x0b\xdb\x52\x0e\x08\xf6\x7f'
                b'\x45\x00\x00\x34\xcc\x6c\x40\x00\x40\x06\x74\x08\x82\xd9\xfa\x8e\x82\xd9\xfa\x0d')
    sll2test2 = SLL2(sll2data2)
