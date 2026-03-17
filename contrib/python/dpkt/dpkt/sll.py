# $Id: sll.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Linux libpcap "cooked" capture encapsulation."""
from __future__ import absolute_import

from . import arp
from . import dpkt
from . import ethernet


class SLL(dpkt.Packet):
    """Linux libpcap "cooked" capture encapsulation.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of SLL.
        TODO.
    """

    __hdr__ = (
        ('type', 'H', 0),  # 0: to us, 1: bcast, 2: mcast, 3: other, 4: from us
        ('hrd', 'H', arp.ARP_HRD_ETH),
        ('hlen', 'H', 6),  # hardware address length
        ('hdr', '8s', b''),  # first 8 bytes of link-layer header
        ('ethtype', 'H', ethernet.ETH_TYPE_IP),
    )
    _typesw = ethernet.Ethernet._typesw

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        try:
            self.data = self._typesw[self.ethtype](self.data)
            setattr(self, self.data.__class__.__name__.lower(), self.data)
        except (KeyError, dpkt.UnpackError):
            pass


def test_sll():
    slldata = (b'\x00\x00\x00\x01\x00\x06\x00\x0b\xdb\x52\x0e\x08\xf6\x7f\x08\x00\x45\x00\x00\x34'
               b'\xcc\x6c\x40\x00\x40\x06\x74\x08\x82\xd9\xfa\x8e\x82\xd9\xfa\x0d')
    slltest = SLL(slldata)
    assert slltest.type == 0
    assert slltest.hrd == 1
    assert slltest.hlen == 6
    assert slltest.hdr == b'\x00\x0b\xdb\x52\x0e\x08\xf6\x7f'
    assert slltest.ethtype == 0x0800

    # give invalid ethtype of 0x1234 to make sure error is caught
    slldata2 = (b'\x00\x00\x00\x01\x00\x06\x00\x0b\xdb\x52\x0e\x08\xf6\x7f\x12\x34\x45\x00\x00\x34'
                b'\xcc\x6c\x40\x00\x40\x06\x74\x08\x82\xd9\xfa\x8e\x82\xd9\xfa\x0d')
    slltest = SLL(slldata2)
