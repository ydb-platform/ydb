# $Id: dtp.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Dynamic Trunking Protocol."""
from __future__ import absolute_import
import struct

from . import dpkt

TRUNK_NAME = 0x01
MAC_ADDR = 0x04


class DTP(dpkt.Packet):
    """Dynamic Trunking Protocol.

    The Dynamic Trunking Protocol (DTP) is a proprietary networking protocol developed by Cisco Systems for the purpose
    of negotiating trunking on a link between two VLAN-aware switches, and for negotiating the type of trunking
    encapsulation to be used. It works on Layer 2 of the OSI model. VLAN trunks formed using DTP may utilize either
    IEEE 802.1Q or Cisco ISL trunking protocols.

    Attributes:
        __hdr__: Header fields of DTP.
            v: (int) Version. (1 byte)
    """

    __hdr__ = (
        ('v', 'B', 0),
    )  # rest is TLVs

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        buf = self.data
        tvs = []
        while buf:
            t, l_ = struct.unpack('>HH', buf[:4])
            v, buf = buf[4:4 + l_], buf[4 + l_:]
            tvs.append((t, v))
        self.data = tvs

    def __bytes__(self):
        return b''.join([struct.pack('>HH', t, len(v)) + v for t, v in self.data])


def test_creation():
    dtp1 = DTP()
    assert dtp1.v == 0

    from binascii import unhexlify
    buf = unhexlify(
        '04'
        '0001'  # type
        '0002'  # length
        '1234'  # value
    )

    dtp2 = DTP(buf)
    assert dtp2.v == 4
    assert len(dtp2.data) == 1
    tlvs = dtp2.data
    tlv = tlvs[0]
    key, value = tlv
    assert key == 1
    assert value == unhexlify('1234')

    assert bytes(dtp2) == buf[1:]
