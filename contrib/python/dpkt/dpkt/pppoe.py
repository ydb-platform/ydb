# $Id: pppoe.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""PPP-over-Ethernet."""
from __future__ import absolute_import

import struct
import codecs

from . import dpkt
from . import ppp

# RFC 2516 codes
PPPoE_PADI = 0x09
PPPoE_PADO = 0x07
PPPoE_PADR = 0x19
PPPoE_PADS = 0x65
PPPoE_PADT = 0xA7
PPPoE_SESSION = 0x00


class PPPoE(dpkt.Packet):
    """PPP-over-Ethernet.

    The Point-to-Point Protocol over Ethernet (PPPoE) is a network protocol for encapsulating Point-to-Point Protocol
    (PPP) frames inside Ethernet frames. It appeared in 1999, in the context of the boom of DSL as the solution for
    tunneling packets over the DSL connection to the ISP's IP network, and from there to the rest of the Internet.

    Attributes:
        __hdr__: Header fields of PPPoE.
        _v_type:
            v: (int): Version (4 bits)
            type: (int): Type (4 bits)
        code: (int): Code. (1 byte)
        session: (int): Session ID. (2 bytes)
        len: (int): Payload length. (2 bytes)
    """

    __hdr__ = (
        ('_v_type', 'B', 0x11),
        ('code', 'B', 0),
        ('session', 'H', 0),
        ('len', 'H', 0)  # payload length
    )
    __bit_fields__ = {
        '_v_type': (
            ('v', 4),
            ('type', 4),
        )
    }

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        try:
            if self.code == 0:
                # We need to use the pppoe.PPP header here, because PPPoE
                # doesn't do the normal encapsulation.
                self.data = self.ppp = PPP(self.data)
        except dpkt.UnpackError:
            pass


class PPP(ppp.PPP):
    # Light version for protocols without the usual encapsulation, for PPPoE
    __hdr__ = (
        # Usuaully two-bytes, but while protocol compression is not recommended, it is supported
        ('p', 'B', ppp.PPP_IP),
    )

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        if self.p & ppp.PFC_BIT == 0:
            try:
                self.p = struct.unpack('>H', buf[:2])[0]
            except struct.error:
                raise dpkt.NeedData
            self.data = self.data[1:]
        try:
            self.data = self._protosw[self.p](self.data)
            setattr(self, self.data.__class__.__name__.lower(), self.data)
        except (KeyError, struct.error, dpkt.UnpackError):
            pass

    def pack_hdr(self):
        try:
            # Protocol compression is *not* recommended (RFC2516), but we do it anyway
            if self.p > 0xff:
                return struct.pack('>H', self.p)
            return dpkt.Packet.pack_hdr(self)
        except struct.error as e:
            raise dpkt.PackError(str(e))


def test_pppoe_discovery():
    s = ("11070000002801010000010300046413"
         "85180102000442524153010400103d0f"
         "0587062484f2df32b9ddfd77bd5b")
    s = codecs.decode(s, 'hex')
    p = PPPoE(s)

    assert p.code == PPPoE_PADO
    assert p.v == 1
    assert p.type == 1

    s = ("11190000002801010000010300046413"
         "85180102000442524153010400103d0f"
         "0587062484f2df32b9ddfd77bd5b")
    s = codecs.decode(s, 'hex')
    p = PPPoE(s)

    assert p.code == PPPoE_PADR

    assert p.pack_hdr() == s[:6]


def test_pppoe_session():
    s = "11000011000cc0210101000a050605fcd459"
    s = codecs.decode(s, 'hex')
    p = PPPoE(s)

    assert p.code == PPPoE_SESSION
    assert isinstance(p.ppp, PPP)
    assert p.data.p == 0xc021   # LCP
    assert len(p.data.data) == 10

    assert p.data.pack_hdr() == b"\xc0\x21"

    s = ("110000110066005760000000003c3a40fc000000000000000000000000000001"
         "fc0000000002010000000000000100018100bf291f9700010102030405060708"
         "090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728"
         "292a2b2c2d2e2f3031323334")
    s = codecs.decode(s, 'hex')
    p = PPPoE(s)
    assert p.code == PPPoE_SESSION
    assert isinstance(p.ppp, PPP)
    assert p.data.p == ppp.PPP_IP6
    assert p.data.data.p == 58   # ICMPv6

    assert p.ppp.pack_hdr() == b"\x57"


def test_ppp_packing():
    p = PPP()
    assert p.pack_hdr() == b"\x21"

    p.p = 0xc021   # LCP
    assert p.pack_hdr() == b"\xc0\x21"


def test_ppp_short():
    import pytest
    pytest.raises(dpkt.NeedData, PPP, b"\x00")


def test_pppoe_properties():
    pppoe = PPPoE()
    assert pppoe.v == 1
    pppoe.v = 7
    assert pppoe.v == 7

    assert pppoe.type == 1
    pppoe.type = 5
    assert pppoe.type == 5


def test_pppoe_unpack_error():
    from binascii import unhexlify
    buf = unhexlify(
        "11"    # v/type
        "00"    # code
        "0011"  # session
        "0066"  # len

        "00"    # data
    )
    # this initialization swallows the UnpackError raised
    pppoe = PPPoE(buf)
    # unparsed data is still available
    assert pppoe.data == b'\x00'


def test_ppp_pack_hdr():
    import pytest
    from binascii import unhexlify

    buf = unhexlify(
        '01'  # protocol, with compression bit set

        'ff'  # incomplete data
    )
    ppp = PPP(buf)
    ppp.p = 1234567
    with pytest.raises(dpkt.PackError):
        ppp.pack_hdr()


# XXX - TODO TLVs, etc.
