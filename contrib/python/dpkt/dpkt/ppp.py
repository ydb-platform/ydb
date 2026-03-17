# $Id: ppp.py 65 2010-03-26 02:53:51Z dugsong $
# -*- coding: utf-8 -*-
"""Point-to-Point Protocol."""
from __future__ import absolute_import

import struct

from . import dpkt

# XXX - finish later

# http://www.iana.org/assignments/ppp-numbers
PPP_IP = 0x21  # Internet Protocol
PPP_IP6 = 0x57  # Internet Protocol v6

# Protocol field compression
PFC_BIT = 0x01


class PPP(dpkt.Packet):
    """Point-to-Point Protocol.

    Point-to-Point Protocol (PPP) is a data link layer (layer 2) communication protocol between two routers directly
    without any host or any other networking in between. It can provide connection authentication, transmission
    encryption and data compression.

    Note: This class is subclassed in PPPoE

    Attributes:
        __hdr__: Header fields of PPP.
            addr: (int): Address. 0xFF, standard broadcast address. (1 byte)
            cntrl: (int): Control. 0x03, unnumbered data. (1 byte)
            p: (int): Protocol. PPP ID of embedded data. (1 byte)
    """

    __hdr__ = (
        ('addr', 'B', 0xff),
        ('cntrl', 'B', 3),
        ('p', 'B', PPP_IP),
    )
    _protosw = {}

    @classmethod
    def set_p(cls, p, pktclass):
        cls._protosw[p] = pktclass

    @classmethod
    def get_p(cls, p):
        return cls._protosw[p]

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        if self.p & PFC_BIT == 0:
            try:
                self.p = struct.unpack('>H', buf[2:4])[0]
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
            if self.p > 0xff:
                return struct.pack('>BBH', self.addr, self.cntrl, self.p)
            return dpkt.Packet.pack_hdr(self)
        except struct.error as e:
            raise dpkt.PackError(str(e))


def __load_protos():
    g = globals()
    for k, v in g.items():
        if k.startswith('PPP_'):
            name = k[4:]
            modname = name.lower()
            try:
                mod = __import__(modname, g, level=1)
                PPP.set_p(v, getattr(mod, name))
            except (ImportError, AttributeError):
                continue


def _mod_init():
    """Post-initialization called when all dpkt modules are fully loaded"""
    if not PPP._protosw:
        __load_protos()


def test_ppp():
    # Test protocol compression
    s = b"\xff\x03\x21"
    p = PPP(s)
    assert p.p == 0x21

    s = b"\xff\x03\x00\x21"
    p = PPP(s)
    assert p.p == 0x21


def test_ppp_short():
    s = b"\xff\x03\x00"

    import pytest
    pytest.raises(dpkt.NeedData, PPP, s)


def test_packing():
    p = PPP()
    assert p.pack_hdr() == b"\xff\x03\x21"

    p.p = 0xc021  # LCP
    assert p.pack_hdr() == b"\xff\x03\xc0\x21"


def test_ppp_classmethods():
    import pytest

    class TestProto(dpkt.Packet):
        pass

    proto_number = 123

    # asserting that this proto is not currently added
    with pytest.raises(KeyError):
        PPP.get_p(proto_number)

    PPP.set_p(proto_number, TestProto)

    assert PPP.get_p(proto_number) == TestProto

    # we need to reset the class, or impact other tests
    del PPP._protosw[proto_number]


def test_unpacking_exceptions():
    from dpkt import ip

    from binascii import unhexlify
    buf_ppp = unhexlify(
        'ff'  # addr
        '03'  # cntrl
        '21'  # p (PPP_IP)
    )
    buf_ip = unhexlify(
        '45'    # _v_hl
        '00'    # tos
        '0014'  # len
        '0000'  # id
        '0000'  # off
        '80'    # ttl
        '06'    # p
        'd47e'  # sum
        '11111111'  # src
        '22222222'  # dst
    )

    buf = buf_ppp + buf_ip
    ppp = PPP(buf)
    assert hasattr(ppp, 'ip')
    assert isinstance(ppp.data, ip.IP)
    assert bytes(ppp) == buf


def test_ppp_packing_error():
    import pytest

    # addr is a 1-byte field, so this will overflow when packing
    ppp = PPP(p=257, addr=1234)
    with pytest.raises(dpkt.PackError):
        ppp.pack_hdr()


def test_proto_loading():
    # test that failure to load protocol handlers isn't catastrophic
    standard_protos = PPP._protosw
    # delete existing protos
    PPP._protosw = {}
    assert not PPP._protosw

    # inject a new global variable to be picked up
    globals()['PPP_NON_EXISTENT_PROTO'] = "FAIL"
    _mod_init()
    # we should get the same answer as if NON_EXISTENT_PROTO didn't exist
    assert PPP._protosw == standard_protos
