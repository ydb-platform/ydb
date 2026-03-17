# $Id: ah.py 34 2007-01-28 07:54:20Z dugsong $
# -*- coding: utf-8 -*-

"""Authentication Header."""
from __future__ import absolute_import

from . import dpkt
from . import ip


class AH(dpkt.Packet):
    """Authentication Header.

    The Authentication Header (AH) protocol provides data origin authentication, data integrity, and replay protection.

    Attributes:
        __hdr__: Header fields of AH.
        auth: Authentication body.
        data: Message data.
    """

    __hdr__ = (
        ('nxt', 'B', 0),
        ('len', 'B', 0),  # payload length
        ('rsvd', 'H', 0),
        ('spi', 'I', 0),
        ('seq', 'I', 0)
    )
    auth = b''

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        auth_len = max(4*self.len - 4, 0)  # see RFC 4302, section 2.2
        self.auth = self.data[:auth_len]
        buf = self.data[auth_len:]

        try:
            self.data = ip.IP.get_proto(self.nxt)(buf)
            setattr(self, self.data.__class__.__name__.lower(), self.data)
        except (KeyError, dpkt.UnpackError):
            self.data = buf

    def __len__(self):
        return self.__hdr_len__ + len(self.auth) + len(self.data)

    def __bytes__(self):
        return self.pack_hdr() + bytes(self.auth) + bytes(self.data)


def test_default_creation():
    ah = AH()
    assert ah.nxt == 0
    assert ah.len == 0
    assert ah.rsvd == 0
    assert ah.spi == 0
    assert ah.seq == 0
    assert len(ah) == ah.__hdr_len__
    assert bytes(ah) == b'\x00' * 12


def test_creation_from_buf():
    from binascii import unhexlify
    buf_ip = unhexlify(
        '04'  # IP
        '0000000000000000000000'
        '4500002200000000401172c001020304'
        '01020304006f00de000ebf35666f6f626172'
    )

    ah = AH(buf_ip)
    assert ah.nxt == 4  # IP
    assert isinstance(ah.data, ip.IP)
    assert len(ah) == 46
    assert bytes(ah) == buf_ip

    buf_not_ip = unhexlify(
        '37'  # Not registered
        '0000000000000000000000'
        '4500002200000000401172c001020304'
        '01020304006f00de000ebf35666f6f626172'
    )
    ah_not_ip = AH(buf_not_ip)
    assert ah_not_ip.nxt == 0x37
    assert isinstance(ah_not_ip.data, bytes)
    assert len(ah_not_ip) == 46
    assert bytes(ah_not_ip) == buf_not_ip
