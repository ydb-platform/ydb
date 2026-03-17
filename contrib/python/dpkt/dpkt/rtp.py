# $Id: rtp.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Real-Time Transport Protocol."""
from __future__ import absolute_import

from .dpkt import Packet

# version 1100 0000 0000 0000 ! 0xC000  14
# p       0010 0000 0000 0000 ! 0x2000  13
# x       0001 0000 0000 0000 ! 0x1000  12
# cc      0000 1111 0000 0000 ! 0x0F00   8
# m       0000 0000 1000 0000 ! 0x0080   7
# pt      0000 0000 0111 1111 ! 0x007F   0
#

_VERSION_MASK = 0xC000
_P_MASK = 0x2000
_X_MASK = 0x1000
_CC_MASK = 0x0F00
_M_MASK = 0x0080
_PT_MASK = 0x007F
_VERSION_SHIFT = 14
_P_SHIFT = 13
_X_SHIFT = 12
_CC_SHIFT = 8
_M_SHIFT = 7
_PT_SHIFT = 0

VERSION = 2


class RTP(Packet):
    """Real-Time Transport Protocol.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of RTP.
        TODO.
    """

    __hdr__ = (
        ('_type', 'H', 0x8000),
        ('seq', 'H', 0),
        ('ts', 'I', 0),
        ('ssrc', 'I', 0),
    )
    csrc = b''

    @property
    def version(self):
        return (self._type & _VERSION_MASK) >> _VERSION_SHIFT

    @version.setter
    def version(self, ver):
        self._type = (ver << _VERSION_SHIFT) | (self._type & ~_VERSION_MASK)

    @property
    def p(self):
        return (self._type & _P_MASK) >> _P_SHIFT

    @p.setter
    def p(self, p):
        self._type = (p << _P_SHIFT) | (self._type & ~_P_MASK)

    @property
    def x(self):
        return (self._type & _X_MASK) >> _X_SHIFT

    @x.setter
    def x(self, x):
        self._type = (x << _X_SHIFT) | (self._type & ~_X_MASK)

    @property
    def cc(self):
        return (self._type & _CC_MASK) >> _CC_SHIFT

    @cc.setter
    def cc(self, cc):
        self._type = (cc << _CC_SHIFT) | (self._type & ~_CC_MASK)

    @property
    def m(self):
        return (self._type & _M_MASK) >> _M_SHIFT

    @m.setter
    def m(self, m):
        self._type = (m << _M_SHIFT) | (self._type & ~_M_MASK)

    @property
    def pt(self):
        return (self._type & _PT_MASK) >> _PT_SHIFT

    @pt.setter
    def pt(self, m):
        self._type = (m << _PT_SHIFT) | (self._type & ~_PT_MASK)

    def __len__(self):
        return self.__hdr_len__ + len(self.csrc) + len(self.data)

    def __bytes__(self):
        return self.pack_hdr() + self.csrc + bytes(self.data)

    def unpack(self, buf):
        super(RTP, self).unpack(buf)
        self.csrc = buf[self.__hdr_len__:self.__hdr_len__ + self.cc * 4]
        self.data = buf[self.__hdr_len__ + self.cc * 4:]


def test_rtp():
    rtp = RTP(
        b'\x80\x08\x4d\x01\x00\x01\x00\xe0\x34\x3f\xfa\x34\x53\x53\x53\x56\x53\x5d\x56\x57\xd5\xd6'
        b'\xd1\xde\xdf\xd3\xd9\xda\xdf\xdc\xdf\xd8\xdd\xd4\xdd\xd9\xd1\xd6\xdc\xda\xde\xdd\xc7\xc1'
        b'\xdf\xdf\xda\xdb\xdd\xdd\xc4\xd9\x55\x57\xd4\x50\x44\x44\x5b\x44\x4f\x4c\x47\x40\x4c\x47'
        b'\x59\x5b\x58\x5d\x56\x56\x53\x56\xd5\xd5\x54\x55\xd6\xd6\xd4\xd1\xd1\xd0\xd1\xd5\xdd\xd6'
        b'\x55\xd4\xd6\xd1\xd4\xd6\xd7\xd7\xd5\xd4\xd0\xd7\xd1\xd4\xd2\xdc\xd6\xdc\xdf\xdc\xdd\xd2'
        b'\xde\xdc\xd0\xdd\xdc\xd0\xd6\xd6\xd6\x55\x54\x55\x57\x57\x56\x50\x50\x5c\x5c\x52\x5d\x5d'
        b'\x5f\x5e\x5d\x5e\x52\x50\x52\x56\x54\x57\x55\x55\xd4\xd7\x55\xd5\x55\x55\x55\x55\x55\x54'
        b'\x57\x54\x55\x55\xd5\xd5\xd7\xd6\xd7\xd1\xd1\xd3\xd2\xd3\xd2\xd2\xd3\xd3'
    )
    assert (rtp.version == 2)
    assert (rtp.p == 0)
    assert (rtp.x == 0)
    assert (rtp.cc == 0)
    assert (rtp.m == 0)
    assert (rtp.pt == 8)
    assert (rtp.seq == 19713)
    assert (rtp.ts == 65760)
    assert (rtp.ssrc == 0x343ffa34)
    assert (len(rtp) == 172)
    assert (bytes(rtp) == (
        b'\x80\x08\x4d\x01\x00\x01\x00\xe0\x34\x3f\xfa\x34\x53\x53\x53\x56\x53\x5d\x56\x57\xd5\xd6'
        b'\xd1\xde\xdf\xd3\xd9\xda\xdf\xdc\xdf\xd8\xdd\xd4\xdd\xd9\xd1\xd6\xdc\xda\xde\xdd\xc7\xc1'
        b'\xdf\xdf\xda\xdb\xdd\xdd\xc4\xd9\x55\x57\xd4\x50\x44\x44\x5b\x44\x4f\x4c\x47\x40\x4c\x47'
        b'\x59\x5b\x58\x5d\x56\x56\x53\x56\xd5\xd5\x54\x55\xd6\xd6\xd4\xd1\xd1\xd0\xd1\xd5\xdd\xd6'
        b'\x55\xd4\xd6\xd1\xd4\xd6\xd7\xd7\xd5\xd4\xd0\xd7\xd1\xd4\xd2\xdc\xd6\xdc\xdf\xdc\xdd\xd2'
        b'\xde\xdc\xd0\xdd\xdc\xd0\xd6\xd6\xd6\x55\x54\x55\x57\x57\x56\x50\x50\x5c\x5c\x52\x5d\x5d'
        b'\x5f\x5e\x5d\x5e\x52\x50\x52\x56\x54\x57\x55\x55\xd4\xd7\x55\xd5\x55\x55\x55\x55\x55\x54'
        b'\x57\x54\x55\x55\xd5\xd5\xd7\xd6\xd7\xd1\xd1\xd3\xd2\xd3\xd2\xd2\xd3\xd3'
    ))

    # the following tests RTP header setters
    rtp = RTP()
    rtp.m = 1
    rtp.pt = 3
    rtp.seq = 1234
    rtp.ts = 5678
    rtp.ssrc = 0xabcdef01
    assert (rtp.m == 1)
    assert (rtp.pt == 3)
    assert (rtp.seq == 1234)
    assert (rtp.ts == 5678)
    assert (rtp.ssrc == 0xabcdef01)


def test_rtp_properties():
    from .compat import compat_izip

    rtp = RTP()
    properties = ['version', 'p', 'x', 'cc', 'm', 'pt']
    defaults = [2, 0, 0, 0, 0, 0]
    for prop, default in compat_izip(properties, defaults):
        assert hasattr(rtp, prop)
        assert getattr(rtp, prop) == default
        setattr(rtp, prop, 1)
        assert getattr(rtp, prop) == 1
