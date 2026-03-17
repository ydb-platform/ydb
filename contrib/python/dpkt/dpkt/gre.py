# $Id: gre.py 75 2010-08-03 14:42:19Z jon.oberheide $
# -*- coding: utf-8 -*-
"""Generic Routing Encapsulation."""
from __future__ import absolute_import

import struct
import codecs

from . import dpkt
from . import ethernet
from .compat import compat_izip

GRE_CP = 0x8000  # Checksum Present
GRE_RP = 0x4000  # Routing Present
GRE_KP = 0x2000  # Key Present
GRE_SP = 0x1000  # Sequence Present
GRE_SS = 0x0800  # Strict Source Route
GRE_AP = 0x0080  # Acknowledgment Present

GRE_opt_fields = (
    (GRE_CP | GRE_RP, 'sum', 'H'), (GRE_CP | GRE_RP, 'off', 'H'),
    (GRE_KP, 'key', 'I'), (GRE_SP, 'seq', 'I'), (GRE_AP, 'ack', 'I')
)


class GRE(dpkt.Packet):
    """Generic Routing Encapsulation.

    Generic Routing Encapsulation, or GRE, is a protocol for encapsulating data packets that use one routing protocol
    inside the packets of another protocol. "Encapsulating" means wrapping one data packet within another data packet,
    like putting a box inside another box. GRE is one way to set up a direct point-to-point connection across a network,
    for the purpose of simplifying connections between separate networks. It works with a variety of network layer
    protocols.

    Attributes:
        __hdr__: Header fields of GRE.
            flags: (int): Flag bits. (2 bytes)
            p: (int): Protocol Type (2 bytes)
    """

    __hdr__ = (
        ('flags', 'H', 0),
        ('p', 'H', 0x0800),  # ETH_TYPE_IP
    )
    sre = ()

    @property
    def v(self):
        return self.flags & 0x7

    @v.setter
    def v(self, v):
        self.flags = (self.flags & ~0x7) | (v & 0x7)

    @property
    def recur(self):
        """Recursion control bits. (3 bits)"""
        return (self.flags >> 5) & 0x7

    @recur.setter
    def recur(self, v):
        self.flags = (self.flags & ~0xe0) | ((v & 0x7) << 5)

    class SRE(dpkt.Packet):
        __hdr__ = [
            ('family', 'H', 0),
            ('off', 'B', 0),
            ('len', 'B', 0)
        ]

        def unpack(self, buf):
            dpkt.Packet.unpack(self, buf)
            self.data = self.data[:self.len]

    def opt_fields_fmts(self):
        if self.v == 0:
            fields, fmts = [], []
            opt_fields = GRE_opt_fields
        else:
            fields, fmts = ['len', 'callid'], ['H', 'H']
            opt_fields = GRE_opt_fields[-2:]
        for flags, field, fmt in opt_fields:
            if self.flags & flags:
                fields.append(field)
                fmts.append(fmt)
        return fields, fmts

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        fields, fmts = self.opt_fields_fmts()
        if fields:
            fmt = ''.join(fmts)
            fmtlen = struct.calcsize(fmt)
            vals = struct.unpack("!" + fmt, self.data[:fmtlen])
            self.data = self.data[fmtlen:]
            self.__dict__.update(dict(compat_izip(fields, vals)))
        if self.flags & GRE_RP:
            l_ = []
            while True:
                sre = self.SRE(self.data)
                self.data = self.data[len(sre):]
                l_.append(sre)
                if not sre.len:
                    break
            self.sre = l_
        try:
            self.data = ethernet.Ethernet._typesw[self.p](self.data)
            setattr(self, self.data.__class__.__name__.lower(), self.data)
        except (KeyError, dpkt.UnpackError):
            # data already set
            pass

    def __len__(self):
        opt_fmtlen = struct.calcsize(''.join(self.opt_fields_fmts()[1]))
        return self.__hdr_len__ + opt_fmtlen + sum(map(len, self.sre)) + len(self.data)

    def __bytes__(self):
        fields, fmts = self.opt_fields_fmts()
        if fields:
            vals = []
            for f in fields:
                vals.append(getattr(self, f))
            opt_s = struct.pack('!' + ''.join(fmts), *vals)
        else:
            opt_s = b''
        return self.pack_hdr() + opt_s + b''.join(map(bytes, self.sre)) + bytes(self.data)


def test_gre_v1():
    # Runs all the test associated with this class/file
    s = codecs.decode("3081880a0067178000068fb100083a76", 'hex') + b"A" * 103
    g = GRE(s)

    assert g.v == 1
    assert g.p == 0x880a
    assert g.seq == 430001
    assert g.ack == 539254
    assert g.callid == 6016
    assert g.len == 103
    assert g.data == b"A" * 103
    assert len(g) == len(s)

    s = codecs.decode("3001880a00b2001100083ab8", 'hex') + b"A" * 178
    g = GRE(s)

    assert g.v == 1
    assert g.p == 0x880a
    assert g.seq == 539320
    assert g.callid == 17
    assert g.len == 178
    assert g.data == b"A" * 178
    assert len(g) == len(s)


def test_gre_len():
    from binascii import unhexlify

    gre = GRE()
    assert len(gre) == 4

    buf = unhexlify("3081880a0067178000068fb100083a76") + b"\x41" * 103
    gre = GRE(buf)
    assert bytes(gre) == buf
    assert len(gre) == len(buf)


def test_gre_accessors():
    gre = GRE()
    for attr in ['v', 'recur']:
        print(attr)
        assert hasattr(gre, attr)
        assert getattr(gre, attr) == 0
        setattr(gre, attr, 1)
        assert getattr(gre, attr) == 1


def test_sre_creation():
    from binascii import unhexlify
    buf = unhexlify(
        '0000'  # family
        '00'    # off
        '02'    # len

        'ffff'
    )
    sre = GRE.SRE(buf)
    assert sre.data == b'\xff\xff'
    assert len(sre) == 6
    assert bytes(sre) == buf


def test_gre_nested_sre():
    from binascii import unhexlify
    buf = unhexlify(
        '4000'  # flags (GRE_RP)
        '0800'  # p (ETH_TYPE_IP)

        '0001'  # sum
        '0002'  # off

        # SRE entry
        '0003'  # family
        '04'    # off
        '02'    # len

        'ffff'

        # SRE entry (no len => last element)
        '0006'  # family
        '00'    # off
        '00'    # len
    )

    gre = GRE(buf)
    assert hasattr(gre, 'sre')
    assert isinstance(gre.sre, list)
    assert len(gre.sre) == 2
    assert len(gre) == len(buf)
    assert bytes(gre) == buf
    assert gre.data == b''


def test_gre_next_layer():
    from binascii import unhexlify

    from . import ipx

    buf = unhexlify(
        '0000'  # flags (NONE)
        '8137'  # p (ETH_TYPE_IPX)

        # IPX packet
        '0000'          # sum
        '0001'          # len
        '02'            # tc
        '03'            # pt
        '0102030405060708090a0b0c'  # dst
        'c0b0a0908070605040302010'  # src
    )
    gre = GRE(buf)
    assert hasattr(gre, 'ipx')
    assert isinstance(gre.data, ipx.IPX)
    assert gre.data.tc == 2
    assert gre.data.src == unhexlify('c0b0a0908070605040302010')
    assert gre.data.dst == unhexlify('0102030405060708090a0b0c')
    assert len(gre) == len(buf)
    assert bytes(gre) == buf
