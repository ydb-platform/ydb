# $Id: mrt.py 29 2007-01-26 02:29:07Z jon.oberheide $
# -*- coding: utf-8 -*-
"""Multi-threaded Routing Toolkit."""
from __future__ import absolute_import

from . import dpkt
from . import bgp

# Multi-threaded Routing Toolkit
# http://www.ietf.org/internet-drafts/draft-ietf-grow-mrt-03.txt

# MRT Types
NULL = 0
START = 1
DIE = 2
I_AM_DEAD = 3
PEER_DOWN = 4
BGP = 5  # Deprecated by BGP4MP
RIP = 6
IDRP = 7
RIPNG = 8
BGP4PLUS = 9  # Deprecated by BGP4MP
BGP4PLUS_01 = 10  # Deprecated by BGP4MP
OSPF = 11
TABLE_DUMP = 12
BGP4MP = 16
BGP4MP_ET = 17
ISIS = 32
ISIS_ET = 33
OSPF_ET = 64

# BGP4MP Subtypes
BGP4MP_STATE_CHANGE = 0
BGP4MP_MESSAGE = 1
BGP4MP_ENTRY = 2
BGP4MP_SNAPSHOT = 3
BGP4MP_MESSAGE_32BIT_AS = 4

# Address Family Types
AFI_IPv4 = 1
AFI_IPv6 = 2


class MRTHeader(dpkt.Packet):
    __hdr__ = (
        ('ts', 'I', 0),
        ('type', 'H', 0),
        ('subtype', 'H', 0),
        ('len', 'I', 0)
    )


class TableDump(dpkt.Packet):
    __hdr__ = (
        ('view', 'H', 0),
        ('seq', 'H', 0),
        ('prefix', 'I', 0),
        ('prefix_len', 'B', 0),
        ('status', 'B', 1),
        ('originated_ts', 'I', 0),
        ('peer_ip', 'I', 0),
        ('peer_as', 'H', 0),
        ('attr_len', 'H', 0)
    )

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        plen = self.attr_len
        l_ = []
        while plen > 0:
            attr = bgp.BGP.Update.Attribute(self.data)
            self.data = self.data[len(attr):]
            plen -= len(attr)
            l_.append(attr)
        self.attributes = l_


class BGP4MPMessage(dpkt.Packet):
    __hdr__ = (
        ('src_as', 'H', 0),
        ('dst_as', 'H', 0),
        ('intf', 'H', 0),
        ('family', 'H', AFI_IPv4),
        ('src_ip', 'I', 0),
        ('dst_ip', 'I', 0)
    )


class BGP4MPMessage_32(dpkt.Packet):
    __hdr__ = (
        ('src_as', 'I', 0),
        ('dst_as', 'I', 0),
        ('intf', 'H', 0),
        ('family', 'H', AFI_IPv4),
        ('src_ip', 'I', 0),
        ('dst_ip', 'I', 0)
    )


def test_tabledump():
    from binascii import unhexlify
    buf_tabledump = unhexlify(
        '0001'           # view
        '0002'           # seq
        '00000003'       # prefix
        '04'             # prefix_len
        '05'             # status
        '00000006'       # originated_ts
        '00000007'       # peer_ip
        '0008'           # peer_as

        '0002'           # attr_len
    )
    buf_attrs = unhexlify(
        '01'  # flags
        '01'  # type (ORIGIN)

        '01'  # length
        '02'  # Origin.type (INCOMPLETE)
    )
    buf = buf_tabledump + buf_attrs
    table_dump = TableDump(buf)
    assert table_dump.view == 1
    assert table_dump.seq == 2
    assert table_dump.prefix == 3
    assert table_dump.prefix_len == 4
    assert table_dump.status == 5
    assert table_dump.originated_ts == 6
    assert table_dump.peer_ip == 7
    assert table_dump.peer_as == 8
    assert table_dump.attr_len == 2

    assert len(table_dump.attributes) == 1
    attr = table_dump.attributes[0]
    assert isinstance(attr, bgp.BGP.Update.Attribute)
    assert isinstance(attr.data, bgp.BGP.Update.Attribute.Origin)
