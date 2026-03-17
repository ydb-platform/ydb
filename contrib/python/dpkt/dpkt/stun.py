# $Id: stun.py 47 2008-05-27 02:10:00Z jon.oberheide $
# -*- coding: utf-8 -*-
"""Simple Traversal of UDP through NAT."""
from __future__ import print_function
from __future__ import absolute_import

import struct

from . import dpkt

# STUN - RFC 3489
# http://tools.ietf.org/html/rfc3489
# Each packet has a 20 byte header followed by 0 or more attribute TLVs.

# Message Types
BINDING_REQUEST = 0x0001
BINDING_RESPONSE = 0x0101
BINDING_ERROR_RESPONSE = 0x0111
SHARED_SECRET_REQUEST = 0x0002
SHARED_SECRET_RESPONSE = 0x0102
SHARED_SECRET_ERROR_RESPONSE = 0x0112

# Message Attributes
MAPPED_ADDRESS = 0x0001
RESPONSE_ADDRESS = 0x0002
CHANGE_REQUEST = 0x0003
SOURCE_ADDRESS = 0x0004
CHANGED_ADDRESS = 0x0005
USERNAME = 0x0006
PASSWORD = 0x0007
MESSAGE_INTEGRITY = 0x0008
ERROR_CODE = 0x0009
UNKNOWN_ATTRIBUTES = 0x000a
REFLECTED_FROM = 0x000b


class STUN(dpkt.Packet):
    """Simple Traversal of UDP through NAT.

    STUN - RFC 3489
    http://tools.ietf.org/html/rfc3489
    Each packet has a 20 byte header followed by 0 or more attribute TLVs.

    Attributes:
        __hdr__: Header fields of STUN.
            type: (int): STUN Message Type (2 bytes)
            len: (int): Message Length (2 bytes)
            xid: (bytes): Magic Cookie and Transaction ID (16 bytes)
    """

    __hdr__ = (
        ('type', 'H', 0),
        ('len', 'H', 0),
        ('xid', '16s', 0)
    )


def tlv(buf):
    n = 4
    t, l_ = struct.unpack('>HH', buf[:n])
    v = buf[n:n + l_]
    pad = (n - l_ % n) % n
    buf = buf[n + l_ + pad:]
    return t, l_, v, buf


def parse_attrs(buf):
    """Parse STUN.data buffer into a list of (attribute, data) tuples."""
    attrs = []
    while buf:
        t, _, v, buf = tlv(buf)
        attrs.append((t, v))
    return attrs


def test_stun_response():
    s = (b'\x01\x01\x00\x0c\x21\x12\xa4\x42\x53\x4f\x70\x43\x69\x69\x35\x4a\x66\x63\x31\x7a\x00\x01'
         b'\x00\x08\x00\x01\x11\x22\x33\x44\x55\x66')
    m = STUN(s)
    assert m.type == BINDING_RESPONSE
    assert m.len == 12

    attrs = parse_attrs(m.data)
    assert attrs == [(MAPPED_ADDRESS, b'\x00\x01\x11\x22\x33\x44\x55\x66'), ]


def test_stun_padded():
    s = (b'\x00\x01\x00\x54\x21\x12\xa4\x42\x35\x59\x53\x6e\x42\x71\x70\x56\x77\x61\x39\x4f\x00\x06'
         b'\x00\x17\x70\x4c\x79\x5a\x48\x52\x3a\x47\x77\x4c\x33\x41\x48\x42\x6f\x76\x75\x62\x4c\x76'
         b'\x43\x71\x6e\x00\x80\x2a\x00\x08\x18\x8b\x10\x4c\x69\x7b\xf6\x5b\x00\x25\x00\x00\x00\x24'
         b'\x00\x04\x6e\x00\x1e\xff\x00\x08\x00\x14\x60\x2b\xc7\xfc\x0d\x10\x63\xaa\xc5\x38\x1c\xcb'
         b'\x96\xa9\x73\x08\x73\x9a\x96\x0c\x80\x28\x00\x04\xd1\x62\xea\x65')
    m = STUN(s)
    assert m.type == BINDING_REQUEST
    assert m.len == 84

    attrs = parse_attrs(m.data)
    assert len(attrs) == 6
    assert attrs[0] == (USERNAME, b'pLyZHR:GwL3AHBovubLvCqn')
    assert attrs[4][0] == MESSAGE_INTEGRITY
