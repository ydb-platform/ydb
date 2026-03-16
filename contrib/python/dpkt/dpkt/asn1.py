# $Id: asn1.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Abstract Syntax Notation #1."""
from __future__ import absolute_import
from __future__ import print_function

import struct
from calendar import timegm

from . import dpkt
from .compat import compat_ord

# Type class
CLASSMASK = 0xc0
UNIVERSAL = 0x00
APPLICATION = 0x40
CONTEXT = 0x80
PRIVATE = 0xc0

# Constructed (vs. primitive)
CONSTRUCTED = 0x20

# Universal-class tags
TAGMASK = 0x1f
INTEGER = 2
BIT_STRING = 3  # arbitrary bit string
OCTET_STRING = 4  # arbitrary octet string
NULL = 5
OID = 6  # object identifier
SEQUENCE = 16  # ordered collection of types
SET = 17  # unordered collection of types
PRINT_STRING = 19  # printable string
T61_STRING = 20  # T.61 (8-bit) character string
IA5_STRING = 22  # ASCII
UTC_TIME = 23


def utctime(buf):
    """Convert ASN.1 UTCTime string to UTC float.

    TODO: Long description here.

    Args:
        buf: A buffer with format "yymnddhhmm"

    Returns:
        A floating point number, indicates seconds since the Epoch.
    """
    yy = int(buf[:2])
    mn = int(buf[2:4])
    dd = int(buf[4:6])
    hh = int(buf[6:8])
    mm = int(buf[8:10])
    try:
        ss = int(buf[10:12])
        buf = buf[12:]
    except TypeError:
        ss = 0
        buf = buf[10:]

    if buf[0] == '+':
        hh -= int(buf[1:3])
        mm -= int(buf[3:5])
    elif buf[0] == '-':
        hh += int(buf[1:3])
        mm += int(buf[3:5])
    return timegm((2000 + yy, mn, dd, hh, mm, ss, 0, 0, 0))


def decode(buf):
    """Sleazy ASN.1 decoder.

    TODO: Long description here.

    Args:
        buf: A buffer with Sleazy ASN.1 data.

    Returns:
        A list of (id, value) tuples from ASN.1 BER/DER encoded buffer.

    Raises:
        UnpackError: An error occurred the ASN.1 length exceed.
    """
    msg = []
    while buf:
        t = compat_ord(buf[0])
        constructed = t & CONSTRUCTED
        tag = t & TAGMASK
        l_ = compat_ord(buf[1])
        c = 0
        if constructed and l_ == 128:
            # XXX - constructed, indefinite length
            msg.append((t, decode(buf[2:])))
        elif l_ >= 128:
            c = l_ & 127
            if c == 1:
                l_ = compat_ord(buf[2])
            elif c == 2:
                l_ = struct.unpack('>H', buf[2:4])[0]
            elif c == 3:
                l_ = struct.unpack('>I', buf[1:5])[0] & 0xfff
                c = 2
            elif c == 4:
                l_ = struct.unpack('>I', buf[2:6])[0]
            else:
                # XXX - can be up to 127 bytes, but...
                raise dpkt.UnpackError('excessive long-form ASN.1 length %d' % l_)

        # Skip type, length
        buf = buf[2 + c:]

        # Parse content
        if constructed:
            msg.append((t, decode(buf)))
        elif tag == INTEGER:
            if l_ == 0:
                n = 0
            elif l_ == 1:
                n = compat_ord(buf[0])
            elif l_ == 2:
                n = struct.unpack('>H', buf[:2])[0]
            elif l_ == 3:
                n = struct.unpack('>I', buf[:4])[0] >> 8
            elif l_ == 4:
                n = struct.unpack('>I', buf[:4])[0]
            else:
                raise dpkt.UnpackError('excessive integer length > %d bytes' % l_)
            msg.append((t, n))
        elif tag == UTC_TIME:
            msg.append((t, utctime(buf[:l_])))
        else:
            msg.append((t, buf[:l_]))

        # Skip content
        buf = buf[l_:]
    return msg


def test_asn1():
    s = (
        b'0\x82\x02Q\x02\x01\x0bc\x82\x02J\x04xcn=Douglas J Song 1, ou=Information Technology Division,'
        b' ou=Faculty and Staff, ou=People, o=University of Michigan, c=US\n\x01\x00\n\x01\x03\x02\x01'
        b'\x00\x02\x01\x00\x01\x01\x00\x87\x0bobjectclass0\x82\x01\xb0\x04\rmemberOfGroup\x04\x03acl'
        b'\x04\x02cn\x04\x05title\x04\rpostalAddress\x04\x0ftelephoneNumber\x04\x04mail\x04\x06member'
        b'\x04\thomePhone\x04\x11homePostalAddress\x04\x0bobjectClass\x04\x0bdescription\x04\x18'
        b'facsimileTelephoneNumber\x04\x05pager\x04\x03uid\x04\x0cuserPassword\x04\x08joinable\x04\x10'
        b'associatedDomain\x04\x05owner\x04\x0erfc822ErrorsTo\x04\x08ErrorsTo\x04\x10rfc822RequestsTo\x04\n'
        b'RequestsTo\x04\tmoderator\x04\nlabeledURL\x04\nonVacation\x04\x0fvacationMessage\x04\x05drink\x04\x0e'
        b'lastModifiedBy\x04\x10lastModifiedTime\x04\rmodifiersname\x04\x0fmodifytimestamp\x04\x0ccreatorsname'
        b'\x04\x0fcreatetimestamp'
    )
    assert decode(s) == [
        (48, [
            (2, 11),
            (99, [
                (4, (
                    b'cn=Douglas J Song 1, '
                    b'ou=Information Technology Division, '
                    b'ou=Faculty and Staff, '
                    b'ou=People, '
                    b'o=University of Michigan, '
                    b'c=US'
                )),
                (10, b'\x00'),
                (10, b'\x03'),
                (2, 0),
                (2, 0),
                (1, b'\x00'),
                (135, b'objectclass'),
                (48, [
                    (4, b'memberOfGroup'),
                    (4, b'acl'),
                    (4, b'cn'),
                    (4, b'title'),
                    (4, b'postalAddress'),
                    (4, b'telephoneNumber'),
                    (4, b'mail'),
                    (4, b'member'),
                    (4, b'homePhone'),
                    (4, b'homePostalAddress'),
                    (4, b'objectClass'),
                    (4, b'description'),
                    (4, b'facsimileTelephoneNumber'),
                    (4, b'pager'),
                    (4, b'uid'),
                    (4, b'userPassword'),
                    (4, b'joinable'),
                    (4, b'associatedDomain'),
                    (4, b'owner'),
                    (4, b'rfc822ErrorsTo'),
                    (4, b'ErrorsTo'),
                    (4, b'rfc822RequestsTo'),
                    (4, b'RequestsTo'),
                    (4, b'moderator'),
                    (4, b'labeledURL'),
                    (4, b'onVacation'),
                    (4, b'vacationMessage'),
                    (4, b'drink'),
                    (4, b'lastModifiedBy'),
                    (4, b'lastModifiedTime'),
                    (4, b'modifiersname'),
                    (4, b'modifytimestamp'),
                    (4, b'creatorsname'),
                    (4, b'createtimestamp'),
                ])
            ])
        ])
    ]


def test_utctime():
    buf = (
        '201005'  # yymndd
        '012345'  # hhmmss
        '+1234'   # +hhmm
    )
    assert utctime(buf) == 1601815785.0

    buf = (
        '201005'  # yymndd
        '012345'  # hhmmss
        '-1234'   # -hhmm
    )
    assert utctime(buf) == 1601906265.0


def test_decode():
    import pytest
    from binascii import unhexlify
    buf = unhexlify(
        '20'   # CONSTRUCTED
        '80'   # 128 | 0
    )
    assert decode(buf) == [(32, []), (32, [])]

    # unpacking UTC_TIME
    buf = unhexlify(
        '17'  # t: code: UTC_TIME
        '81'  # l_: code: 128 | 1 (constructed
        '22'  # data len
        '3230313030353031323334352b30303030'

    )
    assert decode(buf) == [(23, 1601861025.0)]

    # unpacking 2-byte size; zero-length integer
    buf = unhexlify(
        '02'    # t: INTEGER
        '82'    # l_: 128 | 2
        '0000'  # new l_
    )
    assert decode(buf) == [(2, 0)]

    # unpacking 3-byte size
    buf = unhexlify(
        '02'        # t: INTEGER
        '83'        # l_: 128 | 3
        '000001'    # new l_
    )
    assert decode(buf) == [(2, 1)]

    # unpacking 4-byte size
    buf = unhexlify(
        '02'        # t: INTEGER
        '84'        # l_: 128 | 4
        '00000002'  # new l_
        'abcd'
    )
    assert decode(buf) == [(2, 43981)]

    # unpacking 4-byte size
    buf = unhexlify(
        '02'        # t: INTEGER
        '85'        # l_: 128 | 5
    )
    with pytest.raises(dpkt.UnpackError, match="excessive long-form ASN.1 length 133"):
        decode(buf)

    # unpacking 1-byte size; 4-byte integer
    buf = unhexlify(
        '02'        # t: INTEGER
        '81'        # l_: 128 | 1
        '04'        # new l_
        '12345678'  # integer
    )
    assert decode(buf) == [(2, 305419896)]

    # unpacking 1-byte size; 4-byte integer
    buf = unhexlify(
        '02'        # t: INTEGER
        '81'        # l_: 128 | 1
        '05'        # new l_
    )
    with pytest.raises(dpkt.UnpackError, match="excessive integer length > 5 bytes"):
        decode(buf)

    # unpacking 1-byte size; 3-byte integer
    buf = unhexlify(
        '02'        # t: INTEGER
        '81'        # l_: 128 | 1
        '03'        # new l_
        '123456'    # integer

        '02'        # t: INTEGER
        '81'        # l_: 128 | 1
        '00'        # new l_
    )
    assert decode(buf) == [
        (2, 1193046),
        (2, 0),
    ]
