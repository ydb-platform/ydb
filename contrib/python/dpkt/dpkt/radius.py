# $Id: radius.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Remote Authentication Dial-In User Service."""
from __future__ import absolute_import

from . import dpkt
from .compat import compat_ord

# http://www.untruth.org/~josh/security/radius/radius-auth.html
# RFC 2865


class RADIUS(dpkt.Packet):
    """Remote Authentication Dial-In User Service.

    Remote Authentication Dial-In User Service (RADIUS) is a networking protocol that provides centralized
    authentication, authorization, and accounting (AAA) management for users who connect and use a network service.
    RADIUS was developed by Livingston Enterprises in 1991 as an access server authentication and accounting protocol.
    It was later brought into IEEE 802 and IETF standards.

    Attributes:
        __hdr__: Header fields of RADIUS.
            code: (int): Code. (1 byte)
            id: (int): ID (1 byte)
            len: (int): Length (2 bytes)
            auth: (int): Authentication (16 bytes)
    """

    __hdr__ = (
        ('code', 'B', 0),
        ('id', 'B', 0),
        ('len', 'H', 4),
        ('auth', '16s', b'')
    )
    attrs = b''

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        self.attrs = parse_attrs(self.data)
        self.data = b''


def parse_attrs(buf):
    """Parse attributes buffer into a list of (type, data) tuples."""
    attrs = []
    while buf:
        t = compat_ord(buf[0])
        l_ = compat_ord(buf[1])
        if l_ < 2:
            break
        d, buf = buf[2:l_], buf[l_:]
        attrs.append((t, d))
    return attrs


# Codes
RADIUS_ACCESS_REQUEST = 1
RADIUS_ACCESS_ACCEPT = 2
RADIUS_ACCESS_REJECT = 3
RADIUS_ACCT_REQUEST = 4
RADIUS_ACCT_RESPONSE = 5
RADIUS_ACCT_STATUS = 6
RADIUS_ACCESS_CHALLENGE = 11

# Attributes
RADIUS_USER_NAME = 1
RADIUS_USER_PASSWORD = 2
RADIUS_CHAP_PASSWORD = 3
RADIUS_NAS_IP_ADDR = 4
RADIUS_NAS_PORT = 5
RADIUS_SERVICE_TYPE = 6
RADIUS_FRAMED_PROTOCOL = 7
RADIUS_FRAMED_IP_ADDR = 8
RADIUS_FRAMED_IP_NETMASK = 9
RADIUS_FRAMED_ROUTING = 10
RADIUS_FILTER_ID = 11
RADIUS_FRAMED_MTU = 12
RADIUS_FRAMED_COMPRESSION = 13
RADIUS_LOGIN_IP_HOST = 14
RADIUS_LOGIN_SERVICE = 15
RADIUS_LOGIN_TCP_PORT = 16
# unassigned
RADIUS_REPLY_MESSAGE = 18
RADIUS_CALLBACK_NUMBER = 19
RADIUS_CALLBACK_ID = 20
# unassigned
RADIUS_FRAMED_ROUTE = 22
RADIUS_FRAMED_IPX_NETWORK = 23
RADIUS_STATE = 24
RADIUS_CLASS = 25
RADIUS_VENDOR_SPECIFIC = 26
RADIUS_SESSION_TIMEOUT = 27
RADIUS_IDLE_TIMEOUT = 28
RADIUS_TERMINATION_ACTION = 29
RADIUS_CALLED_STATION_ID = 30
RADIUS_CALLING_STATION_ID = 31
RADIUS_NAS_ID = 32
RADIUS_PROXY_STATE = 33
RADIUS_LOGIN_LAT_SERVICE = 34
RADIUS_LOGIN_LAT_NODE = 35
RADIUS_LOGIN_LAT_GROUP = 36
RADIUS_FRAMED_ATALK_LINK = 37
RADIUS_FRAMED_ATALK_NETWORK = 38
RADIUS_FRAMED_ATALK_ZONE = 39
# 40-59 reserved for accounting
RADIUS_CHAP_CHALLENGE = 60
RADIUS_NAS_PORT_TYPE = 61
RADIUS_PORT_LIMIT = 62
RADIUS_LOGIN_LAT_PORT = 63


def test_parse_attrs():
    from binascii import unhexlify
    buf = unhexlify(
        '01'        # type (RADIUS_USER_NAME)
        '06'        # end of attribute value
        '75736572'  # value ('user')

        '00'
        '00'
    )

    attrs = parse_attrs(buf)
    assert len(attrs) == 1

    type0, value0 = attrs[0]
    assert type0 == RADIUS_USER_NAME
    assert value0 == b'user'


def test_parse_multiple_attrs():
    from binascii import unhexlify
    buf = unhexlify(
        '01'                # type (RADIUS_USER_NAME)
        '06'                # end of attribute value
        '75736572'          # value ('user')

        '02'                # type (RADIUS_USER_PASSWORD)
        '0a'                # end of attribute value
        '70617373776f7264'  # value ('password')
    )

    attrs = parse_attrs(buf)
    assert len(attrs) == 2

    type0, value0 = attrs[0]
    assert type0 == RADIUS_USER_NAME
    assert value0 == b'user'

    type1, value1 = attrs[1]
    assert type1 == RADIUS_USER_PASSWORD
    assert value1 == b'password'


def test_radius_unpacking():
    from binascii import unhexlify
    buf_attrs = unhexlify(
        '01'                # type (RADIUS_USER_NAME)
        '06'                # end of attribute value
        '75736572'          # value ('user')
    )
    buf_radius_header = unhexlify(
        '01'                # code
        '34'                # id
        '1234'              # len
        '0123456789abcdef'  # auth
        '0123456789abcdef'  # auth
    )
    buf = buf_radius_header + buf_attrs
    radius = RADIUS(buf)
    assert len(radius.attrs) == 1
    name0, value0 = radius.attrs[0]
    assert name0 == 1
    assert value0 == b'user'
