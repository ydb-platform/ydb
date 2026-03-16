# $Id: rip.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Routing Information Protocol."""
from __future__ import print_function
from __future__ import absolute_import

from . import dpkt

# RIP v2 - RFC 2453
# http://tools.ietf.org/html/rfc2453

REQUEST = 1
RESPONSE = 2


class RIP(dpkt.Packet):
    """Routing Information Protocol.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of RIP.
        TODO.
    """

    __hdr__ = (
        ('cmd', 'B', REQUEST),
        ('v', 'B', 2),
        ('rsvd', 'H', 0)
    )

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        l_ = []
        self.auth = None
        while self.data:
            rte = RTE(self.data[:20])
            if rte.family == 0xFFFF:
                self.auth = Auth(self.data[:20])
            else:
                l_.append(rte)
            self.data = self.data[20:]
        self.data = self.rtes = l_

    def __len__(self):
        n = self.__hdr_len__
        if self.auth:
            n += len(self.auth)
        n += sum(map(len, self.rtes))
        return n

    def __bytes__(self):
        auth = b''
        if self.auth:
            auth = bytes(self.auth)
        return self.pack_hdr() + auth + b''.join(map(bytes, self.rtes))


class RTE(dpkt.Packet):
    __hdr__ = (
        ('family', 'H', 2),
        ('route_tag', 'H', 0),
        ('addr', 'I', 0),
        ('subnet', 'I', 0),
        ('next_hop', 'I', 0),
        ('metric', 'I', 1)
    )


class Auth(dpkt.Packet):
    __hdr__ = (
        ('rsvd', 'H', 0xFFFF),
        ('type', 'H', 2),
        ('auth', '16s', 0)
    )


def test_creation_with_auth():
    from binascii import unhexlify

    buf_auth = unhexlify(
        'ffff'              # rsvd
        '0002'              # type
        '0123456789abcdef'  # auth
        '0123456789abcdef'  # auth
    )
    auth_direct = Auth(buf_auth)
    assert bytes(auth_direct) == buf_auth

    buf_rte = unhexlify(
        '0002'      # family
        '0000'      # route_tag
        '01020300'  # addr
        'ffffff00'  # subnet
        '00000000'  # next_hop
        '00000001'  # metric
    )

    rte = RTE(buf_rte)
    assert bytes(rte) == buf_rte

    buf_rip = unhexlify(
        '02'    # cmd
        '02'    # v
        '0000'  # rsvd
    )
    rip = RIP(buf_rip + buf_auth + buf_rte)

    assert rip.auth
    assert rip.auth.rsvd == 0xffff
    assert rip.auth.type == 2
    assert rip.auth.auth == unhexlify('0123456789abcdef') * 2

    assert len(rip.rtes) == 1

    rte = rip.rtes[0]
    assert rte.family == 2
    assert rte.route_tag == 0
    assert rte.metric == 1

    assert bytes(rip) == buf_rip + buf_auth + buf_rte
    assert len(rip) == len(buf_rip + buf_auth + buf_rte)
