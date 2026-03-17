# $Id: icmp6.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Internet Control Message Protocol for IPv6."""
from __future__ import absolute_import

from . import dpkt

ICMP6_DST_UNREACH = 1  # dest unreachable, codes:
ICMP6_PACKET_TOO_BIG = 2  # packet too big
ICMP6_TIME_EXCEEDED = 3  # time exceeded, code:
ICMP6_PARAM_PROB = 4  # ip6 header bad

ICMP6_ECHO_REQUEST = 128  # echo service
ICMP6_ECHO_REPLY = 129  # echo reply
MLD_LISTENER_QUERY = 130  # multicast listener query
MLD_LISTENER_REPORT = 131  # multicast listener report
MLD_LISTENER_DONE = 132  # multicast listener done

# RFC2292 decls
ICMP6_MEMBERSHIP_QUERY = 130  # group membership query
ICMP6_MEMBERSHIP_REPORT = 131  # group membership report
ICMP6_MEMBERSHIP_REDUCTION = 132  # group membership termination

ND_ROUTER_SOLICIT = 133  # router solicitation
ND_ROUTER_ADVERT = 134  # router advertisement
ND_NEIGHBOR_SOLICIT = 135  # neighbor solicitation
ND_NEIGHBOR_ADVERT = 136  # neighbor advertisement
ND_REDIRECT = 137  # redirect

ICMP6_ROUTER_RENUMBERING = 138  # router renumbering

ICMP6_WRUREQUEST = 139  # who are you request
ICMP6_WRUREPLY = 140  # who are you reply
ICMP6_FQDN_QUERY = 139  # FQDN query
ICMP6_FQDN_REPLY = 140  # FQDN reply
ICMP6_NI_QUERY = 139  # node information request
ICMP6_NI_REPLY = 140  # node information reply

ICMP6_MAXTYPE = 201


class ICMP6(dpkt.Packet):
    """Internet Control Message Protocol for IPv6.

    Internet Control Message Protocol version 6 (ICMPv6) is the implementation of the Internet Control Message Protocol
    (ICMP) for Internet Protocol version 6 (IPv6). ICMPv6 is an integral part of IPv6 and performs error reporting
    and diagnostic functions.

    Attributes:
        __hdr__: Header fields of ICMPv6.
            type: (int): Type. Control messages are identified by the value in the type field.  (1 byte)
            code: (int): Code. The code field gives additional context information for the message. (1 byte)
            sum: (int): Checksum. ICMPv6 provides a minimal level of message integrity verification. (2 bytes)
    """

    __hdr__ = (
        ('type', 'B', 0),
        ('code', 'B', 0),
        ('sum', 'H', 0)
    )

    class Error(dpkt.Packet):
        __hdr__ = (('pad', 'I', 0), )

        def unpack(self, buf):
            dpkt.Packet.unpack(self, buf)
            from . import ip6
            self.data = self.ip6 = ip6.IP6(self.data)

    class Unreach(Error):
        pass

    class TooBig(Error):
        __hdr__ = (('mtu', 'I', 1232), )

    class TimeExceed(Error):
        pass

    class ParamProb(Error):
        __hdr__ = (('ptr', 'I', 0), )

    class Echo(dpkt.Packet):
        __hdr__ = (('id', 'H', 0), ('seq', 'H', 0))

    _typesw = {1: Unreach, 2: TooBig, 3: TimeExceed, 4: ParamProb, 128: Echo, 129: Echo}

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        try:
            self.data = self._typesw[self.type](self.data)
            setattr(self, self.data.__class__.__name__.lower(), self.data)
        except (KeyError, dpkt.UnpackError):
            pass
