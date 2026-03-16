# $Id: ip6.py 87 2013-03-05 19:41:04Z andrewflnr@gmail.com $
# -*- coding: utf-8 -*-
"""Internet Protocol, version 6."""
from __future__ import print_function
from __future__ import absolute_import

from . import dpkt
from . import ip
from . import tcp
from .compat import compat_ord
from .utils import inet_to_str
import struct

# The allowed extension headers and their classes (in order according to RFC).
EXT_HDRS = [ip.IP_PROTO_HOPOPTS, ip.IP_PROTO_ROUTING, ip.IP_PROTO_FRAGMENT, ip.IP_PROTO_AH, ip.IP_PROTO_ESP,
            ip.IP_PROTO_DSTOPTS]
# EXT_HDRS_CLS - classes is below - after all the used classes are defined.


class IP6(dpkt.Packet):
    """Internet Protocol, version 6.

    Internet Protocol version 6 (IPv6) is the most recent version of the Internet Protocol (IP),
    the communications protocol that provides an identification and location system for computers
    on networks and routes traffic across the Internet. IPv6 was developed by the Internet Engineering
    Task Force (IETF) to deal with the long-anticipated problem of IPv4 address exhaustion,
    and is intended to replace IPv4.

    Attributes:
        __hdr__: Header fields of IPv6.
            _v_fc_flow:
                v: (int): Version (4 bits),
                fc (int): Traffic Class (6+2 bits)
                flow (int): Flow Label (20 bits).
            plen: (int): Payload Length (2 bytes). The size of the payload in octets, including any extension headers.
            nxt: (bytes): Next Header (1 byte). Specifies the type of the next header.
            hlim: (bytes): Hop Limit (1 byte). Replaces the time to live field in IPv4.
            src: (int): Source Address (16 bytes). The unicast IPv6 address of the sending node.
            dst: (int): Destination Address (16 bytes). Unicast or multicast address of the destination node(s).
    """

    __hdr__ = (
        ('_v_fc_flow', 'I', 0x60000000),
        ('plen', 'H', 0),  # payload length (not including header)
        ('nxt', 'B', 0),  # next header protocol
        ('hlim', 'B', 0),  # hop limit
        ('src', '16s', b''),
        ('dst', '16s', b'')
    )
    __bit_fields__ = {
        '_v_fc_flow': (
            ('v', 4),      # version, 4 hi bits
            ('fc', 8),     # traffic class, 8 bits
            ('flow', 20),  # flow label, 20 lo bits
        )
    }
    __pprint_funcs__ = {
        'src': inet_to_str,
        'dst': inet_to_str
    }
    _protosw = ip.IP._protosw

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        self.extension_hdrs = {}

        # NOTE: self.extension_hdrs is not accurate, as it doesn't support duplicate header types.
        # According to RFC-1883 "Each extension header should occur at most once, except for the
        # Destination Options header which should occur at most twice".
        # Secondly, the .headers_str() method attempts to pack the extension headers in order as
        # defined in the RFC, however it doesn't adjust the next header (nxt) pointer accordingly.
        # Here we introduce the new field .all_extension_headers; it allows duplicate types and
        # keeps the original order.
        self.all_extension_headers = []

        if self.plen:
            buf = self.data[:self.plen]
        else:  # due to jumbo payload or TSO
            buf = self.data

        next_ext_hdr = self.nxt

        while next_ext_hdr in EXT_HDRS:
            ext = EXT_HDRS_CLS[next_ext_hdr](buf)
            self.extension_hdrs[next_ext_hdr] = ext
            self.all_extension_headers.append(ext)
            buf = buf[ext.length:]
            next_ext_hdr = getattr(ext, 'nxt', None)

        # set the payload protocol id
        if next_ext_hdr is not None:
            self.p = next_ext_hdr

        # do not decode fragments after the first fragment
        # https://github.com/kbandla/dpkt/issues/575
        if self.nxt == 44 and ext.frag_off > 0:  # 44 = IP_PROTO_FRAGMENT
            self.data = buf
            return

        try:
            self.data = self._protosw[next_ext_hdr](buf)
            setattr(self, self.data.__class__.__name__.lower(), self.data)
        except (KeyError, dpkt.UnpackError):
            self.data = buf

    def headers_str(self):
        nxt = self.nxt
        # If all_extension_headers is available, return the headers as they originally appeared
        if hasattr(self, 'all_extension_headers') and self.all_extension_headers:
            # get the nxt header from the last one
            nxt = self.all_extension_headers[-1].nxt
            return nxt, b''.join(bytes(ext) for ext in self.all_extension_headers)

        # Output extension headers in order defined in RFC1883 (except dest opts)
        header_str = b""
        if hasattr(self, 'extension_hdrs'):
            for hdr in EXT_HDRS:
                if hdr in self.extension_hdrs:
                    nxt = self.extension_hdrs[hdr].nxt
                    header_str += bytes(self.extension_hdrs[hdr])
        return nxt, header_str

    def __bytes__(self):
        self.p, hdr_str = self.headers_str()

        # set TCP, UDP, and ICMPv6 checksums
        if ((self.p == 6 or self.p == 17 or self.p == 58) and
           hasattr(self.data, 'sum') and not self.data.sum):
            p = bytes(self.data)
            s = struct.pack('>16s16sxBH', self.src, self.dst, self.p, len(p))
            s = dpkt.in_cksum_add(0, s)
            s = dpkt.in_cksum_add(s, p)
            self.data.sum = dpkt.in_cksum_done(s)

        return self.pack_hdr() + hdr_str + bytes(self.data)

    def __len__(self):
        baselen = self.__hdr_len__ + len(self.data)
        if hasattr(self, 'all_extension_headers') and self.all_extension_headers:
            return baselen + sum(len(hh) for hh in self.all_extension_headers)
        elif hasattr(self, 'extension_hdrs') and self.extension_hdrs:
            return baselen + sum(len(hh) for hh in self.extension_hdrs.values())
        return baselen

    @classmethod
    def set_proto(cls, p, pktclass):
        cls._protosw[p] = pktclass

    @classmethod
    def get_proto(cls, p):
        return cls._protosw[p]


class IP6ExtensionHeader(dpkt.Packet):
    """
    An extension header is very similar to a 'sub-packet'.
    We just want to re-use all the hdr unpacking etc.
    """
    pass


class IP6OptsHeader(IP6ExtensionHeader):
    __hdr__ = (
        ('nxt', 'B', 0),  # next extension header protocol
        ('len', 'B', 0)  # option data length in 8 octet units (ignoring first 8 octets) so, len 0 == 64bit header
    )

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        self.length = (self.len + 1) * 8
        options = []

        index = 0

        while index < self.length - 2:
            try:
                opt_type = compat_ord(self.data[index])

                # PAD1 option
                if opt_type == 0:
                    index += 1
                    continue

                opt_length = compat_ord(self.data[index + 1])

                if opt_type == 1:  # PADN option
                    # PADN uses opt_length bytes in total
                    index += opt_length + 2
                    continue

                options.append({
                    'type': opt_type,
                    'opt_length': opt_length,
                    'data': self.data[index + 2:index + 2 + opt_length]
                })

                # add the two chars and the option_length, to move to the next option
                index += opt_length + 2

            except IndexError:
                raise dpkt.NeedData

        self.options = options
        self.data = buf[2:self.length]  # keep raw data with all pad options, but not the following data


class IP6HopOptsHeader(IP6OptsHeader):
    pass


class IP6DstOptsHeader(IP6OptsHeader):
    pass


class IP6RoutingHeader(IP6ExtensionHeader):
    __hdr__ = (
        ('nxt', 'B', 0),  # next extension header protocol
        ('len', 'B', 0),  # extension data length in 8 octet units (ignoring first 8 octets) (<= 46 for type 0)
        ('type', 'B', 0),  # routing type (currently, only 0 is used)
        ('segs_left', 'B', 0),  # remaining segments in route, until destination (<= 23)
        ('_rsvd_sl_bits', 'I', 0)
    )
    __bit_fields__ = {
        '_rsvd_sl_bits': (
            ('_rsvd', 8),     # reserved (1 byte)
            ('sl_bits', 24),  # strict/loose bitmap for addresses
        )
    }

    def unpack(self, buf):
        hdr_size = 8
        addr_size = 16

        dpkt.Packet.unpack(self, buf)

        addresses = []
        num_addresses = self.len // 2
        buf = buf[hdr_size:hdr_size + num_addresses * addr_size]

        for i in range(num_addresses):
            addresses.append(buf[i * addr_size: i * addr_size + addr_size])

        self.data = buf
        self.addresses = addresses
        self.length = self.len * 8 + 8


class IP6FragmentHeader(IP6ExtensionHeader):
    __hdr__ = (
        ('nxt', 'B', 0),  # next extension header protocol
        ('_resv', 'B', 0),  # reserved, set to 0
        ('_frag_off_resv_m', 'H', 0),
        ('id', 'I', 0)  # fragments id
    )
    __bit_fields__ = {
        '_frag_off_resv_m': (
            ('frag_off', 13),  # frag offset, 13 bits
            ('_resv', 2),      # reserved zero (2 bits)
            ('m_flag', 1),     # more frags flag
        )
    }

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        self.length = self.__hdr_len__
        self.data = b''


class IP6AHHeader(IP6ExtensionHeader):
    __hdr__ = (
        ('nxt', 'B', 0),  # next extension header protocol
        ('len', 'B', 0),  # length of header in 4 octet units (ignoring first 2 units)
        ('_resv', 'H', 0),  # reserved, 2 bytes of 0
        ('spi', 'I', 0),  # SPI security parameter index
        ('seq', 'I', 0)  # sequence no.
    )

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        self.length = (self.len + 2) * 4
        self.auth_data = self.data[:(self.len - 1) * 4]


class IP6ESPHeader(IP6ExtensionHeader):
    __hdr__ = (
        ('spi', 'I', 0),
        ('seq', 'I', 0)
    )

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        self.length = self.__hdr_len__ + len(self.data)


EXT_HDRS_CLS = {ip.IP_PROTO_HOPOPTS: IP6HopOptsHeader,
                ip.IP_PROTO_ROUTING: IP6RoutingHeader,
                ip.IP_PROTO_FRAGMENT: IP6FragmentHeader,
                ip.IP_PROTO_ESP: IP6ESPHeader,
                ip.IP_PROTO_AH: IP6AHHeader,
                ip.IP_PROTO_DSTOPTS: IP6DstOptsHeader}

# Unit tests


def test_ipg():
    s = (b'\x60\x00\x00\x00\x00\x28\x06\x40\xfe\x80\x00\x00\x00\x00\x00\x00\x02\x11\x24\xff\xfe\x8c'
         b'\x11\xde\xfe\x80\x00\x00\x00\x00\x00\x00\x02\xb0\xd0\xff\xfe\xe1\x80\x72\xcd\xca\x00\x16'
         b'\x04\x84\x46\xd5\x00\x00\x00\x00\xa0\x02\xff\xff\xf8\x09\x00\x00\x02\x04\x05\xa0\x01\x03'
         b'\x03\x00\x01\x01\x08\x0a\x7d\x18\x35\x3f\x00\x00\x00\x00')
    _ip = IP6(s)

    # basic properties
    assert _ip.v == 6
    assert _ip.fc == 0
    assert _ip.flow == 0

    _ip.data.sum = 0
    s2 = bytes(_ip)
    assert s == s2


def test_dict():
    s = (b'\x60\x00\x00\x00\x00\x28\x06\x40\xfe\x80\x00\x00\x00\x00\x00\x00\x02\x11\x24\xff\xfe\x8c'
         b'\x11\xde\xfe\x80\x00\x00\x00\x00\x00\x00\x02\xb0\xd0\xff\xfe\xe1\x80\x72\xcd\xca\x00\x16'
         b'\x04\x84\x46\xd5\x00\x00\x00\x00\xa0\x02\xff\xff\xf8\x09\x00\x00\x02\x04\x05\xa0\x01\x03'
         b'\x03\x00\x01\x01\x08\x0a\x7d\x18\x35\x3f\x00\x00\x00\x00')
    _ip = IP6(s)
    d = dict(_ip)

    # basic properties
    assert d['src'] == b'\xfe\x80\x00\x00\x00\x00\x00\x00\x02\x11\x24\xff\xfe\x8c\x11\xde'
    assert d['dst'] == b'\xfe\x80\x00\x00\x00\x00\x00\x00\x02\xb0\xd0\xff\xfe\xe1\x80\x72'


def test_ip6_routing_header():
    s = (b'\x60\x00\x00\x00\x00\x3c\x2b\x40\x20\x48\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
         b'\xde\xca\x20\x47\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xca\xfe\x06\x04\x00\x02'
         b'\x00\x00\x00\x00\x20\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xde\xca\x20\x22'
         b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xde\xca\x00\x14\x00\x50\x00\x00\x00\x00'
         b'\x00\x00\x00\x00\x50\x02\x20\x00\x91\x7f\x00\x00')
    _ip = IP6(s)
    s2 = bytes(_ip)
    # 43 is Routing header id
    assert len(_ip.extension_hdrs[43].addresses) == 2
    assert _ip.tcp
    assert s == s2


def test_ip6_fragment_header():
    s = b'\x06\xee\xff\xf9\x00\x00\xff\xff'
    fh = IP6FragmentHeader(s)
    # s2 = str(fh) variable 's2' is not used
    assert fh.nxt == 6
    assert fh.id == 65535
    assert fh.frag_off == 8191
    assert fh.m_flag == 1

    # test packing
    fh._frag_off_resv_m = 0
    fh.frag_off = 8191
    fh.m_flag = 1
    assert bytes(fh) == s

    # IP6 with fragment header
    s = (b'\x60\x00\x00\x00\x00\x10\x2c\x00\x02\x22\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
         b'\x00\x02\x03\x33\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x29\x00\x00\x01'
         b'\x00\x00\x00\x00\x60\x00\x00\x00\x00\x10\x2c\x00')
    _ip = IP6(s)
    assert bytes(_ip) == s


def test_ip6_options_header():
    s = (b'\x3b\x04\x01\x02\x00\x00\xc9\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
         b'\x00\x00\x01\x00\xc2\x04\x00\x00\x00\x00\x05\x02\x00\x00\x01\x02\x00\x00')
    options = IP6OptsHeader(s).options
    assert len(options) == 3
    assert bytes(IP6OptsHeader(s)) == s


def test_ip6_ah_header():
    s = b'\x3b\x04\x00\x00\x02\x02\x02\x02\x01\x01\x01\x01\x78\x78\x78\x78\x78\x78\x78\x78'
    ah = IP6AHHeader(s)
    assert ah.length == 24
    assert ah.auth_data == b'xxxxxxxx'
    assert ah.spi == 0x2020202
    assert ah.seq == 0x1010101
    assert bytes(ah) == s


def test_ip6_esp_header():
    s = (b'\x00\x00\x01\x00\x00\x00\x00\x44\xe2\x4f\x9e\x68\xf3\xcd\xb1\x5f\x61\x65\x42\x8b\x78\x0b'
         b'\x4a\xfd\x13\xf0\x15\x98\xf5\x55\x16\xa8\x12\xb3\xb8\x4d\xbc\x16\xb2\x14\xbe\x3d\xf9\x96'
         b'\xd4\xa0\x39\x1f\x85\x74\x25\x81\x83\xa6\x0d\x99\xb6\xba\xa3\xcc\xb6\xe0\x9a\x78\xee\xf2'
         b'\xaf\x9a')
    esp = IP6ESPHeader(s)
    assert esp.length == 68
    assert esp.spi == 256
    assert bytes(esp) == s


def test_ip6_extension_headers():
    p = (b'\x60\x00\x00\x00\x00\x3c\x2b\x40\x20\x48\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
         b'\xde\xca\x20\x47\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xca\xfe\x06\x04\x00\x02'
         b'\x00\x00\x00\x00\x20\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xde\xca\x20\x22'
         b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xde\xca\x00\x14\x00\x50\x00\x00\x00\x00'
         b'\x00\x00\x00\x00\x50\x02\x20\x00\x91\x7f\x00\x00')
    _ip = IP6(p)
    o = (b'\x3b\x04\x01\x02\x00\x00\xc9\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
         b'\x00\x00\x01\x00\xc2\x04\x00\x00\x00\x00\x05\x02\x00\x00\x01\x02\x00\x00')
    _ip.extension_hdrs[0] = IP6HopOptsHeader(o)
    fh = b'\x06\xee\xff\xfb\x00\x00\xff\xff'
    _ip.extension_hdrs[44] = IP6FragmentHeader(fh)
    ah = b'\x3b\x04\x00\x00\x02\x02\x02\x02\x01\x01\x01\x01\x78\x78\x78\x78\x78\x78\x78\x78'
    _ip.extension_hdrs[51] = IP6AHHeader(ah)
    do = b'\x3b\x02\x01\x02\x00\x00\xc9\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
    _ip.extension_hdrs[60] = IP6DstOptsHeader(do)
    assert len(_ip.extension_hdrs) == 5

    # this is a legacy unit test predating the addition of .all_extension_headers
    # this way of adding extension headers does not update .all_extension_headers
    # so we need to kick .all_extension_headers to force the __len__() method pick up
    # the updated legacy attribute and calculate the len correctly
    del _ip.all_extension_headers
    assert len(_ip) == len(p) + len(o) + len(fh) + len(ah) + len(do)


def test_ip6_all_extension_headers():  # https://github.com/kbandla/dpkt/pull/403
    s = (b'\x60\x00\x00\x00\x00\x47\x3c\x40\xfe\xd0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01'
         b'\x00\x02\xfe\xd0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x01\x3c\x00\x01\x04'
         b'\x00\x00\x00\x00\x3c\x00\x01\x04\x00\x00\x00\x00\x2c\x00\x01\x04\x00\x00\x00\x00\x2c\x00'
         b'\x00\x00\x00\x00\x00\x00\x3c\x00\x00\x00\x00\x00\x00\x00\x2c\x00\x01\x04\x00\x00\x00\x00'
         b'\x3a\x00\x00\x00\x00\x00\x00\x00\x80\x00\xd8\xe5\x0c\x1a\x00\x00\x50\x61\x79\x4c\x6f\x61'
         b'\x64')
    _ip = IP6(s)
    assert _ip.p == 58  # ICMPv6
    hdrs = _ip.all_extension_headers
    assert len(hdrs) == 7
    assert isinstance(hdrs[0], IP6DstOptsHeader)
    assert isinstance(hdrs[3], IP6FragmentHeader)
    assert isinstance(hdrs[5], IP6DstOptsHeader)
    assert bytes(_ip) == s
    assert len(_ip) == len(s)


def test_ip6_gen_tcp_ack():
    t = tcp.TCP()
    t.win = 8192
    t.dport = 80
    t.sport = 4711
    t.flags = tcp.TH_ACK
    t.seq = 22
    t.ack = 33
    ipp = IP6()
    ipp.src = b'\xfd\x00\x00\x00\x00\x00\x00\x00\xc8\xba\x88\x88\x00\xaa\xbb\x01'
    ipp.dst = b'\x00d\xff\x9b\x00\x00\x00\x00\x00\x00\x00\x00\xc1\n@*'
    ipp.hlim = 64
    ipp.nxt = ip.IP_PROTO_TCP
    ipp.data = t
    ipp.plen = ipp.data.ulen = len(ipp.data)

    assert len(bytes(ipp)) == 60
    assert ipp.p == ip.IP_PROTO_TCP

    # Second part of testing - with ext headers.
    ipp.p = 0
    o = (b'\x3b\x04\x01\x02\x00\x00\xc9\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
         b'\x00\x00\x01\x00\xc2\x04\x00\x00\x00\x00\x05\x02\x00\x00\x01\x02\x00\x00')
    ipp.extension_hdrs = {}
    ipp.extension_hdrs[0] = IP6HopOptsHeader(o)
    ipp.extension_hdrs[0].nxt = ip.IP_PROTO_TCP
    ipp.nxt = ip.proto = ip.IP_PROTO_HOPOPTS
    _p, exthdrs = ipp.headers_str()
    ipp.plen = len(exthdrs) + len(ipp.data)

    assert bytes(ipp)

    assert ipp.p == ip.IP_PROTO_TCP
    assert ipp.nxt == ip.IP_PROTO_HOPOPTS


def test_ip6_opts():
    import pytest
    # https://github.com/kbandla/dpkt/issues/477
    s = (b'\x52\x54\x00\xf3\x83\x6f\x52\x54\x00\x86\x33\xd9\x86\xdd\x60\x00\x00\x00\x05\x08\x3a\xff'
         b'\xfd\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\xfd\x00\x00\x00\x00\x00'
         b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x02\x00\xd2\xf3\x00\x00\x05\x00\x00\x00\x00\x00'
         b'\x00\x00\x00\x00\x00\x01\xfd\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02'
         b'\x00\x50\xd4\x34\x1a\x48\x24\x50\x6d\x8d\xb3\xc2\x80\x10\x01\xf6\x46\xe8\x00\x00\x01\x01'
         b'\x08\x0a\xd7\x9d\x6b\x8a\x3a\xd1\xf4\x58\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61'
         b'\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61'
         b'\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61'
         b'\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x61\x0a')

    from dpkt.ethernet import Ethernet

    assert Ethernet(s)
    assert Ethernet(s).ip6
    assert Ethernet(s).ip6.icmp6
    assert Ethernet(s).ip6.icmp6.data

    with pytest.raises(dpkt.NeedData):
        IP6(Ethernet(s).ip6.icmp6.data)  # should raise NeedData

    from binascii import unhexlify
    buf_ip6_opts = unhexlify(
        '00'  # nxt
        '00'  # len

        '000000000000'  # only padding
    )
    ip6opt = IP6OptsHeader(buf_ip6_opts)
    assert ip6opt.options == []
    assert ip6opt.data == b'\x00' * 6


def test_ip6_routing_properties():
    ip6rh = IP6RoutingHeader()
    assert ip6rh.sl_bits == 0
    ip6rh.sl_bits = 1024
    assert ip6rh.sl_bits == 1024


def test_ip6_fragment_properties():
    ip6fh = IP6FragmentHeader()
    assert ip6fh.frag_off == 0
    ip6fh.frag_off = 1234
    assert ip6fh.frag_off == 1234

    assert ip6fh.m_flag == 0
    ip6fh.m_flag = 1
    assert ip6fh.m_flag == 1


def test_ip6_properties():
    ip6 = IP6()

    assert ip6.v == 6
    ip6.v = 10
    assert ip6.v == 10

    assert ip6.fc == 0
    ip6.fc = 5
    assert ip6.fc == 5

    assert ip6.flow == 0
    ip6.flow = 4
    assert ip6.flow == 4

    # property delete
    del ip6.v
    del ip6.fc
    del ip6.flow
    assert ip6.v == 6
    assert ip6.fc == 0
    assert ip6.flow == 0


def test_proto_accessors():
    class Proto:
        pass

    assert 'PROTO' not in IP6._protosw
    IP6.set_proto('PROTO', Proto)
    assert IP6.get_proto('PROTO') == Proto


def test_ip6_fragment_no_decode():  # https://github.com/kbandla/dpkt/issues/575
    from . import udp

    # fragment 0
    s = (b'\x60\x00'
         b'\x00\x00\x00\x2c\x11\x3f\x20\x01\x06\x38\x05\x01\x8e\xfe\xcc\x4a'
         b'\x48\x39\xfa\x79\x04\xdc\x20\x01\x05\x00\x00\x60\x00\x00\x00\x00'
         b'\x00\x00\x00\x00\x00\x30\xde\xf2\x00\x35\x00\x2c\x61\x50\x4d\x8b'
         b'\x01\x20\x00\x01\x00\x00\x00\x00\x00\x01\x03\x69\x73\x63\x03\x6f'
         b'\x72\x67\x00\x00\xff\x00\x01\x00\x00\x29\x10\x00\x00\x00\x80\x00'
         b'\x00\x00')
    frag0 = IP6(s)
    assert type(frag0.data) == udp.UDP

    s = (b'\x60\x00\x00\x00\x01\x34\x2c\x35\x20\x01\x05\x00\x00\x60\x00\x00'
         b'\x00\x00\x00\x00\x00\x00\x00\x30\x20\x01\x06\x38\x05\x01\x8e\xfe'
         b'\xcc\x4a\x48\x39\xfa\x79\x04\xdc'
         b'\x11\x72\x31\xb9\xc1\x0f\xcf\x7c\x61\x62\x63\x64\x65\x66\x67\x68')  # partial data
    frag2 = IP6(s)
    assert type(frag2.data) == bytes

    # test packing
    assert bytes(frag2) == s
