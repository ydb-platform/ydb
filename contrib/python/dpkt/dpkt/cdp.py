# $Id: cdp.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Cisco Discovery Protocol."""
from __future__ import absolute_import

from . import dpkt

CDP_DEVID = 1  # string
CDP_ADDRESS = 2
CDP_PORTID = 3  # string
CDP_CAPABILITIES = 4  # 32-bit bitmask
CDP_VERSION = 5  # string
CDP_PLATFORM = 6  # string
CDP_IPPREFIX = 7

CDP_VTP_MGMT_DOMAIN = 9  # string
CDP_NATIVE_VLAN = 10  # 16-bit integer
CDP_DUPLEX = 11  # 8-bit boolean
CDP_TRUST_BITMAP = 18  # 8-bit bitmask0x13
CDP_UNTRUST_COS = 19  # 8-bit port
CDP_SYSTEM_NAME = 20  # string
CDP_SYSTEM_OID = 21  # 10-byte binary string
CDP_MGMT_ADDRESS = 22  # 32-bit number of addrs, Addresses
CDP_LOCATION = 23  # string


class CDP(dpkt.Packet):
    """Cisco Discovery Protocol.

    Cisco Discovery Protocol (CDP) is a proprietary Data Link Layer protocol developed by Cisco Systems in 1994
    by Keith McCloghrie and Dino Farinacci. It is used to share information about other directly connected
    Cisco equipment, such as the operating system version and IP address.

    See more on
    https://en.wikipedia.org/wiki/Cisco_Discovery_Protocol

    Attributes:
        __hdr__: Header fields of CDP.
            version: (int): CDP protocol version. (1 byte)
            ttl: (int): Time to live. The amount of time in seconds that a receiver should retain the information
                contained in this packet. (1 byte)
            sum: (int): Checksum. (2 bytes)
    """

    __hdr__ = (
        ('version', 'B', 2),
        ('ttl', 'B', 180),
        ('sum', 'H', 0)
    )

    class TLV(dpkt.Packet):
        """Type–length–value

        When constructing the packet, len is not mandatory:
        if not provided, then self.data must be this exact TLV payload

        Attributes:
            __hdr__: Header fields of TLV.
                type: (int): Type (2 bytes)
                len: (int): The total length in bytes of the Type, Length and Data fields. (2 bytes)
        """

        __hdr__ = (
            ('type', 'H', 0),
            ('len', 'H', 0)
        )

        def data_len(self):
            if self.len:
                return self.len - self.__hdr_len__
            return len(self.data)

        def unpack(self, buf):
            dpkt.Packet.unpack(self, buf)
            self.data = self.data[:self.data_len()]

        def __len__(self):
            return self.__hdr_len__ + len(self.data)

        def __bytes__(self):
            if hasattr(self, 'len') and not self.len:
                self.len = len(self)
            return self.pack_hdr() + bytes(self.data)

    class Address(TLV):
        # XXX - only handle NLPID/IP for now
        __hdr__ = (
            ('ptype', 'B', 1),  # protocol type (NLPID)
            ('plen', 'B', 1),  # protocol length
            ('p', 'B', 0xcc),  # IP
            ('alen', 'H', 4)  # address length
        )

        def data_len(self):
            return self.alen

    class TLV_Addresses(TLV):
        __hdr__ = (
            ('type', 'H', CDP_ADDRESS),
            ('len', 'H', 0),    # 17),
            ('Addresses', 'L', 1),
        )

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        buf = self.data
        l_ = []
        while buf:
            # find the right TLV according to Type value
            tlv_find_type = self.TLV(buf).type
            # if this TLV is not in tlv_types, use the default TLV class
            tlv = self.tlv_types.get(tlv_find_type, self.TLV)(buf)
            l_.append(bytes(tlv))
            buf = buf[len(tlv):]
        self.tlvs = l_
        self.data = b''.join(l_)

    def __len__(self):
        return self.__hdr_len__ + len(self.data)

    def __bytes__(self):
        data = bytes(self.data)
        if not self.sum:
            self.sum = dpkt.in_cksum(self.pack_hdr() + data)
        return self.pack_hdr() + data

    # keep here the TLV classes whose header is different from the generic TLV header (example : TLV_Addresses)
    tlv_types = {CDP_ADDRESS: TLV_Addresses}


def test_cdp():
    import socket
    from . import ethernet

    ss = (b'\x02\xb4\xdf\x93\x00\x01\x00\x09\x63\x69\x73\x63\x6f\x00\x02\x00\x11\x00\x00\x00\x01'
          b'\x01\x01\xcc\x00\x04\xc0\xa8\x01\x67')
    rr1 = CDP(ss)
    assert bytes(rr1) == ss

    # construction
    ss = (b'\x02\xb4\xdf\x93\x00\x01\x00\x09\x63\x69\x73\x63\x6f\x00\x02\x00\x11\x00\x00\x00\x01'
          b'\x01\x01\xcc\x00\x04\xc0\xa8\x01\x67')
    p1 = CDP.TLV_Addresses(data=CDP.Address(data=socket.inet_aton('192.168.1.103')))
    p2 = CDP.TLV(type=CDP_DEVID, data=b'cisco')
    data = p2.pack() + p1.pack()
    rr2 = CDP(data=data)
    assert bytes(rr2) == ss

    s = (b'\x01\x00\x0c\xcc\xcc\xcc\xc4\x022k\x00\x00\x01T\xaa\xaa\x03\x00\x00\x0c \x00\x02\xb4,B'
         b'\x00\x01\x00\x06R2\x00\x05\x00\xffCisco IOS Software, 3700 Software (C3745-ADVENTERPRI'
         b'SEK9_SNA-M), Version 12.4(25d), RELEASE SOFTWARE (fc1)\nTechnical Support: http://www.'
         b'cisco.com/techsupport\nCopyright (c) 1986-2010 by Cisco Systems, Inc.\nCompiled Wed 18'
         b'-Aug-10 08:18 by prod_rel_team\x00\x06\x00\x0eCisco 3745\x00\x02\x00\x11\x00\x00\x00\x01'
         b'\x01\x01\xcc\x00\x04\n\x00\x00\x02\x00\x03\x00\x13FastEthernet0/0\x00\x04\x00\x08\x00'
         b'\x00\x00)\x00\t\x00\x04\x00\x0b\x00\x05\x00')
    eth = ethernet.Ethernet(s)
    assert isinstance(eth.data.data, CDP)
    assert len(eth.data.data.tlvs) == 8  # number of CDP TLVs; ensures they are decoded
    assert str(eth) == str(s)
    assert len(eth) == len(s)


def test_tlv():
    from binascii import unhexlify
    # len field set to 0
    buf_no_len = unhexlify(
        '0000'  # type
        '0000'  # len
        'abcd'  # data
    )

    buf_with_len = unhexlify(
        '0000'  # type
        '0006'  # len
        'abcd'  # data
    )
    tlv = CDP.TLV(buf_no_len)
    assert tlv.type == 0
    assert tlv.len == 0
    assert tlv.data_len() == 2
    assert tlv.data == b'\xab\xcd'
    assert bytes(tlv) == buf_with_len

    # len field set manually
    tlv = CDP.TLV(buf_with_len)
    assert tlv.type == 0
    assert tlv.len == 6
    assert tlv.data_len() == 2
    assert tlv.data == b'\xab\xcd'
    assert bytes(tlv) == buf_with_len


def test_address():
    from binascii import unhexlify
    buf = unhexlify(
        '00'    # ptype
        '11'    # plen
        '22'    # p
        '3333'  # alen
    )
    address = CDP.Address(buf)
    assert address.data_len() == 0x3333
