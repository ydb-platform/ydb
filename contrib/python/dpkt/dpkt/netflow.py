# $Id: netflow.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Cisco Netflow."""
from __future__ import print_function
from __future__ import absolute_import

import struct

from . import dpkt
from .compat import compat_izip


class NetflowBase(dpkt.Packet):
    """Base class for Cisco Netflow packets.

    NetFlow is a feature that was introduced on Cisco routers around 1996 that provides the ability to collect IP
    network traffic as it enters or exits an interface. By analyzing the data provided by NetFlow, a network
    administrator can determine things such as the source and destination of traffic, class of service, and the causes
    of congestion.

    https://www.ciscopress.com/articles/article.asp?p=2812391&seqNum=3

    Attributes:
        __hdr__: Header fields of NetflowBase.
    """

    __hdr__ = (
        ('version', 'H', 1),
        ('count', 'H', 0),
        ('sys_uptime', 'I', 0),
        ('unix_sec', 'I', 0),
        ('unix_nsec', 'I', 0)
    )

    def __len__(self):
        return self.__hdr_len__ + (len(self.data[0]) * self.count)

    def __bytes__(self):
        # for now, don't try to enforce any size limits
        self.count = len(self.data)
        return self.pack_hdr() + b''.join(map(bytes, self.data))

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        buf = self.data
        l_ = []
        while buf:
            flow = self.NetflowRecord(buf)
            l_.append(flow)
            buf = buf[len(flow):]
        self.data = l_

    class NetflowRecordBase(dpkt.Packet):
        """Base class for netflow v1-v7 netflow records.

        TODO: Longer class information....

        Attributes:
            __hdr__: Header fields of NetflowRecordBase.
            TODO.
        """

        # performance optimizations
        def __len__(self):
            # don't bother with data
            return self.__hdr_len__

        def __bytes__(self):
            # don't bother with data
            return self.pack_hdr()

        def unpack(self, buf):
            # don't bother with data
            for k, v in compat_izip(self.__hdr_fields__,
                                    struct.unpack(self.__hdr_fmt__, buf[:self.__hdr_len__])):
                setattr(self, k, v)
            self.data = b""


class Netflow1(NetflowBase):
    """Netflow Version 1.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of Netflow Version 1.
        TODO.
    """

    class NetflowRecord(NetflowBase.NetflowRecordBase):
        """Netflow v1 flow record.

        TODO: Longer class information....

        Attributes:
            __hdr__: Header fields of Netflow Version 1 flow record.
            TODO.
        """

        __hdr__ = (
            ('src_addr', 'I', 0),
            ('dst_addr', 'I', 0),
            ('next_hop', 'I', 0),
            ('input_iface', 'H', 0),
            ('output_iface', 'H', 0),
            ('pkts_sent', 'I', 0),
            ('bytes_sent', 'I', 0),
            ('start_time', 'I', 0),
            ('end_time', 'I', 0),
            ('src_port', 'H', 0),
            ('dst_port', 'H', 0),
            ('pad1', 'H', 0),
            ('ip_proto', 'B', 0),
            ('tos', 'B', 0),
            ('tcp_flags', 'B', 0),
            ('pad2', 'B', 0),
            ('pad3', 'H', 0),
            ('reserved', 'I', 0)
        )


# FYI, versions 2-4 don't appear to have ever seen the light of day.

class Netflow5(NetflowBase):
    """Netflow Version 5.

    Popular NetFlow version on many routers from different vendors. Limited to IPv4 flows.

    Attributes:
        __hdr__: Header fields of Netflow Version 5.
    """

    __hdr__ = NetflowBase.__hdr__ + (
        ('flow_sequence', 'I', 0),
        ('engine_type', 'B', 0),
        ('engine_id', 'B', 0),
        ('reserved', 'H', 0),
    )

    class NetflowRecord(NetflowBase.NetflowRecordBase):
        """Netflow v5 flow record.

        Attributes:
            __hdr__: Header fields of Netflow Version 5 flow record.
        """

        __hdr__ = (
            ('src_addr', 'I', 0),
            ('dst_addr', 'I', 0),
            ('next_hop', 'I', 0),
            ('input_iface', 'H', 0),
            ('output_iface', 'H', 0),
            ('pkts_sent', 'I', 0),
            ('bytes_sent', 'I', 0),
            ('start_time', 'I', 0),
            ('end_time', 'I', 0),
            ('src_port', 'H', 0),
            ('dst_port', 'H', 0),
            ('pad1', 'B', 0),
            ('tcp_flags', 'B', 0),
            ('ip_proto', 'B', 0),
            ('tos', 'B', 0),
            ('src_as', 'H', 0),
            ('dst_as', 'H', 0),
            ('src_mask', 'B', 0),
            ('dst_mask', 'B', 0),
            ('pad2', 'H', 0),
        )


class Netflow6(NetflowBase):
    """Netflow Version 6.

    (Obsolete.) No longer supported by Cisco, but may be found in the field.

    Attributes:
        __hdr__: Header fields of Netflow Version 6.
    """

    __hdr__ = Netflow5.__hdr__

    class NetflowRecord(NetflowBase.NetflowRecordBase):
        """Netflow v6 flow record.

        Attributes:
            __hdr__: Header fields of Netflow Version 6 flow record.
        """

        __hdr__ = (
            ('src_addr', 'I', 0),
            ('dst_addr', 'I', 0),
            ('next_hop', 'I', 0),
            ('input_iface', 'H', 0),
            ('output_iface', 'H', 0),
            ('pkts_sent', 'I', 0),
            ('bytes_sent', 'I', 0),
            ('start_time', 'I', 0),
            ('end_time', 'I', 0),
            ('src_port', 'H', 0),
            ('dst_port', 'H', 0),
            ('pad1', 'B', 0),
            ('tcp_flags', 'B', 0),
            ('ip_proto', 'B', 0),
            ('tos', 'B', 0),
            ('src_as', 'H', 0),
            ('dst_as', 'H', 0),
            ('src_mask', 'B', 0),
            ('dst_mask', 'B', 0),
            ('in_encaps', 'B', 0),
            ('out_encaps', 'B', 0),
            ('peer_nexthop', 'I', 0),
        )


class Netflow7(NetflowBase):
    """Netflow Version 7.

    (Obsolete.) Like version 5, with a source router field.

    Attributes:
        __hdr__: Header fields of Netflow Version 7.
    """

    __hdr__ = NetflowBase.__hdr__ + (
        ('flow_sequence', 'I', 0),
        ('reserved', 'I', 0),
    )

    class NetflowRecord(NetflowBase.NetflowRecordBase):
        """Netflow v6 flow record.

        TODO: Longer class information....

        Attributes:
            __hdr__: Header fields of Netflow Version 6 flow record.
            TODO.
        """

        __hdr__ = (
            ('src_addr', 'I', 0),
            ('dst_addr', 'I', 0),
            ('next_hop', 'I', 0),
            ('input_iface', 'H', 0),
            ('output_iface', 'H', 0),
            ('pkts_sent', 'I', 0),
            ('bytes_sent', 'I', 0),
            ('start_time', 'I', 0),
            ('end_time', 'I', 0),
            ('src_port', 'H', 0),
            ('dst_port', 'H', 0),
            ('flags', 'B', 0),
            ('tcp_flags', 'B', 0),
            ('ip_proto', 'B', 0),
            ('tos', 'B', 0),
            ('src_as', 'H', 0),
            ('dst_as', 'H', 0),
            ('src_mask', 'B', 0),
            ('dst_mask', 'B', 0),
            ('pad2', 'H', 0),
            ('router_sc', 'I', 0),
        )


# No support for v8 or v9 yet.
def test_net_flow_v1_unpack():
    from binascii import unhexlify
    __sample_v1 = unhexlify(
        '00010018677a613c4200fc1c24930870ac012057c0a863f70a0002010003000a0000000100000228677a372c677a372c5c1b0050ac01112c10000'
        '0000004001bac011853ac18d9aac0a832020003001900000001000005dc677a377c677a377cd8e30050ac01062c100000000004001bac011418ac'
        '188dcdc0a832660003000700000001000005dc677a3790677a37908a81176fac0106361000000000040003ac0f2724ac01e51dc0a832060004001'
        'b0000000100000228677a3a38677a3a38a3511236ac2906fd180000000004001bac011645ac23178ec0a832060003001b0000000100000228677a'
        '3a4c677a3a4cc9ff0050ac1f0686020000000003001bac0d09ffac019995c0a832060004001b00000001000005dc677a3a58677a3a58ee390017a'
        'c0106de1000000000040003ac0e4ad8ac01ae2fc0a832060004001b00000001000005dc677a3a68677a3a68b36e0015ac01068110000000000400'
        '1bac012338ac01d92ac0a832060003001b00000001000005dc677a3a74677a3a7400008350ac2101ab100000000003001bac0a6037ac2a934ac0a'
        '832060004001b00000001000005dc677a3a74677a3a7400000000ac0132a91000000000040007ac0a471fac01fd4ac0a832060004001b00000001'
        '00000028677a3a88677a3a8821996987ac1e067e020000000003001bac0128c9ac0142c4c0a83202000300190000000100000028677a3a88677a3'
        'a887d360050ac0106fe100000000004001bac0b08e8ac0146e2c0a832020004001900000001000005dc677a3a9c677a3a9c60696987ac01063b10'
        '0000000004001bac011d24ac3cf0c3c0a832060003001b00000001000005dc677a3a9c677a3a9c46320014ac0106731800000000040003ac0b115'
        '1ac01de06c0a832060004001b00000001000005dc677a3ab0677a3ab0ef231a2bac2906e9100000000004001bac0c52d9ac016fe8c0a832020004'
        '001900000001000005dc677a3ac4677a3ac4136e006eac1906a81000000000030019ac013dddac017deec0a832660003000700000001000000286'
        '77a3ac4677a3ac40000dcbbac0101d3100000000004001bac0f28d1ac01cca5c0a832060004001b00000001000005dc677a3ad8677a3ad8c57317'
        '6fac1906231800000000030007ac0a855bc0a8636e0a0002010004000a00000001000005dc677a3ae4677a3ae4bf6c0050ac0106cf10000000000'
        '40007ac01301fac182145c0a832660003000700000001000005dc677a3b00677a3b00119504bec0a806ea100000000003000aac0130b6ac1ef4aa'
        'c0a832060003001b00000001000005dc677a3b34677a3b3488640017ac01061f100000000004001bac01235fac1eb009c0a832060003001b00000'
        '001000005dc677a3b48677a3b4881530050ac20064e100000000003001bac0104d9ac019463c0a832060003001b0000000100000228677a3b5c67'
        '7a3b5c55100050ac010650180000000004001bac013caeac2aac21c0a832060003001b00000001000000fa677a3b84677a3b840ce70050ac0111f'
        'd100000000004001bac011f1fac17ed69c0a832020003001900000001000005dc677a3b98677a3b98ba170016ac01067c1000000000030007'
    )
    nf = Netflow1(__sample_v1)
    assert len(nf.data) == 24
    # print repr(nfv1)


def test_net_flow_v5_unpack():
    from binascii import unhexlify
    buf_nf5_header = unhexlify(
        '0005001db5fac9d03a0b4142265677de9b73763100010000'
    )

    buf_nf5_records = list(map(unhexlify, (
        'ac0a86a6ac01aaf7c0a83232027100690000000100000228b5fa8114b5fa811435320050000006000000000000000000',
        'ac019144ac1443e4c0a83216006902710000000100000028b5fa9bbdb5fa9bbd005085d7000006000000000000000000',
        'ac17e2d7ac018c56c0a832320271006900000001000005dcb5fa6fb8b5fa6fb876e8176f000006000000000000000000',
        'ac0ef2e5ac0191b2c0a832320271006900000001000000fab5fa81eeb5fa81eed0eb0015000006000000000000000000',
        'ac0a436aac29a7090a000201027100db0000000100000228b5fa8592b5fa85928cb00035000006000000000000000000',
        'ac01963dac151aa8c0a832160069027100000001000005dcb5fa86e0b5fa86e0b4e700c2000006000000000000000000',
        'ac0156d1ac018615c0a832320271006900000001000005dcb5fa7d3ab5fa7d3a5b510050000006000000000000000000',
        'ac32f1b1ac2919ca0a000201027100db00000001000005dcb5fa83c3b5fa83c3162c0015000006000000000000000000',
        'ac0c4134ac019a7ac0a832320271006900000001000005dcb5fa8da7b5fa8da717330015000006000000000000000000',
        'ac1ed284ac29d8d20a000201027100db00000001000005dcb5fa8e97b5fa8e97372a176f000006000000000000000000',
        'ac01854aac2011fcc0a83216006902710000000100000228b5fa8834b5fa8834f5dd008f000006000000000000000000',
        'ac010480ac3c5b6e0a000201027100db00000001000005dcb5fa9d72b5fa9d7273240016000006000000000000000000',
        'ac01b94aac22c9d7c0a83216006902710000000100000028b5fa9072b5fa90720f8d00c2000006000000000000000000',
        'ac2aa310ac01b419c0a83232027100690000000100000028b5fa9203b5fa920370660015000006000000000000000000',
        'ac01ab6fac1e7f69c0a832160069027100000001000005dcb5fa937fb5fa937f00500b98000006000000000000000000',
        'ac0c0aeaac01a115c0a832320271006900000001000005dcb5fa79cfb5fa79cf5b3317e0000006000000000000000000',
        'ac01bbb3ac29758c0a000201006900db00000001000000fab5fa9433b5fa943300501eca000006000000000000000000',
        'ac0f4a60ac01ab94c0a83232027100690000000100000228b5fa875bb5fa875b9ad62fab000006000000000000000000',
        'ac2a0f93ac01b8a3c0a83232027100690000000100000028b5fa89bbb5fa89bb6ee10050000006000000000000000000',
        'ac0193a1ac16800cc0a83216006902710000000100000028b5fa8726b5fa872600000000000001000000000000000000',
        'ac01835aac1f52cdc0a832160069027100000001000005dcb5fa900db5fa900df72a008a000006000000000000000000',
        'ac0ce0adac01a856c0a832320271006900000001000005dcb5fa9cf6b5fa9cf6e57c1a2b000006000000000000000000',
        'ac1ecc54ac3c78260a000201027100db00000001000005dcb5fa80eab5fa80ea0000000000002f000000000000000000',
        'ac01bb18ac017c7ac0a832160069027100000001000000fab5fa8870b5fa887000500b7d000006000000000000000000',
        'ac170e72ac018fddc0a83232027100690000000100000228b5fa89f7b5fa89f70df7008a000006000000000000000000',
        'ac0abb04ac3cb0150a000201027100db00000001000005dcb5fa90a9b5fa90a99cd0008f000006000000000000000000',
        'ac0a7a3fac2903c80a000201027100db00000001000005dcb5fa7565b5fa7565eea60050000006000000000000000000',
        'ac01b505c0a8639f0a000201006900db00000001000005dcb5fa7bc7b5fa7bc7005086a9000006000000000000000000',
        'ac32a51bac2930bf0a000201027100db00000001000000fab5fa9b5ab5fa9b5a43f917e0000006000000000000000000',
    )))

    buf_input = buf_nf5_header + b''.join(buf_nf5_records)
    nf = Netflow5(buf_input)
    assert nf.version == 5
    assert nf.count == 29
    assert nf.sys_uptime == 3053111760
    assert nf.unix_sec == 973816130

    assert len(nf) == len(buf_input)
    assert bytes(nf) == buf_input

    assert len(nf.data) == 29
    for idx, record in enumerate(nf.data):
        assert bytes(record) == buf_nf5_records[idx]
        assert len(record) == 48
