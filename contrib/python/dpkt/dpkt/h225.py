# $Id: h225.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""ITU-T H.225.0 Call Signaling."""
from __future__ import print_function
from __future__ import absolute_import

import struct

from . import dpkt
from . import tpkt


# H225 Call Signaling
#
# Call messages and information elements (IEs) are defined by Q.931:
# http://cvsup.de.openbsd.org/historic/comp/doc/standards/itu/Q/Q.931.ps.gz
#
# The User-to-User IEs of H225 are encoded by PER of ASN.1.

# Call Establishment Messages
ALERTING = 1
CALL_PROCEEDING = 2
CONNECT = 7
CONNECT_ACKNOWLEDGE = 15
PROGRESS = 3
SETUP = 5
SETUP_ACKNOWLEDGE = 13

# Call Information Phase Messages
RESUME = 38
RESUME_ACKNOWLEDGE = 46
RESUME_REJECT = 34
SUSPEND = 37
SUSPEND_ACKNOWLEDGE = 45
SUSPEND_REJECT = 33
USER_INFORMATION = 32

# Call Clearing Messages
DISCONNECT = 69
RELEASE = 77
RELEASE_COMPLETE = 90
RESTART = 70
RESTART_ACKNOWLEDGE = 78

# Miscellaneous Messages
SEGMENT = 96
CONGESTION_CONTROL = 121
INFORMATION = 123
NOTIFY = 110
STATUS = 125
STATUS_ENQUIRY = 117

# Type 1 Single Octet Information Element IDs
RESERVED = 128
SHIFT = 144
CONGESTION_LEVEL = 176
REPEAT_INDICATOR = 208

# Type 2 Single Octet Information Element IDs
MORE_DATA = 160
SENDING_COMPLETE = 161

# Variable Length Information Element IDs
SEGMENTED_MESSAGE = 0
BEARER_CAPABILITY = 4
CAUSE = 8
CALL_IDENTITY = 16
CALL_STATE = 20
CHANNEL_IDENTIFICATION = 24
PROGRESS_INDICATOR = 30
NETWORK_SPECIFIC_FACILITIES = 32
NOTIFICATION_INDICATOR = 39
DISPLAY = 40
DATE_TIME = 41
KEYPAD_FACILITY = 44
SIGNAL = 52
INFORMATION_RATE = 64
END_TO_END_TRANSIT_DELAY = 66
TRANSIT_DELAY_SELECTION_AND_INDICATION = 67
PACKET_LAYER_BINARY_PARAMETERS = 68
PACKET_LAYER_WINDOW_SIZE = 69
PACKET_SIZE = 70
CLOSED_USER_GROUP = 71
REVERSE_CHARGE_INDICATION = 74
CALLING_PARTY_NUMBER = 108
CALLING_PARTY_SUBADDRESS = 109
CALLED_PARTY_NUMBER = 112
CALLED_PARTY_SUBADDRESS = 113
REDIRECTING_NUMBER = 116
TRANSIT_NETWORK_SELECTION = 120
RESTART_INDICATOR = 121
LOW_LAYER_COMPATIBILITY = 124
HIGH_LAYER_COMPATIBILITY = 125
USER_TO_USER = 126
ESCAPE_FOR_EXTENSION = 127


class H225(dpkt.Packet):
    """ITU-T H.225.0 Call Signaling.

    H.225.0 is a key protocol in the H.323 VoIP architecture defined by ITU-T. H.225.0 describes how audio, video,
    data and control information on a packet based network can be managed to provide conversational services in H.323
    equipment. H.225.0 has two major parts: Call signaling and RAS (Registration, Admission and Status).

    Attributes:
        __hdr__: Header fields of H225.
            proto: (int): Protocol Discriminator. The Protocol Discriminator identifies the Layer 3 protocol. (1 byte)
            ref_len: (int): Call Reference Value. Contains the length of the Call Reference Value (CRV) field. (1 byte)
    """

    __hdr__ = (
        ('proto', 'B', 8),
        ('ref_len', 'B', 2)
    )

    def unpack(self, buf):
        # TPKT header
        self.tpkt = tpkt.TPKT(buf)
        if self.tpkt.v != 3:
            raise dpkt.UnpackError('invalid TPKT version')
        if self.tpkt.rsvd != 0:
            raise dpkt.UnpackError('invalid TPKT reserved value')
        n = self.tpkt.len - self.tpkt.__hdr_len__
        if n > len(self.tpkt.data):
            raise dpkt.UnpackError('invalid TPKT length')
        buf = self.tpkt.data

        # Q.931 payload
        dpkt.Packet.unpack(self, buf)
        buf = buf[self.__hdr_len__:]
        self.ref_val = buf[:self.ref_len]
        buf = buf[self.ref_len:]
        self.type = struct.unpack('B', buf[:1])[0]
        buf = buf[1:]

        # Information Elements
        l_ = []
        while buf:
            ie = self.IE(buf)
            l_.append(ie)
            buf = buf[len(ie):]
        self.data = l_

    def __len__(self):
        return self.tpkt.__hdr_len__ + self.__hdr_len__ + sum(map(len, self.data))

    def __bytes__(self):
        return self.tpkt.pack_hdr() + self.pack_hdr() + self.ref_val + \
            struct.pack('B', self.type) + b''.join(map(bytes, self.data))

    class IE(dpkt.Packet):
        __hdr__ = (
            ('type', 'B', 0),
        )

        def unpack(self, buf):
            dpkt.Packet.unpack(self, buf)
            buf = buf[self.__hdr_len__:]

            # single-byte IE
            if self.type & 0x80:
                self.len = 0
                self.data = b''
            # multi-byte IE
            else:
                # special PER-encoded UUIE
                if self.type == USER_TO_USER:
                    self.len = struct.unpack('>H', buf[:2])[0]
                    buf = buf[2:]
                # normal TLV-like IE
                else:
                    self.len = struct.unpack('B', buf[:1])[0]
                    buf = buf[1:]
                self.data = buf[:self.len]

        def __len__(self):
            if self.type & 0x80:
                n = 0
            else:
                if self.type == USER_TO_USER:
                    n = 2
                else:
                    n = 1
            return self.__hdr_len__ + self.len + n

        def __bytes__(self):
            if self.type & 0x80:
                length_str = b''
            else:
                if self.type == USER_TO_USER:
                    length_str = struct.pack('>H', self.len)
                else:
                    length_str = struct.pack('B', self.len)
            return struct.pack('B', self.type) + length_str + self.data


__s = (
    b'\x03\x00\x04\x11\x08\x02\x54\x2b\x05\x04\x03\x88\x93\xa5\x28\x0e\x4a\x6f\x6e\x20\x4f\x62\x65\x72\x68\x65\x69\x64\x65'
    b'\x00\x7e\x03\xf0\x05\x20\xb8\x06\x00\x08\x91\x4a\x00\x04\x01\x40\x0c\x00\x4a\x00\x6f\x00\x6e\x00\x20\x00\x4f\x00\x62'
    b'\x00\x65\x00\x72\x00\x68\x00\x65\x00\x69\x00\x64\x00\x65\x22\xc0\x09\x00\x00\x3d\x06\x65\x6b\x69\x67\x61\x00\x00\x14'
    b'\x32\x2e\x30\x2e\x32\x20\x28\x4f\x50\x41\x4c\x20\x76\x32\x2e\x32\x2e\x32\x29\x00\x00\x00\x01\x40\x15\x00\x74\x00\x63'
    b'\x00\x70\x00\x24\x00\x68\x00\x33\x00\x32\x00\x33\x00\x2e\x00\x76\x00\x6f\x00\x78\x00\x67\x00\x72\x00\x61\x00\x74\x00'
    b'\x69\x00\x61\x00\x2e\x00\x6f\x00\x72\x00\x67\x00\x42\x87\x23\x2c\x06\xb8\x00\x6a\x8b\x1d\x0c\xb7\x06\xdb\x11\x9e\xca'
    b'\x00\x10\xa4\x89\x6d\x6a\x00\xc5\x1d\x80\x04\x07\x00\x0a\x00\x01\x7a\x75\x30\x11\x00\x5e\x88\x1d\x0c\xb7\x06\xdb\x11'
    b'\x9e\xca\x00\x10\xa4\x89\x6d\x6a\x82\x2b\x0e\x30\x40\x00\x00\x06\x04\x01\x00\x4c\x10\x09\x00\x00\x3d\x0f\x53\x70\x65'
    b'\x65\x78\x20\x62\x73\x34\x20\x57\x69\x64\x65\x36\x80\x11\x1c\x00\x01\x00\x98\xa0\x26\x41\x13\x8a\x00\x98\xa0\x26\x41'
    b'\x13\x8b\x26\x00\x00\x64\x0c\x10\x09\x00\x00\x3d\x0f\x53\x70\x65\x65\x78\x20\x62\x73\x34\x20\x57\x69\x64\x65\x36\x80'
    b'\x0b\x0d\x00\x01\x00\x98\xa0\x26\x41\x13\x8b\x00\x2a\x40\x00\x00\x06\x04\x01\x00\x4c\x10\x09\x00\x00\x3d\x09\x69\x4c'
    b'\x42\x43\x2d\x31\x33\x6b\x33\x80\x11\x1c\x00\x01\x00\x98\xa0\x26\x41\x13\x8a\x00\x98\xa0\x26\x41\x13\x8b\x20\x00\x00'
    b'\x65\x0c\x10\x09\x00\x00\x3d\x09\x69\x4c\x42\x43\x2d\x31\x33\x6b\x33\x80\x0b\x0d\x00\x01\x00\x98\xa0\x26\x41\x13\x8b'
    b'\x00\x20\x40\x00\x00\x06\x04\x01\x00\x4e\x0c\x03\x00\x83\x00\x80\x11\x1c\x00\x01\x00\x98\xa0\x26\x41\x13\x8a\x00\x98'
    b'\xa0\x26\x41\x13\x8b\x16\x00\x00\x66\x0e\x0c\x03\x00\x83\x00\x80\x0b\x0d\x00\x01\x00\x98\xa0\x26\x41\x13\x8b\x00\x4b'
    b'\x40\x00\x00\x06\x04\x01\x00\x4c\x10\xb5\x00\x53\x4c\x2a\x02\x00\x00\x00\x00\x00\x40\x01\x00\x00\x40\x01\x02\x00\x08'
    b'\x00\x00\x00\x00\x00\x31\x00\x01\x00\x40\x1f\x00\x00\x59\x06\x00\x00\x41\x00\x00\x00\x02\x00\x40\x01\x00\x00\x80\x11'
    b'\x1c\x00\x01\x00\x98\xa0\x26\x41\x13\x8a\x00\x98\xa0\x26\x41\x13\x8b\x41\x00\x00\x67\x0c\x10\xb5\x00\x53\x4c\x2a\x02'
    b'\x00\x00\x00\x00\x00\x40\x01\x00\x00\x40\x01\x02\x00\x08\x00\x00\x00\x00\x00\x31\x00\x01\x00\x40\x1f\x00\x00\x59\x06'
    b'\x00\x00\x41\x00\x00\x00\x02\x00\x40\x01\x00\x00\x80\x0b\x0d\x00\x01\x00\x98\xa0\x26\x41\x13\x8b\x00\x32\x40\x00\x00'
    b'\x06\x04\x01\x00\x4c\x10\x09\x00\x00\x3d\x11\x53\x70\x65\x65\x78\x20\x62\x73\x34\x20\x4e\x61\x72\x72\x6f\x77\x33\x80'
    b'\x11\x1c\x00\x01\x00\x98\xa0\x26\x41\x13\x8a\x00\x98\xa0\x26\x41\x13\x8b\x28\x00\x00\x68\x0c\x10\x09\x00\x00\x3d\x11'
    b'\x53\x70\x65\x65\x78\x20\x62\x73\x34\x20\x4e\x61\x72\x72\x6f\x77\x33\x80\x0b\x0d\x00\x01\x00\x98\xa0\x26\x41\x13\x8b'
    b'\x00\x1d\x40\x00\x00\x06\x04\x01\x00\x4c\x60\x1d\x80\x11\x1c\x00\x01\x00\x98\xa0\x26\x41\x13\x8a\x00\x98\xa0\x26\x41'
    b'\x13\x8b\x13\x00\x00\x69\x0c\x60\x1d\x80\x0b\x0d\x00\x01\x00\x98\xa0\x26\x41\x13\x8b\x00\x1d\x40\x00\x00\x06\x04\x01'
    b'\x00\x4c\x20\x1d\x80\x11\x1c\x00\x01\x00\x98\xa0\x26\x41\x13\x8a\x00\x98\xa0\x26\x41\x13\x8b\x13\x00\x00\x6a\x0c\x20'
    b'\x1d\x80\x0b\x0d\x00\x01\x00\x98\xa0\x26\x41\x13\x8b\x00\x01\x00\x01\x00\x01\x00\x01\x00\x81\x03\x02\x80\xf8\x02\x70'
    b'\x01\x06\x00\x08\x81\x75\x00\x0b\x80\x13\x80\x01\xf4\x00\x01\x00\x00\x01\x00\x00\x01\x00\x00\x0c\xc0\x01\x00\x01\x80'
    b'\x0b\x80\x00\x00\x20\x20\x09\x00\x00\x3d\x0f\x53\x70\x65\x65\x78\x20\x62\x73\x34\x20\x57\x69\x64\x65\x36\x80\x00\x01'
    b'\x20\x20\x09\x00\x00\x3d\x09\x69\x4c\x42\x43\x2d\x31\x33\x6b\x33\x80\x00\x02\x24\x18\x03\x00\xe6\x00\x80\x00\x03\x20'
    b'\x20\xb5\x00\x53\x4c\x2a\x02\x00\x00\x00\x00\x00\x40\x01\x00\x00\x40\x01\x02\x00\x08\x00\x00\x00\x00\x00\x31\x00\x01'
    b'\x00\x40\x1f\x00\x00\x59\x06\x00\x00\x41\x00\x00\x00\x02\x00\x40\x01\x00\x00\x80\x00\x04\x20\x20\x09\x00\x00\x3d\x11'
    b'\x53\x70\x65\x65\x78\x20\x62\x73\x34\x20\x4e\x61\x72\x72\x6f\x77\x33\x80\x00\x05\x20\xc0\xef\x80\x00\x06\x20\x40\xef'
    b'\x80\x00\x07\x08\xe0\x03\x51\x00\x80\x01\x00\x80\x00\x08\x08\xd0\x03\x51\x00\x80\x01\x00\x80\x00\x09\x83\x01\x50\x80'
    b'\x00\x0a\x83\x01\x10\x80\x00\x0b\x83\x01\x40\x00\x80\x01\x03\x06\x00\x00\x00\x01\x00\x02\x00\x03\x00\x04\x00\x05\x00'
    b'\x06\x01\x00\x07\x00\x08\x00\x00\x09\x01\x00\x0a\x00\x0b\x07\x01\x00\x32\x80\xa6\xff\x4c\x02\x80\x01\x80'
)


def test_pack():
    h = H225(__s)
    assert (__s == bytes(h))
    assert len(h) == 1038  # len(__s) == 1041


def test_unpack():
    h = H225(__s)
    assert (h.tpkt.v == 3)
    assert (h.tpkt.rsvd == 0)
    assert (h.tpkt.len == 1041)
    assert (h.proto == 8)
    assert (h.type == SETUP)
    assert (len(h.data) == 3)

    ie = h.data[0]
    assert (ie.type == BEARER_CAPABILITY)
    assert (ie.len == 3)
    ie = h.data[1]
    assert (ie.type == DISPLAY)
    assert (ie.len == 14)
    ie = h.data[2]
    assert (ie.type == USER_TO_USER)
    assert (ie.len == 1008)


def test_tpkt_unpack_errors():
    import pytest
    from binascii import unhexlify

    # invalid version
    buf_tpkt_version0 = unhexlify(
        '00'    # v
        '00'    # rsvd
        '0000'  # len
    )
    with pytest.raises(dpkt.UnpackError, match="invalid TPKT version"):
        H225(buf_tpkt_version0)

    # invalid reserved value
    buf_tpkt_rsvd = unhexlify(
        '03'    # v
        'ff'    # rsvd
        '0000'  # len
    )
    with pytest.raises(dpkt.UnpackError, match="invalid TPKT reserved value"):
        H225(buf_tpkt_rsvd)

    # invalid len
    buf_tpkt_len = unhexlify(
        '03'    # v
        '00'    # rsvd
        'ffff'  # len
    )
    with pytest.raises(dpkt.UnpackError, match="invalid TPKT length"):
        H225(buf_tpkt_len)


def test_unpack_ie():
    ie = H225.IE(b'\x80')
    assert ie.len == 0
    assert ie.data == b''
    assert len(ie) == 1
    assert bytes(ie) == b'\x80'
