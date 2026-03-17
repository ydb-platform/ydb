# $Id: sctp.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Stream Control Transmission Protocol."""
from __future__ import print_function
from __future__ import absolute_import

from . import dpkt
from . import crc32c

# Stream Control Transmission Protocol
# http://tools.ietf.org/html/rfc2960

# Chunk Types
DATA = 0
INIT = 1
INIT_ACK = 2
SACK = 3
HEARTBEAT = 4
HEARTBEAT_ACK = 5
ABORT = 6
SHUTDOWN = 7
SHUTDOWN_ACK = 8
ERROR = 9
COOKIE_ECHO = 10
COOKIE_ACK = 11
ECNE = 12
CWR = 13
SHUTDOWN_COMPLETE = 14


class SCTP(dpkt.Packet):
    """Stream Control Transmission Protocol.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of SCTP.
        TODO.
    """

    __hdr__ = (
        ('sport', 'H', 0),
        ('dport', 'H', 0),
        ('vtag', 'I', 0),
        ('sum', 'I', 0)
    )

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        l_ = []
        while self.data:
            chunk = Chunk(self.data)
            l_.append(chunk)
            if len(chunk) == 0:
                self.data = b''
                break
            self.data = self.data[len(chunk):]
        self.chunks = l_

    def __len__(self):
        return self.__hdr_len__ + sum(len(x) for x in self.chunks)

    def __bytes__(self):
        l_ = [bytes(x) for x in self.chunks]
        if self.sum == 0:
            s = crc32c.add(0xffffffff, self.pack_hdr())
            for x in l_:
                s = crc32c.add(s, x)
            self.sum = crc32c.done(s)
        return self.pack_hdr() + b''.join(l_)


class Chunk(dpkt.Packet):
    __hdr__ = (
        ('type', 'B', INIT),
        ('flags', 'B', 0),
        ('len', 'H', 0)
    )

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)

        self.data = self.data[:self.len - self.__hdr_len__]
        self.padding = b''  # optional padding for DATA chunks

        # SCTP DATA Chunked is padded, 4-bytes aligned
        if self.type == DATA and self.len % 4:
            plen = 4 - self.len % 4  # padded length
            if plen:
                pos = self.__hdr_len__ + len(self.data)  # end of data in buf
                self.padding = buf[pos:pos + plen]

    def __len__(self):
        return self.len + len(self.padding)

    def __bytes__(self):
        return self.pack_hdr() + bytes(self.data) + self.padding


__s = (b'\x80\x44\x00\x50\x00\x00\x00\x00\x30\xba\xef\x54\x01\x00\x00\x3c\x3b\xb9\x9c\x46\x00\x01'
       b'\xa0\x00\x00\x0a\xff\xff\x2b\x2d\x7e\xb2\x00\x05\x00\x08\x9b\xe6\x18\x9b\x00\x05\x00\x08'
       b'\x9b\xe6\x18\x9c\x00\x0c\x00\x06\x00\x05\x00\x00\x80\x00\x00\x04\xc0\x00\x00\x04\xc0\x06'
       b'\x00\x08\x00\x00\x00\x00')


def test_sctp_pack():
    sctp = SCTP(__s)
    assert (__s == bytes(sctp))
    sctp.sum = 0
    assert (__s == bytes(sctp))


def test_sctp_unpack():
    sctp = SCTP(__s)
    assert (sctp.sport == 32836)
    assert (sctp.dport == 80)
    assert (len(sctp.chunks) == 1)
    assert (len(sctp) == 72)
    chunk = sctp.chunks[0]
    assert (chunk.type == INIT)
    assert (chunk.len == 60)


def test_sctp_data_chunk():  # https://github.com/kbandla/dpkt/issues/499
    # packet 5 from 'sctp-www.cap' downloaded from https://wiki.wireshark.org/SampleCaptures
    # chunk len == 419 so requires padding to a 4-byte boundary
    d = (b'\x80\x44\x00\x50\xd2\x6a\xc1\xe5\x70\xe5\x5b\x4c\x00\x03\x01\xa3\x2b\x2d\x7e\xb2\x00\x00'
         b'\x00\x00\x00\x00\x00\x00\x47\x45\x54\x20\x2f\x20\x48\x54\x54\x50\x2f\x31\x2e\x31\x0d\x0a'
         b'\x48\x6f\x73\x74\x3a\x20\x32\x30\x33\x2e\x32\x35\x35\x2e\x32\x35\x32\x2e\x31\x39\x34\x0d'
         b'\x0a\x55\x73\x65\x72\x2d\x41\x67\x65\x6e\x74\x3a\x20\x4d\x6f\x7a\x69\x6c\x6c\x61\x2f\x35'
         b'\x2e\x30\x20\x28\x58\x31\x31\x3b\x20\x55\x3b\x20\x4c\x69\x6e\x75\x78\x20\x69\x36\x38\x36'
         b'\x3b\x20\x6b\x6f\x2d\x4b\x52\x3b\x20\x72\x76\x3a\x31\x2e\x37\x2e\x31\x32\x29\x20\x47\x65'
         b'\x63\x6b\x6f\x2f\x32\x30\x30\x35\x31\x30\x30\x37\x20\x44\x65\x62\x69\x61\x6e\x2f\x31\x2e'
         b'\x37\x2e\x31\x32\x2d\x31\x0d\x0a\x41\x63\x63\x65\x70\x74\x3a\x20\x74\x65\x78\x74\x2f\x78'
         b'\x6d\x6c\x2c\x61\x70\x70\x6c\x69\x63\x61\x74\x69\x6f\x6e\x2f\x78\x6d\x6c\x2c\x61\x70\x70'
         b'\x6c\x69\x63\x61\x74\x69\x6f\x6e\x2f\x78\x68\x74\x6d\x6c\x2b\x78\x6d\x6c\x2c\x74\x65\x78'
         b'\x74\x2f\x68\x74\x6d\x6c\x3b\x71\x3d\x30\x2e\x39\x2c\x74\x65\x78\x74\x2f\x70\x6c\x61\x69'
         b'\x6e\x3b\x71\x3d\x30\x2e\x38\x2c\x69\x6d\x61\x67\x65\x2f\x70\x6e\x67\x2c\x2a\x2f\x2a\x3b'
         b'\x71\x3d\x30\x2e\x35\x0d\x0a\x41\x63\x63\x65\x70\x74\x2d\x4c\x61\x6e\x67\x75\x61\x67\x65'
         b'\x3a\x20\x6b\x6f\x2c\x65\x6e\x2d\x75\x73\x3b\x71\x3d\x30\x2e\x37\x2c\x65\x6e\x3b\x71\x3d'
         b'\x30\x2e\x33\x0d\x0a\x41\x63\x63\x65\x70\x74\x2d\x45\x6e\x63\x6f\x64\x69\x6e\x67\x3a\x20'
         b'\x67\x7a\x69\x70\x2c\x64\x65\x66\x6c\x61\x74\x65\x0d\x0a\x41\x63\x63\x65\x70\x74\x2d\x43'
         b'\x68\x61\x72\x73\x65\x74\x3a\x20\x45\x55\x43\x2d\x4b\x52\x2c\x75\x74\x66\x2d\x38\x3b\x71'
         b'\x3d\x30\x2e\x37\x2c\x2a\x3b\x71\x3d\x30\x2e\x37\x0d\x0a\x4b\x65\x65\x70\x2d\x41\x6c\x69'
         b'\x76\x65\x3a\x20\x33\x30\x30\x0d\x0a\x43\x6f\x6e\x6e\x65\x63\x74\x69\x6f\x6e\x3a\x20\x6b'
         b'\x65\x65\x70\x2d\x61\x6c\x69\x76\x65\x0d\x0a\x0d\x0a\x00')  # <-- ends with \x00 padding

    sctp = SCTP(d)
    assert sctp.chunks
    assert len(sctp.chunks) == 1

    ch = sctp.chunks[0]
    assert ch.type == DATA
    assert ch.len == 419
    assert len(ch) == 420  # 419 +1 byte padding
    assert ch.data[-14:] == b'keep-alive\r\n\r\n'  # no padding byte at the end

    # no remaining sctp data
    assert sctp.data == b''

    # test packing of the padded chunk
    assert bytes(ch) == d[SCTP.__hdr_len__:]
  


def test_malformed_sctp_data_chunk():  
    # packet 7964 from '4.pcap' downloaded from https://research.unsw.edu.au/projects/unsw-nb15-dataset
    d = (b'\x27\x0f\xe1\xc3\xc2\x73\x4d\x32\x4f\x54\x27\x8c' #header
         b'\x0b\x00\x00\x04' #chunk 0, COOKIE_ACK chunk
         b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00') #chunk 1, malformed DATA chunk, size labeled as 0


    sctp = SCTP(d)
    assert sctp.chunks
    assert len(sctp.chunks) == 2

    ch = sctp.chunks[1]
    assert ch.type == DATA
    assert ch.len == 0
    assert len(ch) == 0 
    assert ch.data == b'\x00\x00'
    
    # no remaining sctp data
    assert sctp.data == b''
