# $Id: gzip.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""GNU zip."""
from __future__ import print_function
from __future__ import absolute_import

import struct
import zlib

from . import dpkt


# RFC 1952
GZIP_MAGIC = b'\x1f\x8b'

# Compression methods
GZIP_MSTORED = 0
GZIP_MCOMPRESS = 1
GZIP_MPACKED = 2
GZIP_MLZHED = 3
GZIP_MDEFLATE = 8

# Flags
GZIP_FTEXT = 0x01
GZIP_FHCRC = 0x02
GZIP_FEXTRA = 0x04
GZIP_FNAME = 0x08
GZIP_FCOMMENT = 0x10
GZIP_FENCRYPT = 0x20
GZIP_FRESERVED = 0xC0

# OS
GZIP_OS_MSDOS = 0
GZIP_OS_AMIGA = 1
GZIP_OS_VMS = 2
GZIP_OS_UNIX = 3
GZIP_OS_VMCMS = 4
GZIP_OS_ATARI = 5
GZIP_OS_OS2 = 6
GZIP_OS_MACOS = 7
GZIP_OS_ZSYSTEM = 8
GZIP_OS_CPM = 9
GZIP_OS_TOPS20 = 10
GZIP_OS_WIN32 = 11
GZIP_OS_QDOS = 12
GZIP_OS_RISCOS = 13
GZIP_OS_UNKNOWN = 255

GZIP_FENCRYPT_LEN = 12


class GzipExtra(dpkt.Packet):
    __byte_order__ = '<'
    __hdr__ = (
        ('id', '2s', b''),
        ('len', 'H', 0)
    )


class Gzip(dpkt.Packet):
    __byte_order__ = '<'
    __hdr__ = (
        ('magic', '2s', GZIP_MAGIC),
        ('method', 'B', GZIP_MDEFLATE),
        ('flags', 'B', 0),
        ('mtime', 'I', 0),
        ('xflags', 'B', 0),
        ('os', 'B', GZIP_OS_UNIX),
    )

    def __init__(self, *args, **kwargs):
        self.extra = None
        self.filename = None
        self.comment = None
        super(Gzip, self).__init__(*args, **kwargs)

    def unpack(self, buf):
        super(Gzip, self).unpack(buf)
        if self.flags & GZIP_FEXTRA:
            if len(self.data) < 2:
                raise dpkt.NeedData('Gzip extra')
            n = struct.unpack('<H', self.data[:2])[0]
            if len(self.data) < 2 + n:
                raise dpkt.NeedData('Gzip extra')
            self.extra = GzipExtra(self.data[2:2 + n])
            self.data = self.data[2 + n:]
        if self.flags & GZIP_FNAME:
            n = self.data.find(b'\x00')
            if n == -1:
                raise dpkt.NeedData('Gzip end of file name not found')
            self.filename = self.data[:n].decode('utf-8')
            self.data = self.data[n + 1:]
        if self.flags & GZIP_FCOMMENT:
            n = self.data.find(b'\x00')
            if n == -1:
                raise dpkt.NeedData('Gzip end of comment not found')
            self.comment = self.data[:n]
            self.data = self.data[n + 1:]
        if self.flags & GZIP_FENCRYPT:
            if len(self.data) < GZIP_FENCRYPT_LEN:
                raise dpkt.NeedData('Gzip encrypt')
            self.data = self.data[GZIP_FENCRYPT_LEN:]  # XXX - skip
        if self.flags & GZIP_FHCRC:
            if len(self.data) < 2:
                raise dpkt.NeedData('Gzip hcrc')
            self.data = self.data[2:]  # XXX - skip

    def pack_hdr(self):
        l_ = []
        if self.extra:
            self.flags |= GZIP_FEXTRA
            s = bytes(self.extra)
            l_.append(struct.pack('<H', len(s)))
            l_.append(s)
        if self.filename:
            self.flags |= GZIP_FNAME
            l_.append(self.filename.encode('utf-8'))
            l_.append(b'\x00')
        if self.comment:
            self.flags |= GZIP_FCOMMENT
            l_.append(self.comment)
            l_.append(b'\x00')
        l_.insert(0, super(Gzip, self).pack_hdr())
        return b''.join(l_)

    def compress(self):
        """Compress self.data."""
        c = zlib.compressobj(
            zlib.Z_BEST_COMPRESSION,
            zlib.DEFLATED,
            -zlib.MAX_WBITS,
            zlib.DEF_MEM_LEVEL,
            zlib.Z_DEFAULT_STRATEGY,
        )
        c.compress(self.data)

        # .compress will return nothing if len(self.data) < the window size.
        self.data = c.flush()

    def decompress(self):
        """Return decompressed payload."""
        d = zlib.decompressobj(-zlib.MAX_WBITS)
        return d.decompress(self.data)


class TestGzip(object):
    """This data is created with the gzip command line tool"""

    @classmethod
    def setup_class(cls):
        from binascii import unhexlify
        cls.data = unhexlify(
            b'1F8B'  # magic
            b'080880C185560003'  # header
            b'68656C6C6F2E74787400'  # filename
            b'F348CDC9C95728CF2FCA4951E40200'  # data
            b'41E4A9B20D000000'  # checksum
        )
        cls.p = Gzip(cls.data)

    def test_method(self):
        assert (self.p.method == GZIP_MDEFLATE)

    def test_flags(self):
        assert (self.p.flags == GZIP_FNAME)

    def test_mtime(self):
        # Fri Jan 01 00:00:00 2016 UTC
        assert (self.p.mtime == 0x5685c180)

    def test_xflags(self):
        assert (self.p.xflags == 0)

    def test_os(self):
        assert (self.p.os == GZIP_OS_UNIX)

    def test_filename(self):
        assert (self.p.filename == "hello.txt")  # always str (utf-8)

    def test_decompress(self):
        assert (self.p.decompress() == b"Hello world!\n")  # always bytes


def test_flags_extra():
    import pytest
    from binascii import unhexlify

    buf = unhexlify(
        '1F8B'      # magic
        '08'        # method
        '04'        # flags (GZIP_FEXTRA)
        '80C18556'  # mtime
        '00'        # xflags
        '03'        # os
    )

    # not enough data to extract
    with pytest.raises(dpkt.NeedData, match='Gzip extra'):
        Gzip(buf)

    buf += unhexlify('0400')  # append the length of the fextra
    # not enough data to extract in extra section
    with pytest.raises(dpkt.NeedData, match='Gzip extra'):
        Gzip(buf)

    buf += unhexlify('494401000102')

    gzip = Gzip(buf)
    assert gzip.extra.id == b'ID'
    assert gzip.extra.len == 1
    assert gzip.data == unhexlify('0102')
    assert bytes(gzip) == buf


def test_flags_filename():
    import pytest
    from binascii import unhexlify

    buf = unhexlify(
        '1F8B'      # magic
        '08'        # method
        '08'        # flags (GZIP_FNAME)
        '80C18556'  # mtime
        '00'        # xflags
        '03'        # os

        '68656C6C6F2E747874'  # filename
    )
    # no trailing null character so unpacking fails
    with pytest.raises(dpkt.NeedData, match='Gzip end of file name not found'):
        Gzip(buf)

    buf += unhexlify('00')
    gzip = Gzip(buf)
    assert gzip.filename == 'hello.txt'
    assert gzip.data == b''
    assert bytes(gzip) == buf


def test_flags_comment():
    import pytest
    from binascii import unhexlify

    buf = unhexlify(
        '1F8B'      # magic
        '08'        # method
        '10'        # flags (GZIP_FCOMMENT)
        '80C18556'  # mtime
        '00'        # xflags
        '03'        # os

        '68656C6C6F2E747874'  # comment
    )
    # no trailing null character so unpacking fails
    with pytest.raises(dpkt.NeedData, match='Gzip end of comment not found'):
        Gzip(buf)

    buf += unhexlify('00')

    gzip = Gzip(buf)
    assert gzip.comment == b'hello.txt'
    assert gzip.data == b''
    assert bytes(gzip) == buf


def test_flags_encrypt():
    import pytest
    from binascii import unhexlify

    buf_header = unhexlify(
        '1F8B'      # magic
        '08'        # method
        '20'        # flags (GZIP_FENCRYPT)
        '80C18556'  # mtime
        '00'        # xflags
        '03'        # os
    )
    # not enough data
    with pytest.raises(dpkt.NeedData, match='Gzip encrypt'):
        Gzip(buf_header)

    encrypted_buffer = unhexlify('0102030405060708090a0b0c')
    data = unhexlify('0123456789abcdef')

    gzip = Gzip(buf_header + encrypted_buffer + data)
    assert gzip.data == data
    assert bytes(gzip) == buf_header + data


def test_flags_hcrc():
    import pytest
    from binascii import unhexlify

    buf_header = unhexlify(
        '1F8B'      # magic
        '08'        # method
        '02'        # flags (GZIP_FHCRC)
        '80C18556'  # mtime
        '00'        # xflags
        '03'        # os
    )
    # not enough data
    with pytest.raises(dpkt.NeedData, match='Gzip hcrc'):
        Gzip(buf_header)

    hcrc = unhexlify('0102')
    data = unhexlify('0123456789abcdef')
    gzip = Gzip(buf_header + hcrc + data)

    assert gzip.data == data
    assert bytes(gzip) == buf_header + data


def test_compress():
    from binascii import unhexlify

    buf_header = unhexlify(
        '1F8B'      # magic
        '08'        # method
        '00'        # flags (NONE)
        '80C18556'  # mtime
        '00'        # xflags
        '03'        # os
    )

    plain_text = b'Hello world!\n'
    compressed_text = unhexlify('F348CDC9C95728CF2FCA4951E40200')

    gzip = Gzip(buf_header + plain_text)
    assert gzip.data == plain_text

    gzip.compress()
    assert gzip.data == compressed_text
    assert bytes(gzip) == buf_header + compressed_text

    assert gzip.decompress() == plain_text
