# $Id$
# -*- coding: utf-8 -*-
"""Snoop file format."""
from __future__ import absolute_import

import time
from abc import abstractmethod

from . import dpkt
from .compat import intround

# RFC 1761

SNOOP_MAGIC = 0x736E6F6F70000000

SNOOP_VERSION = 2

SDL_8023 = 0
SDL_8024 = 1
SDL_8025 = 2
SDL_8026 = 3
SDL_ETHER = 4
SDL_HDLC = 5
SDL_CHSYNC = 6
SDL_IBMCC = 7
SDL_FDDI = 8
SDL_OTHER = 9

dltoff = {SDL_ETHER: 14}


class PktHdr(dpkt.Packet):
    """snoop packet header.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of snoop packet header.
        TODO.
    """

    __byte_order__ = '!'
    __hdr__ = (
        # 32-bit unsigned integer representing the length in octets of the
        # captured packet as received via a network.
        ('orig_len', 'I', 0),
        # 32-bit unsigned integer representing the length of the Packet Data
        # field.  This is the number of octets of the captured packet that are
        # included in this packet record.  If the received packet was
        # truncated, the Included Length field will be less than the Original
        # Length field.
        ('incl_len', 'I', 0),
        # 32-bit unsigned integer representing the total length of this packet
        # record in octets.  This includes the 24 octets of descriptive
        # information, the length of the Packet Data field, and the length of
        # the Pad field.
        ('rec_len', 'I', 0),
        # 32-bit unsigned integer representing the number of packets that were
        # lost by the system that created the packet file between the first
        # packet record in the file and this one.  Packets may be lost because
        # of insufficient resources in the capturing system, or for other
        # reasons.  Note: some implementations lack the ability to count
        # dropped packets.  Those implementations may set the cumulative drops
        # value to zero.
        ('cum_drops', 'I', 0),
        # 32-bit unsigned integer representing the time, in seconds since
        # January 1, 1970, when the packet arrived.
        ('ts_sec', 'I', 0),
        # 32-bit unsigned integer representing microsecond resolution of packet
        # arrival time.
        ('ts_usec', 'I', 0),
    )


class FileHdr(dpkt.Packet):
    """snoop file header.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of snoop file header.
        TODO.
    """

    __byte_order__ = '!'
    __hdr__ = (
        ('magic', 'Q', SNOOP_MAGIC),
        ('v', 'I', SNOOP_VERSION),
        ('linktype', 'I', SDL_ETHER),
    )


class FileWriter(object):
    def __init__(self, fileobj):
        self._f = fileobj
        self.write = self._f.write

    def close(self):
        self._f.close()

    def writepkt(self, pkt, ts=None):
        """Write single packet and optional timestamp to file.

        Args:
            pkt: `bytes` will be called on this and written to file.
            ts (float): Timestamp in seconds. Defaults to current time.
       """
        if ts is None:
            ts = time.time()

        self.writepkt_time(bytes(pkt), ts)

    @abstractmethod
    def writepkt_time(self, pkt, ts):
        """Write single packet and its timestamp to file.

        Args:
            pkt (bytes): Some `bytes` to write to the file
            ts (float): Timestamp in seconds
        """
        pass


class Writer(FileWriter):
    """Simple snoop dumpfile writer.

    TODO: Longer class information....

    Attributes:
        TODO.
    """
    precision_multiplier = 1000000

    def __init__(self, fileobj, linktype=SDL_ETHER):
        super(Writer, self).__init__(fileobj)
        fh = FileHdr(linktype=linktype)

        self._PktHdr = PktHdr()
        self._pack_hdr = self._PktHdr._pack_hdr

        self.write(bytes(fh))

    def writepkt_time(self, pkt, ts):
        """Write single packet and its timestamp to file.

        Args:
            pkt (bytes): Some `bytes` to write to the file
            ts (float): Timestamp in seconds
       """
        pkt_len = len(pkt)
        pad_len = (4 - pkt_len) & 3

        pkt_header = self._pack_hdr(
            pkt_len,
            pkt_len,
            PktHdr.__hdr_len__ + pkt_len + pad_len,
            0,
            int(ts),
            intround(ts % 1 * self.precision_multiplier),
        )
        self.write(pkt_header + pkt + b'\x00' * pad_len)

    def writepkts(self, pkts):
        """Write an iterable of packets to file.

        Timestamps should be in seconds.
        Packets must be of type `bytes` as they will not be cast.

        Args:
            pkts: iterable containing (ts, pkt)
        """
        # take local references to these variables so we don't need to
        # dereference every time in the loop
        write = self.write
        pack_hdr = self._pack_hdr

        for ts, pkt in pkts:
            pkt_len = len(pkt)
            pad_len = (4 - pkt_len) & 3

            pkt_header = pack_hdr(
                pkt_len,
                pkt_len,
                PktHdr.__hdr_len__ + pkt_len + pad_len,
                0,
                int(ts),
                intround(ts % 1 * self.precision_multiplier),
            )
            write(pkt_header + pkt + b'\x00' * pad_len)


class FileReader(object):
    def __init__(self, fileobj):
        self.name = getattr(fileobj, 'name', '<%s>' % fileobj.__class__.__name__)
        self._f = fileobj
        self.filter = ''

    @property
    def fd(self):
        return self._f.fileno()

    def fileno(self):
        return self.fd

    def setfilter(self, value, optimize=1):
        raise NotImplementedError

    def readpkts(self):
        return list(self)

    def dispatch(self, cnt, callback, *args):
        """Collect and process packets with a user callback.

        Return the number of packets processed, or 0 for a savefile.

        Arguments:

        cnt      -- number of packets to process;
                    or 0 to process all packets until EOF
        callback -- function with (timestamp, pkt, *args) prototype
        *args    -- optional arguments passed to callback on execution
       """
        processed = 0
        if cnt > 0:
            for _ in range(cnt):
                try:
                    ts, pkt = next(self)
                except StopIteration:
                    break
                callback(ts, pkt, *args)
                processed += 1
        else:
            for ts, pkt in self:
                callback(ts, pkt, *args)
                processed += 1
        return processed

    def loop(self, callback, *args):
        """
        Convenience method which will apply the callback to all packets.

        Returns the number of packets processed.

        Arguments:

        callback -- function with (timestamp, pkt, *args) prototype
        *args    -- optional arguments passed to callback on execution
        """
        return self.dispatch(0, callback, *args)

    def __iter__(self):
        return self


class Reader(FileReader):
    """Simple pypcap-compatible snoop file reader.

    TODO: Longer class information....

    Attributes:
        TODO.
    """

    def __init__(self, fileobj):
        super(Reader, self).__init__(fileobj)

        buf = self._f.read(FileHdr.__hdr_len__)
        self._fh = FileHdr(buf)
        self._ph = PktHdr

        if self._fh.magic != SNOOP_MAGIC:
            raise ValueError('invalid snoop header')

        self.dloff = dltoff[self._fh.linktype]

    def datalink(self):
        return self._fh.linktype

    def __next__(self):
        buf = self._f.read(self._ph.__hdr_len__)
        if not buf:
            raise StopIteration

        hdr = self._ph(buf)
        buf = self._f.read(hdr.rec_len - self._ph.__hdr_len__)
        return (hdr.ts_sec + (hdr.ts_usec / 1000000.0), buf[:hdr.incl_len])
    next = __next__


def test_snoop_pkt_header():
    from binascii import unhexlify

    buf = unhexlify(
        '000000010000000200000003000000040000000500000006'
    )

    pkt = PktHdr(buf)
    assert pkt.orig_len == 1
    assert pkt.incl_len == 2
    assert pkt.rec_len == 3
    assert pkt.cum_drops == 4
    assert pkt.ts_sec == 5
    assert pkt.ts_usec == 6
    assert bytes(pkt) == buf


def test_snoop_file_header():
    from binascii import unhexlify

    buf = unhexlify(
        '000000000000000b000000160000014d'
    )
    hdr = FileHdr(buf)
    assert hdr.magic == 11
    assert hdr.v == 22
    assert hdr.linktype == 333


class TestSnoopWriter(object):
    @classmethod
    def setup_class(cls):
        from .compat import BytesIO
        from binascii import unhexlify

        cls.fobj = BytesIO()
        # write the file header only
        cls.writer = Writer(cls.fobj)

        cls.file_header = unhexlify(
            '736e6f6f700000000000000200000004'
        )

        cls.pkt = unhexlify(
            '000000010000000200000003000000040000000500000006'
        )

        cls.pkt_and_header = unhexlify(
            '00000018'  # orig_len
            '00000018'  # incl_len
            '00000030'  # rec_len
            '00000000'  # cum_drops
            '00000000'  # ts_sec
            '00000000'  # ts_usec

            # data
            '000000010000000200000003000000040000000500000006'
        )

    def test_snoop_file_writer_filehdr(self):
        # jump to the start and read the file header
        self.fobj.seek(0)
        buf = self.fobj.read()
        assert buf == self.file_header

    def test_writepkt(self):
        loc = self.fobj.tell()
        self.writer.writepkt(self.pkt)

        # jump back to just before the writing of the packet
        self.fobj.seek(loc)
        # read the packet back in
        buf = self.fobj.read()
        # compare everything except the timestamp
        assert buf[:16] == self.pkt_and_header[:16]
        assert buf[24:] == self.pkt_and_header[24:]

    def test_writepkt_time(self):
        loc = self.fobj.tell()
        self.writer.writepkt_time(self.pkt, 0)
        self.fobj.seek(loc)
        # read the packet we just wrote
        buf = self.fobj.read()
        assert buf == self.pkt_and_header

    def test_writepkts(self):
        loc = self.fobj.tell()
        self.writer.writepkts([
            (0, self.pkt),
            (1, self.pkt),
            (2, self.pkt),
        ])
        self.fobj.seek(loc)
        buf = self.fobj.read()

        pkt_len = len(self.pkt_and_header)
        # chunk up the file and check each packet
        for idx in range(0, 3):
            pkt = buf[idx * pkt_len:(idx + 1) * pkt_len]

            assert pkt[:16] == self.pkt_and_header[:16]
            assert pkt[16:20] == dpkt.struct.pack('>I', idx)
            assert pkt[20:] == self.pkt_and_header[20:]

    def test_snoop_writer_close(self):
        assert not self.fobj.closed

        # check that the underlying file object is closed
        self.writer.close()
        assert self.fobj.closed


class TestSnoopReader(object):
    @classmethod
    def setup_class(cls):
        from binascii import unhexlify

        cls.header = unhexlify(
            '736e6f6f700000000000000200000004'
        )

        cls.pkt_header = unhexlify(
            '00000018'  # orig_len
            '00000018'  # incl_len
            '00000030'  # rec_len
            '00000000'  # cum_drops
            '00000000'  # ts_sec
            '00000000'  # ts_usec
        )

        cls.pkt_bytes = unhexlify(
            # data
            '000000010000000200000003000000040000000500000006'
        )

    def setup_method(self):
        from .compat import BytesIO

        self.fobj = BytesIO(
            self.header + self.pkt_header + self.pkt_bytes
        )
        self.reader = Reader(self.fobj)

    def test_open(self):
        assert self.reader.dloff == 14
        assert self.reader.datalink() == SDL_ETHER

    def test_invalid_magic(self):
        import pytest

        self.fobj.seek(0)
        self.fobj.write(b'\x00' * 4)
        self.fobj.seek(0)

        with pytest.raises(ValueError, match='invalid snoop header'):
            Reader(self.fobj)

    def test_read_pkt(self):
        ts, pkt = next(self.reader)
        assert ts == 0
        assert pkt == self.pkt_bytes

    def test_readpkts(self):
        pkts = self.reader.readpkts()
        assert len(pkts) == 1
        ts, buf = pkts[0]
        assert ts == 0
        assert buf == self.pkt_bytes


class TestFileWriter(object):
    def setup_method(self):
        from .compat import BytesIO

        self.fobj = BytesIO()
        self.writer = FileWriter(self.fobj)

    def test_write(self):
        buf = b'\x01' * 10
        self.writer.write(buf)
        self.fobj.seek(0)
        assert self.fobj.read() == buf

    def test_close(self):
        assert not self.fobj.closed
        self.writer.close()
        assert self.fobj.closed


class TestFileReader(object):
    """
    Testing for the FileReader superclass which Reader inherits from.
    """
    pkts = [
        (0, b'000001'),
        (1, b'000002'),
        (2, b'000003'),
    ]

    class SampleReader(FileReader):
        """
        Very simple class which returns index as timestamp, and
        unparsed buffer as packet
        """
        def __init__(self, fobj):
            super(TestFileReader.SampleReader, self).__init__(fobj)

            self._iter = iter(TestFileReader.pkts)

        def __next__(self):
            return next(self._iter)
        next = __next__

    def setup_method(self):
        import tempfile

        self.fd = tempfile.TemporaryFile()
        self.reader = self.SampleReader(self.fd)

    def test_attributes(self):
        import pytest

        assert self.reader.name == self.fd.name
        assert self.reader.fd == self.fd.fileno()
        assert self.reader.fileno() == self.fd.fileno()
        assert self.reader.filter == ''

        with pytest.raises(NotImplementedError):
            self.reader.setfilter(1, 2)

    def test_readpkts_list(self):
        pkts = self.reader.readpkts()
        print(len(pkts))
        for idx, (ts, buf) in enumerate(pkts):
            assert ts == idx
            assert buf == self.pkts[idx][1]

    def test_readpkts_iter(self):
        for idx, (ts, buf) in enumerate(self.reader):
            assert ts == idx
            assert buf == self.pkts[idx][1]

    def test_dispatch_all(self):
        assert self.reader.dispatch(0, lambda ts, pkt: None) == 3

    def test_dispatch_some(self):
        assert self.reader.dispatch(2, lambda ts, pkt: None) == 2

    def test_dispatch_termination(self):
        assert self.reader.dispatch(20, lambda ts, pkt: None) == 3

    def test_loop(self):
        class Count:
            counter = 0

            @classmethod
            def inc(cls):
                cls.counter += 1

        assert self.reader.loop(lambda ts, pkt: Count.inc()) == 3
        assert Count.counter == 3

    def test_next(self):
        ts, buf = next(self.reader)
        assert ts == 0
        assert buf == self.pkts[0][1]
