# $Id: pcap.py 77 2011-01-06 15:59:38Z dugsong $
# -*- coding: utf-8 -*-
"""Libpcap file format."""
from __future__ import print_function
from __future__ import absolute_import

import sys
import time
from decimal import Decimal

from . import dpkt
from .compat import intround

# big endian magics
TCPDUMP_MAGIC = 0xa1b2c3d4
TCPDUMP_MAGIC_NANO = 0xa1b23c4d
MODPCAP_MAGIC = 0xa1b2cd34

# little endian magics
PMUDPCT_MAGIC = 0xd4c3b2a1
PMUDPCT_MAGIC_NANO = 0x4d3cb2a1
PACPDOM_MAGIC = 0x34cdb2a1

PCAP_VERSION_MAJOR = 2
PCAP_VERSION_MINOR = 4

# see http://www.tcpdump.org/linktypes.html for explanations
DLT_NULL = 0
DLT_EN10MB = 1
DLT_EN3MB = 2
DLT_AX25 = 3
DLT_PRONET = 4
DLT_CHAOS = 5
DLT_IEEE802 = 6
DLT_ARCNET = 7
DLT_SLIP = 8
DLT_PPP = 9
DLT_FDDI = 10
DLT_PFSYNC = 18
DLT_PPP_SERIAL = 50
DLT_PPP_ETHER = 51
DLT_ATM_RFC1483 = 100
DLT_RAW = 101
DLT_C_HDLC = 104
DLT_IEEE802_11 = 105
DLT_FRELAY = 107
DLT_LOOP = 108
DLT_LINUX_SLL = 113
DLT_LTALK = 114
DLT_PFLOG = 117
DLT_PRISM_HEADER = 119
DLT_IP_OVER_FC = 122
DLT_SUNATM = 123
DLT_IEEE802_11_RADIO = 127
DLT_ARCNET_LINUX = 129
DLT_APPLE_IP_OVER_IEEE1394 = 138
DLT_MTP2_WITH_PHDR = 139
DLT_MTP2 = 140
DLT_MTP3 = 141
DLT_SCCP = 142
DLT_DOCSIS = 143
DLT_LINUX_IRDA = 144
DLT_USER0 = 147
DLT_USER1 = 148
DLT_USER2 = 149
DLT_USER3 = 150
DLT_USER4 = 151
DLT_USER5 = 152
DLT_USER6 = 153
DLT_USER7 = 154
DLT_USER8 = 155
DLT_USER9 = 156
DLT_USER10 = 157
DLT_USER11 = 158
DLT_USER12 = 159
DLT_USER13 = 160
DLT_USER14 = 161
DLT_USER15 = 162
DLT_IEEE802_11_RADIO_AVS = 163
DLT_BACNET_MS_TP = 165
DLT_PPP_PPPD = 166
DLT_GPRS_LLC = 169
DLT_GPF_T = 170
DLT_GPF_F = 171
DLT_LINUX_LAPD = 177
DLT_BLUETOOTH_HCI_H4 = 187
DLT_USB_LINUX = 189
DLT_PPI = 192
DLT_IEEE802_15_4 = 195
DLT_SITA = 196
DLT_ERF = 197
DLT_BLUETOOTH_HCI_H4_WITH_PHDR = 201
DLT_AX25_KISS = 202
DLT_LAPD = 203
DLT_PPP_WITH_DIR = 204
DLT_C_HDLC_WITH_DIR = 205
DLT_FRELAY_WITH_DIR = 206
DLT_IPMB_LINUX = 209
DLT_IEEE802_15_4_NONASK_PHY = 215
DLT_USB_LINUX_MMAPPED = 220
DLT_FC_2 = 224
DLT_FC_2_WITH_FRAME_DELIMS = 225
DLT_IPNET = 226
DLT_CAN_SOCKETCAN = 227
DLT_IPV4 = 228
DLT_IPV6 = 229
DLT_IEEE802_15_4_NOFCS = 230
DLT_DBUS = 231
DLT_DVB_CI = 235
DLT_MUX27010 = 236
DLT_STANAG_5066_D_PDU = 237
DLT_NFLOG = 239
DLT_NETANALYZER = 240
DLT_NETANALYZER_TRANSPARENT = 241
DLT_IPOIB = 242
DLT_MPEG_2_TS = 243
DLT_NG40 = 244
DLT_NFC_LLCP = 245
DLT_INFINIBAND = 247
DLT_SCTP = 248
DLT_USBPCAP = 249
DLT_RTAC_SERIAL = 250
DLT_BLUETOOTH_LE_LL = 251
DLT_NETLINK = 253
DLT_BLUETOOTH_LINUX_MONITOR = 253
DLT_BLUETOOTH_BREDR_BB = 255
DLT_BLUETOOTH_LE_LL_WITH_PHDR = 256
DLT_PROFIBUS_DL = 257
DLT_PKTAP = 258
DLT_EPON = 259
DLT_IPMI_HPM_2 = 260
DLT_ZWAVE_R1_R2 = 261
DLT_ZWAVE_R3 = 262
DLT_WATTSTOPPER_DLM = 263
DLT_ISO_14443 = 264
DLT_LINUX_SLL2 = 276

if sys.platform.find('openbsd') != -1:
    DLT_LOOP = 12
    DLT_RAW = 14
else:
    DLT_LOOP = 108
    DLT_RAW = 12

dltoff = {DLT_NULL: 4, DLT_EN10MB: 14, DLT_IEEE802: 22, DLT_ARCNET: 6,
          DLT_SLIP: 16, DLT_PPP: 4, DLT_FDDI: 21, DLT_PFLOG: 48, DLT_PFSYNC: 4,
          DLT_LOOP: 4, DLT_LINUX_SLL: 16, DLT_LINUX_SLL2: 20}


class PktHdr(dpkt.Packet):
    """pcap packet header.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of pcap header.
        TODO.
    """
    __hdr__ = (
        ('tv_sec', 'I', 0),
        ('tv_usec', 'I', 0),
        ('caplen', 'I', 0),
        ('len', 'I', 0),
    )


class PktModHdr(dpkt.Packet):
    """modified pcap packet header.
    https://wiki.wireshark.org/Development/LibpcapFileFormat#modified-pcap

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of pcap header.
        TODO.
    """
    __hdr__ = (
        ('tv_sec', 'I', 0),
        ('tv_usec', 'I', 0),
        ('caplen', 'I', 0),
        ('len', 'I', 0),
        ('ifindex', 'I', 0),
        ('protocol', 'H', 0),
        ('pkt_type', 'B', 0),
        ('pad', 'B', 0),
    )


class LEPktHdr(PktHdr):
    __byte_order__ = '<'

class LEPktModHdr(PktModHdr):
    __byte_order__ = '<'


MAGIC_TO_PKT_HDR = {
    TCPDUMP_MAGIC: PktHdr,
    TCPDUMP_MAGIC_NANO: PktHdr,
    MODPCAP_MAGIC: PktModHdr,
    PMUDPCT_MAGIC: LEPktHdr,
    PMUDPCT_MAGIC_NANO: LEPktHdr,
    PACPDOM_MAGIC: LEPktModHdr
}


class FileHdr(dpkt.Packet):
    """pcap file header.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of pcap file header.
        TODO.
    """
    __hdr__ = (
        ('magic', 'I', TCPDUMP_MAGIC),
        ('v_major', 'H', PCAP_VERSION_MAJOR),
        ('v_minor', 'H', PCAP_VERSION_MINOR),
        ('thiszone', 'I', 0),
        ('sigfigs', 'I', 0),
        ('snaplen', 'I', 1500),
        ('linktype', 'I', 1),
    )


class LEFileHdr(FileHdr):
    __byte_order__ = '<'


class Writer(object):
    """Simple pcap dumpfile writer.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of simple pcap dumpfile writer.
        TODO.
    """
    __le = sys.byteorder == 'little'

    def __init__(self, fileobj, snaplen=1500, linktype=DLT_EN10MB, nano=False):
        self.__f = fileobj
        self._precision = 9 if nano else 6
        self._precision_multiplier = 10**self._precision

        magic = TCPDUMP_MAGIC_NANO if nano else TCPDUMP_MAGIC
        if self.__le:
            fh = LEFileHdr(snaplen=snaplen, linktype=linktype, magic=magic)
            self._PktHdr = LEPktHdr()
        else:
            fh = FileHdr(snaplen=snaplen, linktype=linktype, magic=magic)
            self._PktHdr = PktHdr()

        self._pack_hdr = self._PktHdr._pack_hdr
        self.__f.write(bytes(fh))

    def writepkt(self, pkt, ts=None):
        """Write single packet and optional timestamp to file.

        Args:
            pkt: `bytes` will be called on this and written to file.
            ts (float): Timestamp in seconds. Defaults to current time.
        """
        if ts is None:
            ts = time.time()

        self.writepkt_time(bytes(pkt), ts)

    def writepkt_time(self, pkt, ts):
        """Write single packet and its timestamp to file.

        Args:
            pkt (bytes): Some `bytes` to write to the file
            ts (float): Timestamp in seconds
        """
        n = len(pkt)
        sec = int(ts)
        usec = intround(ts % 1 * self._precision_multiplier)
        ph = self._pack_hdr(sec, usec, n, n)
        self.__f.write(ph + pkt)

    def writepkts(self, pkts):
        """Write an iterable of packets to file.

        Timestamps should be in seconds.
        Packets must be of type `bytes` as they will not be cast.

        Args:
            pkts: iterable containing (ts, pkt)
        """
        fd = self.__f
        pack_hdr = self._pack_hdr
        precision_multiplier = self._precision_multiplier

        for ts, pkt in pkts:
            n = len(pkt)
            sec = int(ts)
            usec = intround(ts % 1 * precision_multiplier)
            ph = pack_hdr(sec, usec, n, n)
            fd.write(ph + pkt)

    def close(self):
        self.__f.close()


class Reader(object):
    """Simple pypcap-compatible pcap file reader.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of simple pypcap-compatible pcap file reader.
        TODO.
    """
    def __init__(self, fileobj):
        self.name = getattr(fileobj, 'name', '<%s>' % fileobj.__class__.__name__)
        self.__f = fileobj
        buf = self.__f.read(FileHdr.__hdr_len__)
        self.__fh = FileHdr(buf)

        # save magic
        magic = self.__fh.magic

        if magic in (PMUDPCT_MAGIC, PMUDPCT_MAGIC_NANO, PACPDOM_MAGIC):
            self.__fh = LEFileHdr(buf)

        if magic not in MAGIC_TO_PKT_HDR:
            raise ValueError('invalid tcpdump header')

        self.__ph = MAGIC_TO_PKT_HDR[magic]


        if self.__fh.linktype in dltoff:
            self.dloff = dltoff[self.__fh.linktype]
        else:
            self.dloff = 0
        self._divisor = Decimal('1E9') if magic in (TCPDUMP_MAGIC_NANO, PMUDPCT_MAGIC_NANO) else 1E6
        self.snaplen = self.__fh.snaplen
        self.filter = ''
        self.__iter = iter(self)

    @property
    def fd(self):
        return self.__f.fileno()

    def fileno(self):
        return self.fd

    def datalink(self):
        return self.__fh.linktype

    def setfilter(self, value, optimize=1):
        raise NotImplementedError

    def readpkts(self):
        return list(self)

    def __next__(self):
        return next(self.__iter)
    next = __next__  # Python 2 compat

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
                    ts, pkt = next(iter(self))
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
        self.dispatch(0, callback, *args)

    def __iter__(self):
        while 1:
            buf = self.__f.read(self.__ph.__hdr_len__)
            if not buf:
                break
            hdr = self.__ph(buf)
            buf = self.__f.read(hdr.caplen)
            yield (hdr.tv_sec + (hdr.tv_usec / self._divisor), buf)


class UniversalReader(object):
    """
    Universal pcap reader for the libpcap and pcapng file formats
    """
    def __new__(cls, fileobj):
        try:
            pcap = Reader(fileobj)
        except ValueError as e1:
            fileobj.seek(0)
            try:
                from . import pcapng
                pcap = pcapng.Reader(fileobj)
            except ValueError as e2:
                raise ValueError('unknown pcap format; libpcap error: %s, pcapng error: %s' % (e1, e2))
        return pcap


################################################################################
#                                    TESTS                                     #
################################################################################

class TryExceptException:
    def __init__(self, exception_type, msg=''):
        self.exception_type = exception_type
        self.msg = msg

    def __call__(self, f, *args, **kwargs):
        def wrapper(*args, **kwargs):
            try:
                f()
            except self.exception_type as e:
                if self.msg:
                    assert str(e) == self.msg
            else:
                raise Exception("There should have been an Exception raised")
        return wrapper


@TryExceptException(Exception, msg='There should have been an Exception raised')
def test_TryExceptException():
    """Check that we can catch a function which does not throw an exception when it is supposed to"""
    @TryExceptException(NotImplementedError)
    def fun():
        pass

    try:
        fun()
    except Exception as e:
        raise e


def test_pcap_endian():
    be = b'\xa1\xb2\xc3\xd4\x00\x02\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x60\x00\x00\x00\x01'
    le = b'\xd4\xc3\xb2\xa1\x02\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x60\x00\x00\x00\x01\x00\x00\x00'
    befh = FileHdr(be)
    lefh = LEFileHdr(le)
    assert (befh.linktype == lefh.linktype)


class TestData():
    pcap = (  # full libpcap file with one packet
        b'\xd4\xc3\xb2\xa1\x02\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\x00\x00\x01\x00\x00\x00'
        b'\xb2\x67\x4a\x42\xae\x91\x07\x00\x46\x00\x00\x00\x46\x00\x00\x00\x00\xc0\x9f\x32\x41\x8c\x00\xe0'
        b'\x18\xb1\x0c\xad\x08\x00\x45\x00\x00\x38\x00\x00\x40\x00\x40\x11\x65\x47\xc0\xa8\xaa\x08\xc0\xa8'
        b'\xaa\x14\x80\x1b\x00\x35\x00\x24\x85\xed'
    )
    modified_pcap = (
        b'\x34\xcd\xb2\xa1\x02\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x01\x00\x00\x00'
        b'\x3c\xfb\x80\x61\x6d\x32\x08\x00\x03\x00\x00\x00\x72\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        b'\xff\xff\xff'

    )


def test_reader():
    import pytest

    data = TestData().pcap

    # --- BytesIO tests ---
    from .compat import BytesIO

    # BytesIO
    fobj = BytesIO(data)
    reader = Reader(fobj)
    assert reader.name == '<BytesIO>'
    _, buf1 = next(iter(reader))
    assert buf1 == data[FileHdr.__hdr_len__ + PktHdr.__hdr_len__:]
    assert reader.datalink() == 1

    with pytest.raises(NotImplementedError):
        reader.setfilter(1, 2)

    # --- dispatch() tests ---

    # test count = 0
    fobj.seek(0)
    reader = Reader(fobj)
    assert reader.dispatch(0, lambda ts, pkt: None) == 1

    # test count > 0
    fobj.seek(0)
    reader = Reader(fobj)
    assert reader.dispatch(4, lambda ts, pkt: None) == 1

    # test iterative dispatch
    fobj.seek(0)
    reader = Reader(fobj)
    assert reader.dispatch(1, lambda ts, pkt: None) == 1
    assert reader.dispatch(1, lambda ts, pkt: None) == 0

    # test loop() over all packets
    fobj.seek(0)
    reader = Reader(fobj)

    class Count:
        counter = 0

        @classmethod
        def inc(cls):
            cls.counter += 1

    reader.loop(lambda ts, pkt: Count.inc())
    assert Count.counter == 1


def test_reader_dloff():
    from binascii import unhexlify
    buf_filehdr = unhexlify(
        'a1b2c3d4'    # TCPDUMP_MAGIC
        '0001'        # v_major
        '0002'        # v_minor
        '00000000'    # thiszone
        '00000000'    # sigfigs
        '00000100'    # snaplen
        '00000023'    # linktype (not known)
    )

    buf_pkthdr = unhexlify(
        '00000003'  # tv_sec
        '00000005'  # tv_usec
        '00000004'  # caplen
        '00000004'  # len
    )

    from .compat import BytesIO
    fobj = BytesIO(buf_filehdr + buf_pkthdr + b'\x11' * 4)
    reader = Reader(fobj)

    # confirm that if the linktype is unknown, it defaults to 0
    assert reader.dloff == 0

    assert next(reader) == (3.000005, b'\x11' * 4)


@TryExceptException(ValueError, msg="invalid tcpdump header")
def test_reader_badheader():
    from .compat import BytesIO
    fobj = BytesIO(b'\x00' * 24)
    _ = Reader(fobj)  # noqa


def test_reader_fd():
    data = TestData().pcap

    import tempfile
    with tempfile.TemporaryFile() as fd:
        fd.write(data)
        fd.seek(0)
        reader = Reader(fd)
        assert reader.fd == fd.fileno()
        assert reader.fileno() == fd.fileno()


def test_reader_modified_pcap_type():
    data = TestData().modified_pcap

    import tempfile
    with tempfile.TemporaryFile() as fd:
        fd.write(data)
        fd.seek(0)
        reader = Reader(fd)
        assert reader.fd == fd.fileno()
        assert reader.fileno() == fd.fileno()

        timestamp, pkts = next(reader)
        assert pkts == 3 * b'\xff'
        assert timestamp == 1635842876.537197000


class WriterTestWrap:
    """
    Decorate a writer test function with an instance of this class.

    The test will be provided with a writer object, which it should write some pkts to.

    After the test has run, the BytesIO object will be passed to a Reader,
    which will compare each pkt to the return value of the test.
    """
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __call__(self, f, *args, **kwargs):
        def wrapper(*args, **kwargs):
            from .compat import BytesIO
            for little_endian in [True, False]:
                fobj = BytesIO()
                _sysle = Writer._Writer__le
                Writer._Writer__le = little_endian
                f.__globals__['writer'] = Writer(fobj, **self.kwargs.get('writer', {}))
                f.__globals__['fobj'] = fobj
                pkts = f(*args, **kwargs)
                fobj.flush()
                fobj.seek(0)

                assert pkts
                for (ts_out, pkt_out), (ts_in, pkt_in) in zip(pkts, Reader(fobj).readpkts()):
                    assert ts_out == ts_in
                    assert pkt_out == pkt_in

                # 'noqa' for flake8 to ignore these since writer was injected into globals
                writer.close()  # noqa
                Writer._Writer__le = _sysle
        return wrapper


@WriterTestWrap()
def test_writer_precision_normal():
    ts, pkt = 1454725786.526401, b'foo'
    writer.writepkt(pkt, ts=ts)  # noqa
    return [(ts, pkt)]


@WriterTestWrap(writer={'nano': True})
def test_writer_precision_nano():
    ts, pkt = Decimal('1454725786.010203045'), b'foo'
    writer.writepkt(pkt, ts=ts)  # noqa
    return [(ts, pkt)]


@WriterTestWrap(writer={'nano': False})
def test_writer_precision_nano_fail():
    """if writer is not set to nano, supplying this timestamp should be truncated"""
    ts, pkt = (Decimal('1454725786.010203045'), b'foo')
    writer.writepkt(pkt, ts=ts)  # noqa
    return [(1454725786.010203, pkt)]


@WriterTestWrap()
def test_writepkt_no_time():
    ts, pkt = 1454725786.526401, b'foooo'
    _tmp = time.time
    time.time = lambda: ts
    writer.writepkt(pkt)  # noqa
    time.time = _tmp
    return [(ts, pkt)]


@WriterTestWrap(writer={'snaplen': 10})
def test_writepkt_snaplen():
    ts, pkt = 1454725786.526401, b'foooo'
    writer.writepkt(pkt, ts)  # noqa
    return [(ts, pkt)]


@WriterTestWrap()
def test_writepkt_with_time():
    ts, pkt = 1454725786.526401, b'foooo'
    writer.writepkt(pkt, ts)  # noqa
    return [(ts, pkt)]


@WriterTestWrap()
def test_writepkt_time():
    ts, pkt = 1454725786.526401, b'foooo'
    writer.writepkt_time(pkt, ts)  # noqa
    return [(ts, pkt)]


@WriterTestWrap()
def test_writepkts():
    """writing multiple packets from a list"""
    pkts = [
        (1454725786.526401, b"fooo"),
        (1454725787.526401, b"barr"),
        (3243204320.093211, b"grill"),
        (1454725789.526401, b"lol"),
    ]

    writer.writepkts(pkts)  # noqa
    return pkts


def test_universal_reader():
    import pytest
    from .compat import BytesIO
    from . import pcapng

    # libpcap
    data = TestData().pcap
    fobj = BytesIO(data)
    reader = UniversalReader(fobj)
    assert isinstance(reader, Reader)

    # pcapng
    data = pcapng.define_testdata().valid_pcapng
    fobj = BytesIO(data)
    reader = UniversalReader(fobj)
    assert isinstance(reader, pcapng.Reader)

    # unknown
    fobj = BytesIO(b'\x42' * 1000)
    with pytest.raises(ValueError):
        reader = UniversalReader(fobj)
