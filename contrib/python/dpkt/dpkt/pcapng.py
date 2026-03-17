"""pcap Next Generation file format"""
# Spec: https://pcapng.github.io/pcapng/

# pylint: disable=no-member
# pylint: disable=attribute-defined-outside-init
from __future__ import print_function
from __future__ import absolute_import

from struct import pack as struct_pack, unpack as struct_unpack
from time import time
import sys

from . import dpkt
from .compat import BytesIO, intround

BYTE_ORDER_MAGIC = 0x1A2B3C4D
BYTE_ORDER_MAGIC_LE = 0x4D3C2B1A

PCAPNG_VERSION_MAJOR = 1
PCAPNG_VERSION_MINOR = 0

# Block types
PCAPNG_BT_IDB = 0x00000001     # Interface Description Block
PCAPNG_BT_PB = 0x00000002      # Packet Block (deprecated)
PCAPNG_BT_SPB = 0x00000003     # Simple Packet Block
PCAPNG_BT_EPB = 0x00000006     # Enhanced Packet Block
PCAPNG_BT_SHB = 0x0A0D0D0A     # Section Header Block

# Options
PCAPNG_OPT_ENDOFOPT = 0        # end of options
PCAPNG_OPT_COMMENT = 1         # comment

# SHB options
PCAPNG_OPT_SHB_HARDWARE = 2    # description of the hardware
PCAPNG_OPT_SHB_OS = 3          # name of the operating system
PCAPNG_OPT_SHB_USERAPPL = 4    # name of the application

# IDB options
PCAPNG_OPT_IF_NAME = 2         # interface name
PCAPNG_OPT_IF_DESCRIPTION = 3  # interface description
PCAPNG_OPT_IF_IPV4ADDR = 4     # IPv4 network address and netmask for the interface
PCAPNG_OPT_IF_IPV6ADDR = 5     # IPv6 network address and prefix length for the interface
PCAPNG_OPT_IF_MACADDR = 6      # interface hardware MAC address
PCAPNG_OPT_IF_EUIADDR = 7      # interface hardware EUI address
PCAPNG_OPT_IF_SPEED = 8        # interface speed in bits/s
PCAPNG_OPT_IF_TSRESOL = 9      # timestamp resolution
PCAPNG_OPT_IF_TZONE = 10       # time zone
PCAPNG_OPT_IF_FILTER = 11      # capture filter
PCAPNG_OPT_IF_OS = 12          # operating system
PCAPNG_OPT_IF_FCSLEN = 13      # length of the Frame Check Sequence in bits
PCAPNG_OPT_IF_TSOFFSET = 14    # offset (in seconds) that must be added to packet timestamp

# <copied from pcap.py>
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
DLT_IEEE802_11 = 105
DLT_LINUX_SLL = 113
DLT_PFLOG = 117
DLT_IEEE802_11_RADIO = 127

if sys.platform.find('openbsd') != -1:
    DLT_LOOP = 12
    DLT_RAW = 14
else:
    DLT_LOOP = 108
    DLT_RAW = 12

dltoff = {DLT_NULL: 4, DLT_EN10MB: 14, DLT_IEEE802: 22, DLT_ARCNET: 6,
          DLT_SLIP: 16, DLT_PPP: 4, DLT_FDDI: 21, DLT_PFLOG: 48, DLT_PFSYNC: 4,
          DLT_LOOP: 4, DLT_LINUX_SLL: 16}
# </copied from pcap.py>


def _swap32b(i):
    """Swap endianness of an uint32"""
    return struct_unpack('<I', struct_pack('>I', i))[0]


def _align32b(i):
    """Return int `i` aligned to the 32-bit boundary"""
    r = i % 4
    return i if not r else i + 4 - r


def _padded(s):
    """Return bytes `s` padded with zeroes to align to the 32-bit boundary"""
    return struct_pack('%ss' % _align32b(len(s)), s)


def _padded_tolen(s, tolen):
    """Return bytes `s` padded with `tolen` zeroes to align to the 32-bit boundary"""
    return struct_pack('%ss' % tolen, s)


def _padlen(s):
    """Return size of padding required to align str `s` to the 32-bit boundary"""
    return _align32b(len(s)) - len(s)


class _PcapngBlock(dpkt.Packet):
    """Base class for a pcapng block with Options"""

    __hdr__ = (
        ('type', 'I', 0),  # block type
        ('len', 'I', 12),  # block total length: total size of this block, in octets
        # ( body, variable size )
        ('_len', 'I', 12),  # dup of len
    )

    def unpack_hdr(self, buf):
        dpkt.Packet.unpack(self, buf)

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        if self.len > len(buf):
            raise dpkt.NeedData
        self._do_unpack_options(buf)

    def _do_unpack_options(self, buf, oo=None):
        self.opts = []
        self.data = ''
        oo = oo or self.__hdr_len__ - 4  # options offset
        ol = self.len - oo - 4  # length

        opts_buf = buf[oo:oo + ol]
        while opts_buf:
            opt = (PcapngOptionLE(opts_buf) if self.__hdr_fmt__[0] == '<'
                   else PcapngOption(opts_buf))
            self.opts.append(opt)

            opts_buf = opts_buf[len(opt):]
            if opt.code == PCAPNG_OPT_ENDOFOPT:
                break

        # duplicate total length field
        self._len = struct_unpack(self.__hdr_fmt__[0] + 'I', buf[-4:])[0]
        if self._len != self.len:
            raise dpkt.UnpackError('length fields do not match')

    def _do_pack_options(self):
        if not getattr(self, 'opts', None):
            return b''
        if self.opts[-1].code != PCAPNG_OPT_ENDOFOPT:
            raise dpkt.PackError('options must end with opt_endofopt')
        return b''.join(bytes(o) for o in self.opts)

    def __bytes__(self):
        opts_buf = self._do_pack_options()

        n = len(opts_buf) + self.__hdr_len__
        self.len = n
        self._len = n

        hdr_buf = self._pack_hdr(self.type, n, n)
        return b''.join([hdr_buf[:-4], opts_buf, hdr_buf[-4:]])

    def __len__(self):
        if not getattr(self, 'opts', None):
            return self.__hdr_len__

        opts_len = sum(len(o) for o in self.opts)
        return self.__hdr_len__ + opts_len


class PcapngBlockLE(_PcapngBlock):
    __byte_order__ = '<'


class PcapngOption(dpkt.Packet):
    """A single Option"""

    __hdr__ = (
        ('code', 'H', PCAPNG_OPT_ENDOFOPT),
        ('len', 'H', 0),
    )

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        self.data = buf[self.__hdr_len__:self.__hdr_len__ + self.len]

        # decode comment
        if self.code == PCAPNG_OPT_COMMENT:
            try:
                self.text = self.data.decode('utf-8')
            except UnicodeDecodeError as ude:
                if b'\x00' in self.data:
                    self.text = self.data[:self.data.index(b'\x00')].decode('ascii')
                else:
                    raise ude

    def __bytes__(self):
        # encode comment
        if self.code == PCAPNG_OPT_COMMENT:
            text = getattr(self, 'text', self.data)

            self.data = text.encode('utf-8') if not isinstance(text, bytes) else text

        self.len = len(self.data)
        hdr = self._pack_hdr(self.code, self.len)
        return hdr + _padded(self.data)

    def __len__(self):
        return self.__hdr_len__ + len(self.data) + _padlen(self.data)

    def __repr__(self):
        if self.code == PCAPNG_OPT_ENDOFOPT:
            return '{0}(opt_endofopt)'.format(self.__class__.__name__)
        else:
            return dpkt.Packet.__repr__(self)


class PcapngOptionLE(PcapngOption):
    __byte_order__ = '<'


class SectionHeaderBlock(_PcapngBlock):
    """Section Header block"""

    __hdr__ = (
        ('type', 'I', PCAPNG_BT_SHB),
        ('len', 'I', 28),
        ('bom', 'I', BYTE_ORDER_MAGIC),
        ('v_major', 'H', PCAPNG_VERSION_MAJOR),
        ('v_minor', 'H', PCAPNG_VERSION_MINOR),
        ('sec_len', 'q', -1),  # section length, -1 = auto
        # ( options, variable size )
        ('_len', 'I', 28)
    )

    def __bytes__(self):
        opts_buf = self._do_pack_options()

        n = len(opts_buf) + self.__hdr_len__
        self.len = n
        self._len = n

        hdr_buf = self._pack_hdr(
            self.type,
            n,
            self.bom,
            self.v_major,
            self.v_minor,
            self.sec_len,
            n,
        )
        return b''.join([hdr_buf[:-4], opts_buf, hdr_buf[-4:]])


class SectionHeaderBlockLE(SectionHeaderBlock):
    __byte_order__ = '<'


class InterfaceDescriptionBlock(_PcapngBlock):
    """Interface Description block"""

    __hdr__ = (
        ('type', 'I', PCAPNG_BT_IDB),
        ('len', 'I', 20),
        ('linktype', 'H', DLT_EN10MB),
        ('_reserved', 'H', 0),
        ('snaplen', 'I', 1500),
        # ( options, variable size )
        ('_len', 'I', 20)
    )

    def __bytes__(self):
        opts_buf = self._do_pack_options()

        n = len(opts_buf) + self.__hdr_len__
        self.len = n
        self._len = n

        hdr_buf = self._pack_hdr(
            self.type,
            n,
            self.linktype,
            self._reserved,
            self.snaplen,
            n,
        )
        return b''.join([hdr_buf[:-4], opts_buf, hdr_buf[-4:]])


class InterfaceDescriptionBlockLE(InterfaceDescriptionBlock):
    __byte_order__ = '<'


class EnhancedPacketBlock(_PcapngBlock):
    """Enhanced Packet block"""

    __hdr__ = (
        ('type', 'I', PCAPNG_BT_EPB),
        ('len', 'I', 64),
        ('iface_id', 'I', 0),
        ('ts_high', 'I', 0),  # timestamp high
        ('ts_low', 'I', 0),  # timestamp low
        ('caplen', 'I', 0),  # captured len, size of pkt_data
        ('pkt_len', 'I', 0),  # actual packet len
        # ( pkt_data, variable size )
        # ( options, variable size )
        ('_len', 'I', 64)
    )

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        if self.len > len(buf):
            raise dpkt.NeedData

        # packet data
        po = self.__hdr_len__ - 4  # offset of pkt_data
        self.pkt_data = buf[po:po + self.caplen]

        # skip padding between pkt_data and options
        opts_offset = po + _align32b(self.caplen)
        self._do_unpack_options(buf, opts_offset)

    def __bytes__(self):
        pkt_buf = self.pkt_data

        pkt_len = len(pkt_buf)
        self.caplen = pkt_len
        self.pkt_len = pkt_len

        opts_buf = self._do_pack_options()

        n = self.__hdr_len__ + _align32b(self.caplen) + len(opts_buf)
        self.len = n
        self._len = n

        hdr_buf = self._pack_hdr(
            self.type,
            n,
            self.iface_id,
            self.ts_high,
            self.ts_low,
            pkt_len,
            pkt_len,
            n
        )

        return b''.join([hdr_buf[:-4], _padded(pkt_buf), opts_buf, hdr_buf[-4:]])

    def __len__(self):
        opts_len = sum(len(o) for o in self.opts)
        return self.__hdr_len__ + _align32b(self.caplen) + opts_len


class EnhancedPacketBlockLE(EnhancedPacketBlock):
    __byte_order__ = '<'


class PacketBlock(EnhancedPacketBlock):
    """Packet block (deprecated)"""

    __hdr__ = (
        ('type', 'I', PCAPNG_BT_PB),
        ('len', 'I', 64),
        ('iface_id', 'H', 0),
        ('drops_count', 'H', 0),  # local drop counter
        ('ts_high', 'I', 0),  # timestamp high
        ('ts_low', 'I', 0),  # timestamp low
        ('caplen', 'I', 0),  # captured len, size of pkt_data
        ('pkt_len', 'I', 0),  # actual packet len
        # ( pkt_data, variable size )
        # ( options, variable size )
        ('_len', 'I', 64)
    )

    def __bytes__(self):
        pkt_buf = self.pkt_data

        pkt_len = len(pkt_buf)
        self.caplen = pkt_len
        self.pkt_len = pkt_len

        opts_buf = self._do_pack_options()

        n = self.__hdr_len__ + _align32b(self.caplen) + len(opts_buf)
        self.len = n
        self._len = n

        hdr_buf = self._pack_hdr(
            self.type,
            n,
            self.iface_id,
            self.drops_count,
            self.ts_high,
            self.ts_low,
            pkt_len,
            pkt_len,
            n
        )

        return b''.join([hdr_buf[:-4], _padded(pkt_buf), opts_buf, hdr_buf[-4:]])


class PacketBlockLE(PacketBlock):
    __byte_order__ = '<'


class Writer(object):
    """Simple pcapng dumpfile writer."""

    __le = sys.byteorder == 'little'

    def __init__(self, fileobj, snaplen=1500, linktype=DLT_EN10MB, shb=None, idb=None):
        """
        Create a pcapng dumpfile writer for the given fileobj.

        shb can be an instance of SectionHeaderBlock(LE)
        idb can be an instance of InterfaceDescriptionBlock(LE) (or sequence of them)
        """
        self.__f = fileobj

        if shb:
            self._validate_block('shb', shb, SectionHeaderBlock)
        if idb:
            try:
                for idb_ in idb:
                    self._validate_block('idb', idb_, InterfaceDescriptionBlock)
            except (TypeError, ValueError):  # not iter or _validate_block failed
                self._validate_block('idb', idb, InterfaceDescriptionBlock)
                idb = [idb]

        if self.__le:
            shb = shb or SectionHeaderBlockLE()
            idb = idb or [InterfaceDescriptionBlockLE(snaplen=snaplen, linktype=linktype)]
            self._kls = EnhancedPacketBlockLE
        else:
            shb = shb or SectionHeaderBlock()
            idb = idb or [InterfaceDescriptionBlock(snaplen=snaplen, linktype=linktype)]
            self._kls = EnhancedPacketBlock

        self.__f.write(bytes(shb))
        for idb_ in idb:
            self.__f.write(bytes(idb_))

    def _validate_block(self, arg_name, blk, expected_cls):
        """Check a user-defined block for correct type and endianness"""
        if not isinstance(blk, expected_cls):
            raise ValueError('{0}: expecting class {1}'.format(
                arg_name, expected_cls.__name__))

        if self.__le and blk.__hdr_fmt__[0] == '>':
            raise ValueError('{0}: expecting class {1}LE on a little-endian system'.format(
                arg_name, expected_cls.__name__))

        if not self.__le and blk.__hdr_fmt__[0] == '<':
            raise ValueError('{0}: expecting class {1} on a big-endian system'.format(
                arg_name, expected_cls.__name__.replace('LE', '')))

    def writepkt(self, pkt, ts=None):
        """
        Write a single packet with an optional timestamp.

        Args:
            pkt: buffer or instance of EnhancedPacketBlock(LE)
            ts: Unix timestamp in seconds since Epoch (e.g. 1454725786.99)
        """
        if isinstance(pkt, EnhancedPacketBlock):
            self._validate_block('pkt', pkt, EnhancedPacketBlock)

            if ts is not None:  # ts as an argument gets precedence
                ts = intround(ts * 1e6)
            elif pkt.ts_high == pkt.ts_low == 0:
                ts = intround(time() * 1e6)

            if ts is not None:
                pkt.ts_high = ts >> 32
                pkt.ts_low = ts & 0xffffffff

            self.__f.write(bytes(pkt))
            return

        # pkt is a buffer - wrap it into an EPB
        if ts is None:
            ts = time()
        self.writepkt_time(pkt, ts)

    def writepkt_time(self, pkt, ts):
        """
        Write a single packet with a mandatory timestamp.

        Args:
            pkt: a buffer
            ts: Unix timestamp in seconds since Epoch (e.g. 1454725786.99)
        """
        ts = intround(ts * 1e6)  # to int microseconds

        s = pkt
        n = len(s)

        epb = self._kls(
            ts_high=ts >> 32,
            ts_low=ts & 0xffffffff,
            caplen=n,
            pkt_len=n,
            pkt_data=s
        )
        self.__f.write(bytes(epb))

    def writepkts(self, pkts):
        """
        Take an iterable of (ts, pkt), and write to file.
        """
        kls = self._kls()
        ph = kls._pack_hdr
        fd = self.__f

        iface_id = kls.iface_id
        pkt_type = kls.type

        opts_buf = kls._do_pack_options()
        opts_len = len(opts_buf)

        hdr_len = kls.__hdr_len__
        precalc_n = hdr_len + opts_len

        for ts, pkt in pkts:
            ts = intround(ts * 1e6)  # to int microseconds
            pkt_len = len(pkt)
            pkt_len_align = _align32b(pkt_len)

            n = precalc_n + pkt_len_align
            hdr_buf = ph(
                pkt_type,
                n,
                iface_id,
                ts >> 32,
                ts & 0xffffffff,
                pkt_len,
                pkt_len,
                n
            )
            buf = b''.join([
                hdr_buf[:-4],
                _padded_tolen(pkt, pkt_len_align),
                opts_buf,
                hdr_buf[-4:]
            ])
            fd.write(buf)

    def close(self):
        self.__f.close()


class Reader(object):
    """Simple pypcap-compatible pcapng file reader."""

    def __init__(self, fileobj):
        self.name = getattr(fileobj, 'name', '<{0}>'.format(fileobj.__class__.__name__))
        self.__f = fileobj

        shb = SectionHeaderBlock()
        buf = self.__f.read(shb.__hdr_len__)
        if len(buf) < shb.__hdr_len__:
            raise ValueError('invalid pcapng header')

        # unpack just the header since endianness is not known
        shb.unpack_hdr(buf)
        if shb.type != PCAPNG_BT_SHB:
            raise ValueError('invalid pcapng header: not a SHB')

        # determine the correct byte order and reload full SHB
        if shb.bom == BYTE_ORDER_MAGIC_LE:
            self.__le = True
            buf += self.__f.read(_swap32b(shb.len) - shb.__hdr_len__)
            shb = SectionHeaderBlockLE(buf)
        elif shb.bom == BYTE_ORDER_MAGIC:
            self.__le = False
            buf += self.__f.read(shb.len - shb.__hdr_len__)
            shb = SectionHeaderBlock(buf)
        else:
            raise ValueError('unknown endianness')

        # check if this version is supported
        if shb.v_major != PCAPNG_VERSION_MAJOR:
            raise ValueError('unknown pcapng version {0}.{1}'.format(shb.v_major, shb.v_minor,))

        # look for a mandatory IDB
        idb = None
        while 1:
            buf = self.__f.read(8)
            if len(buf) < 8:
                break

            blk_type, blk_len = struct_unpack('<II' if self.__le else '>II', buf)
            buf += self.__f.read(blk_len - 8)

            if blk_type == PCAPNG_BT_IDB:
                idb = (InterfaceDescriptionBlockLE(buf) if self.__le
                       else InterfaceDescriptionBlock(buf))
                break
            # just skip other blocks

        if idb is None:
            raise ValueError('IDB not found')

        # set timestamp resolution and offset
        self._divisor = float(1e6)  # defaults
        self._tsoffset = 0
        for opt in idb.opts:
            if opt.code == PCAPNG_OPT_IF_TSRESOL:
                # if MSB=0, the remaining bits is a neg power of 10 (e.g. 6 means microsecs)
                # if MSB=1, the remaining bits is a neg power of 2 (e.g. 10 means 1/1024 of second)
                opt_val = struct_unpack('b', opt.data)[0]
                pow_num = 2 if opt_val & 0b10000000 else 10
                self._divisor = float(pow_num ** (opt_val & 0b01111111))

            elif opt.code == PCAPNG_OPT_IF_TSOFFSET:
                # 64-bit int that specifies an offset (in seconds) that must be added to the
                # timestamp of each packet
                self._tsoffset = struct_unpack('<q' if self.__le else '>q', opt.data)[0]

        if idb.linktype in dltoff:
            self.dloff = dltoff[idb.linktype]
        else:
            self.dloff = 0

        self.idb = idb
        self.snaplen = idb.snaplen
        self.filter = ''
        self.__iter = iter(self)

    @property
    def fd(self):
        return self.__f.fileno()

    def fileno(self):
        return self.fd

    def datalink(self):
        return self.idb.linktype

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
            buf = self.__f.read(8)
            if len(buf) < 8:
                break

            blk_type, blk_len = struct_unpack('<II' if self.__le else '>II', buf)
            buf += self.__f.read(blk_len - 8)

            if blk_type == PCAPNG_BT_EPB:
                epb = EnhancedPacketBlockLE(buf) if self.__le else EnhancedPacketBlock(buf)
                ts = self._tsoffset + (((epb.ts_high << 32) | epb.ts_low) / self._divisor)
                yield (ts, epb.pkt_data)
            elif blk_type == PCAPNG_BT_PB:
                pb = PacketBlockLE(buf) if self.__le else PacketBlock(buf)
                ts = self._tsoffset + (((pb.ts_high << 32) | pb.ts_low) / self._divisor)
                yield (ts, pb.pkt_data)

            # just ignore other blocks


#########
# TESTS #
#########

def test_shb():
    """Test SHB with options"""
    buf = (
        b'\x0a\x0d\x0d\x0a\x58\x00\x00\x00\x4d\x3c\x2b\x1a\x01\x00\x00\x00\xff\xff\xff\xff\xff\xff'
        b'\xff\xff\x04\x00\x31\x00\x54\x53\x68\x61\x72\x6b\x20\x31\x2e\x31\x30\x2e\x30\x72\x63\x32'
        b'\x20\x28\x53\x56\x4e\x20\x52\x65\x76\x20\x34\x39\x35\x32\x36\x20\x66\x72\x6f\x6d\x20\x2f'
        b'\x74\x72\x75\x6e\x6b\x2d\x31\x2e\x31\x30\x29\x00\x00\x00\x00\x00\x00\x00\x58\x00\x00\x00')

    opt_buf = b'\x04\x00\x31\x00TShark 1.10.0rc2 (SVN Rev 49526 from /trunk-1.10)\x00\x00\x00'

    # block unpacking
    shb = SectionHeaderBlockLE(buf)
    assert shb.type == PCAPNG_BT_SHB
    assert shb.bom == BYTE_ORDER_MAGIC
    assert shb.v_major == 1
    assert shb.v_minor == 0
    assert shb.sec_len == -1
    assert shb.data == ''

    # options unpacking
    assert len(shb.opts) == 2
    assert shb.opts[0].code == PCAPNG_OPT_SHB_USERAPPL
    assert shb.opts[0].data == b'TShark 1.10.0rc2 (SVN Rev 49526 from /trunk-1.10)'
    assert shb.opts[0].len == len(shb.opts[0].data)

    assert shb.opts[1].code == PCAPNG_OPT_ENDOFOPT
    assert shb.opts[1].len == 0

    # option packing
    assert str(shb.opts[0]) == str(opt_buf)
    assert len(shb.opts[0]) == len(opt_buf)
    assert bytes(shb.opts[1]) == b'\x00\x00\x00\x00'

    # block packing
    assert bytes(shb) == bytes(buf)
    assert str(shb) == str(buf)
    assert len(shb) == len(buf)


def test_idb():
    """Test IDB with options"""
    buf = (
        b'\x01\x00\x00\x00\x20\x00\x00\x00\x01\x00\x00\x00\xff\xff\x00\x00\x09\x00\x01\x00\x06\x00'
        b'\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00')

    # block unpacking
    idb = InterfaceDescriptionBlockLE(buf)
    assert idb.type == PCAPNG_BT_IDB
    assert idb.linktype == DLT_EN10MB
    assert idb.snaplen == 0xffff
    assert idb.data == ''

    # options unpacking
    assert len(idb.opts) == 2
    assert idb.opts[0].code == PCAPNG_OPT_IF_TSRESOL
    assert idb.opts[0].len == 1
    assert idb.opts[0].data == b'\x06'

    assert idb.opts[1].code == PCAPNG_OPT_ENDOFOPT
    assert idb.opts[1].len == 0

    # option packing
    assert bytes(idb.opts[0]) == b'\x09\x00\x01\x00\x06\x00\x00\x00'
    assert len(idb.opts[0]) == 8
    assert bytes(idb.opts[1]) == b'\x00\x00\x00\x00'

    # block packing
    assert bytes(idb) == bytes(buf)
    assert str(idb) == str(buf)
    assert len(idb) == len(buf)


def test_epb():
    """Test EPB with a non-ascii comment option"""
    buf = (
        b'\x06\x00\x00\x00\x80\x00\x00\x00\x00\x00\x00\x00\x73\xe6\x04\x00\xbe\x37\xe2\x19\x4a\x00'
        b'\x00\x00\x4a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x45\x00'
        b'\x00\x3c\x5d\xb3\x40\x00\x40\x06\xdf\x06\x7f\x00\x00\x01\x7f\x00\x00\x01\x98\x34\x11\x4e'
        b'\x95\xcb\x2d\x3a\x00\x00\x00\x00\xa0\x02\xaa\xaa\xfe\x30\x00\x00\x02\x04\xff\xd7\x04\x02'
        b'\x08\x0a\x05\x8f\x70\x89\x00\x00\x00\x00\x01\x03\x03\x07\x00\x00\x01\x00\x0a\x00\xd0\xbf'
        b'\xd0\xb0\xd0\xba\xd0\xb5\xd1\x82\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00')

    # block unpacking
    epb = EnhancedPacketBlockLE(buf)
    assert epb.type == PCAPNG_BT_EPB
    assert epb.caplen == len(epb.pkt_data)
    assert epb.pkt_len == len(epb.pkt_data)
    assert epb.caplen == 74
    assert epb.ts_high == 321139
    assert epb.ts_low == 434255806
    assert epb.data == ''

    # options unpacking
    assert len(epb.opts) == 2
    assert epb.opts[0].code == PCAPNG_OPT_COMMENT
    assert epb.opts[0].text == u'\u043f\u0430\u043a\u0435\u0442'

    assert epb.opts[1].code == PCAPNG_OPT_ENDOFOPT
    assert epb.opts[1].len == 0

    # option packing
    assert bytes(epb.opts[0]) == b'\x01\x00\x0a\x00\xd0\xbf\xd0\xb0\xd0\xba\xd0\xb5\xd1\x82\x00\x00'
    assert len(epb.opts[0]) == 16
    assert bytes(epb.opts[1]) == b'\x00\x00\x00\x00'

    # block packing
    assert bytes(epb) == bytes(buf)
    assert str(epb) == str(buf)
    assert len(epb) == len(buf)


def test_pb():
    """Test PB with a non-ascii comment option"""
    buf = (
        b'\x02\x00\x00\x00\x80\x00\x00\x00\x00\x00\x00\x00\x73\xe6\x04\x00\xbe\x37\xe2\x19\x4a\x00'
        b'\x00\x00\x4a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x45\x00'
        b'\x00\x3c\x5d\xb3\x40\x00\x40\x06\xdf\x06\x7f\x00\x00\x01\x7f\x00\x00\x01\x98\x34\x11\x4e'
        b'\x95\xcb\x2d\x3a\x00\x00\x00\x00\xa0\x02\xaa\xaa\xfe\x30\x00\x00\x02\x04\xff\xd7\x04\x02'
        b'\x08\x0a\x05\x8f\x70\x89\x00\x00\x00\x00\x01\x03\x03\x07\x00\x00\x01\x00\x0a\x00\xd0\xbf'
        b'\xd0\xb0\xd0\xba\xd0\xb5\xd1\x82\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00')

    # block unpacking
    pb = PacketBlockLE(buf)
    assert pb.type == PCAPNG_BT_PB
    assert pb.caplen == len(pb.pkt_data)
    assert pb.iface_id == 0
    assert pb.drops_count == 0
    assert pb.pkt_len == len(pb.pkt_data)
    assert pb.caplen == 74
    assert pb.ts_high == 321139
    assert pb.ts_low == 434255806
    assert pb.data == ''

    # options unpacking
    assert len(pb.opts) == 2
    assert pb.opts[0].code == PCAPNG_OPT_COMMENT
    assert pb.opts[0].text == u'\u043f\u0430\u043a\u0435\u0442'

    assert pb.opts[1].code == PCAPNG_OPT_ENDOFOPT
    assert pb.opts[1].len == 0

    # option packing
    assert bytes(pb.opts[0]) == b'\x01\x00\x0a\x00\xd0\xbf\xd0\xb0\xd0\xba\xd0\xb5\xd1\x82\x00\x00'
    assert len(pb.opts[0]) == 16
    assert bytes(pb.opts[1]) == b'\x00\x00\x00\x00'

    # block packing
    assert bytes(pb) == bytes(buf)
    assert str(pb) == str(buf)
    assert len(pb) == len(buf)


def test_pb_read():
    """ Test PB parsing as part of file """
    pb_packet = (
        b'\x02\x00\x00\x00\x80\x00\x00\x00\x00\x00\x00\x00\x73\xe6\x04\x00\xbe\x37\xe2\x19\x4a\x00'
        b'\x00\x00\x4a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x45\x00'
        b'\x00\x3c\x5d\xb3\x40\x00\x40\x06\xdf\x06\x7f\x00\x00\x01\x7f\x00\x00\x01\x98\x34\x11\x4e'
        b'\x95\xcb\x2d\x3a\x00\x00\x00\x00\xa0\x02\xaa\xaa\xfe\x30\x00\x00\x02\x04\xff\xd7\x04\x02'
        b'\x08\x0a\x05\x8f\x70\x89\x00\x00\x00\x00\x01\x03\x03\x07\x00\x00\x01\x00\x0a\x00\xd0\xbf'
        b'\xd0\xb0\xd0\xba\xd0\xb5\xd1\x82\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00')

    buf = define_testdata().valid_pcapng + pb_packet
    fobj = BytesIO(buf)

    # test reading
    reader = Reader(fobj)

    # first packet is EPB and comes from define_testdata().valid_pcapng
    ts, buf1 = next(iter(reader))
    assert ts == 1442984653.210838

    # second packet is concatenated PB, pb_packet defined above
    ts, buf2 = next(iter(reader))
    assert ts == 1379281936.72595


def test_epb_ascii_comment_option():
    """Test EPB with an ascii comment option"""
    buf = (
        b'\x06\x00\x00\x00\x7c\x00\x00\x00\x01\x00\x00\x00\xff\xff\xff\xff\x79\xd2\xdf\xe1\x44\x00'
        b'\x00\x00\x44\x00\x00\x00\x00\x00\x00\x01\x00\x06\x00\x0b\xdb\x43\xe7\x4b\xf6\x7f\x08\x00'
        b'\x45\x00\x00\x34\x2b\x1f\x40\x00\x40\x06\x15\x63\x82\xd9\xfa\x81\x82\xd9\xfa\x0d\x17\x70'
        b'\xec\x3e\x02\xba\x94\x38\x81\x52\x4a\x39\x80\x10\xbb\x5d\x53\x0d\x00\x00\x01\x01\x08\x0a'
        b'\x03\xf9\xc7\xbf\x04\x02\x38\x28\x01\x00\x0f\x00\x50\x61\x63\x6b\x65\x74\x20\x23\x31\x00'
        b'\x78\x4d\x39\x87\x0c\x00\x00\x00\x00\x00\x7c\x00\x00\x00')

    # block unpacking
    epb = EnhancedPacketBlockLE(buf)

    # options unpacking
    assert len(epb.opts) == 2
    assert epb.opts[0].code == PCAPNG_OPT_COMMENT
    assert epb.opts[0].text == 'Packet #1'

    assert epb.opts[1].code == PCAPNG_OPT_ENDOFOPT
    assert epb.opts[1].len == 0

    # option packing
    assert bytes(epb.opts[0]) == b'\x01\x00\x09\x00\x50\x61\x63\x6b\x65\x74\x20\x23\x31\x00\x00\x00'
    assert len(epb.opts[0]) == 16
    assert bytes(epb.opts[1]) == b'\x00\x00\x00\x00'


def test_epb_invalid_utf8_comment_option():
    """Test EPB with an invalid (non UTF-8, non-zero terminated ascii) comment option"""
    buf = (
        b'\x06\x00\x00\x00\x7c\x00\x00\x00\x01\x00\x00\x00\xff\xff\xff\xff\x79\xd2\xdf\xe1\x44\x00'
        b'\x00\x00\x44\x00\x00\x00\x00\x00\x00\x01\x00\x06\x00\x0b\xdb\x43\xe7\x4b\xf6\x7f\x08\x00'
        b'\x45\x00\x00\x34\x2b\x1f\x40\x00\x40\x06\x15\x63\x82\xd9\xfa\x81\x82\xd9\xfa\x0d\x17\x70'
        b'\xec\x3e\x02\xba\x94\x38\x81\x52\x4a\x39\x80\x10\xbb\x5d\x53\x0d\x00\x00\x01\x01\x08\x0a'
        b'\x03\xf9\xc7\xbf\x04\x02\x38\x28\x01\x00\x0f\x00\x50\x61\x63\x6b\x65\x74\x20\x23\x31\x20'
        b'\x78\x4d\x39\x87\x0c\x00\x00\x00\x00\x00\x7c\x00\x00\x00')

    try:
        EnhancedPacketBlockLE(buf)
    except Exception as e:
        assert isinstance(e, UnicodeDecodeError)


def test_simple_write_read():
    """Test writing a basic pcapng and then reading it"""
    fobj = BytesIO()

    writer = Writer(fobj, snaplen=0x2000, linktype=DLT_LINUX_SLL)
    writer.writepkt(b'foo', ts=1454725786.526401)
    fobj.flush()
    fobj.seek(0)

    reader = Reader(fobj)
    assert reader.snaplen == 0x2000
    assert reader.datalink() == DLT_LINUX_SLL

    ts, buf1 = next(iter(reader))
    assert ts == 1454725786.526401
    assert buf1 == b'foo'

    # test dispatch()
    fobj.seek(0)
    reader = Reader(fobj)
    assert reader.dispatch(1, lambda ts, pkt: None) == 1
    assert reader.dispatch(1, lambda ts, pkt: None) == 0
    fobj.close()


def test_pcapng_header():
    """Reading an empty file will fail as the header length is incorrect"""
    fobj = BytesIO()

    try:
        Reader(fobj)
    except Exception as e:
        assert isinstance(e, ValueError)


def define_testdata():
    class TestData(object):
        def __init__(self):
            self.valid_shb_le = SectionHeaderBlockLE(opts=[
                PcapngOptionLE(code=3, data=b'64-bit Windows 8.1, build 9600'),
                PcapngOptionLE(code=4, data=b'Dumpcap 1.12.7 (v1.12.7-0-g7fc8978 from master-1.12)'),
                PcapngOptionLE()
            ])

            self.valid_shb_be = SectionHeaderBlock(opts=[
                PcapngOption(code=3, data=b'64-bit Windows 8.1, build 9600'),
                PcapngOption(code=4, data=b'Dumpcap 1.12.7 (v1.12.7-0-g7fc8978 from master-1.12)'),
                PcapngOption()
            ])

            self.valid_idb_le = InterfaceDescriptionBlockLE(snaplen=0x40000, opts=[
                PcapngOptionLE(code=2, data=b'\\Device\\NPF_{3BBF21A7-91AE-4DDB-AB2C-C782999C22D5}'),
                PcapngOptionLE(code=9, data=b'\x06'),
                PcapngOptionLE(code=12, data=b'64-bit Windows 8.1, build 9600'),
                PcapngOptionLE()
            ])

            self.valid_idb_be = InterfaceDescriptionBlock(snaplen=0x40000, opts=[
                PcapngOption(code=2, data=b'\\Device\\NPF_{3BBF21A7-91AE-4DDB-AB2C-C782999C22D5}'),
                PcapngOption(code=9, data=b'\x06'),
                PcapngOption(code=12, data=b'64-bit Windows 8.1, build 9600'),
                PcapngOption()
            ])

            self.valid_pcapng = (
                b'\x0a\x0d\x0d\x0a\x7c\x00\x00\x00\x4d\x3c\x2b\x1a\x01\x00\x00'
                b'\x00\xff\xff\xff\xff\xff\xff\xff\xff\x03\x00\x1e\x00\x36\x34'
                b'\x2d\x62\x69\x74\x20\x57\x69\x6e\x64\x6f\x77\x73\x20\x38\x2e'
                b'\x31\x2c\x20\x62\x75\x69\x6c\x64\x20\x39\x36\x30\x30\x00\x00'
                b'\x04\x00\x34\x00\x44\x75\x6d\x70\x63\x61\x70\x20\x31\x2e\x31'
                b'\x32\x2e\x37\x20\x28\x76\x31\x2e\x31\x32\x2e\x37\x2d\x30\x2d'
                b'\x67\x37\x66\x63\x38\x39\x37\x38\x20\x66\x72\x6f\x6d\x20\x6d'
                b'\x61\x73\x74\x65\x72\x2d\x31\x2e\x31\x32\x29\x00\x00\x00\x00'
                b'\x7c\x00\x00\x00\x01\x00\x00\x00\x7c\x00\x00\x00\x01\x00\x00'
                b'\x00\x00\x00\x04\x00\x02\x00\x32\x00\x5c\x44\x65\x76\x69\x63'
                b'\x65\x5c\x4e\x50\x46\x5f\x7b\x33\x42\x42\x46\x32\x31\x41\x37'
                b'\x2d\x39\x31\x41\x45\x2d\x34\x44\x44\x42\x2d\x41\x42\x32\x43'
                b'\x2d\x43\x37\x38\x32\x39\x39\x39\x43\x32\x32\x44\x35\x7d\x00'
                b'\x00\x09\x00\x01\x00\x06\x00\x00\x00\x0c\x00\x1e\x00\x36\x34'
                b'\x2d\x62\x69\x74\x20\x57\x69\x6e\x64\x6f\x77\x73\x20\x38\x2e'
                b'\x31\x2c\x20\x62\x75\x69\x6c\x64\x20\x39\x36\x30\x30\x00\x00'
                b'\x00\x00\x00\x00\x7c\x00\x00\x00\x06\x00\x00\x00\x84\x00\x00'
                b'\x00\x00\x00\x00\x00\x63\x20\x05\x00\xd6\xc4\xab\x0b\x4a\x00'
                b'\x00\x00\x4a\x00\x00\x00\x08\x00\x27\x96\xcb\x7c\x52\x54\x00'
                b'\x12\x35\x02\x08\x00\x45\x00\x00\x3c\xa4\x40\x00\x00\x1f\x01'
                b'\x27\xa2\xc0\xa8\x03\x28\x0a\x00\x02\x0f\x00\x00\x56\xf0\x00'
                b'\x01\x00\x6d\x41\x42\x43\x44\x45\x46\x47\x48\x49\x4a\x4b\x4c'
                b'\x4d\x4e\x4f\x50\x51\x52\x53\x54\x55\x56\x57\x41\x42\x43\x44'
                b'\x45\x46\x47\x48\x49\x00\x00\x01\x00\x0f\x00\x64\x70\x6b\x74'
                b'\x20\x69\x73\x20\x61\x77\x65\x73\x6f\x6d\x65\x00\x00\x00\x00'
                b'\x00\x84\x00\x00\x00'
            )
            self.valid_pkts = [
                (1442984653.210838,
                 (b"\x08\x00'\x96\xcb|RT\x00\x125\x02\x08\x00E\x00\x00<\xa4@"
                  b"\x00\x00\x1f\x01'\xa2\xc0\xa8\x03(\n\x00\x02\x0f\x00\x00V"
                  b"\xf0\x00\x01\x00mABCDEFGHIJKLMNOPQRSTUVWABCDEFGHI"))
            ]

            self.valid_epb_be = EnhancedPacketBlock(opts=[
                PcapngOption(code=1, text=b'dpkt is awesome'),
                PcapngOption()
            ], pkt_data=(
                b'\x08\x00\x27\x96\xcb\x7c\x52\x54\x00\x12\x35\x02\x08\x00\x45'
                b'\x00\x00\x3c\xa4\x40\x00\x00\x1f\x01\x27\xa2\xc0\xa8\x03\x28'
                b'\x0a\x00\x02\x0f\x00\x00\x56\xf0\x00\x01\x00\x6d\x41\x42\x43'
                b'\x44\x45\x46\x47\x48\x49\x4a\x4b\x4c\x4d\x4e\x4f\x50\x51\x52'
                b'\x53\x54\x55\x56\x57\x41\x42\x43\x44\x45\x46\x47\x48\x49'
            ))

            self.valid_epb_le = EnhancedPacketBlockLE(opts=[
                PcapngOptionLE(code=1, text=b'dpkt is awesome'),
                PcapngOptionLE()
            ], pkt_data=(
                b'\x08\x00\x27\x96\xcb\x7c\x52\x54\x00\x12\x35\x02\x08\x00\x45'
                b'\x00\x00\x3c\xa4\x40\x00\x00\x1f\x01\x27\xa2\xc0\xa8\x03\x28'
                b'\x0a\x00\x02\x0f\x00\x00\x56\xf0\x00\x01\x00\x6d\x41\x42\x43'
                b'\x44\x45\x46\x47\x48\x49\x4a\x4b\x4c\x4d\x4e\x4f\x50\x51\x52'
                b'\x53\x54\x55\x56\x57\x41\x42\x43\x44\x45\x46\x47\x48\x49'
            ))

        @property
        def shb_idb_epb_le(self):
            return self.valid_shb_le, self.valid_idb_le, self.valid_epb_le

        @property
        def shb_idb_epb_be(self):
            return self.valid_shb_be, self.valid_idb_be, self.valid_epb_be

        @property
        def shb_idb_epb(self):
            return self.shb_idb_epb_le if sys.byteorder == 'little' else self.shb_idb_epb_be

    return TestData()


def pre_test(f):
    def wrapper(*args, **kwargs):
        fobj = BytesIO()
        f.__globals__['fobj'] = fobj
        ret = f(*args, **kwargs)
        fobj.flush()
        fobj.seek(0)

        return ret
    return wrapper


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

                assert pkts, "You must return the input data from the test"
                for (ts_out, pkt_out), (ts_in, pkt_in) in zip(pkts, iter(Reader(fobj))):
                    assert ts_out == ts_in
                    assert pkt_out == pkt_in

                # 'noqa' for flake8 to ignore these since writer and fobj were injected into globals
                writer.close()  # noqa
                Writer._Writer__le = _sysle
                del f.__globals__['writer']
                del f.__globals__['fobj']
        return wrapper


class PostTest:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __call__(self, f, *args, **kwargs):
        def wrapper(*args, **kwargs):
            ret = f(*args, **kwargs)
            fobj = f.__globals__['fobj']
            test_type = self.kwargs.get('test')
            if test_type == 'assertion':
                isexception = False
                try:
                    Reader(fobj)
                except Exception as e:
                    isexception = True
                    assert isinstance(e, self.kwargs['type'])
                    assert str(e) == self.kwargs['msg']
                assert isexception, "No assertion raised!"

            elif test_type == 'compare_property':
                prop = self.kwargs['property']
                reader = Reader(fobj)
                assert bytes(ret) == bytes(getattr(reader, prop))
            elif test_type == 'compare_method':
                method = self.kwargs['method']
                reader = Reader(fobj)
                comp = getattr(reader, method)()
                assert comp == ret
            else:
                raise Exception("No test type specified")
        return wrapper


@PostTest(test='assertion', type=ValueError, msg='invalid pcapng header: not a SHB')
@pre_test
def test_shb_header():
    shb = define_testdata().valid_shb_le
    shb.type = 123456666
    fobj.write(bytes(shb))  # noqa


@PostTest(test='assertion', type=ValueError, msg='unknown endianness')
@pre_test
def test_shb_bom():
    shb = define_testdata().valid_shb_le
    shb.bom = 12345666
    fobj.write(bytes(shb))  # noqa


@PostTest(test='assertion', type=ValueError, msg='unknown pcapng version 123.45')
@pre_test
def test_shb_version():
    shb = define_testdata().valid_shb_le
    shb.v_major = 123
    shb.v_minor = 45
    fobj.write(bytes(shb))  # noqa


@PostTest(test='assertion', type=ValueError, msg='IDB not found')
@pre_test
def test_no_idb():
    shb = define_testdata().valid_shb_le
    fobj.write(bytes(shb)+b'aaaa')  # noqa


@PostTest(test='compare_property', property='idb')
@pre_test
def test_idb_opt_offset():
    """Test that the timestamp offset is correctly written and read"""
    shb = define_testdata().valid_shb_le
    idb = define_testdata().valid_idb_le
    idb.opts.insert(0, PcapngOptionLE(
        code=PCAPNG_OPT_IF_TSOFFSET,
        data=struct_pack('<q', 123456666))
    )
    fobj.write(bytes(shb)+bytes(idb))  # noqa
    return idb


@PostTest(test='compare_property', property='dloff')
@pre_test
def test_idb_linktype():
    """Test that if the idb.linktype is not in dloff, dloff is set to 0"""
    shb = define_testdata().valid_shb_le
    idb = define_testdata().valid_idb_le
    idb.linktype = 3456
    fobj.write(bytes(shb)+bytes(idb))  # noqa
    return 0


def test_repr():
    """check the __repr__ method for Packet subclass.

    The __repr__ method currently includes the b'' in the string. This means that python2 and python3 will differ.
    """
    real = repr(define_testdata().valid_shb_le)

    python2 = (
        "SectionHeaderBlockLE(opts=[PcapngOptionLE(code=3, data='64-bit Windows 8.1, build 9600'),"
        " PcapngOptionLE(code=4, data='Dumpcap 1.12.7 (v1.12.7-0-g7fc8978 from master-1.12)'),"
        " PcapngOptionLE(opt_endofopt)])")
    python3 = (
        "SectionHeaderBlockLE(opts=[PcapngOptionLE(code=3, data=b'64-bit Windows 8.1, build 9600'),"
        " PcapngOptionLE(code=4, data=b'Dumpcap 1.12.7 (v1.12.7-0-g7fc8978 from master-1.12)'),"
        " PcapngOptionLE(opt_endofopt)])")

    assert real in [python2, python3]


@pre_test
def test_filter():
    buf = define_testdata().valid_pcapng
    fobj.write(buf)  # noqa
    fobj.flush()  # noqa
    fobj.seek(0)  # noqa
    reader = Reader(fobj)  # noqa
    try:
        reader.setfilter(None, None)
    except Exception as e:
        assert isinstance(e, NotImplementedError)


@PostTest(test='compare_method', method='readpkts')
@pre_test
def test_readpkts():
    fobj.write(define_testdata().valid_pcapng)  # noqa
    return define_testdata().valid_pkts


@PostTest(test='compare_method', method='next')
@pre_test
def test_next():
    fobj.write(define_testdata().valid_pcapng)  # noqa
    return define_testdata().valid_pkts[0]


@pre_test
def test_dispatch():
    fobj.write(define_testdata().valid_pcapng)  # noqa
    fobj.flush()  # noqa
    fobj.seek(0)  # noqa

    def callback(timestamp, pkt, *args):
        assert (timestamp, pkt) == define_testdata().valid_pkts[0]

    reader = Reader(fobj)  # noqa
    assert 1 == reader.dispatch(0, callback)


@pre_test
def test_loop():
    fobj.write(define_testdata().valid_pcapng)  # noqa
    fobj.flush()  # noqa
    fobj.seek(0)  # noqa

    def callback(timestamp, pkt, *args):
        assert (timestamp, pkt) == define_testdata().valid_pkts[0]

    reader = Reader(fobj)  # noqa
    reader.loop(callback)


def test_idb_opt_err():
    """Test that options end with opt_endofopt"""
    idb = define_testdata().valid_idb_le
    del idb.opts[-1]
    try:
        bytes(idb)
    except Exception as e:
        assert isinstance(e, dpkt.PackError)
        assert str(e) == 'options must end with opt_endofopt'


def test_custom_read_write():
    """Test a full pcapng file with 1 ICMP packet"""
    buf = define_testdata().valid_pcapng
    fobj = BytesIO(buf)

    # test reading
    reader = Reader(fobj)
    assert reader.snaplen == 0x40000
    assert reader.datalink() == DLT_EN10MB

    assert reader.idb.opts[0].data.decode('utf-8') == '\\Device\\NPF_{3BBF21A7-91AE-4DDB-AB2C-C782999C22D5}'
    assert reader.idb.opts[2].data.decode('utf-8') == '64-bit Windows 8.1, build 9600'

    ts, buf1 = next(iter(reader))
    assert ts == 1442984653.2108380
    assert len(buf1) == 74

    assert buf1.startswith(b'\x08\x00\x27\x96')
    assert buf1.endswith(b'FGHI')
    fobj.close()

    # test pcapng customized writing
    shb, idb, epb = define_testdata().shb_idb_epb

    fobj = BytesIO()
    writer = Writer(fobj, shb=shb, idb=idb)
    writer.writepkt(epb, ts=1442984653.210838)
    # .valid_pcapng buf was collected on a little endian system
    if sys.byteorder == 'little':
        assert fobj.getvalue() == buf
    fobj.close()

    # same with timestamps defined inside EPB
    epb.ts_high = 335971
    epb.ts_low = 195806422

    fobj = BytesIO()
    writer = Writer(fobj, shb=shb, idb=idb)
    writer.writepkt(epb)
    if sys.byteorder == 'little':
        assert fobj.getvalue() == buf
    fobj.close()


def test_multi_idb_writer():
    """Test writing multiple interface description blocks into pcapng and read it"""
    fobj = BytesIO()
    shb, idb, epb = define_testdata().shb_idb_epb

    writer = Writer(fobj, shb=shb, idb=[idb, idb])
    writer.writepkt(epb)
    fobj.flush()
    fobj.seek(0)

    Reader(fobj)
    fobj.close()


@pre_test
def test_writer_validate_instance():
    """System endianness and shb endianness should match"""
    shb = 10

    try:
        writer = Writer(fobj, shb=shb)  # noqa
    except Exception as e:
        assert isinstance(e, ValueError)
        assert str(e) == 'shb: expecting class SectionHeaderBlock'


@pre_test
def test_writepkt_epb_ts():
    """writepkt should assign ts_high/low for epb if they are 0"""
    global time
    shb, idb, epb = define_testdata().shb_idb_epb
    writer = Writer(fobj, shb=shb, idb=idb)  # noqa
    epb.ts_high = epb.ts_low = 0
    ts = 1454725786.526401
    _tmp = time

    def time():
        return ts
    writer.writepkt(epb)
    time = _tmp

    ts_high, ts_low = 338704, 3183502017
    assert epb.ts_high == ts_high
    assert epb.ts_low == ts_low


@pre_test
def test_writer_validate_le():
    """System endianness and shb endianness should match"""
    shb = define_testdata().valid_shb_be
    _sysle = Writer._Writer__le

    Writer._Writer__le = True

    try:
        writer = Writer(fobj, shb=shb)  # noqa
    except Exception as e:
        assert isinstance(e, ValueError)
        assert str(e) == 'shb: expecting class SectionHeaderBlockLE on a little-endian system'

    Writer._Writer__le = _sysle


@pre_test
def test_writer_validate_be():
    """System endianness and shb endianness should match"""
    shb = define_testdata().valid_shb_le
    _sysle = Writer._Writer__le

    Writer._Writer__le = False

    try:
        writer = Writer(fobj, shb=shb)  # noqa
    except Exception as e:
        assert isinstance(e, ValueError)
        assert str(e) == 'shb: expecting class SectionHeaderBlock on a big-endian system'

    Writer._Writer__le = _sysle


@WriterTestWrap()
def test_writepkt_no_time():
    global time
    ts, pkt = 1454725786.526401, b'foooo'
    _tmp = time

    def time():
        return ts
    writer.writepkt(pkt)  # noqa
    time = _tmp
    return [(ts, pkt)]


@WriterTestWrap(writer={'snaplen': 10})
def test_writepkt_snaplen():
    ts, pkt = 1454725786.526401, b'foooo' * 100
    writer.writepkt(pkt, ts)  # noqa
    return [(ts, pkt)]


@WriterTestWrap()
def test_writepkt_with_time():
    ts, pkt = 1454725786.526401, b'foooo'
    writer.writepkt(pkt, ts)  # noqa
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


def test_pcapng_block_pack():
    assert bytes(_PcapngBlock())


def test_pcapng_block_unpack():
    block = _PcapngBlock()
    buf = b'012345678901'
    try:
        block.unpack(buf)
    except Exception as e:
        assert isinstance(e, dpkt.NeedData)


def test_epb_unpack():
    """EnhancedPacketBlock can only unpack data >64 bytes, the length of their header"""
    shb, idb, epb = define_testdata().shb_idb_epb
    buf = b'quite-long-but-not-long-enough-at-least-32'
    try:
        epb.unpack(buf)
    except Exception as e:
        assert isinstance(e, dpkt.NeedData)


def test_epb_unpack_length_mismatch():
    """Force calculated len to be 0 when unpacking epb, this should fail when unpacking"""
    shb, idb, epb = define_testdata().shb_idb_epb

    unpackme = bytes(epb)
    unpackme = unpackme[:-4] + b'\x00' * 4
    try:
        epb.unpack(unpackme)
    except Exception as e:
        assert isinstance(e, dpkt.UnpackError)
        assert str(e) == 'length fields do not match'


def test_pcapng_block_len_no_opts():
    """_PcapngBlock should return its own header __len__ if it has no opts"""
    block = _PcapngBlock()
    assert len(block) == 12


def test_reader_file_descriptor():
    """Reader has .fd and .fileno() convenience members. Compare them to the actual fobj that was passed in"""
    pcapng = define_testdata().valid_pcapng
    import tempfile
    with tempfile.TemporaryFile() as fobj:
        fobj.write(pcapng)
        fobj.seek(0)

        reader = Reader(fobj)
        assert reader.fd == fobj.fileno()
        assert reader.fileno() == fobj.fileno()


def test_posttest():
    """Check that PostTest wrapper doesn't fail silently"""
    @PostTest()
    @pre_test
    def fun():
        pass

    try:
        fun()
    except Exception as e:
        assert str(e) == 'No test type specified'
