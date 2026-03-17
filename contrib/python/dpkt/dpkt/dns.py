# $Id: dns.py 27 2006-11-21 01:22:52Z dahelder $
# -*- coding: utf-8 -*-
"""Domain Name System."""
from __future__ import print_function
from __future__ import absolute_import

import struct
import codecs

from . import dpkt
from .compat import compat_ord

DNS_Q = 0
DNS_R = 1

# Opcodes
DNS_QUERY = 0
DNS_IQUERY = 1
DNS_STATUS = 2
DNS_NOTIFY = 4
DNS_UPDATE = 5

# Flags
DNS_CD = 0x0010  # checking disabled
DNS_AD = 0x0020  # authenticated data
DNS_Z = 0x0040  # unused
DNS_RA = 0x0080  # recursion available
DNS_RD = 0x0100  # recursion desired
DNS_TC = 0x0200  # truncated
DNS_AA = 0x0400  # authoritative answer
DNS_QR = 0x8000  # response ( query / response )

# Response codes
DNS_RCODE_NOERR = 0
DNS_RCODE_FORMERR = 1
DNS_RCODE_SERVFAIL = 2
DNS_RCODE_NXDOMAIN = 3
DNS_RCODE_NOTIMP = 4
DNS_RCODE_REFUSED = 5
DNS_RCODE_YXDOMAIN = 6
DNS_RCODE_YXRRSET = 7
DNS_RCODE_NXRRSET = 8
DNS_RCODE_NOTAUTH = 9
DNS_RCODE_NOTZONE = 10

# RR types
DNS_A = 1
DNS_NS = 2
DNS_CNAME = 5
DNS_SOA = 6
DNS_NULL = 10
DNS_PTR = 12
DNS_HINFO = 13
DNS_MX = 15
DNS_TXT = 16
DNS_AAAA = 28
DNS_SRV = 33
DNS_OPT = 41

# RR classes
DNS_IN = 1
DNS_CHAOS = 3
DNS_HESIOD = 4
DNS_ANY = 255


def pack_name(name, off, label_ptrs):
    name = codecs.encode(name, 'utf-8')
    if name:
        labels = name.split(b'.')
    else:
        labels = []
    labels.append(b'')
    buf = b''
    for i, label in enumerate(labels):
        key = b'.'.join(labels[i:]).upper()
        ptr = label_ptrs.get(key)
        if ptr is None:
            if len(key) > 1:
                ptr = off + len(buf)
                if ptr < 0xc000:
                    label_ptrs[key] = ptr
            i = len(label)
            buf += struct.pack("B", i) + label
        else:
            buf += struct.pack('>H', (0xc000 | ptr))
            break
    return buf


def unpack_name(buf, off):
    name = []
    saved_off = 0
    start_off = off
    name_length = 0
    while True:
        if off >= len(buf):
            raise dpkt.NeedData()
        n = compat_ord(buf[off])
        if n == 0:
            off += 1
            break
        elif (n & 0xc0) == 0xc0:
            ptr = struct.unpack('>H', buf[off:off + 2])[0] & 0x3fff
            if ptr >= start_off:
                raise dpkt.UnpackError('Invalid label compression pointer')
            off += 2
            if not saved_off:
                saved_off = off
            start_off = off = ptr
        elif (n & 0xc0) == 0x00:
            off += 1
            name.append(buf[off:off + n])
            name_length += n + 1
            if name_length > 255:
                raise dpkt.UnpackError('name longer than 255 bytes')
            off += n
        else:
            raise dpkt.UnpackError('Invalid label length %02x' % n)
    if not saved_off:
        saved_off = off
    return codecs.decode(b'.'.join(name), 'utf-8'), saved_off


class DNS(dpkt.Packet):
    """Domain Name System.

    The Domain Name System (DNS) is the hierarchical and decentralized naming system used to identify computers,
    services, and other resources reachable through the Internet or other Internet Protocol (IP) networks.
    The resource records contained in the DNS associate domain names with other forms of information.

    Attributes:
        __hdr__ (tuple(header_name, c_type, offset)): Header fields of DNS.
            id: (int): Identification. Used to match request/reply packets.
            op: (int): Operation
            qd: (int): Query Definition
            an: (int): Answer
            ns: (int): Name Server
            ar: (int): Additional Record
    """

    __hdr__ = (
        ('id', 'H', 0),
        ('op', 'H', DNS_RD),  # recursive query
        # XXX - lists of query, RR objects
        ('qd', 'H', []),
        ('an', 'H', []),
        ('ns', 'H', []),
        ('ar', 'H', [])
    )

    @property
    def qr(self):
        """DNS Query/Response. 1 bit"""
        return int((self.op & DNS_QR) == DNS_QR)

    @qr.setter
    def qr(self, v):
        if v:
            self.op |= DNS_QR
        else:
            self.op &= ~DNS_QR

    @property
    def opcode(self):
        """Operation code. 4 bits."""
        return (self.op >> 11) & 0xf

    @opcode.setter
    def opcode(self, v):
        self.op = (self.op & ~0x7800) | ((v & 0xf) << 11)

    @property
    def aa(self):
        """Authoritative Answer. 1 bit.
        Specifies that the responding name server is an authority for the domain name in question section."""
        return int((self.op & DNS_AA) == DNS_AA)

    @aa.setter
    def aa(self, v):
        if v:
            self.op |= DNS_AA
        else:
            self.op &= ~DNS_AA

    @property
    def tc(self):
        """Truncated. 1 bit. Indicates that only the first 512 bytes of the reply was returned."""
        return int((self.op & DNS_TC) == DNS_TC)

    @tc.setter
    def tc(self, v):
        if v:
            self.op |= DNS_TC
        else:
            self.op &= ~DNS_TC

    @property
    def rd(self):
        """Recursion Desired. 1 bit. May be set in a query and is copied into the response.
        If set, the name server is directed to pursue the query recursively. Recursive query support is optional."""
        return int((self.op & DNS_RD) == DNS_RD)

    @rd.setter
    def rd(self, v):
        if v:
            self.op |= DNS_RD
        else:
            self.op &= ~DNS_RD

    @property
    def ra(self):
        """Recursion Available. 1 bit. Indicates if recursive query support is available in the name server."""
        return int((self.op & DNS_RA) == DNS_RA)

    @ra.setter
    def ra(self, v):
        if v:
            self.op |= DNS_RA
        else:
            self.op &= ~DNS_RA

    @property
    def zero(self):
        """Zero 1 bit"""
        return int((self.op & DNS_Z) == DNS_Z)

    @zero.setter
    def zero(self, v):
        if v:
            self.op |= DNS_Z
        else:
            self.op &= ~DNS_Z

    @property
    def rcode(self):
        """Return code. 4 bits."""
        return self.op & 0xf

    @rcode.setter
    def rcode(self, v):
        self.op = (self.op & ~0xf) | (v & 0xf)

    class Q(dpkt.Packet):
        """DNS question."""
        __hdr__ = (
            ('name', '1025s', b''),
            ('type', 'H', DNS_A),
            ('cls', 'H', DNS_IN)
        )

        # XXX - suk
        def __len__(self):
            raise NotImplementedError

        __str__ = __len__

        def unpack(self, buf):
            raise NotImplementedError

    class RR(Q):
        """DNS resource record."""
        __hdr__ = (
            ('name', '1025s', b''),
            ('type', 'H', DNS_A),
            ('cls', 'H', DNS_IN),
            ('ttl', 'I', 0),
            ('rlen', 'H', 4),
            ('rdata', 's', b'')
        )

        def pack_rdata(self, off, label_ptrs):
            # XXX - yeah, this sux
            if self.rdata:
                return self.rdata
            if self.type == DNS_A:
                return self.ip
            elif self.type == DNS_NS:
                return pack_name(self.nsname, off, label_ptrs)
            elif self.type == DNS_CNAME:
                return pack_name(self.cname, off, label_ptrs)
            elif self.type == DNS_PTR:
                return pack_name(self.ptrname, off, label_ptrs)
            elif self.type == DNS_SOA:
                l_ = []
                l_.append(pack_name(self.mname, off, label_ptrs))
                l_.append(pack_name(self.rname, off + len(l_[0]), label_ptrs))
                l_.append(struct.pack('>IIIII', self.serial, self.refresh,
                                      self.retry, self.expire, self.minimum))
                return b''.join(l_)
            elif self.type == DNS_MX:
                return struct.pack('>H', self.preference) + \
                    pack_name(self.mxname, off + 2, label_ptrs)
            elif self.type == DNS_TXT or self.type == DNS_HINFO:
                return b''.join(struct.pack('B', len(x)) + x for x in self.text)
            elif self.type == DNS_AAAA:
                return self.ip6
            elif self.type == DNS_SRV:
                return struct.pack('>HHH', self.priority, self.weight, self.port) + \
                    pack_name(self.srvname, off + 6, label_ptrs)
            elif self.type == DNS_OPT:
                return b''  # self.rdata
            else:
                raise dpkt.PackError('RR type %s is not supported' % self.type)

        def unpack_rdata(self, buf, off):
            if self.type == DNS_A:
                self.ip = self.rdata
            elif self.type == DNS_NS:
                self.nsname, off = unpack_name(buf, off)
            elif self.type == DNS_CNAME:
                self.cname, off = unpack_name(buf, off)
            elif self.type == DNS_PTR:
                self.ptrname, off = unpack_name(buf, off)
            elif self.type == DNS_SOA:
                self.mname, off = unpack_name(buf, off)
                self.rname, off = unpack_name(buf, off)
                self.serial, self.refresh, self.retry, self.expire, self.minimum = \
                    struct.unpack('>IIIII', buf[off:off + 20])
            elif self.type == DNS_MX:
                self.preference = struct.unpack('>H', self.rdata[:2])
                self.mxname, off = unpack_name(buf, off + 2)
            elif self.type == DNS_TXT or self.type == DNS_HINFO:
                self.text = []
                buf = self.rdata
                while buf:
                    n = compat_ord(buf[0])
                    self.text.append(codecs.decode(buf[1:1 + n], 'utf-8'))
                    buf = buf[1 + n:]
            elif self.type == DNS_AAAA:
                self.ip6 = self.rdata
            elif self.type == DNS_NULL:
                self.null = codecs.encode(self.rdata, 'hex')
            elif self.type == DNS_SRV:
                self.priority, self.weight, self.port = struct.unpack('>HHH', self.rdata[:6])
                self.srvname, off = unpack_name(buf, off + 6)
            elif self.type == DNS_OPT:
                pass  # RFC-6891: OPT is a pseudo-RR not carrying any DNS data
            else:
                raise dpkt.UnpackError('RR type %s is not supported' % self.type)

    def pack_q(self, buf, q):
        """Append packed DNS question and return buf."""
        return buf + pack_name(q.name, len(buf), self.label_ptrs) + struct.pack('>HH', q.type, q.cls)

    def unpack_q(self, buf, off):
        """Return DNS question and new offset."""
        q = self.Q()
        q.name, off = unpack_name(buf, off)
        q.type, q.cls = struct.unpack('>HH', buf[off:off + 4])
        off += 4
        return q, off

    def pack_rr(self, buf, rr):
        """Append packed DNS RR and return buf."""
        name = pack_name(rr.name, len(buf), self.label_ptrs)
        rdata = rr.pack_rdata(len(buf) + len(name) + 10, self.label_ptrs)
        return buf + name + struct.pack('>HHIH', rr.type, rr.cls, rr.ttl, len(rdata)) + rdata

    def unpack_rr(self, buf, off):
        """Return DNS RR and new offset."""
        rr = self.RR()
        rr.name, off = unpack_name(buf, off)
        rr.type, rr.cls, rr.ttl, rdlen = struct.unpack('>HHIH', buf[off:off + 10])
        off += 10
        rr.rdata = buf[off:off + rdlen]
        rr.rlen = rdlen
        rr.unpack_rdata(buf, off)
        off += rdlen
        return rr, off

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        off = self.__hdr_len__
        cnt = self.qd  # FIXME: This relies on this being properly set somewhere else
        self.qd = []
        for _ in range(cnt):
            q, off = self.unpack_q(buf, off)
            self.qd.append(q)
        for x in ('an', 'ns', 'ar'):
            cnt = getattr(self, x, 0)
            setattr(self, x, [])
            for _ in range(cnt):
                rr, off = self.unpack_rr(buf, off)
                getattr(self, x).append(rr)
        self.data = b''

    def __len__(self):
        # XXX - cop out
        return len(bytes(self))

    def __bytes__(self):
        # XXX - compress names on the fly
        self.label_ptrs = {}
        buf = struct.pack(self.__hdr_fmt__, self.id, self.op, len(self.qd),
                          len(self.an), len(self.ns), len(self.ar))
        for q in self.qd:
            buf = self.pack_q(buf, q)
        for x in ('an', 'ns', 'ar'):
            for rr in getattr(self, x):
                buf = self.pack_rr(buf, rr)
        del self.label_ptrs
        return buf


# TESTS

def define_testdata():
    """
    Reference test data is stored in the dynamically defined class.

    It is created in this way so that we can import unhexlify only during
    testing, and not during normal use.
    """
    from binascii import unhexlify

    class TestData(object):
        a_resp = unhexlify(
            "059c8180000100010000000106676f6f676c6503636f6d0000010001c00c00010"
            "0010000012b0004d83ace2e0000290200000000000000"
        )
        aaaa_resp = unhexlify(
            "7f228180000100010000000005676d61696c03636f6d00001c0001c00c001c000"
            "10000012b00102a001450400908020000000000002005"
        )
        cname_resp = unhexlify(
            "a154818000010001000000000377777705676d61696c03636f6d0000010001c00"
            "c000500010000545f000e046d61696c06676f6f676c65c016"
        )
        invalid_rr = unhexlify(
            "000001000000000100000000046e616d650000150001000000000000"
        )
        mx_resp = unhexlify(
            "053b8180000100010000000006676f6f676c6503636f6d00000f0001c00c000f0"
            "001000002570011001e04616c7432056173706d78016cc00c"
        )
        null_resp = unhexlify(
            "12b0840000010001000000000b626c6168626c616836363606706972617465037"
            "3656100000a0001c00c000a00010000000000095641434b4403c5e901"
        )
        opt_resp = unhexlify(
            "8d6e0110000100000000000104783131310678787878313106616b616d6169036"
            "e657400000100010000290fa0000080000000"
        )
        ptr_resp = unhexlify(
            "67028180000100010003000001310131033231310331343107696e2d616464720"
            "46172706100000c0001c00c000c000100000d3600240764656661756c740a762d"
            "756d63652d69667305756d6e657405756d6963680365647500c00e00020001000"
            "00d36000d0673686162627903696673c04fc00e0002000100000d36000f0c6669"
            "73682d6c6963656e7365c06dc00e0002000100000d36000b04646e73320369746"
            "4c04f"
        )
        soa_resp = unhexlify(
            "851f8180000100010000000006676f6f676c6503636f6d0000060001c00c00060"
            "001000000230026036e7332c00c09646e732d61646d696ec00c0a747447000003"
            "8400000384000007080000003c"
        )
        srv_resp = unhexlify(
            "7f2281800001000100000000075f6a6162626572045f746370066a61626265720"
            "3636f6d0000210001c00c0021000100000e0f001a000a000014950764656e6a61"
            "6232066a616262657203636f6d00"
        )
        txt_resp = unhexlify(
            "10328180000100010000000006676f6f676c6503636f6d0000100001c00c00100"
            "0010000010e00100f763d7370663120707472203f616c6c"
        )
    return TestData()


def test_basic():
    buf = define_testdata().a_resp
    my_dns = DNS(buf)

    assert my_dns.qd[0].name == 'google.com'
    assert my_dns.an[0].name == 'google.com'
    assert bytes(my_dns) == buf


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


@TryExceptException(NotImplementedError)
def test_Q_len():
    """Test in place for when the method is written"""
    q = DNS.Q()
    len(q)


@TryExceptException(NotImplementedError)
def test_Q_unpack():
    """Test in place for when the method is written"""
    q = DNS.Q()
    q.unpack(None)


def property_runner(prop, ops, set_to=None):
    if set_to is None:
        set_to = [False, True, False]
    buf = define_testdata().a_resp
    dns = DNS(buf)

    for set_to, op in zip(set_to, ops):
        setattr(dns, prop, set_to)
        assert dns.op == op
        assert getattr(dns, prop) == set_to


def test_qr():
    property_runner('qr', ops=[384, 33152, 384])


def test_opcode():
    property_runner('opcode', ops=[33152, 35200, 33152])


def test_aa():
    property_runner('aa', ops=[33152, 34176, 33152])


def test_tc():
    property_runner('tc', ops=[33152, 33664, 33152])


def test_rd():
    property_runner('rd', ops=[32896, 33152, 32896])


def test_ra():
    property_runner('ra', ops=[33024, 33152, 33024])


def test_zero():
    property_runner('zero', ops=[33152, 33216, 33152])


def test_rcode():
    property_runner('rcode', ops=[33152, 33153, 33152])


def test_PTR():
    buf = define_testdata().ptr_resp
    my_dns = DNS(buf)
    assert my_dns.qd[0].name == '1.1.211.141.in-addr.arpa' and \
           my_dns.an[0].ptrname == 'default.v-umce-ifs.umnet.umich.edu' and \
           my_dns.ns[0].nsname == 'shabby.ifs.umich.edu' and \
           my_dns.ns[1].ttl == 3382 and \
           my_dns.ns[2].nsname == 'dns2.itd.umich.edu'
    assert buf == bytes(my_dns)


def test_OPT():
    buf = define_testdata().opt_resp
    my_dns = DNS(buf)
    my_rr = my_dns.ar[0]
    assert my_rr.type == DNS_OPT
    assert my_rr.rlen == 0 and my_rr.rdata == b''
    assert bytes(my_dns) == buf

    my_rr.rdata = b'\x00\x00\x00\x02\x00\x00'  # add 1 attribute tlv
    my_dns2 = DNS(bytes(my_dns))
    my_rr2 = my_dns2.ar[0]
    assert my_rr2.rlen == 6 and my_rr2.rdata == b'\x00\x00\x00\x02\x00\x00'


def test_pack_name():
    # Empty name is \0
    x = pack_name('', 0, {})
    assert x == b'\0'


@TryExceptException(dpkt.UnpackError)
def test_unpack_name():
    """If the offset is longer than the buffer, there will be an UnpackError"""
    unpack_name(b' ', 0)


@TryExceptException(dpkt.UnpackError)
def test_random_data():
    DNS(b'\x83z0\xd2\x9a\xec\x94_7\xf3\xb7+\x85"?\xf0\xfb')


@TryExceptException(dpkt.UnpackError)
def test_circular_pointers():
    DNS(b'\xc0\x00\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x07example\x03com\xc0\x00')


@TryExceptException(dpkt.UnpackError)
def test_very_long_name():
    DNS(b'\x00\x00\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00' + (b'\x10abcdef0123456789' * 16) + b'\x00')


def test_null_response():
    buf = define_testdata().null_resp
    my_dns = DNS(buf)
    assert my_dns.qd[0].name == 'blahblah666.pirate.sea' and \
           my_dns.an[0].null == b'5641434b4403c5e901'
    assert str(buf) == str(my_dns)


def test_txt_response():
    buf = define_testdata().txt_resp
    my_dns = DNS(buf)
    my_rr = my_dns.an[0]
    assert my_rr.type == DNS_TXT
    assert my_rr.name == 'google.com'
    assert my_rr.text == ['v=spf1 ptr ?all']
    assert str(my_dns) == str(buf)
    assert bytes(my_dns) == buf


def test_rdata_TXT():
    rr = DNS.RR(
        type=DNS_TXT,
        text=[b'v=spf1 ptr ?all', b'a=something']
    )
    packdata = rr.pack_rdata(0, {})
    correct = b'\x0fv=spf1 ptr ?all\x0ba=something'
    assert packdata == correct


def test_rdata_HINFO():
    rr = DNS.RR(
        type=DNS_HINFO,
        text=[b'v=spf1 ptr ?all', b'a=something']
    )
    packdata = rr.pack_rdata(0, {})
    correct = b'\x0fv=spf1 ptr ?all\x0ba=something'
    assert packdata == correct


def test_rdata_rdata():
    rr = DNS.RR(
        name='zc.akadns.org',
        ttl=123446,
        rdata=b'?\xf1\xc76',
    )
    packdata = rr.pack_rdata(0, {})
    correct = b'?\xf1\xc76'
    assert packdata == correct


def test_rdata_A():
    rr = DNS.RR(
        name='zc.akadns.org',
        ttl=123446,
        ip=b'?\xf1\xc76',
        type=DNS_A,
    )
    packdata = rr.pack_rdata(0, {})
    correct = b'?\xf1\xc76'
    assert packdata == correct


def test_rdata_NS():
    rr = DNS.RR(
        nsname='zc.akadns.org',
        ttl=123446,
        ip=b'?\xf1\xc76',
        type=DNS_NS,
    )
    packdata = rr.pack_rdata(0, {})
    correct = b'\x02zc\x06akadns\x03org\x00'
    assert packdata == correct


def test_rdata_CNAME():
    rr = DNS.RR(
        cname='zc.akadns.org',
        ttl=123446,
        ip=b'?\xf1\xc76',
        type=DNS_CNAME,
    )
    packdata = rr.pack_rdata(0, {})
    correct = b'\x02zc\x06akadns\x03org\x00'
    assert packdata == correct


def test_rdata_PTR():
    rr = DNS.RR(
        ptrname='default.v-umce-ifs.umnet.umich.edu',
        ttl=1236,
        ip=b'?\xf1\xc76',
        type=DNS_PTR,
    )
    packdata = rr.pack_rdata(0, {})
    correct = b'\x07default\nv-umce-ifs\x05umnet\x05umich\x03edu\x00'
    assert packdata == correct


def test_rdata_SOA():
    rr = DNS.RR(
        mname='blah.google.com',
        rname='moo.blah.com',
        serial=12345666,
        refresh=123463,
        retry=209834,
        minimum=9000,
        expire=28341,
        type=DNS_SOA,
    )
    packdata = rr.pack_rdata(0, {})
    correct = (
        b'\x04blah\x06google\x03com\x00\x03moo\x04blah\xc0\x0c\x00\xbcaB'
        b'\x00\x01\xe2G\x00\x033\xaa\x00\x00n\xb5\x00\x00#(')
    assert packdata == correct


def test_rdata_MX():
    rr = DNS.RR(
        type=DNS_MX,
        preference=2124,
        mxname='mail.google.com',
    )

    packdata = rr.pack_rdata(0, {})
    correct = b'\x08L\x04mail\x06google\x03com\x00'
    assert packdata == correct


def test_rdata_AAAA():
    ip6 = b'&\x07\xf8\xb0@\x0c\x0c\x03\x00\x00\x00\x00\x00\x00\x00\x1a'
    rr = DNS.RR(
        type=DNS_AAAA,
        ip6=ip6,
    )

    packdata = rr.pack_rdata(0, {})
    correct = ip6
    assert packdata == correct


def test_rdata_SRV():
    rr = DNS.RR(
        type=DNS_SRV,
        ttl=86400,
        priority=0,
        weight=5,
        port=5060,
        srvname='_sip._tcp.example.com',
    )

    packdata = rr.pack_rdata(0, {})
    correct = b'\x00\x00\x00\x05\x13\xc4\x04_sip\x04_tcp\x07example\x03com\x00'
    assert packdata == correct


def test_rdata_OPT():
    rr = DNS.RR(
        type=DNS_OPT,
    )

    # TODO: This is hardcoded to return b''. Is this intentional?
    packdata = rr.pack_rdata(0, {})
    correct = b''
    assert packdata == correct


def test_dns_len():
    my_dns = DNS()
    assert len(my_dns) == 12


@TryExceptException(dpkt.PackError)
def test_rdata_FAIL():
    DNS.RR(type=12345666).pack_rdata(0, {})


def test_soa():
    buf = define_testdata().soa_resp
    soa = DNS(buf)

    assert soa.id == 34079
    assert soa.op == 33152

    assert len(soa.qd) == 1
    q = soa.qd[0]
    assert q.name == 'google.com'
    assert q.type == DNS_SOA
    assert q.cls == DNS_IN

    assert len(soa.an) == 1
    a = soa.an[0]
    assert a.name == 'google.com'
    assert a.type == DNS_SOA
    assert a.cls == DNS_IN
    assert a.ttl == 35
    assert a.retry == 900
    assert a.mname == 'ns2.google.com'
    assert a.minimum == 60
    assert a.refresh == 900
    assert a.expire == 1800
    assert a.serial == 175404103
    assert a.rlen == 38
    assert a.rname == 'dns-admin.google.com'
    assert a.rdata == b'\x03ns2\xc0\x0c\tdns-admin\xc0\x0c\nttG\x00\x00\x03\x84\x00\x00\x03\x84\x00\x00\x07\x08\x00\x00\x00<'
    assert soa.ar == []


def test_mx():
    buf = define_testdata().mx_resp
    mx = DNS(buf)

    assert mx.id == 1339
    assert mx.op == 33152

    assert len(mx.qd) == 1
    q = mx.qd[0]
    assert q.name == 'google.com'
    assert q.type == DNS_MX
    assert q.cls == DNS_IN

    assert len(mx.an) == 1
    a = mx.an[0]
    assert a.type == DNS_MX
    assert a.cls == DNS_IN
    assert a.name == 'google.com'
    assert a.ttl == 599
    assert a.mxname == 'alt2.aspmx.l.google.com'
    assert a.preference == (30,)

    assert a.rlen == 17
    assert a.rdata == b'\x00\x1e\x04alt2\x05aspmx\x01l\xc0\x0c'
    assert mx.ar == []


def test_aaaa():
    buf = define_testdata().aaaa_resp
    aaaa = DNS(buf)

    aaaa.id = 32546
    aaaa.op = 33152

    assert len(aaaa.qd) == 1
    q = aaaa.qd[0]
    assert q.type == DNS_AAAA
    assert q.name == 'gmail.com'

    assert len(aaaa.an) == 1
    a = aaaa.an[0]
    assert a.type == DNS_AAAA
    assert a.cls == DNS_IN
    assert a.name == 'gmail.com'
    assert a.ttl == 299
    assert a.ip6 == b'*\x00\x14P@\t\x08\x02\x00\x00\x00\x00\x00\x00 \x05'

    assert a.rlen == 16
    assert a.rdata == b'*\x00\x14P@\t\x08\x02\x00\x00\x00\x00\x00\x00 \x05'
    assert aaaa.ar == []


def test_srv():
    buf = define_testdata().srv_resp
    srv = DNS(buf)

    srv.id = 32546
    srv.op = 33152

    assert len(srv.qd) == 1
    q = srv.qd[0]
    assert q.type == DNS_SRV
    assert q.name == '_jabber._tcp.jabber.com'
    assert q.cls == DNS_IN

    assert len(srv.an) == 1
    a = srv.an[0]
    assert a.type == DNS_SRV
    assert a.cls == DNS_IN
    assert a.name == '_jabber._tcp.jabber.com'
    assert a.port == 5269
    assert a.ttl == 3599
    assert a.srvname == 'denjab2.jabber.com'
    assert a.priority == 10
    assert a.weight == 0

    assert a.rlen == 26
    assert a.rdata == b'\x00\n\x00\x00\x14\x95\x07denjab2\x06jabber\x03com\x00'
    assert srv.ar == []


def test_cname():
    buf = define_testdata().cname_resp
    cname = DNS(buf)

    cname.id = 41300
    cname.op = 33152

    assert len(cname.qd) == 1
    q = cname.qd[0]
    assert q.type == DNS_A
    assert q.cls == DNS_IN
    assert q.name == 'www.gmail.com'

    assert len(cname.an) == 1
    a = cname.an[0]
    assert a.type == DNS_CNAME
    assert a.cls == DNS_IN
    assert a.name == 'www.gmail.com'
    assert a.ttl == 21599
    assert a.cname == 'mail.google.com'

    assert a.rlen == 14
    assert a.rdata == b'\x04mail\x06google\xc0\x16'
    assert cname.ar == []


@TryExceptException(dpkt.UnpackError)
def test_invalid_rr():
    buf = define_testdata().invalid_rr
    DNS(buf)
