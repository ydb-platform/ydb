# $Id: netbios.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Network Basic Input/Output System."""
from __future__ import absolute_import

import struct

from . import dpkt
from . import dns
from .compat import compat_ord


def encode_name(name):
    """
    Return the NetBIOS first-level encoded name.

    14.1.  FIRST LEVEL ENCODING

    The first level representation consists of two parts:

     -  NetBIOS name
     -  NetBIOS scope identifier

    The 16 byte NetBIOS name is mapped into a 32 byte wide field using a
    reversible, half-ASCII, biased encoding.  Each half-octet of the
    NetBIOS name is encoded into one byte of the 32 byte field.  The
    first half octet is encoded into the first byte, the second half-
    octet into the second byte, etc.

    Each 4-bit, half-octet of the NetBIOS name is treated as an 8-bit,
    right-adjusted, zero-filled binary number.  This number is added to
    value of the ASCII character 'A' (hexadecimal 41).  The resulting 8-
    bit number is stored in the appropriate byte.  The following diagram
    demonstrates this procedure:


                         0 1 2 3 4 5 6 7
                        +-+-+-+-+-+-+-+-+
                        |a b c d|w x y z|          ORIGINAL BYTE
                        +-+-+-+-+-+-+-+-+
                            |       |
                   +--------+       +--------+
                   |                         |     SPLIT THE NIBBLES
                   v                         v
            0 1 2 3 4 5 6 7           0 1 2 3 4 5 6 7
           +-+-+-+-+-+-+-+-+         +-+-+-+-+-+-+-+-+
           |0 0 0 0 a b c d|         |0 0 0 0 w x y z|
           +-+-+-+-+-+-+-+-+         +-+-+-+-+-+-+-+-+
                   |                         |
                   +                         +     ADD 'A'
                   |                         |
            0 1 2 3 4 5 6 7           0 1 2 3 4 5 6 7
           +-+-+-+-+-+-+-+-+         +-+-+-+-+-+-+-+-+
           |0 1 0 0 0 0 0 1|         |0 1 0 0 0 0 0 1|
           +-+-+-+-+-+-+-+-+         +-+-+-+-+-+-+-+-+

    This encoding results in a NetBIOS name being represented as a
    sequence of 32 ASCII, upper-case characters from the set
    {A,B,C...N,O,P}.

    The NetBIOS scope identifier is a valid domain name (without a
    leading dot).

    An ASCII dot (2E hexadecimal) and the scope identifier are appended
    to the encoded form of the NetBIOS name, the result forming a valid
    domain name.
    """
    l_ = []
    for c in struct.pack('16s', name.encode()):
        c = compat_ord(c)
        l_.append(chr((c >> 4) + 0x41))
        l_.append(chr((c & 0xf) + 0x41))
    return ''.join(l_)


def decode_name(nbname):
    """
    Return the NetBIOS first-level decoded nbname.

    """
    if len(nbname) != 32:
        return nbname

    l_ = []
    for i in range(0, 32, 2):
        l_.append(
            chr(
                ((ord(nbname[i]) - 0x41) << 4) |
                ((ord(nbname[i + 1]) - 0x41) & 0xf)
            )
        )
    return ''.join(l_).split('\x00', 1)[0]


# RR types
NS_A = 0x01  # IP address
NS_NS = 0x02  # Name Server
NS_NULL = 0x0A  # NULL
NS_NB = 0x20  # NetBIOS general Name Service
NS_NBSTAT = 0x21  # NetBIOS NODE STATUS

# RR classes
NS_IN = 1

# NBSTAT name flags
NS_NAME_G = 0x8000  # group name (as opposed to unique)
NS_NAME_DRG = 0x1000  # deregister
NS_NAME_CNF = 0x0800  # conflict
NS_NAME_ACT = 0x0400  # active
NS_NAME_PRM = 0x0200  # permanent

# NBSTAT service names
nbstat_svcs = {
    # (service, unique): list of ordered (name prefix, service name) tuples
    (0x00, 0): [('', 'Domain Name')],
    (0x00, 1): [('IS~', 'IIS'), ('', 'Workstation Service')],
    (0x01, 0): [('__MSBROWSE__', 'Master Browser')],
    (0x01, 1): [('', 'Messenger Service')],
    (0x03, 1): [('', 'Messenger Service')],
    (0x06, 1): [('', 'RAS Server Service')],
    (0x1B, 1): [('', 'Domain Master Browser')],
    (0x1C, 0): [('INet~Services', 'IIS'), ('', 'Domain Controllers')],
    (0x1D, 1): [('', 'Master Browser')],
    (0x1E, 0): [('', 'Browser Service Elections')],
    (0x1F, 1): [('', 'NetDDE Service')],
    (0x20, 1): [('Forte_$ND800ZA', 'DCA IrmaLan Gateway Server Service'),
                ('', 'File Server Service')],
    (0x21, 1): [('', 'RAS Client Service')],
    (0x22, 1): [('', 'Microsoft Exchange Interchange(MSMail Connector)')],
    (0x23, 1): [('', 'Microsoft Exchange Store')],
    (0x24, 1): [('', 'Microsoft Exchange Directory')],
    (0x2B, 1): [('', 'Lotus Notes Server Service')],
    (0x2F, 0): [('IRISMULTICAST', 'Lotus Notes')],
    (0x30, 1): [('', 'Modem Sharing Server Service')],
    (0x31, 1): [('', 'Modem Sharing Client Service')],
    (0x33, 0): [('IRISNAMESERVER', 'Lotus Notes')],
    (0x43, 1): [('', 'SMS Clients Remote Control')],
    (0x44, 1): [('', 'SMS Administrators Remote Control Tool')],
    (0x45, 1): [('', 'SMS Clients Remote Chat')],
    (0x46, 1): [('', 'SMS Clients Remote Transfer')],
    (0x4C, 1): [('', 'DEC Pathworks TCPIP service on Windows NT')],
    (0x52, 1): [('', 'DEC Pathworks TCPIP service on Windows NT')],
    (0x87, 1): [('', 'Microsoft Exchange MTA')],
    (0x6A, 1): [('', 'Microsoft Exchange IMC')],
    (0xBE, 1): [('', 'Network Monitor Agent')],
    (0xBF, 1): [('', 'Network Monitor Application')]
}


def node_to_service_name(name_service_flags):
    name, service, flags = name_service_flags
    try:
        unique = int(flags & NS_NAME_G == 0)
        for namepfx, svcname in nbstat_svcs[(service, unique)]:
            if name.startswith(namepfx):
                return svcname
    except KeyError:
        pass
    return ''


class NS(dns.DNS):
    """
    NetBIOS Name Service.

    RFC1002: https://tools.ietf.org/html/rfc1002
    """

    class Q(dns.DNS.Q):
        pass

    class RR(dns.DNS.RR):
        """NetBIOS resource record.

        RFC1001: 14.  REPRESENTATION OF NETBIOS NAMES

        NetBIOS names as seen across the client interface to NetBIOS are
        exactly 16 bytes long.  Within the NetBIOS-over-TCP protocols, a
        longer representation is used.

        There are two levels of encoding.  The first level maps a NetBIOS
        name into a domain system name.  The second level maps the domain
        system name into the "compressed" representation required for
        interaction with the domain name system.

        Except in one packet, the second level representation is the only
        NetBIOS name representation used in NetBIOS-over-TCP packet formats.
        The exception is the RDATA field of a NODE STATUS RESPONSE packet.
        """

        _node_name_struct = struct.Struct('>15s B H')
        _node_name_len = _node_name_struct.size

        def unpack_rdata(self, buf, off):
            if self.type == NS_A:
                self.ip = self.rdata
            elif self.type == NS_NBSTAT:
                num_names = compat_ord(self.rdata[0])

                self.nodenames = [
                    self._node_name_struct.unpack_from(
                        self.rdata, 1+idx*self._node_name_len
                    ) for idx in range(num_names)
                ]
                # XXX - skip stats


class Session(dpkt.Packet):
    """NetBIOS Session Service."""
    __hdr__ = (
        ('type', 'B', 0),
        ('flags', 'B', 0),
        ('len', 'H', 0)
    )


SSN_MESSAGE = 0
SSN_REQUEST = 1
SSN_POSITIVE = 2
SSN_NEGATIVE = 3
SSN_RETARGET = 4
SSN_KEEPALIVE = 5


class Datagram(dpkt.Packet):
    """NetBIOS Datagram Service."""
    __hdr__ = (
        ('type', 'B', 0),
        ('flags', 'B', 0),
        ('id', 'H', 0),
        ('src', 'I', 0),
        ('sport', 'H', 0),
        ('len', 'H', 0),
        ('off', 'H', 0)
    )


DGRAM_UNIQUE = 0x10
DGRAM_GROUP = 0x11
DGRAM_BROADCAST = 0x12
DGRAM_ERROR = 0x13
DGRAM_QUERY = 0x14
DGRAM_POSITIVE = 0x15
DGRAM_NEGATIVE = 0x16


def test_encode_name():
    assert encode_name('The NetBIOS name') == 'FEGIGFCAEOGFHEECEJEPFDCAGOGBGNGF'
    # rfc1002
    assert encode_name('FRED            ') == 'EGFCEFEECACACACACACACACACACACACA'
    # https://github.com/kbandla/dpkt/issues/458
    assert encode_name('*') == 'CKAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'


def test_decode_name():
    assert decode_name('FEGIGFCAEOGFHEECEJEPFDCAGOGBGNGF') == 'The NetBIOS name'
    # original botched example from rfc1001
    assert decode_name('FEGHGFCAEOGFHEECEJEPFDCAHEGBGNGF') == 'Tge NetBIOS tame'
    assert decode_name('CKAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') == '*'

    # decode a name which is not 32 chars long
    assert decode_name('CKAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABB') == 'CKAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABB'


def test_node_to_service_name():
    svcname = node_to_service_name(("ISS", 0x00, 0x0800))
    assert svcname == "Workstation Service"


def test_node_to_service_name_keyerror():
    svcname = node_to_service_name(("ISS", 0xff, 0x0800))
    assert svcname == ""


def test_rr():
    import pytest
    from binascii import unhexlify

    rr = NS.RR()
    with pytest.raises(NotImplementedError):
        len(rr)

    buf = unhexlify(''.join([
        '01',        # A record
        '0001',      # DNS_IN
        '00000000',  # TTL
        '0000',      # rlen
    ]))
    rr.unpack_rdata(buf, 0)
    assert rr.ip == rr.rdata


def test_rr_nbstat():
    from binascii import unhexlify
    buf = unhexlify(''.join([
        '41' * 1025,  # Name
        '0033',       # NS_NBSTAT
        '0001',       # DNS_IN
        '00000000',   # TTL
        '0004',       # rlen
    ]))
    rdata = (
        b'\x02'  # NUM_NAMES
        b'ABCDEFGHIJKLMNO\x2f\x01\x02'
        b'PQRSTUVWXYZABCD\x43\x03\x04'
    )
    rr = NS.RR(
        type=NS_NBSTAT,
        rdata=rdata,
    )

    assert rr.type == NS_NBSTAT
    rr.unpack_rdata(buf, 0)
    assert rr.nodenames == [
        (b'ABCDEFGHIJKLMNO', 0x2f, 0x0102),
        (b'PQRSTUVWXYZABCD', 0x43, 0x0304),
    ]


def test_ns():
    from binascii import unhexlify
    ns = NS()
    correct = unhexlify(
        '0000'
        '0100'
        '0000000000000000'
    )
    assert bytes(ns) == correct
