# $Id: dhcp.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Dynamic Host Configuration Protocol."""
from __future__ import print_function
from __future__ import absolute_import

import struct

from . import arp
from . import dpkt
from .compat import compat_ord

DHCP_OP_REQUEST = 1
DHCP_OP_REPLY = 2

DHCP_MAGIC = 0x63825363

# DHCP option codes
DHCP_OPT_NETMASK = 1  # I: subnet mask
DHCP_OPT_TIMEOFFSET = 2
DHCP_OPT_ROUTER = 3  # s: list of router ips
DHCP_OPT_TIMESERVER = 4
DHCP_OPT_NAMESERVER = 5
DHCP_OPT_DNS_SVRS = 6  # s: list of DNS servers
DHCP_OPT_LOGSERV = 7
DHCP_OPT_COOKIESERV = 8
DHCP_OPT_LPRSERV = 9
DHCP_OPT_IMPSERV = 10
DHCP_OPT_RESSERV = 11
DHCP_OPT_HOSTNAME = 12  # s: client hostname
DHCP_OPT_BOOTFILESIZE = 13
DHCP_OPT_DUMPFILE = 14
DHCP_OPT_DOMAIN = 15  # s: domain name
DHCP_OPT_SWAPSERV = 16
DHCP_OPT_ROOTPATH = 17
DHCP_OPT_EXTENPATH = 18
DHCP_OPT_IPFORWARD = 19
DHCP_OPT_SRCROUTE = 20
DHCP_OPT_POLICYFILTER = 21
DHCP_OPT_MAXASMSIZE = 22
DHCP_OPT_IPTTL = 23
DHCP_OPT_MTUTIMEOUT = 24
DHCP_OPT_MTUTABLE = 25
DHCP_OPT_MTUSIZE = 26
DHCP_OPT_LOCALSUBNETS = 27
DHCP_OPT_BROADCASTADDR = 28
DHCP_OPT_DOMASKDISCOV = 29
DHCP_OPT_MASKSUPPLY = 30
DHCP_OPT_DOROUTEDISC = 31
DHCP_OPT_ROUTERSOLICIT = 32
DHCP_OPT_STATICROUTE = 33
DHCP_OPT_TRAILERENCAP = 34
DHCP_OPT_ARPTIMEOUT = 35
DHCP_OPT_ETHERENCAP = 36
DHCP_OPT_TCPTTL = 37
DHCP_OPT_TCPKEEPALIVE = 38
DHCP_OPT_TCPALIVEGARBAGE = 39
DHCP_OPT_NISDOMAIN = 40
DHCP_OPT_NISSERVERS = 41
DHCP_OPT_NISTIMESERV = 42
DHCP_OPT_VENDSPECIFIC = 43
DHCP_OPT_NBNS = 44
DHCP_OPT_NBDD = 45
DHCP_OPT_NBTCPIP = 46
DHCP_OPT_NBTCPSCOPE = 47
DHCP_OPT_XFONT = 48
DHCP_OPT_XDISPLAYMGR = 49
DHCP_OPT_REQ_IP = 50  # I: IP address
DHCP_OPT_LEASE_SEC = 51  # I: lease seconds
DHCP_OPT_OPTIONOVERLOAD = 52
DHCP_OPT_MSGTYPE = 53  # B: message type
DHCP_OPT_SERVER_ID = 54  # I: server IP address
DHCP_OPT_PARAM_REQ = 55  # s: list of option codes
DHCP_OPT_MESSAGE = 56
DHCP_OPT_MAXMSGSIZE = 57
DHCP_OPT_RENEWTIME = 58
DHCP_OPT_REBINDTIME = 59
DHCP_OPT_VENDOR_ID = 60  # s: vendor class id
DHCP_OPT_CLIENT_ID = 61  # Bs: idtype, id (idtype 0: FQDN, idtype 1: MAC)
DHCP_OPT_NISPLUSDOMAIN = 64
DHCP_OPT_NISPLUSSERVERS = 65
DHCP_OPT_MOBILEIPAGENT = 68
DHCP_OPT_SMTPSERVER = 69
DHCP_OPT_POP3SERVER = 70
DHCP_OPT_NNTPSERVER = 71
DHCP_OPT_WWWSERVER = 72
DHCP_OPT_FINGERSERVER = 73
DHCP_OPT_IRCSERVER = 74
DHCP_OPT_STSERVER = 75
DHCP_OPT_STDASERVER = 76

# DHCP message type values
DHCPDISCOVER = 1
DHCPOFFER = 2
DHCPREQUEST = 3
DHCPDECLINE = 4
DHCPACK = 5
DHCPNAK = 6
DHCPRELEASE = 7
DHCPINFORM = 8


class DHCP(dpkt.Packet):
    """Dynamic Host Configuration Protocol.

    The Dynamic Host Configuration Protocol (DHCP) is a network management protocol used on Internet Protocol (IP)
    networks for automatically assigning IP addresses and other communication parameters to devices connected
    to the network using a client–server architecture.

    Attributes:
        __hdr__: Header fields of DHCP.
            op: (int): Operation. Message op code / message type. 1 = BOOTREQUEST, 2 = BOOTREPLY. (1 byte)
            hrd: (int): Hardware type. Hardware address type, see ARP section in "Assigned
                    Numbers" RFC; e.g., '1' = 10mb ethernet. (1 byte)
            hln: (int): Hardware Length. Hardware address length (e.g.  '6' for 10mb
                    ethernet). (1 byte)
            hops: (int): Hops. Client sets to zero, optionally used by relay agents
                    when booting via a relay agent. (1 byte)
            xid: (int): Transaction ID. A random number chosen by the
                    client, used by the client and server to associate
                    messages and responses between a client and a
                    server. (4 bytes)
            secs: (int): Seconds. Filled in by client, seconds elapsed since client
                    began address acquisition or renewal process. (2 bytes)
            flags: (int): DHCP Flags. (2 bytes)
            ciaddr: (int): Client IP address. Only filled in if client is in
                    BOUND, RENEW or REBINDING state and can respond
                    to ARP requests. (4 bytes)
            yiaddr: (int): User IP address. (4 bytes)
            siaddr: (int): Server IP address. IP address of next server to use in bootstrap;
                    returned in DHCPOFFER, DHCPACK by server. (4 bytes)
            giaddr: (int): Gateway IP address. Relay agent IP address, used in booting via a
                    relay agent. (4 bytes)
            chaddr: (int): Client hardware address. (16 bytes)
            sname: (int): Server Hostname. Optional, null terminated string. (64 bytes)
            file: (int): Boot file name. Null terminated string; "generic"
                    name or null in DHCPDISCOVER, fully qualified
                    directory-path name in DHCPOFFER. (128 bytes)
            magic: (int): Magic cookie. Optional parameters field.  See the options
                    documents for a list of defined options. (4 bytes)
    """

    __hdr__ = (
        ('op', 'B', DHCP_OP_REQUEST),
        ('hrd', 'B', arp.ARP_HRD_ETH),  # just like ARP.hrd
        ('hln', 'B', 6),  # and ARP.hln
        ('hops', 'B', 0),
        ('xid', 'I', 0xdeadbeef),
        ('secs', 'H', 0),
        ('flags', 'H', 0),
        ('ciaddr', 'I', 0),
        ('yiaddr', 'I', 0),
        ('siaddr', 'I', 0),
        ('giaddr', 'I', 0),
        ('chaddr', '16s', 16 * b'\x00'),
        ('sname', '64s', 64 * b'\x00'),
        ('file', '128s', 128 * b'\x00'),
        ('magic', 'I', DHCP_MAGIC),
    )
    opts = (
        (DHCP_OPT_MSGTYPE, chr(DHCPDISCOVER)),
        (DHCP_OPT_PARAM_REQ, ''.join(map(chr, (DHCP_OPT_REQ_IP,
                                               DHCP_OPT_ROUTER,
                                               DHCP_OPT_NETMASK,
                                               DHCP_OPT_DNS_SVRS))))
    )  # list of (type, data) tuples

    def __len__(self):
        return self.__hdr_len__ + \
            sum([2 + len(o[1]) for o in self.opts]) + 1 + len(self.data)

    def __bytes__(self):
        return self.pack_hdr() + self.pack_opts() + bytes(self.data)

    def pack_opts(self):
        """Return packed options string."""
        if not self.opts:
            return b''
        l_ = []
        for t, data in self.opts:
            l_.append(struct.pack("BB%is" % len(data), t, len(data), data))
        l_.append(b'\xff')
        return b''.join(l_)

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        self.chaddr = self.chaddr[:self.hln]
        buf = self.data
        l_ = []
        while buf:
            t = compat_ord(buf[0])
            if t == 0xff:
                buf = buf[1:]
                break
            elif t == 0:
                buf = buf[1:]
            else:
                n = compat_ord(buf[1])
                l_.append((t, buf[2:2 + n]))
                buf = buf[2 + n:]
        self.opts = l_
        self.data = buf


def test_dhcp():
    s = (
        b'\x01\x01\x06\x00\xad\x53\xc8\x63\xb8\x87\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        b'\x00\x00\x00\x00\x02\x55\x82\xf3\xa6\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x63\x82\x53\x63\x35\x01\x01\xfb\x01\x01\x3d\x07\x01\x00'
        b'\x02\x55\x82\xf3\xa6\x32\x04\x0a\x00\x01\x65\x0c\x09\x47\x75\x69\x6e\x65\x76\x65\x72\x65\x3c\x08\x4d'
        b'\x53\x46\x54\x20\x35\x2e\x30\x37\x0a\x01\x0f\x03\x06\x2c\x2e\x2f\x1f\x21\x2b\xff\x00\x00\x00\x00\x00'
    )

    dhcp = DHCP(s)
    assert (s == bytes(dhcp))
    assert len(dhcp) == 300
    assert isinstance(dhcp.chaddr, bytes)
    assert isinstance(dhcp.sname, bytes)
    assert isinstance(dhcp.file, bytes)

    # Test default construction
    dhcp = DHCP()
    assert isinstance(dhcp.chaddr, bytes)
    assert isinstance(dhcp.sname, bytes)
    assert isinstance(dhcp.file, bytes)


def test_no_opts():
    from binascii import unhexlify
    buf_small_hdr = unhexlify(
        '00'        # op
        '00'        # hrd
        '06'        # hln
        '12'        # hops
        'deadbeef'  # xid
        '1234'      # secs
        '9866'      # flags
        '00000000'  # ciaddr
        '00000000'  # yiaddr
        '00000000'  # siaddr
        '00000000'  # giaddr
    )

    buf = b''.join([
        buf_small_hdr,
        b'\x00' * 16,   # chaddr
        b'\x11' * 64,   # sname
        b'\x22' * 128,  # file
        b'\x44' * 4,    # magic

        b'\x00'         # data
    ])

    dhcp = DHCP(buf)
    assert dhcp.opts == []
    assert dhcp.data == b''
    assert dhcp.pack_opts() == b''
