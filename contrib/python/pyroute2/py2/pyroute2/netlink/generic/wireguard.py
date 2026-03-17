'''

Usage::

    # Imports
    from pyroute2 import IPDB, WireGuard

    IFNAME = 'wg1'

    # Create a WireGuard interface
    with IPDB() as ip:
        wg1 = ip.create(kind='wireguard', ifname=IFNAME)
        wg1.add_ip('10.0.0.1/24')
        wg1.up()
        wg1.commit()

    # Create WireGuard object
    wg = WireGuard()

    # Add a WireGuard configuration + first peer
    peer = {'public_key': 'TGFHcm9zc2VCaWNoZV9DJ2VzdExhUGx1c0JlbGxlPDM=',
            'endpoint_addr': '8.8.8.8',
            'endpoint_port': 8888,
            'persistent_keepalive': 15,
            'allowed_ips': ['10.0.0.0/24', '8.8.8.8/32']}
    wg.set(IFNAME, private_key='RCdhcHJlc0JpY2hlLEplU2VyYWlzTGFQbHVzQm9ubmU=',
           fwmark=0x1337, listen_port=2525, peer=peer)

    # Add second peer with preshared key
    peer = {'public_key': 'RCdBcHJlc0JpY2hlLFZpdmVMZXNQcm9iaW90aXF1ZXM=',
            'preshared_key': 'Pz8/V2FudFRvVHJ5TXlBZXJvR3Jvc3NlQmljaGU/Pz8=',
            'endpoint_addr': '8.8.8.8',
            'endpoint_port': 9999,
            'persistent_keepalive': 25,
            'allowed_ips': ['::/0']}
    wg.set(IFNAME, peer=peer)

    # Delete second peer
    peer = {'public_key': 'RCdBcHJlc0JpY2hlLFZpdmVMZXNQcm9iaW90aXF1ZXM=',
            'remove': True}
    wg.set(IFNAME, peer=peer)

    # Get information of the interface
    wg.info(IFNAME)

    # Get specific value from the interface
    wg.info(IFNAME)[0].WGDEVICE_A_PRIVATE_KEY.value


NOTES:

* Using `set` method only requires an interface name.
* The `peer` structure is described as follow::

    struct peer_s {
        public_key:            # Base64 public key - required
        remove:                # Boolean - optional
        preshared_key:         # Base64 preshared key - optional
        endpoint_addr:         # IPv4 or IPv6 endpoint - optional
        endpoint_port :        # endpoint Port - required only if endpoint_addr
        persistent_keepalive:  # time in seconds to send keep alive - optional
        allowed_ips:           # list of CIDRs allowed - optional
    }
'''


from base64 import b64encode, b64decode
from binascii import a2b_hex
import errno
import logging
from socket import inet_ntoa, inet_aton, inet_pton, AF_INET, AF_INET6
from struct import pack, unpack
from time import ctime

from pyroute2.netlink import genlmsg
from pyroute2.netlink import nla
from pyroute2.netlink import NLM_F_ACK
from pyroute2.netlink import NLM_F_DUMP
from pyroute2.netlink import NLA_F_NESTED
from pyroute2.netlink import NLM_F_REQUEST
from pyroute2.netlink.generic import GenericNetlinkSocket


# Defines from uapi/wireguard.h
WG_GENL_NAME = "wireguard"
WG_GENL_VERSION = 1
WG_KEY_LEN = 32

# WireGuard Device commands
WG_CMD_GET_DEVICE = 0
WG_CMD_SET_DEVICE = 1

# Wireguard Device attributes
WGDEVICE_A_UNSPEC = 0
WGDEVICE_A_IFINDEX = 1
WGDEVICE_A_IFNAME = 2
WGDEVICE_A_PRIVATE_KEY = 3
WGDEVICE_A_PUBLIC_KEY = 4
WGDEVICE_A_FLAGS = 5
WGDEVICE_A_LISTEN_PORT = 6
WGDEVICE_A_FWMARK = 7
WGDEVICE_A_PEERS = 8

# WireGuard Device flags
WGDEVICE_F_REPLACE_PEERS = 1

# WireGuard Allowed IP attributes
WGALLOWEDIP_A_UNSPEC = 0
WGALLOWEDIP_A_FAMILY = 1
WGALLOWEDIP_A_IPADDR = 2
WGALLOWEDIP_A_CIDR_MASK = 3

# WireGuard Peer flags
WGPEER_F_REMOVE_ME = 0
WGPEER_F_REPLACE_ALLOWEDIPS = 1
WGPEER_F_UPDATE_ONLY = 2

# Specific defines
WG_MAX_PEERS = 1000
WG_MAX_ALLOWEDIPS = 1000


class wgmsg(genlmsg):
    prefix = 'WGDEVICE_A_'

    nla_map = (('WGDEVICE_A_UNSPEC', 'none'),
               ('WGDEVICE_A_IFINDEX', 'uint32'),
               ('WGDEVICE_A_IFNAME', 'asciiz'),
               ('WGDEVICE_A_PRIVATE_KEY', 'parse_wg_key'),
               ('WGDEVICE_A_PUBLIC_KEY', 'parse_wg_key'),
               ('WGDEVICE_A_FLAGS', 'uint32'),
               ('WGDEVICE_A_LISTEN_PORT', 'uint16'),
               ('WGDEVICE_A_FWMARK', 'uint32'),
               ('WGDEVICE_A_PEERS', '*wgdevice_peer'))

    class wgdevice_peer(nla):
        prefix = 'WGPEER_A_'

        nla_flags = NLA_F_NESTED
        nla_map = (('WGPEER_A_UNSPEC', 'none'),
                   ('WGPEER_A_PUBLIC_KEY', 'parse_peer_key'),
                   ('WGPEER_A_PRESHARED_KEY', 'parse_peer_key'),
                   ('WGPEER_A_FLAGS', 'uint32'),
                   ('WGPEER_A_ENDPOINT', 'parse_endpoint'),
                   ('WGPEER_A_PERSISTENT_KEEPALIVE_INTERVAL', 'uint16'),
                   ('WGPEER_A_LAST_HANDSHAKE_TIME',
                    'parse_handshake_time'),
                   ('WGPEER_A_RX_BYTES', 'uint64'),
                   ('WGPEER_A_TX_BYTES', 'uint64'),
                   ('WGPEER_A_ALLOWEDIPS', '*wgpeer_allowedip'),
                   ('WGPEER_A_PROTOCOL_VERSION', 'uint32'))

        class parse_peer_key(nla):
            fields = (('key', 's'), )

            def decode(self):
                nla.decode(self)
                self['value'] = b64encode(self['value'])

            def encode(self):
                self['key'] = b64decode(self['value'])
                nla.encode(self)

        class parse_endpoint(nla):
            fields = (('family', 'H'),
                      ('port', '>H'),
                      ('addr4', '>I'),
                      ('addr6', 's'))

            def decode(self):
                nla.decode(self)
                if self['family'] == AF_INET:
                    self['addr'] = inet_ntoa(pack('>I', self['addr4']))
                else:
                    self['addr'] = inet_ntoa(AF_INET6, self['addr6'])
                del self['addr4']
                del self['addr6']

            def encode(self):
                if self['addr'].find(":") > -1:
                    self['family'] = AF_INET6
                    self['addr4'] = 0  # Set to NULL
                    self['addr6'] = inet_pton(AF_INET6, self['addr'])
                else:
                    self['family'] = AF_INET
                    self['addr4'] = unpack('>I',
                                           inet_aton(self['addr']))[0]
                    self['addr6'] = b'\x00\x00\x00\x00\x00\x00\x00\x00'
                self['port'] = int(self['port'])
                nla.encode(self)

        class parse_handshake_time(nla):
            fields = (('tv_sec', 'Q'),
                      ('tv_nsec', 'Q'))

            def decode(self):
                nla.decode(self)
                self['latest handshake'] = ctime(self['tv_sec'])

        class wgpeer_allowedip(nla):
            prefix = 'WGALLOWEDIP_A_'

            nla_flags = NLA_F_NESTED
            nla_map = (('WGALLOWEDIP_A_UNSPEC', 'none'),
                       ('WGALLOWEDIP_A_FAMILY', 'uint16'),
                       ('WGALLOWEDIP_A_IPADDR', 'hex'),
                       ('WGALLOWEDIP_A_CIDR_MASK', 'uint8'))

            def decode(self):
                nla.decode(self)
                if self.get_attr('WGALLOWEDIP_A_FAMILY') == AF_INET:
                    pre = (self
                           .get_attr('WGALLOWEDIP_A_IPADDR')
                           .replace(':', ''))
                    self['addr'] = inet_ntoa(a2b_hex(pre))
                else:
                    self['addr'] = (self
                                    .get_attr('WGALLOWEDIP_A_IPADDR'))
                wgaddr = self.get_attr('WGALLOWEDIP_A_CIDR_MASK')
                self['addr'] = '{0}/{1}'.format(self['addr'], wgaddr)

    class parse_wg_key(nla):
        fields = (('key', 's'), )

        def decode(self):
            nla.decode(self)
            self['value'] = b64encode(self['value'])

        def encode(self):
            self['key'] = b64decode(self['value'])
            nla.encode(self)


class WireGuard(GenericNetlinkSocket):

    def __init__(self):
        GenericNetlinkSocket.__init__(self)
        self.bind(WG_GENL_NAME, wgmsg)

    def info(self,
             interface):
        msg = wgmsg()
        msg['cmd'] = WG_CMD_GET_DEVICE
        msg['attrs'].append(['WGDEVICE_A_IFNAME', interface])
        return self.nlm_request(msg,
                                msg_type=self.prid,
                                msg_flags=NLM_F_REQUEST | NLM_F_DUMP)

    def set(self,
            interface,
            listen_port=None,
            fwmark=None,
            private_key=None,
            peer=None):
        msg = wgmsg()
        msg['attrs'].append(['WGDEVICE_A_IFNAME', interface])

        if private_key is not None:
            self._wg_test_key(private_key)
            msg['attrs'].append(['WGDEVICE_A_PRIVATE_KEY', private_key])

        if listen_port is not None:
            msg['attrs'].append(['WGDEVICE_A_LISTEN_PORT', listen_port])

        if fwmark is not None:
            msg['attrs'].append(['WGDEVICE_A_FWMARK', fwmark])

        if peer is not None:
            self._wg_set_peer(msg, peer)

        # Message attributes
        msg['cmd'] = WG_CMD_SET_DEVICE
        msg['version'] = WG_GENL_VERSION
        msg['header']['type'] = self.prid
        msg['header']['flags'] = NLM_F_REQUEST | NLM_F_ACK
        msg['header']['pid'] = self.pid
        msg.encode()
        self.sendto(msg.data, (0, 0))
        msg = self.get()[0]
        err = msg['header'].get('error', None)
        if err is not None:
            if hasattr(err, 'code') and err.code == errno.ENOENT:
                logging.error('Generic netlink protocol %s not found'
                              % self.prid)
                logging.error('Please check if the protocol module is loaded')
            raise err
        return msg

    def _wg_test_key(self, key):
        try:
            if len(b64decode(key)) != WG_KEY_LEN:
                raise ValueError('Invalid WireGuard key length')
        except TypeError:
            raise ValueError('Failed to decode Base64 key')

    def _wg_set_peer(self, msg, peer):
        attrs = []
        wg_peer = [{'attrs': attrs}]
        if 'public_key' not in peer:
            raise ValueError('Peer Public key required')

        # Check public key validity
        public_key = peer['public_key']
        self._wg_test_key(public_key)
        attrs.append(['WGPEER_A_PUBLIC_KEY', public_key])

        # If peer removal is set to True
        if 'remove' in peer and peer['remove']:
            attrs.append(['WGPEER_A_FLAGS', WGDEVICE_F_REPLACE_PEERS])
            msg['attrs'].append(['WGDEVICE_A_PEERS', wg_peer])
            return

        # Set Endpoint
        if 'endpoint_addr' in peer and 'endpoint_port' in peer:
            attrs.append(['WGPEER_A_ENDPOINT',
                          {'addr': peer['endpoint_addr'],
                           'port': peer['endpoint_port']}])

        # Set Preshared key
        if 'preshared_key' in peer:
            pkey = peer['preshared_key']
            self._wg_test_key(pkey)
            attrs.append(['WGPEER_A_PRESHARED_KEY', pkey])

        # Set Persistent Keepalive time
        if 'persistent_keepalive' in peer:
            keepalive = peer['persistent_keepalive']
            attrs.append(['WGPEER_A_PERSISTENT_KEEPALIVE_INTERVAL',
                          keepalive])

        # Set Peer flags
        attrs.append(['WGPEER_A_FLAGS', WGPEER_F_UPDATE_ONLY])

        # Set allowed IPs
        if 'allowed_ips' in peer:
            allowed_ips = self._wg_build_allowedips(peer['allowed_ips'])
            attrs.append(['WGPEER_A_ALLOWEDIPS', allowed_ips])

        msg['attrs'].append(['WGDEVICE_A_PEERS', wg_peer])

    def _wg_build_allowedips(self, allowed_ips):
        ret = []

        for index, ip in enumerate(allowed_ips):
            allowed_ip = []
            ret.append({'attrs': allowed_ip})

            if ip.find("/") == -1:
                raise ValueError('No CIDR set in allowed ip #{}'.format(index))

            addr, mask = ip.split('/')
            if addr.find(":") > -1:
                allowed_ip.append(['WGALLOWEDIP_A_FAMILY', AF_INET6])
                allowed_ip.append(['WGALLOWEDIP_A_IPADDR', inet_pton(AF_INET6,
                                                                     addr)])
            else:
                allowed_ip.append(['WGALLOWEDIP_A_FAMILY', AF_INET])
                allowed_ip.append(['WGALLOWEDIP_A_IPADDR', inet_aton(addr)])
            allowed_ip.append(['WGALLOWEDIP_A_CIDR_MASK', int(mask)])

        return ret
