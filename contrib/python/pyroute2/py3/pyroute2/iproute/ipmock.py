import copy
import errno
import os
import socket
import struct
import threading
from itertools import count

from pyroute2.config import AF_NETLINK
from pyroute2.netlink import (
    NLM_F_DUMP,
    NLM_F_MULTI,
    NLMSG_DONE,
    nlmsg,
    nlmsgerr,
)
from pyroute2.netlink.core import Stats
from pyroute2.netlink.rtnl import (
    RTM_DELADDR,
    RTM_DELROUTE,
    RTM_GETADDR,
    RTM_GETLINK,
    RTM_GETROUTE,
    RTM_NEWADDR,
    RTM_NEWLINK,
    RTM_NEWROUTE,
)
from pyroute2.netlink.rtnl.ifaddrmsg import ifaddrmsg
from pyroute2.netlink.rtnl.ifinfmsg import ifinfmsg
from pyroute2.netlink.rtnl.marshal import MarshalRtnl
from pyroute2.netlink.rtnl.rtmsg import rtmsg

interface_counter = count(3)
MAGIC_CLOSE = 0x42


class MockLink:
    def __init__(
        self,
        index=0,
        ifname='',
        address='00:00:00:00:00:00',
        broadcast='ff:ff:ff:ff:ff:ff',
        perm_address=None,
        flags=1,
        rx_bytes=0,
        tx_bytes=0,
        rx_packets=0,
        tx_packets=0,
        mtu=0,
        qdisc='noqueue',
        kind=None,
        link=None,
        vlan_id=None,
        master=0,
        br_max_age=0,
        br_forward_delay=0,
        alt_ifname_list=None,
    ):
        self.index = index if index > 0 else next(interface_counter)
        self.ifname = ifname
        self.flags = flags
        self.address = address
        self.broadcast = broadcast
        self.perm_address = perm_address
        self.rx_bytes = rx_bytes
        self.tx_bytes = tx_bytes
        self.rx_packets = rx_packets
        self.tx_packets = tx_packets
        self.mtu = mtu
        self.qdisc = qdisc
        self.kind = kind
        self.link = link
        self.vlan_id = vlan_id
        self.master = master
        self.br_max_age = br_max_age
        self.br_forward_delay = br_forward_delay
        self.alt_ifname_list = alt_ifname_list or []

    def update_from_msg(self, msg):
        [
            setattr(self, x, msg.get(x))
            for x in ['address', 'broadcast', 'mtu', 'ifname', 'master']
            if msg.get(x) is not None
        ]
        #
        self.kind = msg.get(('linkinfo', 'kind'))
        if msg.get('change') != 0:
            self.flags = msg.get('flags')
        # vlan
        if self.kind == 'vlan':
            self.vlan_id = msg.get(('linkinfo', 'data', 'vlan_id'))
            self.link = msg.get('link')
        elif self.kind == 'bridge':
            self.br_max_age = msg.get(('linkinfo', 'data', 'br_max_age'))
            self.br_forward_delay = msg.get(
                ('linkinfo', 'data', 'br_forward_delay')
            )

    @classmethod
    def load_from_msg(cls, msg):
        ret = cls()
        ret.update_from_msg(msg)
        return ret

    def export(self):
        ret = {
            'attrs': [
                ['IFLA_IFNAME', self.ifname],
                ['IFLA_TXQLEN', 1000],
                ['IFLA_OPERSTATE', 'UNKNOWN'],
                ['IFLA_LINKMODE', 0],
                ['IFLA_MTU', self.mtu],
                ['IFLA_GROUP', 0],
                ['IFLA_PROMISCUITY', 0],
                ['IFLA_NUM_TX_QUEUES', 1],
                ['IFLA_GSO_MAX_SEGS', 65535],
                ['IFLA_GSO_MAX_SIZE', 65536],
                ['IFLA_GRO_MAX_SIZE', 65536],
                ['IFLA_NUM_RX_QUEUES', 1],
                ['IFLA_CARRIER', 1],
                ['IFLA_QDISC', self.qdisc],
                ['IFLA_CARRIER_CHANGES', 0],
                ['IFLA_CARRIER_UP_COUNT', 0],
                ['IFLA_CARRIER_DOWN_COUNT', 0],
                ['IFLA_PROTO_DOWN', 0],
                [
                    'IFLA_MAP',
                    {
                        'base_addr': 0,
                        'dma': 0,
                        'irq': 0,
                        'mem_end': 0,
                        'mem_start': 0,
                        'port': 0,
                    },
                ],
                ['IFLA_ADDRESS', self.address],
                ['IFLA_BROADCAST', self.broadcast],
                [
                    'IFLA_STATS64',
                    {
                        'collisions': 0,
                        'multicast': 0,
                        'rx_bytes': self.rx_bytes,
                        'rx_compressed': 0,
                        'rx_crc_errors': 0,
                        'rx_dropped': 0,
                        'rx_errors': 0,
                        'rx_fifo_errors': 0,
                        'rx_frame_errors': 0,
                        'rx_length_errors': 0,
                        'rx_missed_errors': 0,
                        'rx_over_errors': 0,
                        'rx_packets': self.rx_packets,
                        'tx_aborted_errors': 0,
                        'tx_bytes': self.tx_bytes,
                        'tx_carrier_errors': 0,
                        'tx_compressed': 0,
                        'tx_dropped': 0,
                        'tx_errors': 0,
                        'tx_fifo_errors': 0,
                        'tx_heartbeat_errors': 0,
                        'tx_packets': self.tx_packets,
                        'tx_window_errors': 0,
                    },
                ],
                [
                    'IFLA_STATS',
                    {
                        'collisions': 0,
                        'multicast': 0,
                        'rx_bytes': self.rx_bytes,
                        'rx_compressed': 0,
                        'rx_crc_errors': 0,
                        'rx_dropped': 0,
                        'rx_errors': 0,
                        'rx_fifo_errors': 0,
                        'rx_frame_errors': 0,
                        'rx_length_errors': 0,
                        'rx_missed_errors': 0,
                        'rx_over_errors': 0,
                        'rx_packets': self.rx_packets,
                        'tx_aborted_errors': 0,
                        'tx_bytes': self.tx_bytes,
                        'tx_carrier_errors': 0,
                        'tx_compressed': 0,
                        'tx_dropped': 0,
                        'tx_errors': 0,
                        'tx_fifo_errors': 0,
                        'tx_heartbeat_errors': 0,
                        'tx_packets': self.tx_packets,
                        'tx_window_errors': 0,
                    },
                ],
                ['IFLA_XDP', {'attrs': [['IFLA_XDP_ATTACHED', None]]}],
                (
                    'IFLA_PERM_ADDRESS',
                    self.perm_address if self.perm_address else self.address,
                ),
                [
                    'IFLA_AF_SPEC',
                    {
                        'attrs': [
                            [
                                'AF_INET',
                                {
                                    'accept_local': 0,
                                    'accept_redirects': 1,
                                    'accept_source_route': 0,
                                    'arp_accept': 0,
                                    'arp_announce': 0,
                                    'arp_ignore': 0,
                                    'arp_notify': 0,
                                    'arpfilter': 0,
                                    'bootp_relay': 0,
                                    'dummy': 65672,
                                    'force_igmp_version': 0,
                                    'forwarding': 1,
                                    'log_martians': 0,
                                    'mc_forwarding': 0,
                                    'medium_id': 0,
                                    'nopolicy': 1,
                                    'noxfrm': 1,
                                    'promote_secondaries': 1,
                                    'proxy_arp': 0,
                                    'proxy_arp_pvlan': 0,
                                    'route_localnet': 0,
                                    'rp_filter': 2,
                                    'secure_redirects': 1,
                                    'send_redirects': 1,
                                    'shared_media': 1,
                                    'src_vmark': 0,
                                    'tag': 0,
                                },
                            ]
                        ]
                    },
                ],
            ],
            'change': 0,
            'event': 'RTM_NEWLINK',
            'family': 0,
            'flags': self.flags,
            'header': {
                'error': None,
                'flags': 2,
                'length': 1364,
                'pid': 303471,
                'sequence_number': 260,
                'stats': Stats(qsize=0, delta=0, delay=0),
                'target': 'localhost',
                'type': 16,
            },
            'ifi_type': 772,
            'index': self.index,
            'state': 'up' if self.flags & 1 else 'down',
        }
        linkinfo = None
        infodata = {'attrs': []}
        if self.kind is not None:
            linkinfo = {'attrs': [('IFLA_INFO_KIND', self.kind)]}
        if self.kind not in (None, 'dummy'):
            linkinfo['attrs'].append(('IFLA_INFO_DATA', infodata))
        if self.kind == 'vlan':
            infodata['attrs'].append(('IFLA_VLAN_ID', self.vlan_id))
            ret['attrs'].append(('IFLA_LINK', self.link))
        if self.kind == 'bridge':
            infodata['attrs'].extend(
                (
                    ('IFLA_BR_MAX_AGE', self.br_max_age),
                    ('IFLA_BR_FORWARD_DELAY', self.br_forward_delay),
                )
            )
        if linkinfo is not None:
            ret['attrs'].append(('IFLA_LINKINFO', linkinfo))
        if self.master != 0:
            ret['attrs'].append(('IFLA_MASTER', self.master))
        return ret


class MockAddress:
    def __init__(
        self,
        index=0,
        address=None,
        prefixlen=None,
        broadcast=None,
        label=None,
        family=2,
        local=None,
        **kwarg,
    ):
        self.address = address
        self.local = local
        self.broadcast = broadcast
        self.prefixlen = prefixlen
        self.index = index
        self.label = label
        self.family = family

    def update_from_msg(self, msg):
        [
            setattr(self, x, msg.get(x))
            for x in ['index', 'address', 'broadcast', 'prefixlen']
            if msg.get(x) is not None
        ]

    @classmethod
    def load_from_msg(cls, msg):
        ret = cls()
        ret.update_from_msg(msg)
        return ret

    def export(self):
        ret = {
            'family': self.family,
            'prefixlen': self.prefixlen,
            'flags': 0,
            'scope': 0,
            'index': self.index,
            'attrs': [
                ('IFA_ADDRESS', self.address),
                ('IFA_LOCAL', self.local if self.local else self.address),
                ('IFA_FLAGS', 512),
                (
                    'IFA_CACHEINFO',
                    {
                        'ifa_preferred': 3476,
                        'ifa_valid': 3476,
                        'cstamp': 138655779,
                        'tstamp': 141288674,
                    },
                ),
            ],
            'header': {
                'length': 88,
                'type': 20,
                'flags': 2,
                'sequence_number': 256,
                'pid': 320994,
                'error': None,
                'target': 'localhost',
                'stats': Stats(qsize=0, delta=0, delay=0),
            },
            'event': 'RTM_NEWADDR',
        }
        if self.label is not None:
            ret['attrs'].append(('IFA_LABEL', self.label))
        if self.broadcast is not None:
            ret['attrs'].append(('IFA_BROADCAST', self.broadcast))
        return ret


class MockRoute:
    def __init__(
        self,
        dst=None,
        oif=0,
        gateway=None,
        prefsrc=None,
        family=2,
        dst_len=24,
        table=254,
        scope=253,
        proto=2,
        route_type=1,
        **kwarg,
    ):
        self.dst = dst
        self.gateway = gateway
        self.prefsrc = prefsrc
        self.oif = oif
        self.family = family
        self.dst_len = dst_len
        self.table = table
        self.scope = scope
        self.proto = proto
        self.route_type = route_type
        self.priority = kwarg.get('priority', 0)
        self.tos = kwarg.get('tos', 0)

    def update_from_msg(self, msg):
        [
            setattr(self, x, msg.get(x))
            for x in [
                'dst',
                'dst_len',
                'gateway',
                'oif',
                'family',
                'table',
                'priority',
            ]
            if msg.get(x) is not None
        ]

    @classmethod
    def load_from_msg(cls, msg):
        ret = cls()
        ret.update_from_msg(msg)
        return ret

    def export(self):
        ret = {
            'family': self.family,
            'dst_len': self.dst_len,
            'src_len': 0,
            'tos': self.tos,
            'table': self.table if self.table <= 255 else 252,
            'proto': self.proto,
            'scope': self.scope,
            'type': self.route_type,
            'flags': 0,
            'attrs': [('RTA_TABLE', self.table), ('RTA_OIF', self.oif)],
            'header': {
                'length': 60,
                'type': 24,
                'flags': 2,
                'sequence_number': 255,
                'pid': 325359,
                'error': None,
                'target': 'localhost',
                'stats': Stats(qsize=0, delta=0, delay=0),
            },
            'event': 'RTM_NEWROUTE',
        }
        if self.dst is not None:
            ret['attrs'].append(('RTA_DST', self.dst))
        if self.prefsrc is not None:
            ret['attrs'].append(('RTA_PREFSRC', self.prefsrc))
        if self.gateway is not None:
            ret['attrs'].append(('RTA_GATEWAY', self.gateway))
        if self.priority > 0:
            ret['attrs'].append(('RTA_PRIORITY', self.priority))
        return ret


presets = {
    'default': {
        'links': [
            MockLink(
                index=1,
                ifname='lo',
                address='00:00:00:00:00:00',
                broadcast='00:00:00:00:00:00',
                rx_bytes=43309665,
                tx_bytes=43309665,
                rx_packets=173776,
                tx_packets=173776,
                mtu=65536,
                qdisc='noqueue',
            ),
            MockLink(
                index=2,
                ifname='eth0',
                address='52:54:00:72:58:b2',
                broadcast='ff:ff:ff:ff:ff:ff',
                rx_bytes=175340,
                tx_bytes=175340,
                rx_packets=10251,
                tx_packets=10251,
                mtu=1500,
                qdisc='fq_codel',
            ),
        ],
        'addr': [
            MockAddress(
                index=1,
                label='lo',
                address='127.0.0.1',
                broadcast='127.255.255.255',
                prefixlen=8,
            ),
            MockAddress(
                index=2,
                label='eth0',
                address='192.168.122.28',
                broadcast='192.168.122.255',
                prefixlen=24,
            ),
        ],
        'routes': [
            MockRoute(
                dst=None,
                gateway='192.168.122.1',
                oif=2,
                dst_len=0,
                table=254,
                scope=0,
            ),
            MockRoute(dst='192.168.122.0', oif=2, dst_len=24, table=254),
            MockRoute(
                dst='127.0.0.0', oif=1, dst_len=8, table=255, route_type=2
            ),
            MockRoute(
                dst='127.0.0.1', oif=1, dst_len=32, table=255, route_type=2
            ),
            MockRoute(
                dst='127.255.255.255',
                oif=1,
                dst_len=32,
                table=255,
                route_type=3,
            ),
            MockRoute(
                dst='192.168.122.28',
                oif=2,
                dst_len=32,
                table=255,
                route_type=2,
            ),
            MockRoute(
                dst='192.168.122.255',
                oif=2,
                dst_len=32,
                table=255,
                route_type=3,
            ),
        ],
    },
    'netns': {
        'links': [
            MockLink(
                index=1,
                ifname='lo',
                address='00:00:00:00:00:00',
                broadcast='00:00:00:00:00:00',
                rx_bytes=43309665,
                tx_bytes=43309665,
                rx_packets=173776,
                tx_packets=173776,
                mtu=65536,
                qdisc='noqueue',
            )
        ],
        'addr': [
            MockAddress(
                index=1,
                label='lo',
                address='127.0.0.1',
                broadcast='127.255.255.255',
                prefixlen=8,
            )
        ],
        'routes': [
            MockRoute(
                dst='127.0.0.0', oif=1, dst_len=8, table=255, route_type=2
            ),
            MockRoute(
                dst='127.0.0.1', oif=1, dst_len=32, table=255, route_type=2
            ),
            MockRoute(
                dst='127.255.255.255',
                oif=1,
                dst_len=32,
                table=255,
                route_type=3,
            ),
        ],
    },
}


class IPEngine:
    '''Mock network objects database with the socket API.

    WIP: work in progress.

    A drop-in replacement to use instead of a low level RTNL socket.
    Implements all the required socket properties and provides a
    network objects database with RTNL protocol.

    Example::

        >>> ipe = IPEngine()
        >>> ipr = IPRoute(use_socket=ipe)
        >>> [ x.get('ifname') for x in ipr.link('dump') ]
        ['lo', 'eth0']

    '''

    def __init__(
        self,
        sfamily=AF_NETLINK,
        stype=socket.SOCK_DGRAM,
        sproto=0,
        netns='default',
        flags=os.O_CREAT,
    ):
        self.marshal = MarshalRtnl()
        self.netns = netns
        self.flags = flags
        self.magic = 0
        self._stype = stype
        self._sfamily = sfamily
        self._sproto = sproto
        self._local = threading.local()
        self._lock = threading.Lock()
        self._broadcast = set()
        self.processors = {
            RTM_GETADDR: self.RTM_GETADDR,
            RTM_GETLINK: self.RTM_GETLINK,
            RTM_NEWADDR: self.RTM_NEWADDR,
            RTM_DELADDR: self.RTM_DELADDR,
            RTM_NEWLINK: self.RTM_NEWLINK,
            RTM_DELROUTE: self.RTM_DELROUTE,
            RTM_NEWROUTE: self.RTM_NEWROUTE,
            RTM_GETROUTE: self.RTM_GETROUTE,
        }
        self.initdb()

    @property
    def loopback_r(self):
        if not hasattr(self._local, 'loopback_r'):
            self._local.loopback_r, self._local.loopback_w = socket.socketpair(
                socket.AF_UNIX, socket.SOCK_DGRAM
            )
        return self._local.loopback_r

    @property
    def loopback_w(self):
        if not hasattr(self._local, 'loopback_w'):
            self._local.loopback_r, self._local.loopback_w = socket.socketpair(
                socket.AF_UNIX, socket.SOCK_DGRAM
            )
        return self._local.loopback_w

    def initdb(self):
        if self.netns not in presets:
            if not self.flags & os.O_CREAT:
                raise FileNotFoundError()
            presets[self.netns] = copy.deepcopy(presets['netns'])
        self.database = copy.deepcopy(presets[self.netns])

    def close(self):
        if self.magic == MAGIC_CLOSE:
            self.loopback_r.close()
            self.loopback_w.close()

    def bind(self, address=None):
        self._broadcast.add(self.loopback_w)
        msg = rtmsg()
        msg.load(self.database['routes'][0].export())
        msg.encode()
        self.loopback_w.send(msg.data)

    def fileno(self):
        return self.loopback_r.fileno()

    def recv(self, bufsize, flags=0):
        return self.loopback_r.recv(bufsize, flags)

    def recvfrom(self, bufsize, flags=0):
        return self.loopback_r.recvfrom(bufsize, flags)

    def recvmsg(self, bufsize, ancbufsize=0, flags=0):
        return self.loopback_r.recvmsg(bufsize, ancbufsize, flags)

    def recv_into(self, buffer, nbytes=0, flags=0):
        return self.loopback_r.recv_into(buffer, nbytes, flags)

    def recvfrom_into(self, buffer, nbytes=0, flags=0):
        return self.loopback_r.recvfrom_into(buffer, nbytes, flags)

    def recvmsg_into(self, buffers, ancbufsize=0, flags=0):
        return self.loopback_r.recvmsg_into(buffers, ancbufsize, flags)

    def send(self, data, flags=0):
        return self.nl_handle(data)

    def sendall(self, data, flags=0):
        return self.nl_handle(data)

    def sendto(self, data, flags, address=0):
        return self.nl_handle(data)

    def sendmsg(self, buffers, ancdata=None, flags=None, address=None):
        raise NotImplementedError()

    def setblocking(self, flag):
        return self.loopback_r.setblocking(flag)

    def getblocking(self):
        return self.loopback_r.getblocking()

    def getsockname(self):
        return self.loopback_r.getsockname()

    def getpeername(self):
        return self.loopback_r.getpeername()

    @property
    def type(self):
        return self._stype

    @property
    def family(self):
        return self._sfamily

    @property
    def proto(self):
        return self._sproto

    def nl_handle(self, data):
        with self._lock:
            for msg in self.marshal.parse(data):
                key = msg['header']['type']
                tag = msg['header']['sequence_number']
                if key in self.processors:
                    self.processors[key](msg)
                else:
                    self.nl_done(tag)
            return len(data)

    def nl_dump(self, registry, msg_class, tag):
        for item in registry:
            msg = msg_class()
            msg.load(item.export())
            msg['header']['flags'] = NLM_F_MULTI
            msg['header']['sequence_number'] = tag
            msg.encode()
            yield msg

    def nl_broadcast(self, msg):
        for sock in self._broadcast:
            msg['header']['sequence_number'] = 0
            msg.reset()
            msg.encode()
            sock.send(msg.data)

    def nl_done(self, tag):
        msg = nlmsg()
        msg['header']['type'] = NLMSG_DONE
        msg['header']['sequence_number'] = tag
        msg.encode()
        self.loopback_w.send(msg.data)

    def nl_error(self, tag, code):
        msg = nlmsgerr()
        msg['header']['sequence_number'] = tag
        msg['error'] = code
        msg.encode()
        self.loopback_w.send(msg.data)

    def RTM_GETROUTE(self, req):
        tag = req['header']['sequence_number']
        for msg in self.nl_dump(self.database['routes'], rtmsg, tag):
            self.loopback_w.send(msg.data)
        self.nl_done(tag)

    def RTM_GETADDR(self, msg_in):
        tag = msg_in['header']['sequence_number']
        for msg_out in self.nl_dump(self.database['addr'], ifaddrmsg, tag):
            self.loopback_w.send(msg_out.data)
        self.nl_done(tag)

    def RTM_GETLINK(self, msg_in):
        tag = msg_in['header']['sequence_number']
        database = self.database['links']
        if msg_in.get('index') > 0:
            database = [
                x
                for x in self.database['links']
                if x.index == msg_in.get('index')
            ]
        elif msg_in.get('ifname'):
            database = [
                x
                for x in self.database['links']
                if x.ifname == msg_in.get('ifname')
            ]
        for msg_out in self.nl_dump(database, ifinfmsg, tag):
            self.loopback_w.send(msg_out.data)
        if msg_in.get(('header', 'flags')) & NLM_F_DUMP:
            return self.nl_done(tag)
        self.nl_error(tag, 0)

    def RTM_NEWROUTE(self, req):
        tag = req['header']['sequence_number']
        if not req.get('oif'):
            (gateway,) = struct.unpack(
                '>I', socket.inet_aton(req.get('gateway'))
            )
            for route in self.database['routes']:
                if route.dst is None:
                    continue
                (dst,) = struct.unpack('>I', socket.inet_aton(route.dst))
                if (gateway & (0xFFFFFFFF << (32 - route.dst_len))) == dst:
                    req['attrs'].append(('RTA_OIF', route.oif))
                    break
            else:
                return self.nl_error(tag, errno.ENOENT)
        idx = {
            (x.dst, x.dst_len, x.oif, x.priority, x.gateway, x.table): x
            for x in self.database['routes']
        }
        req_index = (
            req.get('dst'),
            req.get('dst_len'),
            req.get('oif'),
            req.get('priority'),
            req.get('gateway'),
            req.get('IFLA_TABLE') or req.get('table'),
        )
        if req_index in idx:
            return self.nl_error(tag, errno.EEXIST)
        route = MockRoute.load_from_msg(req)
        self.database['routes'].append(route)
        self.nl_error(tag, 0)
        msg = rtmsg()
        msg.load(route.export())
        self.nl_broadcast(msg)

    def RTM_DELROUTE(self, req):
        idx = {
            (x.dst, x.dst_len, x.oif, x.priority, x.table): x
            for x in self.database['routes']
        }
        req_index = (
            req.get('dst'),
            req.get('dst_len'),
            req.get('oif'),
            req.get('priority'),
            req.get('IFLA_TABLE') or req.get('table'),
        )
        tag = req['header']['sequence_number']
        if req_index not in idx:
            return self.nl_error(tag, errno.ENOENT)
        self.database['routes'].remove(idx[req_index])
        self.nl_error(tag, 0)
        self.nl_broadcast(req)

    def RTM_DELADDR(self, req):
        idx = {
            (x.index, x.address, x.prefixlen): x for x in self.database['addr']
        }
        req_index = (
            req.get("index"),
            req.get("address"),
            req.get("prefixlen"),
        )
        tag = req['header']['sequence_number']
        if req_index not in idx:
            return self.nl_error(tag, errno.ENOENT)
        self.database['addr'].remove(idx[req_index])
        self.nl_error(tag, 0)
        self.nl_broadcast(req)

    def RTM_NEWADDR(self, req):
        idx = {
            (x.index, x.address, x.prefixlen) for x in self.database['addr']
        }
        req_index = (
            req.get("index"),
            req.get("address"),
            req.get("prefixlen"),
        )
        tag = req['header']['sequence_number']
        if req_index in idx:
            return self.nl_error(tag, errno.EEXIST)
        addr = MockAddress.load_from_msg(req)
        self.database['addr'].append(addr)
        self.nl_error(tag, 0)
        msg = ifaddrmsg()
        msg.load(addr.export())
        self.nl_broadcast(msg)

    def RTM_NEWLINK(self, req):
        idx = {x.index: x for x in self.database['links']}
        nmx = {x.ifname: x for x in self.database['links']}
        tag = req['header']['sequence_number']
        if req.get('index') in idx:
            link = idx[req.get('index')]
            if link.ifname == req.get('ifname'):
                return self.nl_error(tag, errno.EEXIST)
            link.update_from_msg(req)
        elif req.get('ifname') in nmx:
            return self.nl_error(tag, errno.EEXIST)
        else:
            link = MockLink.load_from_msg(req)
            self.database['links'].append(link)
        self.nl_error(tag, 0)
        msg = ifinfmsg()
        msg.load(link.export())
        self.nl_broadcast(msg)
