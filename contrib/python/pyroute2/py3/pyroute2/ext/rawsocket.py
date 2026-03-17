import asyncio
import logging
from socket import AF_PACKET, SOCK_RAW, SOL_SOCKET, errno, error, htons, socket
from typing import Optional

from pyroute2.ext import bpf
from pyroute2.iproute.linux import AsyncIPRoute
from pyroute2.netlink.rtnl import RTMGRP_LINK

LOG = logging.getLogger(__name__)
ETH_P_IP = 0x0800
ETH_P_ALL = 3
SO_ATTACH_FILTER = 26
SO_DETACH_FILTER = 27


total_filter = [[0x06, 0, 0, 0]]


class AsyncRawSocket(socket):
    '''
    This raw socket binds to an interface and optionally installs a BPF
    filter.
    When created, the socket's buffer is cleared to remove packets that
    arrived before bind() or the BPF filter is installed.  Doing so
    requires calling recvfrom() which may raise an exception if the
    interface is down.
    In order to allow creating the socket when the interface is
    down, the ENETDOWN exception is caught and discarded.
    '''

    fprog = None

    def __init__(self, ifname: str, bpf: Optional[list[list[int]]] = None):
        self.ifname = ifname
        self.bpf = bpf
        # start watching for mac addr changes
        self._l2addr_watcher: Optional[asyncio.Task] = None

    async def __aexit__(self, *_):
        self._l2addr_watcher.cancel()
        self.close()

    async def __aenter__(self):
        # lookup the interface details
        async with AsyncIPRoute() as ip:
            async for link in await ip.get_links():
                if link.get_attr('IFLA_IFNAME') == self.ifname:
                    break
            else:
                raise IOError(2, 'Link not found')
        self.l2addr: str = link.get_attr('IFLA_ADDRESS')
        self.ifindex: int = link['index']
        # bring up the socket
        socket.__init__(self, AF_PACKET, SOCK_RAW, htons(ETH_P_ALL))
        socket.setblocking(self, False)
        socket.bind(self, (self.ifname, ETH_P_ALL))
        if self.bpf:
            self.clear_buffer()
            fstring, self.fprog = bpf.compile(self.bpf)
            socket.setsockopt(self, SOL_SOCKET, SO_ATTACH_FILTER, fstring)
        else:
            # FIXME: should be async
            self.clear_buffer(remove_total_filter=True)
        # change self.l2addr if it changes
        self._l2addr_watcher = asyncio.create_task(
            self._watch_l2addr_changes(),
            name=f'Watch {self.ifname} for l2addr changes',
        )
        return self

    async def _watch_l2addr_changes(self):
        '''Updates self.l2addr when the interfaces's mac changes.

        During the lifetime of the socket, the interface's mac can change, and
        since it's read it at startup & used to build packets, they will then
        have the wrong mac.
        '''
        async with AsyncIPRoute() as ipr:
            await ipr.bind(RTMGRP_LINK)
            while True:
                async for msg in ipr.get():
                    if msg.get('IFLA_IFNAME') != self.ifname:
                        continue
                    new_l2addr = msg.get_attr('IFLA_ADDRESS')
                    if new_l2addr and new_l2addr != self.l2addr:
                        LOG.info(
                            'l2addr for %s changed from %s to %s',
                            self.ifname,
                            self.l2addr,
                            new_l2addr,
                        )
                        self.l2addr = new_l2addr

    def clear_buffer(self, remove_total_filter: bool = False):
        # there is a window of time after the socket has been created and
        # before bind/attaching a filter where packets can be queued onto the
        # socket buffer
        # see comments in function set_kernel_filter() in libpcap's
        # pcap-linux.c. libpcap sets a total filter which does not match any
        # packet.  It then clears what is already in the socket
        # before setting the desired filter
        total_fstring, prog = bpf.compile(total_filter)
        socket.setsockopt(self, SOL_SOCKET, SO_ATTACH_FILTER, total_fstring)
        while True:
            try:
                self.recvfrom(0)
            except error as e:
                if e.args[0] == errno.ENETDOWN:
                    # we only get this exception once per down event
                    # there may be more packets left to clean
                    pass
                elif e.args[0] in [errno.EAGAIN, errno.EWOULDBLOCK]:
                    break
                else:
                    raise
        if remove_total_filter:
            # total_fstring ignored
            socket.setsockopt(
                self, SOL_SOCKET, SO_DETACH_FILTER, total_fstring
            )

    @staticmethod
    def csum(data: bytes) -> int:
        '''Compute the "Internet checksum" for the given bytes.'''
        if len(data) % 2:
            data += b'\x00'
        csum: int = 0
        # pretty much the fastest way to compute this in Python
        for i in range(len(data) // 2):
            offset = i * 2
            csum += (data[offset] << 8) + data[offset + 1]
        csum = (csum >> 16) + (csum & 0xFFFF)
        csum += csum >> 16
        return ~csum & 0xFFFF
