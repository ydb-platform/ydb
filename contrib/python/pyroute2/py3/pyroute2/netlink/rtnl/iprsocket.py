import errno
import os
import struct
from collections.abc import Callable
from typing import Optional
from unittest import mock

from pyroute2 import config
from pyroute2.common import msg_done
from pyroute2.iproute.ipmock import IPEngine
from pyroute2.netlink import NETLINK_ROUTE, NLM_F_REQUEST, nlmsg, rtnl
from pyroute2.netlink.exceptions import NetlinkError
from pyroute2.netlink.nlsocket import (
    AsyncNetlinkSocket,
    ChaoticNetlinkSocket,
    NetlinkRequest,
    NetlinkSocket,
)
from pyroute2.netlink.rtnl.ifinfmsg.tuntap import manage_tun, manage_tuntap
from pyroute2.netlink.rtnl.marshal import MarshalRtnl
from pyroute2.netlink.rtnl.probe_msg import proxy_newprobe
from pyroute2.netns import setns
from pyroute2.process import ChildProcess, ChildProcessReturnValue


def _run_in_netns(
    netns: str, target: Callable[[nlmsg], bytes], msg: nlmsg
) -> ChildProcessReturnValue:
    setns(netns)
    return ChildProcessReturnValue(target(msg), [])


class IPRouteProxy:
    route: dict[
        int,
        tuple[tuple[Callable[[nlmsg], bool], Callable[[nlmsg], bytes]], ...],
    ] = {
        rtnl.RTM_NEWLINK: (
            (lambda x: x.get(('linkinfo', 'kind')) == 'tuntap', manage_tuntap),
            (lambda x: x.get(('linkinfo', 'kind')) == 'tun', manage_tun),
        ),
        rtnl.RTM_NEWPROBE: ((lambda x: True, proxy_newprobe),),
    }

    def __init__(self, netns: Optional[str] = None):
        self.netns = netns

    def handle(self, msg: nlmsg) -> bytes:
        ret: bytes = b''
        key = msg['header']['type']
        if key not in self.route:
            return ret
        for predicate, target in self.route[key]:
            if predicate(msg):
                try:
                    if self.netns is not None:
                        with ChildProcess(
                            target=_run_in_netns,
                            args=[self.netns, target, msg],
                        ) as proc:
                            ret = proc.get_data(timeout=4)
                    else:
                        ret = target(msg)
                    return ret or msg_done(msg)
                except Exception as e:
                    # errmsg
                    if isinstance(e, (OSError, IOError)):
                        code = e.errno or errno.ENODATA
                    elif isinstance(e, NetlinkError):
                        code = e.code
                    else:
                        code = errno.ECOMM
                    newmsg = struct.pack('HH', 2, 0)
                    newmsg += msg.data[8:16]
                    newmsg += struct.pack('I', code)
                    newmsg += msg.data
                    newmsg = struct.pack('I', len(newmsg) + 4) + newmsg
                    return newmsg
        return b''


class AsyncIPRSocket(AsyncNetlinkSocket):
    '''A low-level class to provide RTNL socket.

    This is a low-level class designed to provide an RTNL
    asyncio-controlled socket. It does not include high-level
    methods like those found in AsyncIPRoute. Instead, it provides
    only common netlink methods such as `get()` and `put()`. For
    more details, refer to the `AsyncNetlinkSocket` documentation.

    .. testcode::
        :hide:

        from pyroute2 import AsyncIPRSocket

        iprsock = AsyncIPRSocket()
        assert callable(iprsock.get)
        assert callable(iprsock.put)
        assert callable(iprsock.nlm_request)
        assert callable(iprsock.bind)

    Since the underlying socket is controlled by asyncio, it is
    not possible to use it in poll/select loops. If you want
    such API, consider using synchronous `IPRSocket`.

    .. warning::

        Your code must process incoming messages quickly enough to
        prevent the RCVBUF from overflowing. If the RCVBUF overflows,
        all subsequent socket operations will raise an OSError:

    .. code::

        >>> [ x async for x in ipr.get() ]
        Traceback (most recent call last):
          File ".../python3.13/futures/_base.py", line 456, in result
            return self.__get_result()
                   ~~~~~~~~~~~~~~~~~^^
          ...
        OSError: [Errno 105] No buffer space available


    If this exception occurs, the only solution is to close the
    socket and create a new one.

    This class does not handle protocol-level error propagation; it
    only provides socket-level error handling. It is the user's
    responsibility to catch and manage protocol-level errors:

    .. testsetup:: as0

        from pyroute2.netlink import nlmsgerr, NLMSG_ERROR
        msg = nlmsgerr()
        msg['header']['type'] = NLMSG_ERROR
        msg['error'] = 42
        msg.reset()
        msg.encode()
        msg.decode()

    .. testcode:: as0

        if msg.get(('header', 'type')) == NLMSG_ERROR:
            # prints error code and the request that
            # triggered the error
            print(
                msg.get('error'),
                msg.get('msg'),
            )

    .. testoutput:: as0
        :hide:

        42 None


    '''

    def __init__(
        self,
        port=None,
        pid=None,
        fileno=None,
        sndbuf=1048576,
        rcvbuf=1048576,
        rcvsize=16384,
        all_ns=False,
        async_qsize=None,
        nlm_generator=None,
        target='localhost',
        ext_ack=False,
        strict_check=False,
        groups=rtnl.RTMGRP_DEFAULTS,
        nlm_echo=False,
        netns=None,
        netns_path=None,
        flags=os.O_CREAT,
        libc=None,
        use_socket=None,
        use_event_loop=None,
        telemetry=None,
        exception_factory=None,
    ):
        if config.mock_netlink:
            use_socket = IPEngine()
            if netns is not None:
                use_socket.netns = netns
            use_socket.flags = flags
            use_socket.initdb()
        self.marshal = MarshalRtnl()
        super().__init__(
            family=NETLINK_ROUTE,
            port=port,
            pid=pid,
            fileno=fileno,
            sndbuf=sndbuf,
            rcvbuf=rcvbuf,
            rcvsize=rcvsize,
            all_ns=all_ns,
            async_qsize=async_qsize,
            nlm_generator=nlm_generator,
            target=target,
            ext_ack=ext_ack,
            strict_check=strict_check,
            groups=groups,
            nlm_echo=nlm_echo,
            use_socket=use_socket,
            netns=netns,
            flags=flags,
            libc=libc,
            use_event_loop=use_event_loop,
            telemetry=telemetry,
            exception_factory=exception_factory,
        )
        if not config.mock_netlink:
            self.request_proxy = IPRouteProxy(netns)
        self.status['netns_path'] = netns_path or config.netns_path

    async def bind(self, groups=None, **kwarg):
        return await super().bind(
            groups if groups is not None else self.status['groups'], **kwarg
        )


class NotLocal:
    event_loop = None
    msg_queue = mock.Mock()


class IPRSocket(NetlinkSocket):
    '''Synchronous select-compatible netlink socket.

    `IPRSocket` is the synchronous counterpart to `AsyncIPRSocket`.
    A key feature of `IPRSocket` is that the underlying netlink
    socket operates out of asyncio control, allowing it to be
    used in poll/select loops.

    .. warning::

        Your code must process incoming messages quickly enough to
        prevent the RCVBUF from overflowing. If the RCVBUF overflows,
        all subsequent socket operations will raise an OSError:

    .. code::

        >>> iprsock.get()
        Traceback (most recent call last):
          File "<python-input-12>", line 1, in <module>
            iprsock.get()
            ~~~~~~~~^^
          File ".../pyroute2/netlink/rtnl/iprsocket.py", line 276, in get
            data = self.socket.recv(16384)
        OSError: [Errno 105] No buffer space available
        >>>

    If this exception occurs, the only solution is to close the
    socket and create a new one.

    Some usage examples:

    .. testcode::

        import select

        from pyroute2 import IPRSocket
        from pyroute2.netlink import NLM_F_DUMP, NLM_F_REQUEST
        from pyroute2.netlink.rtnl import RTM_GETLINK
        from pyroute2.netlink.rtnl.ifinfmsg import ifinfmsg

        with IPRSocket() as iprsock:
            iprsock.put(
                ifinfmsg(),
                msg_type=RTM_GETLINK,
                msg_flags=NLM_F_REQUEST | NLM_F_DUMP
            )

            ret = []

            while True:
                rl, wl, xl = select.select([iprsock], [], [], 0)
                if not len(rl):
                    break
                ret.extend(iprsock.get())

            for link in ret:
                if link.get('event') == 'RTM_NEWLINK':
                    print(
                        link.get('ifname'),
                        link.get('state'),
                        link.get('address'),
                    )

    .. testoutput::

        lo up 00:00:00:00:00:00
        eth0 up 52:54:00:72:58:b2

    Threadless RT netlink monitoring with blocking I/O calls:

        >>> from pyroute2 import IPRSocket
        >>> from pprint import pprint
        >>> s = IPRSocket()
        >>> s.bind()
        >>> pprint(s.get())
        [{'attrs': [('RTA_TABLE', 254),
                    ('RTA_OIF', 2),
                    ('RTA_GATEWAY', '192.168.122.1')],
          'dst_len': 0,
          'event': 'RTM_NEWROUTE',
          'family': 2,
          'flags': 0,
          'header': {'error': None,
                     'flags': 2,
                     'length': 52,
                     'pid': 325359,
                     'sequence_number': 255,
                     'type': 24},
          'proto': 2,
          'scope': 0,
          'src_len': 0,
          'table': 254,
          'tos': 0,
          'type': 1}]
        >>>

    Like `AsyncIPRSocket`, it does not perform response reassembly,
    protocol-level error propagation, or packet buffering.
    '''

    def __init__(
        self,
        port=None,
        pid=None,
        fileno=None,
        sndbuf=1048576,
        rcvbuf=1048576,
        rcvsize=16384,
        all_ns=False,
        async_qsize=None,
        nlm_generator=None,
        target='localhost',
        ext_ack=False,
        strict_check=False,
        groups=rtnl.RTMGRP_DEFAULTS,
        nlm_echo=False,
        netns=None,
        netns_path=None,
        flags=os.O_CREAT,
        libc=None,
        use_socket=None,
        use_event_loop=None,
    ):
        self.asyncore = AsyncIPRSocket(
            port=port,
            pid=pid,
            fileno=fileno,
            sndbuf=sndbuf,
            rcvbuf=rcvbuf,
            rcvsize=rcvsize,
            all_ns=all_ns,
            target=target,
            ext_ack=ext_ack,
            strict_check=strict_check,
            groups=groups,
            nlm_echo=nlm_echo,
            netns=netns,
            netns_path=netns_path,
            flags=flags,
            libc=libc,
            use_socket=use_socket,
            use_event_loop=use_event_loop,
        )
        self.asyncore.local = NotLocal()

    @property
    def addr_pool(self):
        return self.asyncore.addr_pool

    @property
    def socket(self):
        return self.asyncore.socket

    @property
    def fileno(self):
        return self.asyncore.local.socket.fileno

    def bind(self, groups=None, pid=None, **kwarg):
        self.asyncore._sync_bind(groups, pid, **kwarg)

    def put(
        self,
        msg,
        msg_type,
        msg_flags=NLM_F_REQUEST,
        addr=(0, 0),
        msg_seq=0,
        msg_pid=None,
    ):
        if msg is None:
            msg_class = self.marshal.msg_map[msg_type]
            msg = msg_class()

        request = NetlinkRequest(
            self,
            msg,
            msg_type=msg_type,
            msg_flags=msg_flags,
            msg_seq=msg_seq,
            msg_pid=msg_pid,
        )
        request.msg.encode()
        return request.sock.send(request.msg.data)

    def get(self, msg_seq=0, terminate=None, callback=None, noraise=False):
        data = self.socket.recv(16384)
        return [x for x in self.marshal.parse(data)]


class ChaoticIPRSocket(AsyncIPRSocket, ChaoticNetlinkSocket):
    pass
