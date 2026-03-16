'''
Base netlink socket and marshal
===============================

All the netlink providers are derived from the socket
class, so they provide normal socket API, including
`getsockopt()`, `setsockopt()`, they can be used in
poll/select I/O loops etc.

asynchronous I/O
----------------

To run async reader thread, one should call
`NetlinkSocket.bind(async_cache=True)`. In that case
a background thread will be launched. The thread will
automatically collect all the messages and store
into a userspace buffer.

.. note::
    There is no need to turn on async I/O, if you
    don't plan to receive broadcast messages.

ENOBUF and async I/O
--------------------

When Netlink messages arrive faster than a program
reads then from the socket, the messages overflow
the socket buffer and one gets ENOBUF on `recv()`::

    ... self.recv(bufsize)
    error: [Errno 105] No buffer space available

One way to avoid ENOBUF, is to use async I/O. Then the
library not only reads and buffers all the messages, but
also re-prioritizes threads. Suppressing the parser
activity, the library increases the response delay, but
spares CPU to read and enqueue arriving messages as
fast, as it is possible.

With logging level DEBUG you can notice messages, that
the library started to calm down the parser thread::

    DEBUG:root:Packet burst: the reader thread priority
        is increased, beware of delays on netlink calls
        Counters: delta=25 qsize=25 delay=0.1

This state requires no immediate action, but just some
more attention. When the delay between messages on the
parser thread exceeds 1 second, DEBUG messages become
WARNING ones::

    WARNING:root:Packet burst: the reader thread priority
        is increased, beware of delays on netlink calls
        Counters: delta=2525 qsize=213536 delay=3

This state means, that almost all the CPU resources are
dedicated to the reader thread. It doesn't mean, that
the reader thread consumes 100% CPU -- it means, that the
CPU is reserved for the case of more intensive bursts. The
library will return to the normal state only when the
broadcast storm will be over, and then the CPU will be
100% loaded with the parser for some time, when it will
process all the messages queued so far.

when async I/O doesn't help
---------------------------

Sometimes, even turning async I/O doesn't fix ENOBUF.
Mostly it means, that in this particular case the Python
performance is not enough even to read and store the raw
data from the socket. There is no workaround for such
cases, except of using something *not* Python-based.

One can still play around with SO_RCVBUF socket option,
but it doesn't help much. So keep it in mind, and if you
expect massive broadcast Netlink storms, perform stress
testing prior to deploy a solution in the production.

classes
-------
'''

import asyncio
import errno
import logging
import os
import random
import struct
from dataclasses import dataclass
from socket import SO_RCVBUF, SO_SNDBUF, SOCK_DGRAM, SOL_SOCKET
from typing import Optional

from pyroute2 import config, netns
from pyroute2.common import AddrPool, basestring, msg_done
from pyroute2.config import AF_NETLINK
from pyroute2.netlink import (
    NETLINK_ADD_MEMBERSHIP,
    NETLINK_DROP_MEMBERSHIP,
    NETLINK_EXT_ACK,
    NETLINK_GENERIC,
    NETLINK_GET_STRICT_CHK,
    NETLINK_LISTEN_ALL_NSID,
    NLM_F_ACK,
    NLM_F_APPEND,
    NLM_F_ATOMIC,
    NLM_F_CREATE,
    NLM_F_DUMP,
    NLM_F_ECHO,
    NLM_F_EXCL,
    NLM_F_REPLACE,
    NLM_F_REQUEST,
    NLM_F_ROOT,
    SOL_NETLINK,
)
from pyroute2.netlink.core import (
    AsyncCoreSocket,
    CoreConfig,
    CoreDatagramProtocol,
    CoreSocketSpec,
    SyncAPI,
)
from pyroute2.netlink.exceptions import ChaoticException, NetlinkError
from pyroute2.netlink.marshal import Marshal
from pyroute2.netlink.rt_files import NlProtosFile
from pyroute2.requests.main import RequestFilter

log = logging.getLogger(__name__)


class CompileContext:
    def __init__(self, netlink_socket):
        self.netlink_socket = netlink_socket
        self.netlink_socket.compiled = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        self.netlink_socket.compiled = None


# 8<-----------------------------------------------------------
# Singleton, containing possible modifiers to the NetlinkSocket
# bind() call.
#
# Normally, you can open only one netlink connection for one
# process, but there is a hack. Current PID_MAX_LIMIT is 2^22,
# so we can use the rest to modify the pid field.
#
# See also libnl library, lib/socket.c:generate_local_port()
sockets = AddrPool(minaddr=0x0, maxaddr=0x3FF, reverse=True)
# 8<-----------------------------------------------------------


@dataclass
class NetlinkConfig(CoreConfig):
    family: int = NETLINK_GENERIC
    port: Optional[int] = None
    pid: Optional[int] = None
    fileno: Optional[int] = None
    rcvbuf: int = 1048576
    sndbuf: int = 1048576
    all_ns: bool = False
    ext_ack: bool = False
    strict_check: bool = False
    nlm_echo: bool = False


class NetlinkSocketSpecFilter(RequestFilter):
    def set_target(self, context, value):
        if 'target' in context:
            return {'target': context['target']}
        return {'target': value}

    def set_netns(self, context, value):
        if 'target' in context:
            return {'netns': value}
        return {'target': value, 'netns': value}

    def set_pid(self, context, value):
        if value is None:
            return {'pid': os.getpid() & 0x3FFFFF, 'port': context['port']}
        elif value == 0:
            return {'pid': os.getpid(), 'port': 0}
        else:
            return {'pid': value, 'port': 0}

    def set_port(self, context, value):
        if isinstance(value, int):
            return {'port': value, 'epid': context['pid'] + (value << 22)}


class NetlinkSocketSpec(CoreSocketSpec):
    defaults = {
        'pid': 0,
        'epid': 0,
        'port': 0,
        'closed': False,
        'uname': config.uname,
        'use_socket': False,
        'event_loop': 'none',
        'transport_mode': 'none',
    }
    status_filters = [NetlinkSocketSpecFilter]


class AsyncNetlinkSocket(AsyncCoreSocket):
    '''
    Netlink socket
    '''

    marshal_class = Marshal

    def __init__(
        self,
        family=NETLINK_GENERIC,
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
        groups=0,
        nlm_echo=False,
        use_socket=None,
        netns=None,
        flags=os.O_CREAT,
        libc=None,
        use_event_loop=None,
        telemetry=None,
        exception_factory=None,
    ):
        if isinstance(family, str):
            family = NlProtosFile().get_rt_id(family)

        # 8<-----------------------------------------
        self.spec = NetlinkSocketSpec(
            NetlinkConfig(
                family=family,
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
                tag_field='sequence_number',
                netns=netns,
                flags=flags,
                use_libc=libc is not None,
                use_socket=use_socket is not None,
                use_event_loop=use_event_loop is not None,
                telemetry=telemetry,
            )
        )
        # TODO: merge capabilities to self.status
        self.capabilities = {
            'create_bridge': config.kernel > [3, 2, 0],
            'create_bond': config.kernel > [3, 2, 0],
            'create_dummy': True,
            'provide_master': config.kernel[0] > 2,
        }
        super().__init__(
            libc=libc, use_socket=use_socket, use_event_loop=use_event_loop
        )
        self.marshal = self.marshal_class()
        self.request_proxy = None
        self.batch = None
        self.exception_factory = exception_factory

    async def setup_endpoint(self):
        if getattr(self.local, 'transport', None) is not None:
            return
        self.local.transport, self.local.protocol = (
            await self.event_loop.create_datagram_endpoint(
                lambda: CoreDatagramProtocol(
                    self.connection_lost,
                    self.enqueue,
                    self._error_event,
                    self.status,
                ),
                sock=self.socket,
            )
        )

    def setup_socket(self):
        if self.status['use_socket']:
            return self.use_socket
        sock = netns.create_socket(
            netns=self.spec['netns'],
            family=AF_NETLINK,
            socket_type=SOCK_DGRAM,
            proto=self.spec['family'],  # netlink family = socket proto
            fileno=self.spec['fileno'],
            flags=self.spec['flags'],
            libc=self.libc,
        )
        sock.setsockopt(SOL_SOCKET, SO_SNDBUF, self.status['sndbuf'])
        sock.setsockopt(SOL_SOCKET, SO_RCVBUF, self.status['rcvbuf'])
        if self.status['ext_ack']:
            sock.setsockopt(SOL_NETLINK, NETLINK_EXT_ACK, 1)
        if self.status['all_ns']:
            sock.setsockopt(SOL_NETLINK, NETLINK_LISTEN_ALL_NSID, 1)
        if self.status['strict_check']:
            sock.setsockopt(SOL_NETLINK, NETLINK_GET_STRICT_CHK, 1)
        return sock

    @property
    def uname(self):
        return self.status['uname']

    @property
    def groups(self):
        return self.status['groups']

    @property
    def pid(self):
        return self.status['pid']

    @property
    def port(self):
        return self.status['port']

    @property
    def epid(self):
        return self.status['epid']

    @property
    def target(self):
        return self.status['target']

    def _sync_bind(self, groups=0, pid=None, **kwarg):
        self.spec['groups'] = groups
        # if we have pre-defined port, use it strictly
        self.spec['pid'] = pid
        if pid is None:
            for port in range(20, 200):
                try:
                    self.spec['port'] = port
                    self.socket.bind(
                        (self.status['epid'], self.status['groups'])
                    )
                    break
                except Exception as e:
                    # create a new underlying socket -- on kernel 4
                    # one failed bind() makes the socket useless
                    log.debug(e)
            else:
                raise KeyError('no free address available')

    async def bind(self, groups=0, pid=None, **kwarg):
        '''
        Bind the socket to given multicast groups, using
        given pid.

            - If pid is None, use automatic port allocation
            - If pid == 0, use process' pid
            - If pid == <int>, use the value instead of pid
        '''
        await self.setup_endpoint()
        self._sync_bind(groups, pid, **kwarg)

    def add_membership(self, group):
        self.socket.setsockopt(SOL_NETLINK, NETLINK_ADD_MEMBERSHIP, group)

    def drop_membership(self, group):
        self.socket.setsockopt(SOL_NETLINK, NETLINK_DROP_MEMBERSHIP, group)

    def enqueue(self, data, addr):
        # calculate msg_seq
        tag = struct.unpack_from('I', data, 8)[0]
        return self.msg_queue.put_nowait(tag, data)

    def compile(self):
        return CompileContext(self)

    def make_request_type(self, command, command_map):
        if isinstance(command, basestring):
            return (lambda x: (x[0], self.make_request_flags(x[1])))(
                command_map[command]
            )
        elif isinstance(command, int):
            return command, self.make_request_flags('create')
        elif isinstance(command, (list, tuple)):
            return command
        else:
            raise TypeError('allowed command types: int, str, list, tuple')

    def make_request_flags(self, mode):
        flags = {
            'dump': NLM_F_REQUEST | NLM_F_DUMP,
            'get': NLM_F_REQUEST | NLM_F_ACK,
            'req': NLM_F_REQUEST | NLM_F_ACK,
            'put': NLM_F_REQUEST | NLM_F_CREATE,
        }
        flags['create'] = flags['req'] | NLM_F_CREATE | NLM_F_EXCL
        flags['append'] = flags['req'] | NLM_F_CREATE | NLM_F_APPEND
        flags['change'] = flags['req'] | NLM_F_REPLACE
        flags['replace'] = flags['change'] | NLM_F_CREATE

        return flags[mode] | (
            NLM_F_ECHO
            if (self.status['nlm_echo'] and mode not in ('get', 'dump'))
            else 0
        )

    async def put(
        self,
        msg,
        msg_type,
        msg_flags=NLM_F_REQUEST,
        addr=(0, 0),
        msg_seq=0,
        msg_pid=None,
    ):
        request = NetlinkRequest(
            self,
            msg,
            msg_type=msg_type,
            msg_flags=msg_flags,
            msg_seq=msg_seq,
            msg_pid=msg_pid,
        )
        await request.send()
        return request

    async def nlm_request_batch(self, msgs, noraise=False):
        expected_responses = []
        data = b''
        for message in msgs:
            request = NetlinkRequest(self, message)
            await request.prepare()
            data += request.msg.data
            if (request.msg['header']['flags'] & NLM_F_ACK) or (
                request.msg['header']['flags'] & NLM_F_DUMP
            ):
                expected_responses.append(request)
            else:
                request.cleanup()

        self.send(data)
        for request in expected_responses:
            try:
                async for msg in request.response():
                    yield msg
            except NetlinkError:
                if not noraise:
                    raise

    async def nlm_request(
        self,
        msg,
        msg_type,
        msg_flags=NLM_F_REQUEST | NLM_F_DUMP,
        terminate=None,
        callback=None,
        parser=None,
        exception_factory=None,
    ):
        request = NetlinkRequest(
            self,
            msg,
            terminate=terminate,
            callback=callback,
            exception_factory=exception_factory,
        )
        request.msg['header']['type'] = msg_type
        request.msg['header']['flags'] = msg_flags
        await request.send()
        return request.response()


class NetlinkRequest:
    # request flags
    flags = {
        'dump': NLM_F_REQUEST | NLM_F_DUMP,
        'root': NLM_F_REQUEST | NLM_F_ROOT | NLM_F_ATOMIC,
        'get': NLM_F_REQUEST | NLM_F_ACK,
        'req': NLM_F_REQUEST | NLM_F_ACK,
    }
    flags['create'] = flags['req'] | NLM_F_CREATE | NLM_F_EXCL
    flags['append'] = flags['req'] | NLM_F_CREATE | NLM_F_APPEND
    flags['change'] = flags['req'] | NLM_F_REPLACE
    flags['replace'] = flags['change'] | NLM_F_CREATE

    def __init__(
        self,
        sock,
        msg,
        command=None,
        command_map=None,
        dump_filter=None,
        request_filter=None,
        terminate=None,
        callback=None,
        parser=None,
        msg_type=None,
        msg_flags=None,
        msg_seq=None,
        msg_pid=None,
        exception_factory=None,
    ):
        self.sock = sock
        self.addr_pool = sock.addr_pool
        self.status = sock.status
        self.epid = sock.epid if msg_pid is None else msg_pid
        self.marshal = sock.marshal
        self.parser = parser
        self.exception_factory = exception_factory or getattr(
            sock, "exception_factory", None
        )
        # if not isinstance(msg, nlmsg):
        #    msg_class = self.marshal.msg_map[msg_type]
        #    msg = msg_class(msg)
        self.msg_seq = self.addr_pool.alloc() if msg_seq is None else msg_seq

        # prio 3: message object
        # prio 2: direct msg_type & msg_flags arguments
        # prio 1: command map
        if command_map is not None:
            msg_type, msg_flags = self.calculate_request_type(
                command, command_map, self.status['nlm_echo']
            )
        if msg_type is not None:
            msg['header']['type'] = msg_type
        if msg_flags is not None:
            msg['header']['flags'] = msg_flags

        # if there is no type & flags yet, set defaults
        # FIXME: collect usecases
        if msg['header'].get('type') is None:
            msg['header']['type'] = 0
        if msg['header'].get('flags') is None:
            msg['header']['flags'] = NLM_F_REQUEST | NLM_F_DUMP

        msg['header']['sequence_number'] = self.msg_seq
        msg['header']['pid'] = self.epid or os.getpid()
        msg.reset()
        # set fields
        if request_filter is not None:
            for field in msg.fields:
                msg[field[0]] = request_filter.get_value(
                    field[0], default=0, mode='field'
                )
            # attach NLAs
            for key, value in request_filter.items():
                nla = type(msg).name2nla(key)
                if msg.valid_nla(nla) and value is not None:
                    msg['attrs'].append([nla, value])
            # extend with custom NLAs
            if 'attrs' in request_filter:
                msg['attrs'].extend(request_filter['attrs'])
        self.msg = msg
        self.dump_filter = dump_filter
        self.terminate = terminate
        self.callback = callback
        self.command = command

    @classmethod
    def calculate_request_type(cls, command, command_map, echo=False):
        if isinstance(command, basestring):
            return (lambda x: (x[0], cls.calculate_request_flags(x[1], echo)))(
                command_map[command]
            )
        elif isinstance(command, int):
            return command, cls.calculate_request_flags('create', echo)
        elif isinstance(command, (list, tuple)):
            return command
        else:
            raise TypeError('allowed command types: int, str, list, tuple')

    @classmethod
    def calculate_request_flags(cls, mode, echo):
        return cls.flags[mode] | (
            NLM_F_ECHO if (echo and mode not in ('get', 'dump')) else 0
        )

    @staticmethod
    def match_one_message(dump_filter, msg):
        if hasattr(dump_filter, '__call__'):
            return dump_filter(msg)
        elif isinstance(dump_filter, dict):
            matches = []
            for key in dump_filter:
                # get the attribute
                if not isinstance(key, (str, tuple)):
                    continue
                value = msg.get(key)
                if value is not None and callable(dump_filter[key]):
                    matches.append(dump_filter[key](value))
                else:
                    matches.append(dump_filter[key] == value)
            return all(matches)

    async def prepare(self):
        await self.sock.setup_endpoint()
        self.msg.encode()
        self.sock.msg_queue.ensure_tag(self.msg_seq)
        if self.parser is not None:
            self.marshal.seq_map[self.msg_seq] = self.parser

    def cleanup(self):
        self.addr_pool.free(self.msg_seq, ban=0xFF)
        self.sock.msg_queue.free_tag(self.msg_seq)
        if self.msg_seq in self.marshal.seq_map:
            self.marshal.seq_map.pop(self.msg_seq)

    async def proxy(self):
        if self.sock.batch is not None:
            self.sock.batch += self.msg.data
            await self.sock.msg_queue.put(self.msg_seq, msg_done(self.msg))
            return True
        if self.sock.request_proxy is None:
            return False
        ret = self.sock.request_proxy.handle(self.msg)
        if ret == b'':
            return False
        await self.sock.msg_queue.put(self.msg_seq, ret)
        return True

    async def send(self):
        await self.prepare()
        if await self.proxy():
            return len(self.msg.data)
        count = 0
        exc = RuntimeError('Max attempts sending message')
        for count in range(30):
            try:
                return self.sock.send(self.msg.data)
            except NetlinkError as e:
                if e.code != errno.EBUSY:
                    exc = e
                    break
                log.warning(f'Error 16, retry {count}')
                await asyncio.sleep(0.3)
            except Exception as e:
                exc = e
                break
        self.cleanup()
        raise exc

    async def response(self):
        coro = self.sock.get(
            msg_seq=self.msg_seq,
            terminate=self.terminate,
            callback=self.callback,
            exception_factory=self.exception_factory,
        )
        try:
            async for msg in coro:
                if (
                    self.dump_filter is not None
                    and not self.match_one_message(self.dump_filter, msg)
                ):
                    continue
                for cr in self.sock.callbacks:
                    try:
                        if cr[0](msg):
                            cr[1](msg, *cr[2])
                    except Exception:
                        log.warning("Callback fail: %{cr}")
                yield msg
        finally:
            await coro.aclose()
            self.cleanup()


class NetlinkSocket(SyncAPI):
    async_class = AsyncNetlinkSocket

    def __init__(
        self,
        family=NETLINK_GENERIC,
        port=None,
        pid=None,
        fileno=None,
        sndbuf=1048576,
        rcvbuf=1048576,
        rcvsize=16384,
        all_ns=False,
        async_qsize=None,
        nlm_generator=True,
        target='localhost',
        ext_ack=False,
        strict_check=False,
        groups=0,
        nlm_echo=False,
        netns=None,
        flags=os.O_CREAT,
        libc=None,
        use_socket=None,
        use_event_loop=None,
        telemetry=None,
    ):
        self.asyncore = self.async_class(
            family=family,
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
            flags=flags,
            libc=libc,
            use_socket=use_socket,
            use_event_loop=use_event_loop,
            telemetry=telemetry,
        )
        self.asyncore.local.keep_event_loop = True
        self.asyncore.status['event_loop'] = 'new'
        self.asyncore.status['nlm_generator'] = nlm_generator
        # FIXME: temporary override from a class attribute
        if hasattr(self, 'class_gen_sync'):
            self.asyncore.status['nlm_generator'] = self.class_gen_sync
        self.asyncore.event_loop.run_until_complete(
            self.asyncore.setup_endpoint()
        )
        if self.asyncore.socket.fileno() == -1:
            raise OSError(9, 'Bad file descriptor')

    def bind(self, *argv, **kwarg):
        with self.lock:
            self.asyncore._check_tid(tag='bind', level=logging.WARN)
            self.asyncore.local.keep_event_loop = True
            self._run_with_cleanup(self.asyncore.bind, *argv, **kwarg)
            self.asyncore._register_loop_ref()

    def put(
        self,
        msg,
        msg_type,
        msg_flags=NLM_F_REQUEST,
        addr=(0, 0),
        msg_seq=None,
        msg_pid=None,
    ):
        if msg is None:
            msg_class = self.marshal.msg_map[msg_type]
            msg = msg_class()
        return self._run_with_cleanup(
            self.asyncore.put, msg, msg_type, msg_flags, addr, msg_seq, msg_pid
        )

    def nlm_request_batch(self, msgs, noraise=False):
        async def collect_data():
            return [
                x async for x in self.asyncore.nlm_request_batch(msgs, noraise)
            ]

        return self._run_with_cleanup(collect_data)

    def nlm_request(
        self,
        msg,
        msg_type,
        msg_flags=NLM_F_REQUEST | NLM_F_DUMP,
        terminate=None,
        callback=None,
        parser=None,
    ):
        ret = self._generate_with_cleanup(
            self.asyncore.nlm_request,
            msg,
            msg_type,
            msg_flags,
            terminate,
            callback,
            parser,
        )
        if self.status['nlm_generator']:
            return ret
        return tuple(ret)

    def get(self, msg_seq=0, terminate=None, callback=None, noraise=False):
        async def collect_data():
            return [
                i
                async for i in self.asyncore.get(
                    msg_seq, terminate, callback, noraise
                )
            ]

        return self._run_with_cleanup(collect_data)


class ChaoticNetlinkSocket(NetlinkSocket):
    success_rate = 1

    def __init__(self, *argv, **kwarg):
        self.success_rate = kwarg.pop('success_rate', 0.7)
        super(ChaoticNetlinkSocket, self).__init__(*argv, **kwarg)

    def get(self, *argv, **kwarg):
        if random.random() > self.success_rate:
            raise ChaoticException()
        return super(ChaoticNetlinkSocket, self).get(*argv, **kwarg)
