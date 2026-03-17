# -*- coding: utf-8 -*-
"""Sun RPC version 2 -- RFC1057

This file is drawn from Python's RPC demo, updated for python 3.

XXX There should be separate exceptions for the various reasons why
XXX an RPC can fail, rather than using RuntimeError for everything

XXX The UDP version of the protocol resends requests when it does
XXX not receive a timely reply -- use only for idempotent calls!

Original source:
    http://svn.python.org/projects/python/trunk/Demo/rpc/rpc.py


:copyright: 2014-2024 by PyVISA-py Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import enum
import select
import socket
import struct
import sys
import time

from ..common import LOGGER
from . import xdrlib

#: Version of the protocol
RPCVERSION = 2


class MessagegType(enum.IntEnum):
    call = 0
    reply = 1


class AuthorizationFlavor(enum.IntEnum):
    null = 0
    unix = 1
    short = 2
    des = 3


class ReplyStatus(enum.IntEnum):
    accepted = 0
    denied = 1


class AcceptStatus(enum.IntEnum):
    #: RPC executed successfully
    success = 0

    #: remote hasn't exported program
    program_unavailable = 1

    #: remote can't support version
    program_mismatch = 2

    #: program can't support procedure
    procedure_unavailable = 3

    #: procedure can't decode params
    garbage_args = 4


class RejectStatus(enum.IntEnum):
    #: RPC version number != 2
    rpc_mismatch = 0

    #: remote can't authenticate caller
    auth_error = 1


class AuthStatus(enum.IntEnum):
    ok = 0

    #: bad credentials (seal broken)
    bad_credentials = 1

    #: client must begin new session
    rejected_credentials = 2

    #: bad verifier (seal broken)
    bad_verifier = 3

    #: verifier expired or replayed
    rejected_verifier = 4

    #: rejected for security reasons
    too_weak = 5


# Exceptions
class RPCError(Exception):
    pass


class RPCBadFormat(RPCError):
    pass


class RPCBadVersion(RPCError):
    pass


class RPCGarbageArgs(RPCError):
    pass


class RPCUnpackError(RPCError):
    pass


def make_auth_null():
    return b""


class Packer(xdrlib.Packer):
    def pack_auth(self, auth):
        flavor, stuff = auth
        self.pack_enum(flavor)
        self.pack_opaque(stuff)

    def pack_auth_unix(self, stamp, machinename, uid, gid, gids):
        self.pack_uint(stamp)
        self.pack_string(machinename)
        self.pack_uint(uid)
        self.pack_uint(gid)
        self.pack_uint(len(gids))
        for i in gids:
            self.pack_uint(i)

    def pack_callheader(self, xid, prog, vers, proc, cred, verf):
        self.pack_uint(xid)
        self.pack_enum(MessagegType.call)
        self.pack_uint(RPCVERSION)
        self.pack_uint(prog)
        self.pack_uint(vers)
        self.pack_uint(proc)
        self.pack_auth(cred)
        self.pack_auth(verf)
        # Caller must add procedure-specific part of call

    def pack_replyheader(self, xid, verf):
        self.pack_uint(xid)
        self.pack_enum(MessagegType.reply)
        self.pack_uint(ReplyStatus.accepted)
        self.pack_auth(verf)
        self.pack_enum(AcceptStatus.success)
        # Caller must add procedure-specific part of reply


class Unpacker(xdrlib.Unpacker):
    def unpack_auth(self):
        flavor = self.unpack_enum()
        stuff = self.unpack_opaque()
        return flavor, stuff

    def unpack_callheader(self):
        xid = self.unpack_uint()
        temp = self.unpack_enum()
        if temp != MessagegType.call:
            raise RPCBadFormat("no CALL but %r" % (temp,))
        temp = self.unpack_uint()
        if temp != RPCVERSION:
            raise RPCBadVersion("bad RPC version %r" % (temp,))
        prog = self.unpack_uint()
        vers = self.unpack_uint()
        proc = self.unpack_uint()
        cred = self.unpack_auth()
        verf = self.unpack_auth()
        return xid, prog, vers, proc, cred, verf
        # Caller must add procedure-specific part of call

    def unpack_replyheader(self):
        xid = self.unpack_uint()
        mtype = self.unpack_enum()
        if mtype != MessagegType.reply:
            raise RPCUnpackError("no reply but %r" % (mtype,))
        stat = self.unpack_enum()
        if stat == ReplyStatus.denied:
            stat = self.unpack_enum()
            if stat == RejectStatus.rpc_mismatch:
                low = self.unpack_uint()
                high = self.unpack_uint()
                raise RPCUnpackError("denied: rpc_mismatch: %r" % ((low, high),))
            if stat == RejectStatus.auth_error:
                stat = self.unpack_uint()
                raise RPCUnpackError("denied: auth_error: %r" % (stat,))
            raise RPCUnpackError("denied: %r" % (stat,))
        if stat != ReplyStatus.accepted:
            raise RPCUnpackError("Neither denied nor accepted: %r" % (stat,))
        verf = self.unpack_auth()
        stat = self.unpack_enum()
        if stat == AcceptStatus.program_unavailable:
            raise RPCUnpackError("call failed: program_unavailable")
        if stat == AcceptStatus.program_mismatch:
            low = self.unpack_uint()
            high = self.unpack_uint()
            raise RPCUnpackError("call failed: program_mismatch: %r" % ((low, high),))
        if stat == AcceptStatus.procedure_unavailable:
            raise RPCUnpackError("call failed: procedure_unavailable")
        if stat == AcceptStatus.garbage_args:
            raise RPCGarbageArgs
        if stat != AcceptStatus.success:
            raise RPCUnpackError("call failed: %r" % (stat,))
        return xid, verf
        # Caller must get procedure-specific part of reply


class Client(object):
    """Common base class for clients."""

    def __init__(self, host, prog, vers, port):
        self.host = host
        self.prog = prog
        self.vers = vers
        self.port = port
        self.lastxid = 0  # XXX should be more random?
        self.cred = None
        self.verf = None

    def make_call(self, proc, args, pack_func, unpack_func):
        # Don't normally override this (but see Broadcast)
        LOGGER.debug("Make call %r, %r, %r, %r", proc, args, pack_func, unpack_func)

        if pack_func is None and args is not None:
            raise TypeError("non-null args with null pack_func")
        self.start_call(proc)
        if pack_func:
            pack_func(args)
        self.do_call()
        if unpack_func:
            result = unpack_func()
        else:
            result = None
        # N.B. Some devices may pad responses beyond RFC 1014 4-byte
        #   alignment, so skip self.unpacker.done() call here which
        #   would raise an exception in that case.  See issue #225.
        return result

    def start_call(self, proc):
        # Don't override this
        self.lastxid += 1
        cred = self.mkcred()
        verf = self.mkverf()
        p = self.packer
        p.reset()
        p.pack_callheader(self.lastxid, self.prog, self.vers, proc, cred, verf)
        p.proc = proc

    def do_call(self):
        # This MUST be overridden
        raise RPCError("do_call not defined")

    def mkcred(self):
        # Override this to use more powerful credentials
        if self.cred is None:
            self.cred = (AuthorizationFlavor.null, make_auth_null())
        return self.cred

    def mkverf(self):
        # Override this to use a more powerful verifier
        if self.verf is None:
            self.verf = (AuthorizationFlavor.null, make_auth_null())
        return self.verf

    def call_0(self):
        # Procedure 0 is always like this
        return self.make_call(0, None, None, None)


# Record-Marking standard support


def _sendto(sock, data, address):
    """
    loops calling sock.sendto() until all data is sent.
    """
    ptr = 0
    while data[ptr:]:
        ptr += sock.sendto(data[ptr:], address)


# XXX sendfrag() should have been deleted when it was inlined into
# _sendrecord() during refactoring
def sendfrag(sock, last, frag):
    x = len(frag)
    if last:
        x = x | 0x80000000
    header = struct.pack(">I", x)
    sock.sendall(header + frag)


def _sendrecord(sock, record, fragsize=None, timeout=None):
    LOGGER.debug("Sending record through %s: %r", sock, record)
    if timeout is not None:
        r, w, x = select.select([], [sock], [], timeout)
        if sock not in w:
            msg = "socket.timeout: The instrument seems to have stopped responding."
            raise socket.timeout(msg)

    last = False
    if not fragsize:
        fragsize = 0x7FFFFFFF
    while not last:
        record_len = len(record)
        if record_len <= fragsize:
            fragsize = record_len
            last = True
        if last:
            fragsize = fragsize | 0x80000000
        header = struct.pack(">I", fragsize)
        sock.sendall(header + record[:fragsize])
        record = record[fragsize:]


def _recvrecord(sock, timeout, read_fun=None, min_packages=0):
    record = bytearray()
    buffer = bytearray()
    if not read_fun:
        read_fun = sock.recv

    wait_header = True
    last = False
    exp_length = 4
    packages_received = 0

    if min_packages != 0:
        LOGGER.debug("Start receiving at least %i packages" % min_packages)

    # minimum is in interval 1 - 100ms based on timeout or for infinite it is
    # 1 sec

    min_select_timeout = (
        max(min(timeout / 100.0, 0.1), 0.001) if timeout is not None else 1.0
    )
    # initial 'select_timeout' is half of timeout or max 2 secs
    # (max blocking time).
    # min is from 'min_select_timeout'
    select_timeout = (
        max(min(timeout / 2.0, 2.0), min_select_timeout) if timeout is not None else 1.0
    )
    # time, when loop shall finish
    finish_time = time.time() + timeout if timeout is not None else 0
    while True:
        # if more data for the current fragment is needed, use select
        # to wait for read ready, max `select_timeout` seconds
        if len(buffer) < exp_length:
            r, w, x = select.select([sock], [], [], select_timeout)
            read_data = b""
            if sock in r:
                read_data = read_fun(exp_length)
                buffer.extend(read_data)
                LOGGER.debug("received %r" % read_data)
            # Timeout was reached
            if not read_data:  # no response or empty response
                if timeout is not None and time.time() >= finish_time:
                    LOGGER.debug(
                        (
                            "Time out encountered in %s."
                            "Already receieved %d bytes. Last fragment is %d "
                            "bytes long and we were expecting %d"
                        ),
                        sock,
                        len(record),
                        len(buffer),
                        exp_length,
                    )
                    msg = (
                        "socket.timeout: The instrument seems to have stopped "
                        "responding."
                    )
                    raise socket.timeout(msg)
                elif min_packages != 0 and packages_received >= min_packages:
                    LOGGER.debug(
                        "Stop receiving after %i of %i requested packages. Received record through %s: %r",
                        packages_received,
                        min_packages,
                        sock,
                        record,
                    )
                    return bytes(record)
                else:
                    # `select_timeout` decreased to 50% of previous or
                    # min_select_timeout
                    select_timeout = max(select_timeout / 2.0, min_select_timeout)
                    continue

        if wait_header:
            # need to find header
            if len(buffer) >= exp_length:
                header = buffer[:exp_length]
                buffer = buffer[exp_length:]
                x = struct.unpack(">I", header)[0]
                last = (x & 0x80000000) != 0
                exp_length = int(x & 0x7FFFFFFF)
                wait_header = False
        else:
            if len(buffer) >= exp_length:
                record.extend(buffer[:exp_length])
                buffer = buffer[exp_length:]
                if last:
                    LOGGER.debug("Received record through %s: %r", sock, record)
                    return bytes(record)
                else:
                    wait_header = True
                    exp_length = 4
                    packages_received += 1


def _connect(sock, host, port, timeout=0):
    try:
        sock.setblocking(0)
        sock.connect_ex((host, port))
    except Exception:
        sock.close()
        return False
    finally:
        sock.setblocking(1)

    # minimum is in interval 100 - 500ms based on timeout
    min_select_timeout = max(min(timeout / 10.0, 0.5), 0.1)
    # initial 'select_timout' is half of timeout or max 2 secs
    # (max blocking time).
    # min is from 'min_select_timeout'
    select_timout = max(min(timeout / 2.0, 2.0), min_select_timeout)
    # time, when loop shall finish
    finish_time = time.time() + timeout
    while True:
        # use select to wait for socket ready, max `select_timout` seconds
        r, w, x = select.select([sock], [sock], [], select_timout)
        if sock in r or sock in w:
            return True

        if time.time() >= finish_time:
            # reached timeout
            sock.close()
            return False

        # `select_timout` decreased to 50% of previous or min_select_timeout
        select_timout = max(select_timout / 2.0, min_select_timeout)


class RawTCPClient(Client):
    """Client using TCP to a specific port."""

    def __init__(self, host, prog, vers, port, open_timeout=5000):
        Client.__init__(self, host, prog, vers, port)
        open_timeout = open_timeout if open_timeout is not None else 5000
        self.connect(1e-3 * open_timeout)
        # self.timeout defaults higher than the default 2 second VISA timeout,
        # ensuring that VISA timeouts take precedence.
        self.timeout = 4.0

    def make_call(self, proc, args, pack_func, unpack_func):
        """Overridden to allow for utilizing io_timeout (passed in args)."""
        if proc == 11:
            # vxi11.DEVICE_WRITE
            self.timeout = args[1] / 1000.0
        elif proc in (12, 22):
            # vxi11.DEVICE_READ or vxi11.DEVICE_DOCMD
            self.timeout = args[2] / 1000.0
        elif proc in (13, 14, 15, 16, 17):
            # vxi11.DEVICE_READSTB, vxi11.DEVICE_TRIGGER, vxi11.DEVICE_CLEAR,
            # vxi11.DEVICE_REMOTE, or vxi11.DEVICE_LOCAL
            self.timeout = args[3] / 1000.0
        else:
            self.timeout = 4.0

        # In case of a timeout because the instrument cannot answer, the
        # instrument should let use something went wrong. If we hit the hard
        # timeout of the rpc, it means something worse happened (cable
        # unplugged).
        self.timeout += 1.0

        return super(RawTCPClient, self).make_call(proc, args, pack_func, unpack_func)

    def connect(self, timeout=5.0):
        LOGGER.debug(
            "RawTCPClient: connecting to socket at (%s, %s)", self.host, self.port
        )
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if not _connect(self.sock, self.host, self.port, timeout):
            raise RPCError("can't connect to server")

    def close(self):
        LOGGER.debug("RawTCPClient: closing socket")
        self.sock.close()

    def do_call(self):
        call = self.packer.get_buf()

        _sendrecord(self.sock, call, timeout=self.timeout)

        try:
            min_packages = int(self.packer.proc == 3)
            LOGGER.debug("RawTCPClient: procedure type %i" % self.packer.proc)
            # if the command is get_port, we only expect one package.
            # This is a workaround for misbehaving instruments.
        except AttributeError:
            min_packages = 0

        while True:
            reply = _recvrecord(self.sock, self.timeout, min_packages=min_packages)
            u = self.unpacker
            u.reset(reply)
            xid, verf = u.unpack_replyheader()
            if xid == self.lastxid:
                # xid matches, we're done
                return
            elif xid < self.lastxid:
                # Stale data in buffer due to interruption
                # Discard and fetch another record
                continue
            else:
                # xid larger than expected - packet from the future?
                raise RPCError(
                    "wrong xid in reply %r instead of %r" % (xid, self.lastxid)
                )


class RawUDPClient(Client):
    """Client using UDP to a specific port."""

    def __init__(self, host, prog, vers, port):
        Client.__init__(self, host, prog, vers, port)
        self.connect()

    def connect(self):
        LOGGER.debug(
            "RawTCPClient: connecting to socket at (%s, %s)", self.host, self.port
        )
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.connect((self.host, self.port))

    def close(self):
        LOGGER.debug("RawTCPClient: closing socket")
        self.sock.close()

    def do_call(self):
        call = self.packer.get_buf()
        self.sock.sendall(call)

        BUFSIZE = 8192  # Max UDP buffer size
        timeout = 1
        count = 5
        while 1:
            r, w, x = [self.sock], [], []
            if select:
                r, w, x = select.select(r, w, x, timeout)
            if self.sock not in r:
                count = count - 1
                if count < 0:
                    raise RPCError("timeout")
                if timeout < 25:
                    timeout = timeout * 2
                self.sock.sendall(call)
                continue
            reply = self.sock.recv(BUFSIZE)
            u = self.unpacker
            u.reset(reply)
            xid, verf = u.unpack_replyheader()
            if xid != self.lastxid:
                continue
            break


class RawBroadcastUDPClient(RawUDPClient):
    """Client using UDP broadcast to a specific port."""

    def __init__(self, bcastaddr, prog, vers, port):
        RawUDPClient.__init__(self, bcastaddr, prog, vers, port)
        self.reply_handler = None
        self.timeout = 30

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    def set_reply_handler(self, reply_handler):
        self.reply_handler = reply_handler

    def set_timeout(self, timeout):
        self.timeout = timeout  # Use None for infinite timeout

    def make_call(self, proc, args, pack_func, unpack_func):
        if pack_func is None and args is not None:
            raise TypeError("non-null args with null pack_func")
        self.start_call(proc)
        if pack_func:
            pack_func(args)
        call = self.packer.get_buf()
        _sendto(self.sock, call, (self.host, self.port))

        BUFSIZE = 8192  # Max UDP buffer size (for reply)
        replies = []
        if unpack_func is None:

            def dummy():
                pass

            unpack_func = dummy
        while 1:
            r, w, x = [self.sock], [], []
            if select:
                if self.timeout is None:
                    r, w, x = select.select(r, w, x)
                else:
                    r, w, x = select.select(r, w, x, self.timeout)
            if self.sock not in r:
                break
            reply, fromaddr = self.sock.recvfrom(BUFSIZE)
            u = self.unpacker
            u.reset(reply)
            xid, verf = u.unpack_replyheader()
            if xid != self.lastxid:
                continue
            reply = unpack_func()
            self.unpacker.done()
            replies.append((reply, fromaddr))
            if self.reply_handler:
                self.reply_handler(reply, fromaddr)
        return replies

    def send_call(self, proc, args, pack_func):
        if pack_func is None and args is not None:
            raise TypeError("non-null args with null pack_func")
        self.start_call(proc)
        if pack_func:
            pack_func(args)
        call = self.packer.get_buf()
        try:
            _sendto(self.sock, call, (self.host, self.port))
        except OSError as exc:
            raise RPCError("unable to send broadcast") from exc

    def recv_call(self, unpack_func):
        BUFSIZE = 8192  # Max UDP buffer size (for reply)
        replies = []
        if unpack_func is None:

            def dummy():
                pass

            unpack_func = dummy
        while 1:
            r, w, x = [self.sock], [], []
            if select:
                if self.timeout is None:
                    r, w, x = select.select(r, w, x)
                else:
                    r, w, x = select.select(r, w, x, self.timeout)
            if self.sock not in r:
                break
            try:
                reply, fromaddr = self.sock.recvfrom(BUFSIZE)
            except OSError as exc:
                raise RPCError("unable to recieve broadcast") from exc
            u = self.unpacker
            u.reset(reply)
            xid, verf = u.unpack_replyheader()
            if xid != self.lastxid:
                continue
            reply = unpack_func()
            self.unpacker.done()
            replies.append((reply, fromaddr))
            if self.reply_handler:
                self.reply_handler(reply, fromaddr)
        return replies


# Port mapper interface

# Program number, version and port number
PMAP_PROG = 100000
PMAP_VERS = 2
PMAP_PORT = 111


class PortMapperVersion(enum.IntEnum):
    #: (void) -> void
    null = 0
    #: (mapping) -> bool
    set = 1
    #: (mapping) -> bool
    unset = 2
    #: (mapping) -> unsigned int
    get_port = 3
    #: (void) -> pmaplist
    dump = 4
    #: (call_args) -> call_result
    call_it = 5


# A mapping is (prog, vers, prot, port) and prot is one of:
IPPROTO_TCP = 6
IPPROTO_UDP = 17

# A pmaplist is a variable-length list of mappings, as follows:
# either (1, mapping, pmaplist) or (0).

# A call_args is (prog, vers, proc, args) where args is opaque;
# a call_result is (port, res) where res is opaque.


class PortMapperPacker(Packer):
    def pack_mapping(self, mapping):
        prog, vers, prot, port = mapping
        self.pack_uint(prog)
        self.pack_uint(vers)
        self.pack_uint(prot)
        self.pack_uint(port)

    def pack_pmaplist(self, list):
        self.pack_list(list, self.pack_mapping)

    def pack_call_args(self, ca):
        prog, vers, proc, args = ca
        self.pack_uint(prog)
        self.pack_uint(vers)
        self.pack_uint(proc)
        self.pack_opaque(args)


class PortMapperUnpacker(Unpacker):
    def unpack_mapping(self):
        prog = self.unpack_uint()
        vers = self.unpack_uint()
        prot = self.unpack_uint()
        port = self.unpack_uint()
        return prog, vers, prot, port

    def unpack_pmaplist(self):
        return self.unpack_list(self.unpack_mapping)

    def unpack_call_result(self):
        port = self.unpack_uint()
        res = self.unpack_opaque()
        return port, res


class PartialPortMapperClient(object):
    def __init__(self):
        self.packer = PortMapperPacker()
        self.unpacker = PortMapperUnpacker("")

    def set(self, mapping):
        return self.make_call(
            PortMapperVersion.set,
            mapping,
            self.packer.pack_mapping,
            self.unpacker.unpack_uint,
        )

    def unset(self, mapping):
        return self.make_call(
            PortMapperVersion.unset,
            mapping,
            self.packer.pack_mapping,
            self.unpacker.unpack_uint,
        )

    def get_port(self, mapping):
        return self.make_call(
            PortMapperVersion.get_port,
            mapping,
            self.packer.pack_mapping,
            self.unpacker.unpack_uint,
        )

    def send_port(self, mapping):
        return self.send_call(
            PortMapperVersion.get_port,
            mapping,
            self.packer.pack_mapping,
        )

    def recv_port(self, mapping):
        return self.recv_call(
            self.unpacker.unpack_uint,
        )

    def dump(self):
        return self.make_call(
            PortMapperVersion.dump, None, None, self.unpacker.unpack_pmaplist
        )

    def callit(self, ca):
        return self.make_call(
            PortMapperVersion.call_it,
            ca,
            self.packer.pack_call_args,
            self.unpacker.unpack_call_result,
        )


class TCPPortMapperClient(PartialPortMapperClient, RawTCPClient):
    def __init__(self, host, open_timeout=5000):
        RawTCPClient.__init__(self, host, PMAP_PROG, PMAP_VERS, PMAP_PORT, open_timeout)
        PartialPortMapperClient.__init__(self)


class UDPPortMapperClient(PartialPortMapperClient, RawUDPClient):
    def __init__(self, host):
        RawUDPClient.__init__(self, host, PMAP_PROG, PMAP_VERS, PMAP_PORT)
        PartialPortMapperClient.__init__(self)


class BroadcastUDPPortMapperClient(PartialPortMapperClient, RawBroadcastUDPClient):
    def __init__(self, bcastaddr):
        RawBroadcastUDPClient.__init__(self, bcastaddr, PMAP_PROG, PMAP_VERS, PMAP_PORT)
        PartialPortMapperClient.__init__(self)


class TCPClient(RawTCPClient):
    """A TCP Client that find their server through the Port mapper"""

    def __init__(self, host, prog, vers, open_timeout=5000):
        pmap = TCPPortMapperClient(host, open_timeout)
        port = pmap.get_port((prog, vers, IPPROTO_TCP, 0))
        pmap.close()
        if port == 0:
            raise RPCError("program not registered")
        RawTCPClient.__init__(self, host, prog, vers, port, open_timeout)


class UDPClient(RawUDPClient):
    """A UDP Client that find their server through the Port mapper"""

    def __init__(self, host, prog, vers):
        pmap = UDPPortMapperClient(host)
        port = pmap.get_port((prog, vers, IPPROTO_UDP, 0))
        pmap.close()
        if port == 0:
            raise RPCError("program not registered")
        RawUDPClient.__init__(self, host, prog, vers, port)


class BroadcastUDPClient(Client):
    """A Broadcast UDP Client that find their server through the Port mapper"""

    def __init__(self, bcastaddr, prog, vers):
        self.pmap = BroadcastUDPPortMapperClient(bcastaddr)
        self.pmap.set_reply_handler(self.my_reply_handler)
        self.prog = prog
        self.vers = vers
        self.user_reply_handler = None
        self.addpackers()

    def close(self):
        self.pmap.close()

    def set_reply_handler(self, reply_handler):
        self.user_reply_handler = reply_handler

    def set_timeout(self, timeout):
        self.pmap.set_timeout(timeout)

    def my_reply_handler(self, reply, fromaddr):
        port, res = reply
        self.unpacker.reset(res)
        result = self.unpack_func()
        self.unpacker.done()
        self.replies.append((result, fromaddr))
        if self.user_reply_handler is not None:
            self.user_reply_handler(result, fromaddr)

    def make_call(self, proc, args, pack_func, unpack_func):
        self.packer.reset()
        if pack_func:
            pack_func(args)
        if unpack_func is None:

            def dummy():
                pass

            self.unpack_func = dummy
        else:
            self.unpack_func = unpack_func
        self.replies = []
        packed_args = self.packer.get_buf()
        _ = self.pmap.Callit((self.prog, self.vers, proc, packed_args))
        return self.replies


# Server classes

# These are not symmetric to the Client classes
# XXX No attempt is made to provide authorization hooks yet


class Server(object):
    def __init__(self, host, prog, vers, port):
        self.host = host  # Should normally be '' for default interface
        self.prog = prog
        self.vers = vers
        self.port = port  # Should normally be 0 for random port
        self.port = port
        self.addpackers()

    def register(self):
        mapping = self.prog, self.vers, self.prot, self.port
        p = TCPPortMapperClient(self.host)
        if not p.set(mapping):
            raise RPCError("register failed")

    def unregister(self):
        mapping = self.prog, self.vers, self.prot, self.port
        p = TCPPortMapperClient(self.host)
        if not p.unset(mapping):
            raise RPCError("unregister failed")

    def handle(self, call):
        # Don't use unpack_header but parse the header piecewise
        # XXX I have no idea if I am using the right error responses!
        self.unpacker.reset(call)
        self.packer.reset()
        xid = self.unpacker.unpack_uint()
        self.packer.pack_uint(xid)
        temp = self.unpacker.unpack_enum()
        if temp != MessagegType.call:
            return None  # Not worthy of a reply
        self.packer.pack_uint(MessagegType.reply)
        temp = self.unpacker.unpack_uint()
        if temp != RPCVERSION:
            self.packer.pack_uint(ReplyStatus.denied)
            self.packer.pack_uint(RejectStatus.rpc_mismatch)
            self.packer.pack_uint(RPCVERSION)
            self.packer.pack_uint(RPCVERSION)
            return self.packer.get_buf()
        self.packer.pack_uint(ReplyStatus.accepted)
        self.packer.pack_auth((AuthorizationFlavor.null, make_auth_null()))
        prog = self.unpacker.unpack_uint()
        if prog != self.prog:
            self.packer.pack_uint(AcceptStatus.program_unavailable)
            return self.packer.get_buf()
        vers = self.unpacker.unpack_uint()
        if vers != self.vers:
            self.packer.pack_uint(AcceptStatus.program_mismatch)
            self.packer.pack_uint(self.vers)
            self.packer.pack_uint(self.vers)
            return self.packer.get_buf()
        proc = self.unpacker.unpack_uint()
        methname = "handle_" + repr(proc)
        try:
            meth = getattr(self, methname)
        except AttributeError:
            self.packer.pack_uint(AcceptStatus.procedure_unavailable)
            return self.packer.get_buf()
        cred = self.unpacker.unpack_auth()  # noqa
        verf = self.unpacker.unpack_auth()  # noqa
        try:
            meth()  # Unpack args, call turn_around(), pack reply
        except (EOFError, RPCGarbageArgs):
            # Too few or too many arguments
            self.packer.reset()
            self.packer.pack_uint(xid)
            self.packer.pack_uint(MessagegType.reply)
            self.packer.pack_uint(ReplyStatus.accepted)
            self.packer.pack_auth((AuthorizationFlavor.null, make_auth_null()))
            self.packer.pack_uint(AcceptStatus.garbage_args)
        return self.packer.get_buf()

    def turn_around(self):
        try:
            self.unpacker.done()
        except RuntimeError:
            raise RPCGarbageArgs
        self.packer.pack_uint(AcceptStatus.success)

    def handle_0(self):
        # Handle NULL message
        self.turn_around()

    def addpackers(self):
        # Override this to use derived classes from Packer/Unpacker
        self.packer = Packer()
        self.unpacker = Unpacker("")


class TCPServer(Server):
    def __init__(self, host, prog, vers, port):
        Server.__init__(self, host, prog, vers, port)
        self.connect()

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.prot = IPPROTO_TCP
        self.sock.bind((self.host, self.port))

    def loop(self):
        self.sock.listen(0)
        while 1:
            self.session(self.sock.accept())

    def session(self, connection):
        sock, (host, port) = connection
        while 1:
            try:
                call = _recvrecord(sock, None)
            except EOFError:
                break
            except socket.error:
                LOGGER.exception("socket error: %r", sys.exc_info()[0])
                break
            reply = self.handle(call)
            if reply is not None:
                _sendrecord(sock, reply)

    def forkingloop(self):
        # Like loop but uses forksession()
        self.sock.listen(0)
        while 1:
            self.forksession(self.sock.accept())

    def forksession(self, connection):
        # Like session but forks off a subprocess
        import os

        # Wait for deceased children
        try:
            while 1:
                pid, sts = os.waitpid(0, 1)
        except os.error:
            pass
        pid = None
        try:
            pid = os.fork()
            if pid:  # Parent
                connection[0].close()
                return
            # Child
            self.session(connection)
        finally:
            # Make sure we don't fall through in the parent
            if pid == 0:
                os._exit(0)


class UDPServer(Server):
    def __init__(self, host, prog, vers, port):
        Server.__init__(self, host, prog, vers, port)
        self.connect()

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.prot = IPPROTO_UDP
        self.sock.bind((self.host, self.port))

    def loop(self):
        while 1:
            self.session()

    def session(self):
        call, host_port = self.sock.recvfrom(8192)
        reply = self.handle(call)
        if reply is not None:
            _sendto(self.sock, reply, host_port)
