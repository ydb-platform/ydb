# $Id: rpc.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Remote Procedure Call."""
from __future__ import absolute_import

import struct

from . import dpkt

# RPC.dir
CALL = 0
REPLY = 1

# RPC.Auth.flavor
AUTH_NONE = AUTH_NULL = 0
AUTH_UNIX = 1
AUTH_SHORT = 2
AUTH_DES = 3

# RPC.Reply.stat
MSG_ACCEPTED = 0
MSG_DENIED = 1

# RPC.Reply.Accept.stat
SUCCESS = 0
PROG_UNAVAIL = 1
PROG_MISMATCH = 2
PROC_UNAVAIL = 3
GARBAGE_ARGS = 4
SYSTEM_ERR = 5

# RPC.Reply.Reject.stat
RPC_MISMATCH = 0
AUTH_ERROR = 1


class RPC(dpkt.Packet):
    """Remote Procedure Call.

    RFC 5531: https://tools.ietf.org/html/rfc5531

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of RPC.
        TODO.
    """

    __hdr__ = (
        ('xid', 'I', 0),
        ('dir', 'I', CALL)
    )

    class Auth(dpkt.Packet):
        __hdr__ = (('flavor', 'I', AUTH_NONE), )

        def unpack(self, buf):
            dpkt.Packet.unpack(self, buf)
            n = struct.unpack('>I', self.data[:4])[0]
            self.data = self.data[4:4 + n]

        def __len__(self):
            return 8 + len(self.data)

        def __bytes__(self):
            return self.pack_hdr() + struct.pack('>I', len(self.data)) + bytes(self.data)

    class Call(dpkt.Packet):
        __hdr__ = (
            ('rpcvers', 'I', 2),
            ('prog', 'I', 0),
            ('vers', 'I', 0),
            ('proc', 'I', 0)
        )

        def unpack(self, buf):
            dpkt.Packet.unpack(self, buf)
            self.cred = RPC.Auth(self.data)
            self.verf = RPC.Auth(self.data[len(self.cred):])
            self.data = self.data[len(self.cred) + len(self.verf):]

        def __len__(self):
            return len(bytes(self))  # XXX

        def __bytes__(self):
            return dpkt.Packet.__bytes__(self) + \
                bytes(getattr(self, 'cred', RPC.Auth())) + \
                bytes(getattr(self, 'verf', RPC.Auth())) + \
                bytes(self.data)

    class Reply(dpkt.Packet):
        __hdr__ = (('stat', 'I', MSG_ACCEPTED), )

        class Accept(dpkt.Packet):
            __hdr__ = (('stat', 'I', SUCCESS), )

            def unpack(self, buf):
                self.verf = RPC.Auth(buf)
                buf = buf[len(self.verf):]
                self.stat = struct.unpack('>I', buf[:4])[0]
                if self.stat == SUCCESS:
                    self.data = buf[4:]
                elif self.stat == PROG_MISMATCH:
                    self.low, self.high = struct.unpack('>II', buf[4:12])
                    self.data = buf[12:]

            def __len__(self):
                if self.stat == PROG_MISMATCH:
                    n = 8
                else:
                    n = 0
                return len(self.verf) + 4 + n + len(self.data)

            def __bytes__(self):
                if self.stat == PROG_MISMATCH:
                    return bytes(self.verf) + \
                        struct.pack('>III', self.stat, self.low, self.high) + self.data
                return bytes(self.verf) + dpkt.Packet.__bytes__(self)

        class Reject(dpkt.Packet):
            __hdr__ = (('stat', 'I', AUTH_ERROR), )

            def unpack(self, buf):
                dpkt.Packet.unpack(self, buf)
                if self.stat == RPC_MISMATCH:
                    self.low, self.high = struct.unpack('>II', self.data[:8])
                    self.data = self.data[8:]
                elif self.stat == AUTH_ERROR:
                    self.why = struct.unpack('>I', self.data[:4])[0]
                    self.data = self.data[4:]

            def __len__(self):
                if self.stat == RPC_MISMATCH:
                    n = 8
                elif self.stat == AUTH_ERROR:
                    n = 4
                else:
                    n = 0
                return 4 + n + len(self.data)

            def __bytes__(self):
                if self.stat == RPC_MISMATCH:
                    return struct.pack('>III', self.stat, self.low, self.high) + self.data
                elif self.stat == AUTH_ERROR:
                    return struct.pack('>II', self.stat, self.why) + self.data
                return dpkt.Packet.__bytes__(self)

        def unpack(self, buf):
            dpkt.Packet.unpack(self, buf)
            if self.stat == MSG_ACCEPTED:
                self.data = self.accept = self.Accept(self.data)
            elif self.stat == MSG_DENIED:
                self.data = self.reject = self.Reject(self.data)

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        if self.dir == CALL:
            self.data = self.call = self.Call(self.data)
        elif self.dir == REPLY:
            self.data = self.reply = self.Reply(self.data)


def unpack_xdrlist(cls, buf):
    l_ = []
    while buf:
        if buf.startswith(b'\x00\x00\x00\x01'):
            p = cls(buf[4:])
            l_.append(p)
            buf = p.data
        elif buf.startswith(b'\x00\x00\x00\x00'):
            break
        else:
            raise dpkt.UnpackError('invalid XDR list')
    return l_


def pack_xdrlist(*args):
    return b'\x00\x00\x00\x01'.join(map(bytes, args)) + b'\x00\x00\x00\x00'


def test_auth():
    from binascii import unhexlify
    auth1 = RPC.Auth()
    assert auth1.flavor == AUTH_NONE
    buf = unhexlify('0000000000000000')
    assert bytes(auth1) == buf

    auth2 = RPC.Auth(buf)
    assert auth2.flavor == AUTH_NONE

    assert len(auth2) == 8


def test_call():
    from binascii import unhexlify
    call1 = RPC.Call()
    assert call1.rpcvers == 2
    assert call1.prog == 0
    assert call1.vers == 0
    assert call1.proc == 0

    buf = unhexlify(
        '0000000200000000000000000000000000000000000000000000000000000000'
    )
    assert bytes(call1) == buf

    call2 = RPC.Call(buf)
    assert call2.rpcvers == 2
    assert call2.prog == 0
    assert call2.vers == 0
    assert call2.proc == 0

    assert len(call2) == 32
    assert bytes(call2) == buf


def test_reply():
    from binascii import unhexlify
    reply1 = RPC.Reply()
    assert reply1.stat == MSG_ACCEPTED
    assert bytes(reply1) == b'\00' * 4

    buf_accepted = unhexlify(
        '00000000'          # MSG_ACCEPTED
        '0000000000000000'  # Auth
        '00000000'          # SUCCESS
        '0000000000000000'  # Auth
    )

    reply_accepted = RPC.Reply(buf_accepted)
    assert reply_accepted.stat == MSG_ACCEPTED
    assert bytes(reply_accepted) == buf_accepted
    assert len(reply_accepted) == 24

    buf_denied = unhexlify(
        '00000001'          # MSG_DENIED
        '00000000'          # RPC_MISMATCH
        '00000000'          # low
        'FFFFFFFF'          # high
        '0000000000000000'  # Auth
    )
    reply_denied = RPC.Reply(buf_denied)
    assert reply_denied.stat == MSG_DENIED
    assert bytes(reply_denied) == buf_denied
    assert len(reply_denied) == 24


def test_accept():
    from binascii import unhexlify
    accept1 = RPC.Reply.Accept()
    assert accept1.stat == SUCCESS

    buf_success = unhexlify(
        '0000000000000000'  # Auth
        '00000000'          # SUCCESS
        '0000000000000000'  # Auth
    )
    accept_success = RPC.Reply.Accept(buf_success)
    assert accept_success.stat == SUCCESS
    assert len(accept_success) == 20
    assert bytes(accept_success) == buf_success

    buf_prog_mismatch = unhexlify(
        '0000000000000000'  # Auth
        '00000002'          # PROG_MISMATCH
        '0000000000000000'  # Auth
    )
    accept_prog_mismatch = RPC.Reply.Accept(buf_prog_mismatch)
    assert accept_prog_mismatch.stat == PROG_MISMATCH
    assert len(accept_prog_mismatch) == 20
    assert bytes(accept_prog_mismatch) == buf_prog_mismatch


def test_reject():
    from binascii import unhexlify
    reject1 = RPC.Reply.Reject()
    assert reject1.stat == AUTH_ERROR

    buf_rpc_mismatch = unhexlify(
        '00000000'          # RPC_MISMATCH
        '00000000'          # low
        'FFFFFFFF'          # high
        '0000000000000000'  # Auth
    )
    reject2 = RPC.Reply.Reject(buf_rpc_mismatch)
    assert bytes(reject2) == buf_rpc_mismatch
    assert reject2.low == 0
    assert reject2.high == 0xffffffff
    assert len(reject2) == 20

    buf_auth_error = unhexlify(
        '00000001'          # AUTH_ERROR
        '00000000'          # low
        'FFFFFFFF'          # high
        '0000000000000000'  # Auth
    )
    reject3 = RPC.Reply.Reject(buf_auth_error)
    assert bytes(reject3) == buf_auth_error
    assert len(reject3) == 20

    buf_other = unhexlify(
        '00000002'          # NOT IMPLEMENTED
        '00000000'          # low
        'FFFFFFFF'          # high
        '0000000000000000'  # Auth
    )
    reject4 = RPC.Reply.Reject(buf_other)
    assert bytes(reject4) == buf_other
    assert len(reject4) == 20


def test_rpc():
    from binascii import unhexlify
    rpc = RPC()
    assert rpc.xid == 0
    assert rpc.dir == CALL

    buf_call = unhexlify(
        '00000000'  # xid
        '00000000'  # CALL

        '0000000200000000000000000000000000000000000000000000000000000000'
    )
    rpc_call = RPC(buf_call)
    assert bytes(rpc_call) == buf_call

    buf_reply = unhexlify(
        '00000000'  # xid
        '00000001'  # REPLY

        '00000000'          # MSG_ACCEPTED
        '0000000000000000'  # Auth
        '00000000'          # SUCCESS
        '0000000000000000'  # Auth
    )
    rpc_reply = RPC(buf_reply)
    assert bytes(rpc_reply) == buf_reply
