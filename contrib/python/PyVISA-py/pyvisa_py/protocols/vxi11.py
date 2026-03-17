# -*- coding: utf-8 -*-
"""Implements a VX11 Session using Python Standard Library.

Based on Python Sun RPC Demo and Alex Forencich python-vx11

This file is an offspring of the Lantz project.

:copyright: 2014-2024 by PyVISA-py Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import enum
import socket

from . import rpc

# fmt: off
# VXI-11 RPC constants

# Device async
DEVICE_ASYNC_PROG = 0x0607B0
DEVICE_ASYNC_VERS = 1
DEVICE_ABORT      = 1

# Device core
DEVICE_CORE_PROG  = 0x0607AF
DEVICE_CORE_VERS  = 1
CREATE_LINK       = 10
DEVICE_WRITE      = 11
DEVICE_READ       = 12
DEVICE_READSTB    = 13
DEVICE_TRIGGER    = 14
DEVICE_CLEAR      = 15
DEVICE_REMOTE     = 16
DEVICE_LOCAL      = 17
DEVICE_LOCK       = 18
DEVICE_UNLOCK     = 19
DEVICE_ENABLE_SRQ = 20
DEVICE_DOCMD      = 22
DESTROY_LINK      = 23
CREATE_INTR_CHAN  = 25
DESTROY_INTR_CHAN = 26

# Device intr
DEVICE_INTR_PROG = 0x0607B1
DEVICE_INTR_VERS = 1
DEVICE_INTR_SRQ  = 30


# Error states
class ErrorCodes(enum.IntEnum):
    no_error                      = 0
    syntax_error                  = 1
    device_not_accessible         = 3
    invalid_link_identifier       = 4
    parameter_error               = 5
    channel_not_established       = 6
    operation_not_supported       = 8
    out_of_resources              = 9
    device_locked_by_another_link = 11
    no_lock_held_by_this_link     = 12
    io_timeout                    = 15
    io_error                      = 17
    abort                         = 23
    channel_already_established   = 29


# Flags
OP_FLAG_WAIT_BLOCK   = 1
OP_FLAG_END          = 8
OP_FLAG_TERMCHAR_SET = 128

RX_REQCNT = 1
RX_CHR    = 2
RX_END    = 4

# fmt: on


class Vxi11Error(Exception):
    pass


class Vxi11Packer(rpc.Packer):
    def pack_device_link(self, link):
        self.pack_int(link)

    def pack_create_link_parms(self, params):
        id, lock_device, lock_timeout, device = params
        self.pack_int(id)
        self.pack_bool(lock_device)
        self.pack_uint(lock_timeout)
        self.pack_string(device.encode("ascii"))

    def pack_device_write_parms(self, params):
        link, io_timeout, lock_timeout, flags, data = params
        self.pack_int(link)
        self.pack_uint(io_timeout)
        self.pack_uint(lock_timeout)
        self.pack_int(flags)
        self.pack_opaque(data)

    def pack_device_read_parms(self, params):
        link, request_size, io_timeout, lock_timeout, flags, term_char = params
        self.pack_int(link)
        self.pack_uint(request_size)
        self.pack_uint(io_timeout)
        self.pack_uint(lock_timeout)
        self.pack_int(flags)
        self.pack_int(term_char)

    def pack_device_generic_parms(self, params):
        link, flags, lock_timeout, io_timeout = params
        self.pack_int(link)
        self.pack_int(flags)
        self.pack_uint(lock_timeout)
        self.pack_uint(io_timeout)

    def pack_device_remote_func_parms(self, params):
        host_addr, host_port, prog_num, prog_vers, prog_family = params
        self.pack_uint(host_addr)
        self.pack_uint(host_port)
        self.pack_uint(prog_num)
        self.pack_uint(prog_vers)
        self.pack_int(prog_family)

    def pack_device_enable_srq_parms(self, params):
        link, enable, handle = params
        self.pack_int(link)
        self.pack_bool(enable)
        if len(handle) > 40:
            raise Vxi11Error("array length too long")
        self.pack_opaque(handle)

    def pack_device_lock_parms(self, params):
        link, flags, lock_timeout = params
        self.pack_int(link)
        self.pack_int(flags)
        self.pack_uint(lock_timeout)

    def pack_device_docmd_parms(self, params):
        (
            link,
            flags,
            io_timeout,
            lock_timeout,
            cmd,
            network_order,
            datasize,
            data_in,
        ) = params
        self.pack_int(link)
        self.pack_int(flags)
        self.pack_uint(io_timeout)
        self.pack_uint(lock_timeout)
        self.pack_int(cmd)
        self.pack_bool(network_order)
        self.pack_int(datasize)
        self.pack_opaque(data_in)


class Vxi11Unpacker(rpc.Unpacker):
    def unpack_device_link(self):
        return self.unpack_int()

    def unpack_device_error(self):
        return self.unpack_int()

    def unpack_create_link_resp(self):
        error = self.unpack_int()
        link = self.unpack_int()
        abort_port = self.unpack_uint()
        max_recv_size = self.unpack_uint()
        return error, link, abort_port, max_recv_size

    def unpack_device_write_resp(self):
        error = self.unpack_int()
        size = self.unpack_uint()
        return error, size

    def unpack_device_read_resp(self):
        error = self.unpack_int()
        reason = self.unpack_int()
        data = self.unpack_opaque()
        return error, reason, data

    def unpack_device_read_stb_resp(self):
        error = self.unpack_int()
        stb = self.unpack_uint()
        return error, stb

    def unpack_device_docmd_resp(self):
        error = self.unpack_int()
        data_out = self.unpack_opaque()
        return error, data_out


class CoreClient(rpc.TCPClient):
    def __init__(self, host, open_timeout=5000):
        self.packer = Vxi11Packer()
        self.unpacker = Vxi11Unpacker("")
        super(CoreClient, self).__init__(
            host, DEVICE_CORE_PROG, DEVICE_CORE_VERS, open_timeout
        )

    def create_link(self, id, lock_device, lock_timeout, name):
        params = (id, lock_device, lock_timeout, name)
        try:
            return self.make_call(
                CREATE_LINK,
                params,
                self.packer.pack_create_link_parms,
                self.unpacker.unpack_create_link_resp,
            )
        except socket.timeout:
            return ErrorCodes.device_not_accessible, None, None, None

    def device_write(self, link, io_timeout, lock_timeout, flags, data):
        params = (link, io_timeout, lock_timeout, flags, data)
        try:
            return self.make_call(
                DEVICE_WRITE,
                params,
                self.packer.pack_device_write_parms,
                self.unpacker.unpack_device_write_resp,
            )
        except socket.timeout as e:
            return ErrorCodes.io_error, e.args[0]

    def device_read(
        self, link, request_size, io_timeout, lock_timeout, flags, term_char
    ):
        params = (link, request_size, io_timeout, lock_timeout, flags, term_char)
        try:
            return self.make_call(
                DEVICE_READ,
                params,
                self.packer.pack_device_read_parms,
                self.unpacker.unpack_device_read_resp,
            )
        except socket.timeout as e:
            return ErrorCodes.io_error, e.args[0], ""

    def device_read_stb(self, link, flags, lock_timeout, io_timeout):
        params = (link, flags, lock_timeout, io_timeout)
        return self.make_call(
            DEVICE_READSTB,
            params,
            self.packer.pack_device_generic_parms,
            self.unpacker.unpack_device_read_stb_resp,
        )

    def device_trigger(self, link, flags, lock_timeout, io_timeout):
        params = (link, flags, lock_timeout, io_timeout)
        return self.make_call(
            DEVICE_TRIGGER,
            params,
            self.packer.pack_device_generic_parms,
            self.unpacker.unpack_device_error,
        )

    def device_clear(self, link, flags, lock_timeout, io_timeout):
        params = (link, flags, lock_timeout, io_timeout)
        return self.make_call(
            DEVICE_CLEAR,
            params,
            self.packer.pack_device_generic_parms,
            self.unpacker.unpack_device_error,
        )

    def device_remote(self, link, flags, lock_timeout, io_timeout):
        params = (link, flags, lock_timeout, io_timeout)
        return self.make_call(
            DEVICE_REMOTE,
            params,
            self.packer.pack_device_generic_parms,
            self.unpacker.unpack_device_error,
        )

    def device_local(self, link, flags, lock_timeout, io_timeout):
        params = (link, flags, lock_timeout, io_timeout)
        return self.make_call(
            DEVICE_LOCAL,
            params,
            self.packer.pack_device_generic_parms,
            self.unpacker.unpack_device_error,
        )

    def device_lock(self, link, flags, lock_timeout):
        params = (link, flags, lock_timeout)
        return self.make_call(
            DEVICE_LOCK,
            params,
            self.packer.pack_device_lock_parms,
            self.unpacker.unpack_device_error,
        )

    def device_unlock(self, link):
        return self.make_call(
            DEVICE_UNLOCK,
            link,
            self.packer.pack_device_link,
            self.unpacker.unpack_device_error,
        )

    def device_enable_srq(self, link, enable, handle):
        params = (link, enable, handle)
        return self.make_call(
            DEVICE_ENABLE_SRQ,
            params,
            self.packer.pack_device_enable_srq_parms,
            self.unpacker.unpack_device_error,
        )

    def device_docmd(
        self,
        link,
        flags,
        io_timeout,
        lock_timeout,
        cmd,
        network_order,
        datasize,
        data_in,
    ):
        params = (
            link,
            flags,
            io_timeout,
            lock_timeout,
            cmd,
            network_order,
            datasize,
            data_in,
        )
        return self.make_call(
            DEVICE_DOCMD,
            params,
            self.packer.pack_device_docmd_parms,
            self.unpacker.unpack_device_docmd_resp,
        )

    def destroy_link(self, link):
        return self.make_call(
            DESTROY_LINK,
            link,
            self.packer.pack_device_link,
            self.unpacker.unpack_device_error,
        )

    def create_intr_chan(self, host_addr, host_port, prog_num, prog_vers, prog_family):
        params = (host_addr, host_port, prog_num, prog_vers, prog_family)
        return self.make_call(
            CREATE_INTR_CHAN,
            params,
            self.packer.pack_device_docmd_parms,
            self.unpacker.unpack_device_error,
        )

    def destroy_intr_chan(self):
        return self.make_call(
            DESTROY_INTR_CHAN, None, None, self.unpacker.unpack_device_error
        )
