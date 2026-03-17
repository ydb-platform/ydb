#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
#
# Copyright (C) 2014, Zebra Technologies
# Authors: Matt Hooks <me@matthooks.com>
#          Zachary Lorusso <zlorusso@gmail.com>
#
# Copyright (C) 2024, LeXtudio Inc. <support@lextudio.com>
#
# License: https://www.pysnmp.com/pysnmp/license.html
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright
#   notice, this list of conditions and the following disclaimer in the
#   documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
# IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
# THE POSSIBILITY OF SUCH DAMAGE.
#
"""
DgramAsyncioProtocol is a base class for asyncio datagram transport, designed to be used with AsyncioDispatcher.

Attributes:
    SOCK_FAMILY (int): Socket family type.
    ADDRESS_TYPE (type): Type of the transport address.
    transport (asyncio.DatagramTransport | None): The asyncio transport instance.
    loop (asyncio.AbstractEventLoop): The event loop instance.

Methods:
    __init__(sock=None, sockMap=None, loop=None):
        Initializes the datagram protocol object for asyncio.

    datagram_received(datagram, transportAddress):
        Processes incoming datagram.

    connection_made(transport):
        Prepares to send datagrams.

    connection_lost(exc):
        Cleans up after connection is lost.

    open_client_mode(iface=None, allow_broadcast=False):
        Opens client mode.

    open_server_mode(iface=None, sock=None):
        Opens server mode.

    close_transport():
        Closes the transport.

    send_message(outgoingMessage, transportAddress):
        Sends a message to the transport.

    normalize_address(transportAddress):
        Returns a transport address object.
"""
import asyncio
import sys
import traceback
import warnings
from socket import socket

from pysnmp import debug
from pysnmp.carrier import error
from pysnmp.carrier.asyncio.base import AbstractAsyncioTransport
from pysnmp.carrier.base import AbstractTransportAddress


class DgramAsyncioProtocol(asyncio.DatagramProtocol, AbstractAsyncioTransport):
    """Base Asyncio datagram Transport, to be used with AsyncioDispatcher."""

    SOCK_FAMILY: int = 0
    ADDRESS_TYPE: type
    transport: "asyncio.DatagramTransport | None" = None
    loop: asyncio.AbstractEventLoop

    def __init__(
        self, sock=None, sockMap=None, loop: "asyncio.AbstractEventLoop | None" = None
    ):
        """Create a datagram protocol object for asyncio."""
        self._writeQ = []
        self._lport = None
        if loop is None:
            loop = asyncio.get_event_loop()
        self.loop = loop

    def datagram_received(self, datagram, transportAddress: AbstractTransportAddress):
        """Process incoming datagram."""
        if self._callback_function is None:
            raise error.CarrierError("Unable to call cbFun")
        else:
            self.loop.call_soon(
                self._callback_function, self, transportAddress, datagram
            )

    def connection_made(self, transport: asyncio.DatagramTransport):
        """Prepare to send datagrams."""
        self.transport = transport
        debug.logger & debug.FLAG_IO and debug.logger("connection_made: invoked")
        while self._writeQ:
            outgoingMessage, transportAddress = self._writeQ.pop(0)
            debug.logger & debug.FLAG_IO and debug.logger(
                "connection_made: transportAddress %r outgoingMessage %s"
                % (transportAddress, debug.hexdump(outgoingMessage))
            )
            try:
                self.transport.sendto(
                    outgoingMessage, self.normalize_address(transportAddress)  # type: ignore
                )
            except Exception:
                raise error.CarrierError(
                    ";".join(traceback.format_exception(*sys.exc_info()))
                )

    def connection_lost(self, exc):
        """Clean up after connection is lost."""
        debug.logger & debug.FLAG_IO and debug.logger("connection_lost: invoked")

    # AbstractAsyncioTransport API

    def open_client_mode(
        self, iface: "tuple[str, int] | None" = None, allow_broadcast: bool = False
    ):
        """Open client mode."""
        if self.loop.is_closed():
            raise error.CarrierError("Event loop is closed")

        try:
            c = self.loop.create_datagram_endpoint(
                lambda: self,
                local_addr=iface,
                family=self.SOCK_FAMILY,
                allow_broadcast=allow_broadcast,
            )
            # Avoid deprecation warning for asyncio.async()
            self._lport = asyncio.ensure_future(c)

        except Exception:
            raise error.CarrierError(
                ";".join(traceback.format_exception(*sys.exc_info()))
            )
        return self

    def open_server_mode(
        self, iface: "tuple[str, int] | None" = None, sock: "socket | None" = None
    ):
        """Open server mode."""
        if iface is None and sock is None:
            raise error.CarrierError("either iface or sock is required")

        if self.loop.is_closed():
            raise error.CarrierError("Event loop is closed")

        try:
            if sock:
                c = self.loop.create_datagram_endpoint(lambda: self, sock=sock)
            else:
                c = self.loop.create_datagram_endpoint(
                    lambda: self, local_addr=iface, family=self.SOCK_FAMILY
                )
            # Avoid deprecation warning for asyncio.async()
            self._lport = asyncio.ensure_future(c)
        except Exception:
            raise error.CarrierError(
                ";".join(traceback.format_exception(*sys.exc_info()))
            )
        return self

    def close_transport(self):
        """Close the transport."""
        if self._lport is not None:
            self._lport.cancel()
        if self.transport is not None:
            self.transport.close()
        AbstractAsyncioTransport.close_transport(self)

    def send_message(
        self,
        outgoingMessage,
        transportAddress: "AbstractTransportAddress | tuple[str, int]",
    ):
        """Send a message to the transport."""
        debug.logger & debug.FLAG_IO and debug.logger(
            "sendMessage: {} transportAddress {!r} outgoingMessage {}".format(
                (self.transport is None and "queuing" or "sending"),
                transportAddress,
                debug.hexdump(outgoingMessage),
            )
        )
        if self.transport is None:
            self._writeQ.append((outgoingMessage, transportAddress))
        else:
            try:
                self.transport.sendto(
                    outgoingMessage, self.normalize_address(transportAddress)  # type: ignore
                )
            except Exception:
                raise error.CarrierError(
                    ";".join(traceback.format_exception(*sys.exc_info()))
                )

    def normalize_address(
        self, transportAddress: "AbstractTransportAddress | tuple[str, int]"
    ):
        """Return a transport address object."""
        if not isinstance(transportAddress, self.ADDRESS_TYPE):
            transportAddress = self.ADDRESS_TYPE(transportAddress)
        return transportAddress

    # Old to new attribute mapping
    deprecated_attributes = {
        "openClientMode": "open_client_mode",
        "openServerMode": "open_server_mode",
        "closeTransport": "close_transport",
        "sendMessage": "send_message",
        "normalizeAddr": "normalize_address",
    }

    def __getattr__(self, attr: str):
        """Handle deprecated attributes."""
        if new_attr := self.deprecated_attributes.get(attr):
            warnings.warn(
                f"{attr} is deprecated. Please use {new_attr} instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            return getattr(self, new_attr)

        raise AttributeError(
            f"class '{self.__class__.__name__}' has no attribute '{attr}'"
        )
