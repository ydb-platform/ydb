# Copyright (c) 2020 Yubico AB
# All rights reserved.
#
#   Redistribution and use in source and binary forms, with or
#   without modification, are permitted provided that the following
#   conditions are met:
#
#    1. Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#    2. Redistributions in binary form must reproduce the above
#       copyright notice, this list of conditions and the following
#       disclaimer in the documentation and/or other materials provided
#       with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

from __future__ import annotations

from .base import HidDescriptor
from ..ctap import CtapDevice, CtapError, STATUS
from ..utils import LOG_LEVEL_TRAFFIC
from threading import Event
from enum import IntEnum, IntFlag, unique
from typing import Tuple, Optional, Callable, Iterator
import struct
import sys
import os
import logging

logger = logging.getLogger(__name__)


if sys.platform.startswith("linux"):
    from . import linux as backend
elif sys.platform.startswith("win32"):
    from . import windows as backend
elif sys.platform.startswith("darwin"):
    from . import macos as backend
elif sys.platform.startswith("freebsd"):
    from . import freebsd as backend
elif sys.platform.startswith("netbsd"):
    from . import netbsd as backend
elif sys.platform.startswith("openbsd"):
    from . import openbsd as backend
else:
    raise Exception("Unsupported platform")


list_descriptors = backend.list_descriptors
get_descriptor = backend.get_descriptor
open_connection = backend.open_connection


@unique
class CTAPHID(IntEnum):
    PING = 0x01
    MSG = 0x03
    LOCK = 0x04
    INIT = 0x06
    WINK = 0x08
    CBOR = 0x10
    CANCEL = 0x11

    ERROR = 0x3F
    KEEPALIVE = 0x3B

    VENDOR_FIRST = 0x40


@unique
class CAPABILITY(IntFlag):
    WINK = 0x01
    LOCK = 0x02  # Not used
    CBOR = 0x04
    NMSG = 0x08

    def supported(self, flags: CAPABILITY) -> bool:
        return bool(flags & self)


TYPE_INIT = 0x80


class CtapHidDevice(CtapDevice):
    """
    CtapDevice implementation using the HID transport.

    :cvar descriptor: Device descriptor.
    """

    def __init__(self, descriptor: HidDescriptor, connection):
        self.descriptor = descriptor
        self._packet_size = descriptor.report_size_out
        self._connection = connection

        nonce = os.urandom(8)
        self._channel_id = 0xFFFFFFFF
        response = self.call(CTAPHID.INIT, nonce)
        r_nonce, response = response[:8], response[8:]
        if r_nonce != nonce:
            raise Exception("Wrong nonce")
        (
            self._channel_id,
            self._u2fhid_version,
            v1,
            v2,
            v3,
            self._capabilities,
        ) = struct.unpack_from(">IBBBBB", response)
        self._device_version = (v1, v2, v3)

    def __repr__(self):
        return f"CtapHidDevice({self.descriptor.path!r})"

    @property
    def version(self) -> int:
        """CTAP HID protocol version."""
        return self._u2fhid_version

    @property
    def device_version(self) -> Tuple[int, int, int]:
        """Device version number."""
        return self._device_version

    @property
    def capabilities(self) -> int:
        """Capabilities supported by the device."""
        return self._capabilities

    @property
    def product_name(self) -> Optional[str]:
        """Product name of device."""
        return self.descriptor.product_name

    @property
    def serial_number(self) -> Optional[str]:
        """Serial number of device."""
        return self.descriptor.serial_number

    def _send_cancel(self):
        packet = struct.pack(">IB", self._channel_id, TYPE_INIT | CTAPHID.CANCEL).ljust(
            self._packet_size, b"\0"
        )
        logger.log(LOG_LEVEL_TRAFFIC, "SEND: %s", packet.hex())
        self._connection.write_packet(packet)

    def call(
        self,
        cmd: int,
        data: bytes = b"",
        event: Optional[Event] = None,
        on_keepalive: Optional[Callable[[int], None]] = None,
    ) -> bytes:
        event = event or Event()
        remaining = data
        seq = 0

        # Send request
        header = struct.pack(">IBH", self._channel_id, TYPE_INIT | cmd, len(remaining))
        while remaining or seq == 0:
            size = min(len(remaining), self._packet_size - len(header))
            body, remaining = remaining[:size], remaining[size:]
            packet = header + body
            logger.log(LOG_LEVEL_TRAFFIC, "SEND: %s", packet.hex())
            self._connection.write_packet(packet.ljust(self._packet_size, b"\0"))
            header = struct.pack(">IB", self._channel_id, 0x7F & seq)
            seq += 1

        try:
            # Read response
            seq = 0
            response = b""
            last_ka = None
            while True:
                if event.is_set():
                    # Cancel
                    logger.debug("Sending cancel...")
                    self._send_cancel()

                recv = self._connection.read_packet()
                logger.log(LOG_LEVEL_TRAFFIC, "RECV: %s", recv.hex())

                r_channel = struct.unpack_from(">I", recv)[0]
                recv = recv[4:]
                if r_channel != self._channel_id:
                    raise Exception("Wrong channel")

                if not response:  # Initialization packet
                    r_cmd, r_len = struct.unpack_from(">BH", recv)
                    recv = recv[3:]
                    if r_cmd == TYPE_INIT | cmd:
                        pass  # first data packet
                    elif r_cmd == TYPE_INIT | CTAPHID.KEEPALIVE:
                        ka_status = struct.unpack_from(">B", recv)[0]
                        logger.debug(f"Got keepalive status: {ka_status:02x}")
                        if on_keepalive and ka_status != last_ka:
                            try:
                                ka_status = STATUS(ka_status)
                            except ValueError:
                                pass  # Unknown status value
                            last_ka = ka_status
                            on_keepalive(ka_status)
                        continue
                    elif r_cmd == TYPE_INIT | CTAPHID.ERROR:
                        raise CtapError(struct.unpack_from(">B", recv)[0])
                    else:
                        raise CtapError(CtapError.ERR.INVALID_COMMAND)
                else:  # Continuation packet
                    r_seq = struct.unpack_from(">B", recv)[0]
                    recv = recv[1:]
                    if r_seq != seq:
                        raise Exception("Wrong sequence number")
                    seq += 1

                response += recv
                if len(response) >= r_len:
                    break

            return response[:r_len]
        except KeyboardInterrupt:
            logger.debug("Keyboard interrupt, cancelling...")
            self._send_cancel()

            raise

    def wink(self) -> None:
        """Causes the authenticator to blink."""
        self.call(CTAPHID.WINK)

    def ping(self, msg: bytes = b"Hello FIDO") -> bytes:
        """Sends data to the authenticator, which echoes it back.

        :param msg: The data to send.
        :return: The response from the authenticator.
        """
        return self.call(CTAPHID.PING, msg)

    def lock(self, lock_time: int = 10) -> None:
        """Locks the channel."""
        self.call(CTAPHID.LOCK, struct.pack(">B", lock_time))

    def close(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None

    @classmethod
    def list_devices(cls) -> Iterator[CtapHidDevice]:
        for d in list_descriptors():
            yield cls(d, open_connection(d))


def list_devices() -> Iterator[CtapHidDevice]:
    return CtapHidDevice.list_devices()


def open_device(path) -> CtapHidDevice:
    descriptor = get_descriptor(path)
    return CtapHidDevice(descriptor, open_connection(descriptor))
