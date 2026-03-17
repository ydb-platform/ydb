# Copyright (c) 2019 Yubico AB
# Copyright (c) 2019 Oleg Moiseenko
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

from .ctap import CtapDevice, CtapError, STATUS
from .hid import CAPABILITY, CTAPHID
from .utils import LOG_LEVEL_TRAFFIC
from smartcard import System
from smartcard.CardConnection import CardConnection
from smartcard.pcsc.PCSCExceptions import ListReadersException
from smartcard.pcsc.PCSCContext import PCSCContext

from threading import Event
from typing import Tuple, Optional, Callable, Iterator
import struct
import logging


AID_FIDO = b"\xa0\x00\x00\x06\x47\x2f\x00\x01"
SW_SUCCESS = (0x90, 0x00)
SW_UPDATE = (0x91, 0x00)
SW1_MORE_DATA = 0x61


logger = logging.getLogger(__name__)


class CtapPcscDevice(CtapDevice):
    """
    CtapDevice implementation using pyscard (PCSC).

    This class is intended for use with NFC readers.
    """

    def __init__(self, connection: CardConnection, name: str):
        self._capabilities = CAPABILITY(0)
        self.use_ext_apdu = False
        self._conn = connection
        self._conn.connect()
        self._name = name
        self._select()

        try:  # Probe for CTAP2 by calling GET_INFO
            self.call(CTAPHID.CBOR, b"\x04")
            self._capabilities |= CAPABILITY.CBOR
        except CtapError:
            if not self._capabilities:
                raise ValueError("Unsupported device")

    def __repr__(self):
        return f"CtapPcscDevice({self._name})"

    @property
    def version(self) -> int:
        """CTAPHID protocol version."""
        return 2 if CAPABILITY.CBOR in self._capabilities else 1

    @property
    def capabilities(self) -> CAPABILITY:
        """Capabilities supported by the device."""
        return self._capabilities

    @property
    def product_name(self) -> Optional[str]:
        """Product name of device."""
        return None

    @property
    def serial_number(self) -> Optional[int]:
        """Serial number of device."""
        return None

    def get_atr(self) -> bytes:
        """Get the ATR/ATS of the connected card."""
        return bytes(self._conn.getATR())

    def apdu_exchange(
        self, apdu: bytes, protocol: Optional[int] = None
    ) -> Tuple[bytes, int, int]:
        """Exchange data with smart card.

        :param apdu: byte string. data to exchange with card
        :return: byte string. response from card
        """

        logger.log(LOG_LEVEL_TRAFFIC, "SEND: %s", apdu.hex())
        resp, sw1, sw2 = self._conn.transmit(list(apdu), protocol)
        response = bytes(resp)
        logger.log(
            LOG_LEVEL_TRAFFIC, "RECV: %s SW=%04X", response.hex(), sw1 << 8 + sw2
        )

        return response, sw1, sw2

    def control_exchange(self, control_code: int, control_data: bytes = b"") -> bytes:
        """Sends control sequence to reader's driver.

        :param control_code: int. code to send to reader driver.
        :param control_data: byte string. data to send to driver
        :return: byte string. response
        """

        logger.log(LOG_LEVEL_TRAFFIC, "Send control: %s", control_data.hex())
        response = self._conn.control(control_code, list(control_data))
        response = bytes(response)
        logger.log(LOG_LEVEL_TRAFFIC, "Control response: %s", response.hex())

        return response

    def _select(self) -> None:
        apdu = b"\x00\xa4\x04\x00" + struct.pack("!B", len(AID_FIDO)) + AID_FIDO
        resp, sw1, sw2 = self.apdu_exchange(apdu)
        if (sw1, sw2) != SW_SUCCESS:
            raise ValueError("FIDO applet selection failure.")
        if resp == b"U2F_V2":
            self._capabilities |= CAPABILITY.NMSG

    def _chain_apdus(
        self, cla: int, ins: int, p1: int, p2: int, data: bytes = b""
    ) -> Tuple[bytes, int, int]:
        if self.use_ext_apdu:
            header = struct.pack("!BBBBBH", cla, ins, p1, p2, 0x00, len(data))
            resp, sw1, sw2 = self.apdu_exchange(header + data)
            return resp, sw1, sw2
        else:
            while len(data) > 250:
                to_send, data = data[:250], data[250:]
                header = struct.pack("!BBBBB", 0x10 | cla, ins, p1, p2, len(to_send))
                resp, sw1, sw2 = self.apdu_exchange(header + to_send)
                if (sw1, sw2) != SW_SUCCESS:
                    return resp, sw1, sw2
            apdu = struct.pack("!BBBB", cla, ins, p1, p2)
            if data:
                apdu += struct.pack("!B", len(data)) + data
            resp, sw1, sw2 = self.apdu_exchange(apdu + b"\x00")
            while sw1 == SW1_MORE_DATA:
                apdu = b"\x00\xc0\x00\x00" + struct.pack("!B", sw2)  # sw2 == le
                lres, sw1, sw2 = self.apdu_exchange(apdu)
                resp += lres
            return resp, sw1, sw2

    def _call_apdu(self, apdu: bytes) -> bytes:
        if len(apdu) >= 7 and apdu[4] == 0:
            # Extended APDU
            data_len = struct.unpack("!H", apdu[5:7])[0]
            data = apdu[7 : 7 + data_len]
        elif len(apdu) == 4:
            data = b""
        else:
            # Short APDU
            data_len = apdu[4]
            data = apdu[5 : 5 + data_len]
        (cla, ins, p1, p2) = apdu[:4]

        resp, sw1, sw2 = self._chain_apdus(cla, ins, p1, p2, data)
        return resp + struct.pack("!BB", sw1, sw2)

    def _call_cbor(
        self,
        data: bytes = b"",
        event: Optional[Event] = None,
        on_keepalive: Optional[Callable[[int], None]] = None,
    ) -> bytes:
        event = event or Event()
        # NFCCTAP_MSG
        resp, sw1, sw2 = self._chain_apdus(0x80, 0x10, 0x80, 0x00, data)
        last_ka = None

        while not event.is_set():
            while (sw1, sw2) == SW_UPDATE:
                ka_status = resp[0]
                if on_keepalive and last_ka != ka_status:
                    try:
                        ka_status = STATUS(ka_status)
                    except ValueError:
                        pass  # Unknown status value
                    last_ka = ka_status
                    on_keepalive(ka_status)

                # NFCCTAP_GETRESPONSE
                resp, sw1, sw2 = self._chain_apdus(0x80, 0x11, 0x00, 0x00)

            if (sw1, sw2) != SW_SUCCESS:
                raise CtapError(CtapError.ERR.OTHER)  # TODO: Map from SW error

            return resp

        raise CtapError(CtapError.ERR.KEEPALIVE_CANCEL)

    def call(
        self,
        cmd: int,
        data: bytes = b"",
        event: Optional[Event] = None,
        on_keepalive: Optional[Callable[[int], None]] = None,
    ) -> bytes:
        if cmd == CTAPHID.CBOR:
            return self._call_cbor(data, event, on_keepalive)
        elif cmd == CTAPHID.MSG:
            return self._call_apdu(data)
        else:
            raise CtapError(CtapError.ERR.INVALID_COMMAND)

    def close(self) -> None:
        self._conn.disconnect()

    @classmethod
    def list_devices(cls, name: str = "") -> Iterator[CtapPcscDevice]:
        for reader in _list_readers():
            if name in reader.name:
                try:
                    yield cls(reader.createConnection(), reader.name)
                except Exception as e:
                    logger.debug("Error %r", e)


def _list_readers():
    try:
        return System.readers()
    except ListReadersException:
        # If the PCSC system has restarted the context might be stale, try
        # forcing a new context (This happens on Windows if the last reader is
        # removed):
        PCSCContext.instance = None
        return System.readers()
