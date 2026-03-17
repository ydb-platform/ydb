# Copyright (c) 2013 Yubico AB
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

from .hid import CTAPHID
from .ctap import CtapDevice
from .utils import websafe_encode, websafe_decode, bytes2int, ByteBuffer
from .cose import ES256
from .attestation import FidoU2FAttestation
from enum import IntEnum, unique
from dataclasses import dataclass
import struct


@unique
class APDU(IntEnum):
    """APDU response codes."""

    OK = 0x9000
    USE_NOT_SATISFIED = 0x6985
    WRONG_DATA = 0x6A80


class ApduError(Exception):
    """An Exception thrown when a response APDU doesn't have an OK (0x9000)
    status.

    :param code: APDU response code.
    :param data: APDU response body.

    """

    def __init__(self, code: int, data: bytes = b""):
        self.code = code
        self.data = data

    def __repr__(self):
        return f"APDU error: 0x{self.code:04X} {len(self.data):d} bytes of data"


@dataclass(init=False)
class RegistrationData(bytes):
    """Binary response data for a CTAP1 registration.

    :param _: The binary contents of the response data.
    :ivar public_key: Binary representation of the credential public key.
    :ivar key_handle: Binary key handle of the credential.
    :ivar certificate: Attestation certificate of the authenticator, DER
        encoded.
    :ivar signature: Attestation signature.
    """

    public_key: bytes
    key_handle: bytes
    certificate: bytes
    signature: bytes

    def __init__(self, _):
        super().__init__()

        reader = ByteBuffer(self)
        if reader.unpack("B") != 0x05:
            raise ValueError("Reserved byte != 0x05")

        self.public_key = reader.read(65)
        self.key_handle = reader.read(reader.unpack("B"))

        cert_buf = reader.read(2)  # Tag and first length byte
        cert_len = cert_buf[1]
        if cert_len > 0x80:  # Multi-byte length
            n_bytes = cert_len - 0x80
            len_bytes = reader.read(n_bytes)
            cert_buf += len_bytes
            cert_len = bytes2int(len_bytes)
        self.certificate = cert_buf + reader.read(cert_len)
        self.signature = reader.read()

    @property
    def b64(self) -> str:
        """Websafe base64 encoded string of the RegistrationData."""
        return websafe_encode(self)

    def verify(self, app_param: bytes, client_param: bytes) -> None:
        """Verify the included signature with regard to the given app and client
        params.

        :param app_param: SHA256 hash of the app ID used for the request.
        :param client_param: SHA256 hash of the ClientData used for the request.
        """
        FidoU2FAttestation.verify_signature(
            app_param,
            client_param,
            self.key_handle,
            self.public_key,
            self.certificate,
            self.signature,
        )

    @classmethod
    def from_b64(cls, data: str) -> RegistrationData:
        """Parse a RegistrationData from a websafe base64 encoded string.

        :param data: Websafe base64 encoded string.
        :return: The decoded and parsed RegistrationData.
        """
        return cls(websafe_decode(data))


@dataclass(init=False)
class SignatureData(bytes):
    """Binary response data for a CTAP1 authentication.

    :param _: The binary contents of the response data.
    :ivar user_presence: User presence byte.
    :ivar counter: Signature counter.
    :ivar signature: Cryptographic signature.
    """

    user_presence: int
    counter: int
    signature: bytes

    def __init__(self, _):
        super().__init__()

        reader = ByteBuffer(self)
        self.user_presence = reader.unpack("B")
        self.counter = reader.unpack(">I")
        self.signature = reader.read()

    @property
    def b64(self) -> str:
        """str: Websafe base64 encoded string of the SignatureData."""
        return websafe_encode(self)

    def verify(self, app_param: bytes, client_param: bytes, public_key: bytes) -> None:
        """Verify the included signature with regard to the given app and client
        params, using the given public key.

        :param app_param: SHA256 hash of the app ID used for the request.
        :param client_param: SHA256 hash of the ClientData used for the request.
        :param public_key: Binary representation of the credential public key.
        """
        m = app_param + self[:5] + client_param
        ES256.from_ctap1(public_key).verify(m, self.signature)

    @classmethod
    def from_b64(cls, data: str) -> SignatureData:
        """Parse a SignatureData from a websafe base64 encoded string.

        :param data: Websafe base64 encoded string.
        :return: The decoded and parsed SignatureData.
        """
        return cls(websafe_decode(data))


class Ctap1:
    """Implementation of the CTAP1 specification.

    :param device: A CtapHidDevice handle supporting CTAP1.
    """

    @unique
    class INS(IntEnum):
        REGISTER = 0x01
        AUTHENTICATE = 0x02
        VERSION = 0x03

    def __init__(self, device: CtapDevice):
        self.device = device

    def send_apdu(
        self, cla: int = 0, ins: int = 0, p1: int = 0, p2: int = 0, data: bytes = b""
    ) -> bytes:
        """Packs and sends an APDU for use in CTAP1 commands.
        This is a low-level method mainly used internally. Avoid calling it
        directly if possible, and use the get_version, register, and
        authenticate methods if possible instead.

        :param cla: The CLA parameter of the request.
        :param ins: The INS parameter of the request.
        :param p1: The P1 parameter of the request.
        :param p2: The P2 parameter of the request.
        :param data: The body of the request.
        :return: The response APDU data of a successful request.
        :raise: ApduError
        """
        apdu = struct.pack(">BBBBBH", cla, ins, p1, p2, 0, len(data)) + data + b"\0\0"

        response = self.device.call(CTAPHID.MSG, apdu)
        status = struct.unpack(">H", response[-2:])[0]
        data = response[:-2]
        if status != APDU.OK:
            raise ApduError(status, data)
        return data

    def get_version(self) -> str:
        """Get the U2F version implemented by the authenticator.
        The only version specified is "U2F_V2".

        :return: A U2F version string.
        """
        return self.send_apdu(ins=Ctap1.INS.VERSION).decode()

    def register(self, client_param: bytes, app_param: bytes) -> RegistrationData:
        """Register a new U2F credential.

        :param client_param: SHA256 hash of the ClientData used for the request.
        :param app_param: SHA256 hash of the app ID used for the request.
        :return: The registration response from the authenticator.
        """
        data = client_param + app_param
        response = self.send_apdu(ins=Ctap1.INS.REGISTER, data=data)
        return RegistrationData(response)

    def authenticate(
        self,
        client_param: bytes,
        app_param: bytes,
        key_handle: bytes,
        check_only: bool = False,
    ) -> SignatureData:
        """Authenticate a previously registered credential.

        :param client_param: SHA256 hash of the ClientData used for the request.
        :param app_param: SHA256 hash of the app ID used for the request.
        :param key_handle: The binary key handle of the credential.
        :param check_only: True to send a "check-only" request, which is used to
            determine if a key handle is known.
        :return: The authentication response from the authenticator.
        """
        data = (
            client_param + app_param + struct.pack(">B", len(key_handle)) + key_handle
        )
        p1 = 0x07 if check_only else 0x03
        response = self.send_apdu(ins=Ctap1.INS.AUTHENTICATE, p1=p1, data=data)
        return SignatureData(response)
