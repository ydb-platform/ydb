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

from .. import cbor
from ..utils import sha256
from .base import Ctap2, Info
from .pin import PinProtocol, _PinUv

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.exceptions import InvalidTag

from typing import Optional, Any, Sequence, Mapping, cast
import struct
import zlib
import os


def _compress(data):
    o = zlib.compressobj(wbits=-zlib.MAX_WBITS)
    return o.compress(data) + o.flush()


def _decompress(data):
    o = zlib.decompressobj(wbits=-zlib.MAX_WBITS)
    return o.decompress(data) + o.flush()


def _lb_ad(orig_size):
    return b"blob" + struct.pack("<Q", orig_size)


def _lb_pack(key, data):
    orig_size = len(data)
    nonce = os.urandom(12)
    aesgcm = AESGCM(key)

    ciphertext = aesgcm.encrypt(nonce, _compress(data), _lb_ad(orig_size))

    return {
        1: ciphertext,
        2: nonce,
        3: orig_size,
    }


def _lb_unpack(key, entry):
    try:
        ciphertext = entry[1]
        nonce = entry[2]
        orig_size = entry[3]
        aesgcm = AESGCM(key)
        compressed = aesgcm.decrypt(nonce, ciphertext, _lb_ad(orig_size))
        return compressed, orig_size
    except (TypeError, IndexError, KeyError):
        raise ValueError("Invalid entry")
    except InvalidTag:
        raise ValueError("Wrong key")


class LargeBlobs:
    """Implementation of the CTAP2.1 Large Blobs API.

    Getting a largeBlobKey for a credential is done via the LargeBlobKey extension.

    :param ctap: An instance of a CTAP2 object.
    :param pin_uv_protocol: An instance of a PinUvAuthProtocol.
    :param pin_uv_token: A valid PIN/UV Auth Token for the current CTAP session.
    """

    @staticmethod
    def is_supported(info: Info) -> bool:
        return info.options.get("largeBlobs") is True

    def __init__(
        self,
        ctap: Ctap2,
        pin_uv_protocol: Optional[PinProtocol] = None,
        pin_uv_token: Optional[bytes] = None,
    ):
        if not self.is_supported(ctap.info):
            raise ValueError("Authenticator does not support LargeBlobs")

        self.ctap = ctap
        self.max_fragment_length = self.ctap.info.max_msg_size - 64
        self.pin_uv = (
            _PinUv(pin_uv_protocol, pin_uv_token)
            if pin_uv_protocol and pin_uv_token
            else None
        )

    def read_blob_array(self) -> Sequence[Mapping[int, Any]]:
        """Gets the entire contents of the Large Blobs array.

        :return: The CBOR decoded list of Large Blobs.
        """
        offset = 0
        buf = b""
        while True:
            fragment = self.ctap.large_blobs(offset, get=self.max_fragment_length)[1]
            buf += fragment
            if len(fragment) < self.max_fragment_length:
                break
            offset += self.max_fragment_length

        data, check = buf[:-16], buf[-16:]
        if check != sha256(data)[:-16]:
            return []
        return cast(Sequence[Mapping[int, Any]], cbor.decode(data))

    def write_blob_array(self, blob_array: Sequence[Mapping[int, Any]]) -> None:
        """Writes the entire Large Blobs array.

        :param blob_array: A list to write to the Authenticator.
        """
        if not isinstance(blob_array, list):
            raise TypeError("large-blob array must be a list")

        data = cbor.encode(blob_array)
        data += sha256(data)[:16]
        offset = 0
        size = len(data)

        while offset < size:
            ln = min(size - offset, self.max_fragment_length)
            _set = data[offset : offset + ln]

            if self.pin_uv:
                msg = (
                    b"\xff" * 32
                    + b"\x0c\x00"
                    + struct.pack("<I", offset)
                    + sha256(_set)
                )
                pin_uv_protocol = self.pin_uv.protocol.VERSION
                pin_uv_param = self.pin_uv.protocol.authenticate(self.pin_uv.token, msg)
            else:
                pin_uv_param = None
                pin_uv_protocol = None

            self.ctap.large_blobs(
                offset,
                set=_set,
                length=size if offset == 0 else None,
                pin_uv_protocol=pin_uv_protocol,
                pin_uv_param=pin_uv_param,
            )

            offset += ln

    def get_blob(self, large_blob_key: bytes) -> Optional[bytes]:
        """Gets the Large Blob stored for a single credential.

        :param large_blob_key: The largeBlobKey for the credential, or None.
        :returns: The decrypted and deflated value stored for the credential.
        """
        for entry in self.read_blob_array():
            try:
                compressed, orig_size = _lb_unpack(large_blob_key, entry)
                decompressed = _decompress(compressed)
                if len(decompressed) == orig_size:
                    return decompressed
            except (ValueError, zlib.error):
                continue
        return None

    def put_blob(self, large_blob_key: bytes, data: Optional[bytes]) -> None:
        """Stores a Large Blob for a single credential.

        Any existing entries for the same credential will be replaced.

        :param large_blob_key: The largeBlobKey for the credential.
        :param data: The data to compress, encrypt and store.
        """
        modified = data is not None
        entries = []

        for entry in self.read_blob_array():
            try:
                _lb_unpack(large_blob_key, entry)
                modified = True
            except ValueError:
                entries.append(entry)

        if data is not None:
            entries.append(_lb_pack(large_blob_key, data))

        if modified:
            self.write_blob_array(entries)

    def delete_blob(self, large_blob_key: bytes) -> None:
        """Deletes any Large Blob(s) stored for a single credential.

        :param large_blob_key: The largeBlobKey for the credential.
        """
        self.put_blob(large_blob_key, None)
