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

"""Various utility functions.

This module contains various functions used throughout the rest of the project.
"""

from __future__ import annotations

from base64 import urlsafe_b64decode, urlsafe_b64encode
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hmac, hashes
from io import BytesIO
from dataclasses import fields, Field
from abc import abstractmethod
from typing import (
    Union,
    Optional,
    Sequence,
    Mapping,
    Any,
    TypeVar,
    Hashable,
    get_type_hints,
)
import struct
import warnings

__all__ = [
    "websafe_encode",
    "websafe_decode",
    "sha256",
    "hmac_sha256",
    "bytes2int",
    "int2bytes",
]


LOG_LEVEL_TRAFFIC = 5


def sha256(data: bytes) -> bytes:
    """Produces a SHA256 hash of the input.

    :param data: The input data to hash.
    :return: The resulting hash.
    """
    h = hashes.Hash(hashes.SHA256(), default_backend())
    h.update(data)
    return h.finalize()


def hmac_sha256(key: bytes, data: bytes) -> bytes:
    """Performs an HMAC-SHA256 operation on the given data, using the given key.

    :param key: The key to use.
    :param data: The input data to hash.
    :return: The resulting hash.
    """
    h = hmac.HMAC(key, hashes.SHA256(), default_backend())
    h.update(data)
    return h.finalize()


def bytes2int(value: bytes) -> int:
    """Parses an arbitrarily sized integer from a byte string.

    :param value: A byte string encoding a big endian unsigned integer.
    :return: The parsed int.
    """
    return int.from_bytes(value, "big")


def int2bytes(value: int, minlen: int = -1) -> bytes:
    """Encodes an int as a byte string.

    :param value: The integer value to encode.
    :param minlen: An optional minimum length for the resulting byte string.
    :return: The value encoded as a big endian byte string.
    """
    ba = []
    while value > 0xFF:
        ba.append(0xFF & value)
        value >>= 8
    ba.append(value)
    ba.extend([0] * (minlen - len(ba)))
    return bytes(reversed(ba))


def websafe_decode(data: Union[str, bytes]) -> bytes:
    """Decodes a websafe-base64 encoded string.
    See: "Base 64 Encoding with URL and Filename Safe Alphabet" from Section 5
    in RFC4648 without padding.

    :param data: The input to decode.
    :return: The decoded bytes.
    """
    if isinstance(data, str):
        data = data.encode("ascii")
    else:
        warnings.warn(
            "Calling websafe_decode on a byte value is deprecated, "
            "and will no longer be allowed starting in python-fido2 2.0",
            DeprecationWarning,
        )

    data += b"=" * (-len(data) % 4)
    return urlsafe_b64decode(data)


def websafe_encode(data: bytes) -> str:
    """Encodes a byte string into websafe-base64 encoding.

    :param data: The input to encode.
    :return: The encoded string.
    """
    return urlsafe_b64encode(data).replace(b"=", b"").decode("ascii")


class ByteBuffer(BytesIO):
    """BytesIO-like object with the ability to unpack values."""

    def unpack(self, fmt: str):
        """Reads and unpacks a value from the buffer.

        :param fmt: A struct format string yielding a single value.
        :return: The unpacked value.
        """
        s = struct.Struct(fmt)
        return s.unpack(self.read(s.size))[0]

    def read(self, size: Optional[int] = -1) -> bytes:
        """Like BytesIO.read(), but checks the number of bytes read and raises an error
        if fewer bytes were read than expected.
        """
        data = super().read(size)
        if size is not None and size > 0 and len(data) != size:
            raise ValueError(
                "Not enough data to read (need: %d, had: %d)." % (size, len(data))
            )
        return data


def _snake2camel(name: str) -> str:
    parts = name.split("_")
    return parts[0] + "".join(p.title() for p in parts[1:])


def _parse_value(t, value):
    if value is None:
        return None

    if Optional[t] == t:  # Optional, get the type
        t = t.__args__[0]

    # Handle list of values
    if issubclass(getattr(t, "__origin__", object), Sequence):
        t = t.__args__[0]
        return [_parse_value(t, v) for v in value]

    # Handle Mappings
    if issubclass(getattr(t, "__origin__", object), Mapping) and isinstance(
        value, Mapping
    ):
        return value

    # Check if type is already correct
    try:
        if isinstance(value, t):
            return value
    except TypeError:
        pass

    # Check for subclass of _DataClassMapping
    try:
        is_dataclass = issubclass(t, _DataClassMapping)
    except TypeError:
        is_dataclass = False

    if is_dataclass:
        # Recursively call from_dict for nested _DataClassMappings
        return t.from_dict(value)

    # Convert to enum values, other wrappers
    return t(value)


_T = TypeVar("_T", bound=Hashable)


class _DataClassMapping(Mapping[_T, Any]):
    # TODO: This requires Python 3.9, and fixes the tpye errors we now ignore
    # __dataclass_fields__: ClassVar[Dict[str, Field[Any]]]

    def __post_init__(self):
        hints = get_type_hints(type(self))
        for f in fields(self):  # type: ignore
            value = getattr(self, f.name)
            if value is None:
                continue
            try:
                value = _parse_value(hints[f.name], value)
            except (TypeError, KeyError, ValueError):
                raise ValueError(
                    f"Error parsing field {f.name} for {self.__class__.__name__}"
                )
            object.__setattr__(self, f.name, value)

    @classmethod
    @abstractmethod
    def _get_field_key(cls, field: Field) -> _T:
        raise NotImplementedError()

    def __getitem__(self, key):
        for f in fields(self):  # type: ignore
            if key == self._get_field_key(f):
                value = getattr(self, f.name)
                serialize = f.metadata.get("serialize")
                if serialize:
                    return serialize(value)
                if isinstance(value, _DataClassMapping):
                    return dict(value)
                if isinstance(value, Sequence) and all(
                    isinstance(x, _DataClassMapping) for x in value
                ):
                    return [dict(x) for x in value]
                return value
        raise KeyError(key)

    def __iter__(self):
        return (
            self._get_field_key(f)
            for f in fields(self)  # type: ignore
            if getattr(self, f.name) is not None
        )

    def __len__(self):
        return len(list(iter(self)))

    @classmethod
    def from_dict(cls, data: Optional[Mapping[_T, Any]]):
        if data is None:
            return None
        if isinstance(data, cls):
            return data
        if not isinstance(data, Mapping):
            raise TypeError(
                f"{cls.__name__}.from_dict called with non-Mapping data of type"
                f"{type(data)}"
            )

        kwargs = {}
        for f in fields(cls):  # type: ignore
            key = cls._get_field_key(f)
            if key in data:
                value = data[key]
                if value is not None:
                    deserialize = f.metadata.get("deserialize")
                    if deserialize:
                        value = deserialize(value)
                    kwargs[f.name] = value
        return cls(**kwargs)


class _CamelCaseDataObject(_DataClassMapping[str]):
    @classmethod
    def _get_field_key(cls, field: Field) -> str:
        return field.metadata.get("name", _snake2camel(field.name))
