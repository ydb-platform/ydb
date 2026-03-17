__all__ = [
    'DidCodec',
    'AsciiCodec'
]

import struct

from typing import Optional, Any


class DidCodec:
    """
    This class defines how to encode/decode a Data Identifier value to/from a binary payload.

    One should extend this class and override the ``encode``, ``decode``, ``__len__`` methods as they will be used
    to generate or parse binary payloads.

            - ``encode`` Must receive any Python object and must return a bytes payload
            - ``decode`` Must receive a bytes payload and may return any Python object
            - ``__len__`` Must return the length of the bytes payload or raise a ``DidCodec.ReadAllRemainingData`` to read the whole payload. Reading the whole payload is not a feature proposed by ISO-14229.

    If a data can be processed by a pack string, then this class may be used as is, without being extended.

    :param packstr: A pack string used with struct.pack / struct.unpack. 
    :type packstr: string
    """

    class ReadAllRemainingData(Exception):
        pass

    packstr: Optional[str]

    def __init__(self, packstr: Optional[str] = None):
        self.packstr = packstr

    def encode(self, *did_value: Any) -> bytes:
        if self.packstr is None:
            raise NotImplementedError('Cannot encode DID to binary payload. Codec has no "encode" implementation')

        return struct.pack(self.packstr, *did_value)

    def decode(self, did_payload: bytes) -> Any:
        if self.packstr is None:
            raise NotImplementedError('Cannot decode DID from binary payload. Codec has no "decode" implementation')

        return struct.unpack(self.packstr, did_payload)

    # Must tell the size of the payload encoded or expected for decoding
    def __len__(self) -> int:
        if self.packstr is None:
            raise NotImplementedError('Cannot tell the payload size. Codec has no "__len__" implementation')
        return struct.calcsize(self.packstr)


class AsciiCodec(DidCodec):
    string_len: int

    def __init__(self, string_len: int):
        self.string_len = string_len

    def encode(self, string_ascii: Any) -> bytes:  # type: ignore
        if not isinstance(string_ascii, str):
            raise ValueError("AsciiCodec requires a string for encoding")

        if len(string_ascii) != self.string_len:
            raise ValueError('String must be %d long' % self.string_len)
        return string_ascii.encode('ascii')

    def decode(self, string_bin: bytes) -> Any:
        string_ascii = string_bin.decode(encoding='ascii', errors="replace")
        if len(string_ascii) != self.string_len:
            raise ValueError('Trying to decode a string of %d bytes but codec expects %d bytes' % (len(string_ascii), self.string_len))
        return string_ascii

    def __len__(self) -> int:
        return self.string_len
