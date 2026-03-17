__all__ = ['DataFormatIdentifier']

import struct
from udsoncan import tools

from typing import Union


class DataFormatIdentifier:
    """
    Defines the compression and encryption method of a specific chunk of data. 
    Mainly used by the :ref:`RequestUpload<RequestUpload>` and :ref:`RequestDownload<RequestDownload>` services

    :param compression: Value between 0 and 0xF specifying the compression method. Only the value 0 has a meaning defined by UDS standard and it is `No compression`. 
            All other values are ECU manufacturer specific.
    :type compression: int 

    :param encryption: Value between 0 and 0xF specifying the encryption method. Only the value 0 has a meaning defined by UDS standard and it is `No encryption`. 
            All other values are ECU manufacturer specific.
    :type encryption: int

    """

    compression: int
    encryption: int

    def __init__(self, compression: int = 0, encryption: int = 0):
        for param in (compression, encryption):
            tools.validate_int(param, min=0, max=0xF, name="compression and encryption method")

        self.compression = compression
        self.encryption = encryption

    @classmethod
    def from_byte(cls, byte: Union[bytes, int]) -> "DataFormatIdentifier":
        byte = int(byte)
        return cls(compression=(byte >> 4) & 0xF, encryption=(byte & 0xF))

    def get_byte_as_int(self) -> int:
        return ((self.compression & 0xF) << 4) | (self.encryption & 0xF)

    def get_byte(self) -> bytes:
        return struct.pack('B', self.get_byte_as_int())

    def __str__(self) -> str:
        return 'Compression:0x%x, Encryption:0x%x' % (self.compression, self.encryption)

    def __repr__(self) -> str:
        return '<%s: %s at 0x%08x>' % (self.__class__.__name__, str(self), id(self))
