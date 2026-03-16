__all__ = ['MemoryLocation']

import struct
import math

from udsoncan.common.AddressAndLengthFormatIdentifier import AddressAndLengthFormatIdentifier

from typing import Optional


class MemoryLocation:
    """
    This class defines a memory block location including : address, size, AddressAndLengthFormatIdentifier (address format and memory size format)

    :param address: A memory address pointing to the beginning of the memory block
    :type address: int

    :param memorysize: The size of the memory block
    :type memorysize: int

    :param address_format: The number of bits on which an address should be encoded. Possible values are 8, 16, 24, 32, 40.
            If ``None`` is specified, the smallest size required to store the given address will be used
    :type address_format: int or None

    :param memorysize_format: The number of bits on which a memory size should be encoded. Possible values are 8, 16, 24, 32
            If ``None`` is specified, the smallest size required to store the given memorysize will be used
    :type memorysize_format: int or None	

    """

    address: int
    memorysize: int
    address_format: Optional[int]
    memorysize_format: Optional[int]

    def __init__(self, address: int, memorysize: int, address_format: Optional[int] = None, memorysize_format: Optional[int] = None):
        self.address = address
        self.memorysize = memorysize
        self.address_format = address_format
        self.memorysize_format = memorysize_format

        if address_format is None:
            address_format = self.autosize_address(address)

        if memorysize_format is None:
            memorysize_format = self.autosize_memorysize(memorysize)

        self.alfid = AddressAndLengthFormatIdentifier(memorysize_format=memorysize_format, address_format=address_format)

    # This is used by the client/server to set a format from a config object while letting the user override it
    def set_format_if_none(self, address_format: Optional[int] = None, memorysize_format: Optional[int] = None) -> None:
        previous_address_format = self.address_format
        previous_memorysize_format = self.memorysize_format
        try:
            if address_format is not None:
                if self.address_format is None:
                    self.address_format = address_format

            if memorysize_format is not None:
                if self.memorysize_format is None:
                    self.memorysize_format = memorysize_format

            address_format = self.address_format if self.address_format is not None else self.autosize_address(self.address)
            memorysize_format = self.memorysize_format if self.memorysize_format is not None else self.autosize_memorysize(self.memorysize)

            self.alfid = AddressAndLengthFormatIdentifier(memorysize_format=memorysize_format, address_format=address_format)
        except:
            self.address_format = previous_address_format
            self.memorysize_format = previous_memorysize_format
            raise

    # Finds the smallest size that fits the address
    def autosize_address(self, val: int) -> int:
        fmt = math.ceil(val.bit_length() / 8) * 8
        if fmt > 64:
            raise ValueError("address size must be smaller or equal than 64 bits")
        return fmt

    # Finds the smallest size that fits the memory size
    def autosize_memorysize(self, val: int) -> int:
        fmt = math.ceil(val.bit_length() / 8) * 8
        if fmt > 64:
            raise ValueError("memory size must be smaller or equal than 64 bits")
        return fmt

    # Gets the address byte in the requested format
    def get_address_bytes(self) -> bytes:
        n = AddressAndLengthFormatIdentifier.address_map[self.alfid.address_format]

        data = struct.pack('>q', self.address)
        return data[-n:]

    # Gets the memory size byte in the requested format

    def get_memorysize_bytes(self) -> bytes:
        n = AddressAndLengthFormatIdentifier.memsize_map[self.alfid.memorysize_format]

        data = struct.pack('>q', self.memorysize)
        return data[-n:]

    # Generates an instance from the byte stream
    @classmethod
    def from_bytes(cls, address_bytes: bytes, memorysize_bytes: bytes) -> "MemoryLocation":
        if not isinstance(address_bytes, bytes):
            raise ValueError('address_bytes must be a valid bytes object')

        if not isinstance(memorysize_bytes, bytes):
            raise ValueError('memorysize_bytes must be a valid bytes object')

        if len(address_bytes) > 5:
            raise ValueError('Address must be at most 40 bits long')

        if len(memorysize_bytes) > 4:
            raise ValueError('Memory size must be at most 32 bits long')

        address_bytes_padded = b'\x00' * (8 - len(address_bytes)) + address_bytes
        memorysize_bytes_padded = b'\x00' * (8 - len(memorysize_bytes)) + memorysize_bytes

        address = struct.unpack('>q', address_bytes_padded)[0]
        memorysize = struct.unpack('>q', memorysize_bytes_padded)[0]
        address_format = len(address_bytes) * 8
        memorysize_format = len(memorysize_bytes) * 8

        return cls(address=address, memorysize=memorysize, address_format=address_format, memorysize_format=memorysize_format)

    def __str__(self) -> str:
        return 'Address=0x%x (%d bits), Size=0x%x (%d bits)' % (self.address, self.alfid.address_format, self.memorysize, self.alfid.memorysize_format)

    def __repr__(self) -> str:
        return '<%s: %s at 0x%08x>' % (self.__class__.__name__, str(self), id(self))
