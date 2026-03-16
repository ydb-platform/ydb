import struct

__all__ = ['AddressAndLengthFormatIdentifier']


class AddressAndLengthFormatIdentifier:
    """
    This class defines how many bytes of a memorylocation, composed of an address and a memorysize, should be encoded when sent over the underlying protocol.
    Mainly used by :ref:`ReadMemoryByAddress<ReadMemoryByAddress>`, :ref:`WriteMemoryByAddress<WriteMemoryByAddress>`, :ref:`RequestDownload<RequestDownload>` and :ref:`RequestUpload<RequestUpload>` services

    Defined by ISO-14229:2020, Annex H

    :param address_format: The number of bits on which an address should be encoded. Possible values are 8, 16, 24, 32, 40, 48, 56, 64
    :type address_format: int

    :param memorysize_format: The number of bits on which a memory size should be encoded. Possible values are 8, 16, 24, 32, 40, 48, 56, 64
    :type memorysize_format: int

    """
    address_map = {
        8: 1,
        16: 2,
        24: 3,
        32: 4,
        40: 5,
        48: 6,
        56: 7,
        64: 8
    }

    memsize_map = {
        8: 1,
        16: 2,
        24: 3,
        32: 4,
        40: 5,
        48: 6,
        56: 7,
        64: 8
    }

    memorysize_format: int
    address_format: int

    def __init__(self, address_format: int, memorysize_format: int):
        if address_format not in self.address_map:
            raise ValueError('address_format must ba an integer selected from : %s ' % (self.address_map.keys()))

        if not isinstance(memorysize_format, int) or not isinstance(address_format, int):
            raise ValueError('memorysize_format and address_format must be integers')

        if memorysize_format not in self.memsize_map:
            raise ValueError('memorysize_format must be an integer selected from : %s' % (self.memsize_map.keys()))

        self.memorysize_format = memorysize_format
        self.address_format = address_format

    def get_byte_as_int(self) -> int:
        return ((self.memsize_map[self.memorysize_format] << 4) | (self.address_map[self.address_format])) & 0xFF

    # Byte given alongside a memory address and a length so that they are decoded properly.
    def get_byte(self) -> bytes:
        return struct.pack('B', self.get_byte_as_int())
