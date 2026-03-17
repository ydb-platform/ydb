"""Modbus Payload Builders.

A collection of utilities for building and decoding
modbus messages payloads.
"""
from __future__ import annotations


__all__ = [
    "BinaryPayloadBuilder",
    "BinaryPayloadDecoder",
]

from array import array

# pylint: disable=missing-type-doc
from struct import pack, unpack

from pymodbus.constants import Endian
from pymodbus.exceptions import ParameterException
from pymodbus.logging import Log
from pymodbus.pdu.pdu import (
    pack_bitstring,
    unpack_bitstring,
)


class BinaryPayloadBuilder:
    """A utility that helps build payload messages to be written with the various modbus messages.

    It really is just a simple wrapper around the struct module,
    however it saves time looking up the format strings.
    What follows is a simple example::

        builder = BinaryPayloadBuilder(byteorder=Endian.Little)
        builder.add_8bit_uint(1)
        builder.add_16bit_uint(2)
        payload = builder.build()
    """

    @classmethod
    def deprecate(cls):
        """Log warning."""
        Log.warning(
            "BinaryPayloadBuilder is deprecated and will be removed in v3.9.0 !\n"
            "Please use \"client.convert_from_registers()\" or \"client.convert_to_registers\"\n"
            "See documentation: \"https://pymodbus.readthedocs.io/en/latest/source/client.html#pymodbus.client.mixin.ModbusClientMixin.convert_from_registers\""
        )

    def __init__(
        self, payload=None, byteorder=Endian.LITTLE, wordorder=Endian.BIG, repack=False
    ):
        """Initialize a new instance of the payload builder.

        :param payload: Raw binary payload data to initialize with
        :param byteorder: The endianness of the bytes in the words
        :param wordorder: The endianness of the word (when wordcount is >= 2)
        :param repack: Repack the provided payload based on BO
        """
        # self.deprecate()
        self._payload = payload or []
        self._byteorder = byteorder
        self._wordorder = wordorder
        self._repack = repack

    def _pack_words(self, fstring: str, value) -> bytes:
        """Pack words based on the word order and byte order.

        # ---------------------------------------------- #
        # pack in to network ordered value               #
        # unpack in to network ordered  unsigned integer #
        # Change Word order if little endian word order  #
        # Pack values back based on correct byte order   #
        # ---------------------------------------------- #

        :param fstring:
        :param value: Value to be packed
        :return:
        """
        value = pack(f"!{fstring}", value)
        if Endian.LITTLE in {self._byteorder, self._wordorder}:
            value = array("H", value)
            if self._byteorder == Endian.LITTLE:
                value.byteswap()
            if self._wordorder == Endian.LITTLE:
                value.reverse()
            value = value.tobytes()
        return value

    def encode(self) -> bytes:
        """Get the payload buffer encoded in bytes."""
        # self.deprecate()
        return b"".join(self._payload)

    def __str__(self) -> str:
        """Return the payload buffer as a string.

        :returns: The payload buffer as a string
        """
        return self.encode().decode("utf-8")

    def reset(self) -> None:
        """Reset the payload buffer."""
        # self.deprecate()
        self._payload = []

    def to_registers(self):
        """Convert the payload buffer to register layout that can be used as a context block.

        :returns: The register layout to use as a block
        """
        # self.deprecate()
        # fstring = self._byteorder+"H"
        fstring = "!H"
        payload = self.build()
        if self._repack:
            payload = [unpack(self._byteorder + "H", value)[0] for value in payload]
        else:
            payload = [unpack(fstring, value)[0] for value in payload]
        Log.debug("{}", payload)
        return payload

    def to_coils(self) -> list[bool]:
        """Convert the payload buffer into a coil layout that can be used as a context block.

        :returns: The coil layout to use as a block
        """
        # self.deprecate()
        payload = self.to_registers()
        coils = [bool(int(bit)) for reg in payload for bit in format(reg, "016b")]
        return coils

    def build(self) -> list[bytes]:
        """Return the payload buffer as a list.

        This list is two bytes per element and can
        thus be treated as a list of registers.

        :returns: The payload buffer as a list
        """
        # self.deprecate()
        buffer = self.encode()
        length = len(buffer)
        buffer += b"\x00" * (length % 2)
        return [buffer[i : i + 2] for i in range(0, length, 2)]

    def add_bits(self, values: list[bool]) -> None:
        """Add a collection of bits to be encoded.

        If these are less than a multiple of eight,
        they will be left padded with 0 bits to make
        it so.

        :param values: The value to add to the buffer
        """
        # self.deprecate()
        value = pack_bitstring(values)
        self._payload.append(value)

    def add_8bit_uint(self, value: int) -> None:
        """Add a 8 bit unsigned int to the buffer.

        :param value: The value to add to the buffer
        """
        # self.deprecate()
        fstring = self._byteorder + "B"
        self._payload.append(pack(fstring, value))

    def add_16bit_uint(self, value: int) -> None:
        """Add a 16 bit unsigned int to the buffer.

        :param value: The value to add to the buffer
        """
        # self.deprecate()
        fstring = self._byteorder + "H"
        self._payload.append(pack(fstring, value))

    def add_32bit_uint(self, value: int) -> None:
        """Add a 32 bit unsigned int to the buffer.

        :param value: The value to add to the buffer
        """
        # self.deprecate()
        fstring = "I"
        # fstring = self._byteorder + "I"
        p_string = self._pack_words(fstring, value)
        self._payload.append(p_string)

    def add_64bit_uint(self, value: int) -> None:
        """Add a 64 bit unsigned int to the buffer.

        :param value: The value to add to the buffer
        """
        # self.deprecate()
        fstring = "Q"
        p_string = self._pack_words(fstring, value)
        self._payload.append(p_string)

    def add_8bit_int(self, value: int) -> None:
        """Add a 8 bit signed int to the buffer.

        :param value: The value to add to the buffer
        """
        # self.deprecate()
        fstring = self._byteorder + "b"
        self._payload.append(pack(fstring, value))

    def add_16bit_int(self, value: int) -> None:
        """Add a 16 bit signed int to the buffer.

        :param value: The value to add to the buffer
        """
        # self.deprecate()
        fstring = self._byteorder + "h"
        self._payload.append(pack(fstring, value))

    def add_32bit_int(self, value: int) -> None:
        """Add a 32 bit signed int to the buffer.

        :param value: The value to add to the buffer
        """
        # self.deprecate()
        fstring = "i"
        p_string = self._pack_words(fstring, value)
        self._payload.append(p_string)

    def add_64bit_int(self, value: int) -> None:
        """Add a 64 bit signed int to the buffer.

        :param value: The value to add to the buffer
        """
        # self.deprecate()
        fstring = "q"
        p_string = self._pack_words(fstring, value)
        self._payload.append(p_string)

    def add_16bit_float(self, value: float) -> None:
        """Add a 16 bit float to the buffer.

        :param value: The value to add to the buffer
        """
        # self.deprecate()
        fstring = "e"
        p_string = self._pack_words(fstring, value)
        self._payload.append(p_string)

    def add_32bit_float(self, value: float) -> None:
        """Add a 32 bit float to the buffer.

        :param value: The value to add to the buffer
        """
        # self.deprecate()
        fstring = "f"
        p_string = self._pack_words(fstring, value)
        self._payload.append(p_string)

    def add_64bit_float(self, value: float) -> None:
        """Add a 64 bit float(double) to the buffer.

        :param value: The value to add to the buffer
        """
        # self.deprecate()
        fstring = "d"
        p_string = self._pack_words(fstring, value)
        self._payload.append(p_string)

    def add_string(self, value: str) -> None:
        """Add a string to the buffer.

        :param value: The value to add to the buffer
        """
        # self.deprecate()
        fstring = self._byteorder + str(len(value)) + "s"
        self._payload.append(pack(fstring, value.encode()))


class BinaryPayloadDecoder:
    """A utility that helps decode payload messages from a modbus response message.

    It really is just a simple wrapper around
    the struct module, however it saves time looking up the format
    strings. What follows is a simple example::

        decoder = BinaryPayloadDecoder(payload)
        first   = decoder.decode_8bit_uint()
        second  = decoder.decode_16bit_uint()
    """

    @classmethod
    def deprecate(cls):
        """Log warning."""
        Log.warning(
            "BinaryPayloadDecoder is deprecated and will be removed in v3.9.0 !\n"
            "Please use \"client.convert_from_registers()\" or \"client.convert_to_registers\"\n"
            "See documentation: \"https://pymodbus.readthedocs.io/en/latest/source/client.html#pymodbus.client.mixin.ModbusClientMixin.convert_from_registers\""
        )

    def __init__(self, payload, byteorder=Endian.LITTLE, wordorder=Endian.BIG):
        """Initialize a new payload decoder.

        :param payload: The payload to decode with
        :param byteorder: The endianness of the payload
        :param wordorder: The endianness of the word (when wordcount is >= 2)
        """
        # self.deprecate()
        self._payload = payload
        self._pointer = 0x00
        self._byteorder = byteorder
        self._wordorder = wordorder


    @classmethod
    def fromRegisters(
        cls,
        registers,
        byteorder=Endian.LITTLE,
        wordorder=Endian.BIG,
    ):
        """Initialize a payload decoder.

        With the result of reading a collection of registers from a modbus device.

        The registers are treated as a list of 2 byte values.
        We have to do this because of how the data has already
        been decoded by the rest of the library.

        :param registers: The register results to initialize with
        :param byteorder: The Byte order of each word
        :param wordorder: The endianness of the word (when wordcount is >= 2)
        :returns: An initialized PayloadDecoder
        :raises ParameterException:
        """
        cls.deprecate()
        Log.debug("{}", registers)
        if isinstance(registers, list):  # repack into flat binary
            payload = pack(f"!{len(registers)}H", *registers)
            return cls(payload, byteorder, wordorder)
        raise ParameterException("Invalid collection of registers supplied")

    @classmethod
    def bit_chunks(cls, coils, size=8):
        """Return bit chunks."""
        cls.deprecate()
        chunks = [coils[i : i + size] for i in range(0, len(coils), size)]
        return chunks

    @classmethod
    def fromCoils(
        cls,
        coils,
        byteorder=Endian.LITTLE,
        _wordorder=Endian.BIG,
    ):
        """Initialize a payload decoder with the result of reading of coils."""
        cls.deprecate()
        if isinstance(coils, list):
            payload = b""
            if padding := len(coils) % 8:  # Pad zeros
                extra = [False] * padding
                coils = extra + coils
            chunks = cls.bit_chunks(coils)
            for chunk in chunks:
                payload += pack_bitstring(chunk[::-1])
            return cls(payload, byteorder)
        raise ParameterException("Invalid collection of coils supplied")

    def _unpack_words(self, handle) -> bytes:
        """Unpack words based on the word order and byte order.

        # ---------------------------------------------- #
        # Unpack in to network ordered unsigned integer  #
        # Change Word order if little endian word order  #
        # Pack values back based on correct byte order   #
        # ---------------------------------------------- #
        """
        if Endian.LITTLE in {self._byteorder, self._wordorder}:
            handle = array("H", handle)
            if self._byteorder == Endian.LITTLE:
                handle.byteswap()
            if self._wordorder == Endian.LITTLE:
                handle.reverse()
            handle = handle.tobytes()
        Log.debug("handle: {}", handle)
        return handle

    def reset(self):
        """Reset the decoder pointer back to the start."""
        # self.deprecate()
        self._pointer = 0x00

    def decode_8bit_uint(self):
        """Decode a 8 bit unsigned int from the buffer."""
        # self.deprecate()
        self._pointer += 1
        fstring = self._byteorder + "B"
        handle = self._payload[self._pointer - 1 : self._pointer]
        return unpack(fstring, handle)[0]

    def decode_bits(self, package_len=1):
        """Decode a byte worth of bits from the buffer."""
        # self.deprecate()
        self._pointer += package_len
        # fstring = self._endian + "B"
        handle = self._payload[self._pointer - 1 : self._pointer]
        return unpack_bitstring(handle)

    def decode_16bit_uint(self):
        """Decode a 16 bit unsigned int from the buffer."""
        # self.deprecate()
        self._pointer += 2
        fstring = self._byteorder + "H"
        handle = self._payload[self._pointer - 2 : self._pointer]
        return unpack(fstring, handle)[0]

    def decode_32bit_uint(self):
        """Decode a 32 bit unsigned int from the buffer."""
        # self.deprecate()
        self._pointer += 4
        fstring = "I"
        handle = self._payload[self._pointer - 4 : self._pointer]
        handle = self._unpack_words(handle)
        return unpack("!" + fstring, handle)[0]

    def decode_64bit_uint(self):
        """Decode a 64 bit unsigned int from the buffer."""
        # self.deprecate()
        self._pointer += 8
        fstring = "Q"
        handle = self._payload[self._pointer - 8 : self._pointer]
        handle = self._unpack_words(handle)
        return unpack("!" + fstring, handle)[0]

    def decode_8bit_int(self):
        """Decode a 8 bit signed int from the buffer."""
        # self.deprecate()
        self._pointer += 1
        fstring = self._byteorder + "b"
        handle = self._payload[self._pointer - 1 : self._pointer]
        return unpack(fstring, handle)[0]

    def decode_16bit_int(self):
        """Decode a 16 bit signed int from the buffer."""
        # self.deprecate()
        self._pointer += 2
        fstring = self._byteorder + "h"
        handle = self._payload[self._pointer - 2 : self._pointer]
        return unpack(fstring, handle)[0]

    def decode_32bit_int(self):
        """Decode a 32 bit signed int from the buffer."""
        # self.deprecate()
        self._pointer += 4
        fstring = "i"
        handle = self._payload[self._pointer - 4 : self._pointer]
        handle = self._unpack_words(handle)
        return unpack("!" + fstring, handle)[0]

    def decode_64bit_int(self):
        """Decode a 64 bit signed int from the buffer."""
        # self.deprecate()
        self._pointer += 8
        fstring = "q"
        handle = self._payload[self._pointer - 8 : self._pointer]
        handle = self._unpack_words(handle)
        return unpack("!" + fstring, handle)[0]

    def decode_16bit_float(self):
        """Decode a 16 bit float from the buffer."""
        # self.deprecate()
        self._pointer += 2
        fstring = "e"
        handle = self._payload[self._pointer - 2 : self._pointer]
        handle = self._unpack_words(handle)
        return unpack("!" + fstring, handle)[0]

    def decode_32bit_float(self):
        """Decode a 32 bit float from the buffer."""
        # self.deprecate()
        self._pointer += 4
        fstring = "f"
        handle = self._payload[self._pointer - 4 : self._pointer]
        handle = self._unpack_words(handle)
        return unpack("!" + fstring, handle)[0]

    def decode_64bit_float(self):
        """Decode a 64 bit float(double) from the buffer."""
        # self.deprecate()
        self._pointer += 8
        fstring = "d"
        handle = self._payload[self._pointer - 8 : self._pointer]
        handle = self._unpack_words(handle)
        return unpack("!" + fstring, handle)[0]

    def decode_string(self, size=1):
        """Decode a string from the buffer.

        :param size: The size of the string to decode
        """
        # self.deprecate()
        self._pointer += size
        return self._payload[self._pointer - size : self._pointer]

    def skip_bytes(self, nbytes):
        """Skip n bytes in the buffer.

        :param nbytes: The number of bytes to skip
        """
        # self.deprecate()
        self._pointer += nbytes
