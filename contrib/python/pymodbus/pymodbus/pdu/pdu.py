"""Contains base classes for modbus request/response/error packets."""
from __future__ import annotations

import asyncio
import struct
from abc import abstractmethod

from pymodbus.datastore import ModbusSlaveContext
from pymodbus.exceptions import NotImplementedException


class ModbusPDU:
    """Base class for all Modbus messages."""

    function_code: int = 0
    sub_function_code: int = -1
    rtu_frame_size: int = 0
    rtu_byte_count_pos: int = 0

    def __init__(self,
            dev_id: int = 0,
            transaction_id: int = 0,
            address: int = 0,
            count: int = 0,
            bits: list[bool] | None = None,
            registers: list[int] | None = None,
            status: int = 1,
        ) -> None:
        """Initialize the base data for a modbus request."""
        self.dev_id: int = dev_id
        self.transaction_id: int = transaction_id
        self.address: int = address
        self.bits: list[bool] = bits or []
        self.registers: list[int] = registers or []
        self.count: int = count or len(self.registers)
        self.status: int = status
        self.exception_code: int = 0
        self.fut: asyncio.Future

    def isError(self) -> bool:
        """Check if the error is a success or failure."""
        return self.function_code > 0x80

    def verifyCount(self, max_count: int, count: int = -1) -> None:
        """Validate API supplied count."""
        if count == -1:
            count = self.count
        if not 1 <= count <= max_count:
            raise ValueError(f"1 < count {count} < {max_count} !")

    def verifyAddress(self, address: int = -1) -> None:
        """Validate API supplied address."""
        if address == -1:
            address = self.address
        if not 0 <= address <= 65535:
            raise ValueError(f"0 < address {address} < 65535 !")

    def __str__(self) -> str:
        """Build a representation of a Modbus response."""
        return (
            f"{self.__class__.__name__}("
            f"dev_id={self.dev_id}, "
            f"transaction_id={self.transaction_id}, "
            f"address={self.address}, "
            f"count={self.count}, "
            f"bits={self.bits!s}, "
            f"registers={self.registers!s}, "
            f"status={self.status!s})"
        )

    def get_response_pdu_size(self) -> int:
        """Calculate response pdu size."""
        return 0

    @abstractmethod
    def encode(self) -> bytes:
        """Encode the message."""

    @abstractmethod
    def decode(self, data: bytes) -> None:
        """Decode data part of the message."""

    async def update_datastore(self, context: ModbusSlaveContext) -> ModbusPDU:
        """Run request against a datastore."""
        _ = context
        return ExceptionResponse(0, 0)

    @classmethod
    def calculateRtuFrameSize(cls, data: bytes) -> int:
        """Calculate the size of a PDU."""
        if cls.rtu_frame_size:
            return cls.rtu_frame_size
        if cls.rtu_byte_count_pos:
            if len(data) < cls.rtu_byte_count_pos +1:
                return 0
            return int(data[cls.rtu_byte_count_pos]) + cls.rtu_byte_count_pos + 3
        raise NotImplementedException(
            f"Cannot determine RTU frame size for {cls.__name__}"
        )


class ExceptionResponse(ModbusPDU):
    """Base class for a modbus exception PDU."""

    rtu_frame_size = 5

    ILLEGAL_FUNCTION = 0x01
    ILLEGAL_ADDRESS = 0x02
    ILLEGAL_VALUE = 0x03
    SLAVE_FAILURE = 0x04
    ACKNOWLEDGE = 0x05
    SLAVE_BUSY = 0x06
    NEGATIVE_ACKNOWLEDGE = 0x07
    MEMORY_PARITY_ERROR = 0x08
    GATEWAY_PATH_UNAVIABLE = 0x0A
    GATEWAY_NO_RESPONSE = 0x0B

    def __init__(
            self,
            function_code: int,
            exception_code: int = 0,
            slave: int = 1,
            transaction: int = 0) -> None:
        """Initialize the modbus exception response."""
        super().__init__(transaction_id=transaction, dev_id=slave)
        self.function_code = function_code | 0x80
        self.exception_code = exception_code

    def __str__(self) -> str:
        """Build a representation of an exception response."""
        return (
            f"{self.__class__.__name__}("
            f"dev_id={self.dev_id}, "
            f"function_code={self.function_code}, "
            f"exception_code={self.exception_code})"
        )

    def encode(self) -> bytes:
        """Encode a modbus exception response."""
        return struct.pack(">B", self.exception_code)

    def decode(self, data: bytes) -> None:
        """Decode a modbus exception response."""
        self.exception_code = int(data[0])


def pack_bitstring(bits: list[bool], align_byte=True) -> bytes:
    """Create a bytestring out of a list of bits.

    example::

        bits   = [True, False, False, False] +
                 [False, False, False, True] +
                 [True, False, True, False] +
                 [False, False, False, False]
        result = pack_bitstring(bits)
        bytes 0x05 0x81
    """
    ret = b""
    i = packed = 0
    t_bits = bits
    bits_extra = 8 if align_byte else 16
    if (extra := len(bits) % bits_extra):
        t_bits += [False] * (bits_extra - extra)
    for byte_inx in range(0, len(t_bits), 8):
        for bit in reversed(t_bits[byte_inx:byte_inx+8]):
            packed <<= 1
            if bit:
                packed += 1
            i += 1
            if i == 8:
                ret += struct.pack(">B", packed)
                i = packed = 0
    return ret


def unpack_bitstring(data: bytes) -> list[bool]:
    """Create bit list out of a bytestring.

    example::

        bytes 0x05 0x81
        result = unpack_bitstring(bytes)

        [True, False, True, False] + [False, False, False, False]
        [True, False, False, False] + [False, False, False, True]
    """
    res = []
    for _, t_byte in enumerate(data):
        for bit in (1, 2, 4, 8, 16, 32, 64, 128):
            res.append(bool(t_byte & bit))
    return res
