"""Register Reading Request/Response."""
from __future__ import annotations

import struct
from typing import cast

from pymodbus.datastore import ModbusSlaveContext
from pymodbus.exceptions import ModbusIOException
from pymodbus.pdu.pdu import ExceptionResponse, ModbusPDU


class ReadHoldingRegistersRequest(ModbusPDU):
    """ReadHoldingRegistersRequest."""

    function_code = 3
    rtu_frame_size = 8

    def encode(self) -> bytes:
        """Encode the request packet."""
        self.verifyAddress()
        self.verifyCount(125)
        return struct.pack(">HH", self.address, self.count)

    def decode(self, data: bytes) -> None:
        """Decode a register request packet."""
        self.address, self.count = struct.unpack(">HH", data)

    def get_response_pdu_size(self) -> int:
        """Get response pdu size.

        Func_code (1 byte) + Byte Count(1 byte) + 2 * Quantity of registers (== byte count).
        """
        return 1 + 1 + 2 * self.count

    async def update_datastore(self, context: ModbusSlaveContext) -> ModbusPDU:
        """Run a read holding request against a datastore."""
        values = await context.async_getValues(
            self.function_code, self.address, self.count
        )
        if isinstance(values, int):
            return ExceptionResponse(self.function_code, values)
        response_class = (ReadHoldingRegistersResponse if self.function_code == 3 else ReadInputRegistersResponse)
        return response_class(registers=cast(list[int], values), dev_id=self.dev_id, transaction_id=self.transaction_id)


class ReadHoldingRegistersResponse(ModbusPDU):
    """ReadHoldingRegistersResponse."""

    function_code = 3
    rtu_byte_count_pos = 2

    def encode(self) -> bytes:
        """Encode the response packet."""
        result = struct.pack(">B", len(self.registers) * 2)
        for register in self.registers:
            result += struct.pack(">H", register)
        return result

    def decode(self, data: bytes) -> None:
        """Decode a register response packet."""
        self.registers = []
        if (data_len := int(data[0])) >= len(data):
            raise ModbusIOException(f"byte_count {data_len} > length of packet {len(data)}")
        for i in range(1, data_len, 2):
            self.registers.append(struct.unpack(">H", data[i : i + 2])[0])


class ReadInputRegistersRequest(ReadHoldingRegistersRequest):
    """ReadInputRegistersRequest."""

    function_code = 4


class ReadInputRegistersResponse(ReadHoldingRegistersResponse):
    """ReadInputRegistersResponse."""

    function_code = 4


class ReadWriteMultipleRegistersRequest(ModbusPDU):
    """ReadWriteMultipleRegistersRequest."""

    function_code = 23
    rtu_byte_count_pos = 10

    def __init__(self,
            read_address: int = 0x00,
            read_count: int = 0,
            write_address: int = 0x00,
            write_registers: list[int] | None = None,
            dev_id: int = 1,
            transaction_id: int = 0) -> None:
        """Initialize a new request message."""
        if not write_registers:
            write_registers = []
        super().__init__(transaction_id=transaction_id, dev_id=dev_id)
        self.read_address = read_address
        self.read_count = read_count
        self.write_address = write_address
        self.write_registers = write_registers
        self.write_count = len(self.write_registers)
        self.write_byte_count = self.write_count * 2

    def encode(self) -> bytes:
        """Encode the request packet."""
        self.verifyAddress(address=self.read_address)
        self.verifyAddress(address=self.write_address)
        self.verifyCount(125, count=self.read_count)
        self.verifyCount(121, count=self.write_count)
        result = struct.pack(
            ">HHHHB",
            self.read_address,
            self.read_count,
            self.write_address,
            self.write_count,
            self.write_byte_count,
        )
        for register in self.write_registers:
            result += struct.pack(">H", register)
        return result

    def decode(self, data: bytes) -> None:
        """Decode the register request packet."""
        (
            self.read_address,
            self.read_count,
            self.write_address,
            self.write_count,
            self.write_byte_count,
        ) = struct.unpack(">HHHHB", data[:9])
        self.write_registers = []
        for i in range(9, self.write_byte_count + 9, 2):
            register = struct.unpack(">H", data[i : i + 2])[0]
            self.write_registers.append(register)

    async def update_datastore(self, context: ModbusSlaveContext) -> ModbusPDU:
        """Run a write single register request against a datastore."""
        if not (1 <= self.read_count <= 0x07D):
            return ExceptionResponse(self.function_code, ExceptionResponse.ILLEGAL_VALUE)
        if not 1 <= self.write_count <= 0x079:
            return ExceptionResponse(self.function_code, ExceptionResponse.ILLEGAL_VALUE)
        rc = await context.async_setValues(
            self.function_code, self.write_address, self.write_registers
        )
        if rc:
            return ExceptionResponse(self.function_code, rc)
        registers = await context.async_getValues(
            self.function_code, self.read_address, self.read_count
        )
        if isinstance(registers, int):
            return ExceptionResponse(self.function_code, registers)
        return ReadWriteMultipleRegistersResponse(registers=cast(list[int], registers), dev_id=self.dev_id, transaction_id=self.transaction_id)

    def get_response_pdu_size(self) -> int:
        """Get response pdu size.

        Func_code (1 byte) + Byte Count(1 byte) + 2 * Quantity of Coils (n Bytes)
        """
        return 1 + 1 + 2 * self.read_count


class ReadWriteMultipleRegistersResponse(ReadHoldingRegistersResponse):
    """ReadWriteMultipleRegistersResponse."""

    function_code = 23


class WriteSingleRegisterResponse(ModbusPDU):
    """WriteSingleRegisterResponse."""

    function_code = 6
    rtu_frame_size = 8

    def encode(self) -> bytes:
        """Encode a write single register packet packet request."""
        return struct.pack(">HH", self.address, self.registers[0])

    def decode(self, data: bytes) -> None:
        """Decode a write single register packet packet request."""
        self.address, register = struct.unpack(">HH", data)
        self.registers = [register]


class WriteSingleRegisterRequest(WriteSingleRegisterResponse):
    """WriteSingleRegisterRequest."""

    async def update_datastore(self, context: ModbusSlaveContext) -> ModbusPDU:
        """Run a write single register request against a datastore."""
        if not 0 <= self.registers[0] <= 0xFFFF:
            return ExceptionResponse(self.function_code, ExceptionResponse.ILLEGAL_VALUE)
        rc = await context.async_setValues(
            self.function_code, self.address, self.registers
        )
        if rc:
            return ExceptionResponse(self.function_code, rc)
        values = await context.async_getValues(self.function_code, self.address, 1)
        if isinstance(values, int):
            return ExceptionResponse(self.function_code, values)
        return WriteSingleRegisterResponse(address=self.address, registers=cast(list[int], values))

    def get_response_pdu_size(self) -> int:
        """Get response pdu size.

        Func_code (1 byte) + Register Address(2 byte) + Register Value (2 bytes)
        """
        return 1 + 2 + 2


class WriteMultipleRegistersRequest(ModbusPDU):
    """WriteMultipleRegistersRequest."""

    function_code = 16
    rtu_byte_count_pos = 6
    _pdu_length = 5  # func + adress1 + adress2 + outputQuant1 + outputQuant2

    def encode(self) -> bytes:
        """Encode a write single register packet packet request."""
        packet = struct.pack(">HHB", self.address, self.count, self.count * 2)
        for value in self.registers:
            packet += struct.pack(">H", value)
        return packet

    def decode(self, data: bytes) -> None:
        """Decode a write single register packet packet request."""
        self.address, self.count, _byte_count = struct.unpack(">HHB", data[:5])
        self.registers = []
        for idx in range(5, (self.count * 2) + 5, 2):
            self.registers.append(struct.unpack(">H", data[idx : idx + 2])[0])

    async def update_datastore(self, context: ModbusSlaveContext) -> ModbusPDU:
        """Run a write single register request against a datastore."""
        if not 1 <= self.count <= 0x07B:
            return ExceptionResponse(self.function_code, ExceptionResponse.ILLEGAL_VALUE)
        rc = await context.async_setValues(
            self.function_code, self.address, self.registers
          )
        if rc:
            return ExceptionResponse(self.function_code, rc)
        return WriteMultipleRegistersResponse(address=self.address, count=self.count, dev_id=self.dev_id, transaction_id=self.transaction_id)

    def get_response_pdu_size(self) -> int:
        """Get response pdu size.

        Func_code (1 byte) + Starting Address (2 byte) + Quantity of Registers  (2 Bytes)
        """
        return 1 + 2 + 2


class WriteMultipleRegistersResponse(ModbusPDU):
    """WriteMultipleRegistersResponse."""

    function_code = 16
    rtu_frame_size = 8

    def encode(self) -> bytes:
        """Encode a write single register packet packet request."""
        return struct.pack(">HH", self.address, self.count)

    def decode(self, data: bytes) -> None:
        """Decode a write single register packet packet request."""
        self.address, self.count = struct.unpack(">HH", data)


class MaskWriteRegisterRequest(ModbusPDU):
    """MaskWriteRegisterRequest."""

    function_code = 0x16
    rtu_frame_size = 10

    def __init__(self, address=0x0000, and_mask=0xFFFF, or_mask=0x0000, dev_id=1, transaction_id=0) -> None:
        """Initialize a new instance."""
        super().__init__(transaction_id=transaction_id, dev_id=dev_id, address=address)
        self.and_mask = and_mask
        self.or_mask = or_mask

    def encode(self) -> bytes:
        """Encode the request packet."""
        return struct.pack(">HHH", self.address, self.and_mask, self.or_mask)

    def decode(self, data: bytes) -> None:
        """Decode the incoming request."""
        self.address, self.and_mask, self.or_mask = struct.unpack(">HHH", data)

    async def update_datastore(self, context: ModbusSlaveContext) -> ModbusPDU:
        """Run a mask write register request against the store."""
        if not 0x0000 <= self.and_mask <= 0xFFFF:
            return ExceptionResponse(self.function_code, ExceptionResponse.ILLEGAL_VALUE)
        if not 0x0000 <= self.or_mask <= 0xFFFF:
            return ExceptionResponse(self.function_code, ExceptionResponse.ILLEGAL_VALUE)
        values = await context.async_getValues(self.function_code, self.address, 1)
        if isinstance(values, int):
            return ExceptionResponse(self.function_code, values)
        values = (values[0] & self.and_mask) | (self.or_mask & ~self.and_mask)
        rc = await context.async_setValues(
            self.function_code, self.address, cast(list[int], [values])
        )
        if rc:
            return ExceptionResponse(self.function_code, rc)
        return MaskWriteRegisterResponse(address=self.address, and_mask=self.and_mask, or_mask=self.or_mask, dev_id=self.dev_id, transaction_id=self.transaction_id)


class MaskWriteRegisterResponse(ModbusPDU):
    """MaskWriteRegisterResponse."""

    function_code = 0x16
    rtu_frame_size = 10

    def __init__(self, address=0x0000, and_mask=0xFFFF, or_mask=0x0000, dev_id=1, transaction_id=0) -> None:
        """Initialize new instance."""
        super().__init__(transaction_id=transaction_id, dev_id=dev_id, address=address)
        self.and_mask = and_mask
        self.or_mask = or_mask

    def encode(self) -> bytes:
        """Encode the response."""
        self.verifyAddress()
        return struct.pack(">HHH", self.address, self.and_mask, self.or_mask)

    def decode(self, data: bytes) -> None:
        """Decode a the response."""
        self.address, self.and_mask, self.or_mask = struct.unpack(">HHH", data)
