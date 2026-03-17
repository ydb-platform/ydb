"""Bit Reading Request/Response messages."""

import struct
from typing import cast

from pymodbus.constants import ModbusStatus
from pymodbus.datastore import ModbusSlaveContext
from pymodbus.pdu.pdu import (
    ExceptionResponse,
    ModbusPDU,
    pack_bitstring,
    unpack_bitstring,
)


class ReadCoilsRequest(ModbusPDU):
    """ReadCoilsRequest."""

    rtu_frame_size = 8
    function_code = 1

    def encode(self) -> bytes:
        """Encode a request pdu."""
        self.verifyCount(2000)
        self.verifyAddress()
        return struct.pack(">HH", self.address, self.count)

    def decode(self, data: bytes) -> None:
        """Decode a request pdu."""
        self.address, self.count = struct.unpack(">HH", data)

    def get_response_pdu_size(self) -> int:
        """Get response pdu size.

        Func_code (1 byte) + Byte Count(1 byte) + Quantity of Coils (n Bytes)/8,
        if the remainder is different of 0 then N = N+1
        """
        return 1 + 1 + (self.count + 7) // 8

    async def update_datastore(self, context: ModbusSlaveContext) -> ModbusPDU:
        """Run request against a datastore."""
        values = await context.async_getValues(
            self.function_code, self.address, self.count
        )
        if isinstance(values, int):
            return ExceptionResponse(self.function_code, values)
        response_class = (ReadCoilsResponse if self.function_code == 1 else ReadDiscreteInputsResponse)
        return response_class(dev_id=self.dev_id, transaction_id=self.transaction_id, bits=cast(list[bool], values))


class ReadDiscreteInputsRequest(ReadCoilsRequest):
    """ReadDiscreteInputsRequest."""

    function_code = 2


class ReadCoilsResponse(ModbusPDU):
    """ReadCoilsResponse."""

    function_code = 1
    rtu_byte_count_pos = 2

    def encode(self) -> bytes:
        """Encode response pdu."""
        result = pack_bitstring(self.bits)
        return struct.pack(">B", len(result)) + result

    def decode(self, data: bytes) -> None:
        """Decode response pdu."""
        self.bits = unpack_bitstring(data[1:])


class ReadDiscreteInputsResponse(ReadCoilsResponse):
    """ReadDiscreteInputsResponse."""

    function_code = 2


class WriteSingleCoilResponse(ModbusPDU):
    """WriteSingleCoilResponse."""

    function_code = 5
    rtu_frame_size = 8

    def encode(self) -> bytes:
        """Encode write coil request."""
        self.verifyAddress()
        val = ModbusStatus.ON if self.bits[0] else ModbusStatus.OFF
        return struct.pack(">HH", self.address, val)

    def decode(self, data: bytes) -> None:
        """Decode a write coil request."""
        self.address, value = struct.unpack(">HH", data)
        self.bits = [bool(value)]


class WriteSingleCoilRequest(WriteSingleCoilResponse):
    """WriteSingleCoilRequest."""

    async def update_datastore(self, context: ModbusSlaveContext) -> ModbusPDU:
        """Run a request against a datastore."""
        if (rc := await context.async_setValues(self.function_code, self.address, self.bits)):
            return ExceptionResponse(self.function_code, rc)
        values = await context.async_getValues(self.function_code, self.address, 1)
        if isinstance(values, int):
            return ExceptionResponse(self.function_code, values)

        return WriteSingleCoilResponse(address=self.address, bits=cast(list[bool], values), dev_id=self.dev_id, transaction_id=self.transaction_id)

    def get_response_pdu_size(self) -> int:
        """Get response pdu size.

        Func_code (1 byte) + Output Address (2 byte) + Output Value  (2 Bytes)
        """
        return 1 + 2 + 2


class WriteMultipleCoilsRequest(ModbusPDU):
    """WriteMultipleCoilsRequest."""

    function_code = 15
    rtu_byte_count_pos = 6

    def encode(self) -> bytes:
        """Encode write coils request."""
        self.count = len(self.bits)
        self.verifyAddress()
        self.verifyCount(2000)
        byte_count = (self.count + 7) // 8
        return struct.pack(">HHB", self.address, self.count, byte_count) + pack_bitstring(self.bits)

    def decode(self, data: bytes) -> None:
        """Decode a write coils request."""
        self.address, count, _byte_count = struct.unpack(">HHB", data[0:5])
        self.bits = unpack_bitstring(data[5:])[:count]

    async def update_datastore(self, context: ModbusSlaveContext) -> ModbusPDU:
        """Run a request against a datastore."""
        count = len(self.bits)
        rc = await context.async_setValues(
            self.function_code, self.address, self.bits
        )
        if rc:
            return ExceptionResponse(self.function_code, rc)

        return WriteMultipleCoilsResponse(address=self.address, count=count, dev_id=self.dev_id, transaction_id=self.transaction_id)

    def get_response_pdu_size(self) -> int:
        """Get response pdu size.

        Func_code (1 byte) + Output Address (2 byte) + Quantity of Outputs  (2 Bytes)
        :return:
        """
        return 1 + 2 + 2


class WriteMultipleCoilsResponse(ModbusPDU):
    """WriteMultipleCoilsResponse."""

    function_code = 15
    rtu_frame_size = 8

    def encode(self) -> bytes:
        """Encode write coils response."""
        return struct.pack(">HH", self.address, self.count)

    def decode(self, data: bytes) -> None:
        """Decode a write coils response."""
        self.address, self.count = struct.unpack(">HH", data)
