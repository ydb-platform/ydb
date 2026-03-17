"""File Record Read/Write Messages."""
from __future__ import annotations

import struct
from dataclasses import dataclass

from pymodbus.datastore import ModbusSlaveContext
from pymodbus.exceptions import ModbusException
from pymodbus.pdu.pdu import ModbusPDU


@dataclass
class FileRecord:
    """Represents a file record and its relevant data."""

    file_number: int = 0
    record_number: int = 0
    record_data: bytes = b''
    record_length: int = 0

    def __post_init__(self) -> None:
        """Initialize a new instance."""
        if self.record_data:
            if self.record_length:
                raise ModbusException("Use either record_data= or record_length=")
            self.record_length = len(self.record_data)
        if self.record_length % 2:
            raise ModbusException("length of record_data must be a multiple of 2")
        self.record_length //= 2


class ReadFileRecordRequest(ModbusPDU):
    """ReadFileRecordRequest."""

    function_code = 0x14
    rtu_byte_count_pos = 2

    def __init__(self, records: list[FileRecord] | None = None,  dev_id: int = 1, transaction_id: int = 0) -> None:
        """Initialize a new instance."""
        super().__init__(transaction_id=transaction_id, dev_id=dev_id)
        self.records: list[FileRecord] = records or []

    def encode(self) -> bytes:
        """Encode the request packet."""
        packet = struct.pack("B", len(self.records) * 7)
        for record in self.records:
            packet += struct.pack(
                ">BHHH",
                0x06,
                record.file_number,
                record.record_number,
                record.record_length,
            )
        return packet

    def decode(self, data: bytes) -> None:
        """Decode the incoming request."""
        self.records = []
        byte_count = int(data[0])
        for count in range(1, byte_count, 7):
            decoded = struct.unpack(">BHHH", data[count : count + 7])
            record = FileRecord(
                file_number=decoded[1],
                record_number=decoded[2],
                record_length=decoded[3],
            )
            self.records.append(record)

    def get_response_pdu_size(self) -> int:
        """Get response pdu size.

        Func_code (1 byte) + Quantity of record (each 7 bytes),
        """
        return 1 + 7 * len(self.records)

    async def update_datastore(self, _context: ModbusSlaveContext) -> ModbusPDU:
        """Run a read exception status request against the store."""
        for record in self.records:
            record.record_data = b'SERVER DUMMY RECORD.'
            record.record_length = len(record.record_data) // 2
        return ReadFileRecordResponse(records=self.records,dev_id=self.dev_id, transaction_id=self.transaction_id)


class ReadFileRecordResponse(ModbusPDU):
    """ReadFileRecordResponse."""

    function_code = 0x14
    rtu_byte_count_pos = 2

    def __init__(self, records: list[FileRecord] | None = None, dev_id: int = 1, transaction_id: int = 0) -> None:
        """Initialize a new instance."""
        super().__init__(transaction_id=transaction_id, dev_id=dev_id)
        self.records: list[FileRecord] = records or []

    def encode(self) -> bytes:
        """Encode the response."""
        total = sum(len(record.record_data) + 1 + 1 for record in self.records)
        packet = struct.pack("B", total)
        for record in self.records:
            packet += struct.pack(">BB", len(record.record_data) + 1, 0x06)
            packet += record.record_data
        return packet

    def decode(self, data: bytes) -> None:
        """Decode the response."""
        count, self.records = 1, []
        byte_count = int(data[0])
        while count < byte_count:
            calc_length, _ = struct.unpack(
                ">BB", data[count : count + 2]
            )
            count += 2

            record_length = calc_length - 1 # response length includes the type byte
            record = FileRecord(
                record_data=data[count : count + record_length],
            )
            count += record_length
            self.records.append(record)


class WriteFileRecordRequest(ModbusPDU):
    """WriteFileRecordRequest."""

    function_code = 0x15
    rtu_byte_count_pos = 2

    def __init__(self, records: list[FileRecord] | None = None, dev_id: int = 1, transaction_id: int = 0) -> None:
        """Initialize a new instance."""
        super().__init__(transaction_id=transaction_id, dev_id=dev_id)
        self.records: list[FileRecord] = records or []

    def encode(self) -> bytes:
        """Encode the request packet."""
        total_length = sum((record.record_length * 2) + 7 for record in self.records)
        packet = struct.pack("B", total_length)

        for record in self.records:
            packet += struct.pack(
                ">BHHH",
                0x06,
                record.file_number,
                record.record_number,
                record.record_length,
            )
            packet += record.record_data
        return packet

    def decode(self, data: bytes) -> None:
        """Decode the incoming request."""
        byte_count = int(data[0])
        count, self.records = 1, []
        while count < byte_count:
            decoded = struct.unpack(">BHHH", data[count : count + 7])
            calc_length = decoded[3] * 2
            count += calc_length + 7
            record = FileRecord(
                file_number=decoded[1],
                record_number=decoded[2],
                record_data=data[count - calc_length : count],
            )
            record.record_length = decoded[3]
            self.records.append(record)

    def get_response_pdu_size(self) -> int:
        """Get response pdu size.

        Func_code (1 byte) + Quantity of record (each 7 bytes),
        """
        return 1 + 7 * len(self.records)

    async def update_datastore(self, _context: ModbusSlaveContext) -> ModbusPDU:
        """Run the write file record request against the context."""
        return WriteFileRecordResponse(records=self.records, dev_id=self.dev_id, transaction_id=self.transaction_id)


class WriteFileRecordResponse(ModbusPDU):
    """The normal response is an echo of the request."""

    function_code = 0x15
    rtu_byte_count_pos = 2

    def __init__(self, records: list[FileRecord] | None = None, dev_id: int = 1, transaction_id: int = 0) -> None:
        """Initialize a new instance."""
        super().__init__(transaction_id=transaction_id, dev_id=dev_id)
        self.records: list[FileRecord] = records or []

    def encode(self) -> bytes:
        """Encode the response."""
        total_length = sum((record.record_length * 2) + 7 for record in self.records)
        packet = struct.pack("B", total_length)
        for record in self.records:
            packet += struct.pack(
                ">BHHH",
                0x06,
                record.file_number,
                record.record_number,
                record.record_length,
            )
            packet += record.record_data
        return packet

    def decode(self, data: bytes) -> None:
        """Decode the incoming request."""
        count, self.records = 1, []
        byte_count = int(data[0])
        while count < byte_count:
            decoded = struct.unpack(">BHHH", data[count : count + 7])
            calc_length = decoded[3] * 2
            count += calc_length + 7
            record = FileRecord(
                file_number=decoded[1],
                record_number=decoded[2],
                record_data=data[count - calc_length : count],
            )
            record.record_length = decoded[3]
            self.records.append(record)


class ReadFifoQueueRequest(ModbusPDU):
    """ReadFifoQueueRequest."""

    function_code = 0x18
    rtu_frame_size = 6

    def __init__(self, address: int = 0, dev_id: int = 1, transaction_id: int = 0) -> None:
        """Initialize a new instance."""
        super().__init__(transaction_id=transaction_id, dev_id=dev_id)
        self.address = address
        self.verifyAddress()

    def encode(self) -> bytes:
        """Encode the request packet."""
        return struct.pack(">H", self.address)

    def decode(self, data: bytes) -> None:
        """Decode the incoming request."""
        self.address = struct.unpack(">H", data)[0]

    async def update_datastore(self, _context: ModbusSlaveContext) -> ModbusPDU:
        """Run a read exception status request against the store."""
        values = [0, 1, 2, 3] # server dummy response (should be in datastore)
        return ReadFifoQueueResponse(values=values, dev_id=self.dev_id, transaction_id=self.transaction_id)


class ReadFifoQueueResponse(ModbusPDU):
    """ReadFifoQueueResponse."""

    function_code = 0x18

    @classmethod
    def calculateRtuFrameSize(cls, buffer: bytes) -> int:
        """Calculate the size of the message."""
        hi_byte = int(buffer[2])
        lo_byte = int(buffer[3])
        return (hi_byte << 16) + lo_byte + 6

    def __init__(self, values: list[int] | None = None, dev_id: int = 1, transaction_id:int  = 0) -> None:
        """Initialize a new instance."""
        super().__init__(transaction_id=transaction_id, dev_id=dev_id)
        self.values = values or []

    def encode(self) -> bytes:
        """Encode the response."""
        length = len(self.values) * 2
        packet = struct.pack(">HH", 2 + length, length)
        for value in self.values:
            packet += struct.pack(">H", value)
        return packet

    def decode(self, data: bytes) -> None:
        """Decode a the response."""
        self.values = []
        _, count = struct.unpack(">HH", data[0:4])
        for index in range(0, count - 4):
            idx = 4 + index * 2
            self.values.append(struct.unpack(">H", data[idx : idx + 2])[0])
