"""Diagnostic Record Read/Write."""
from __future__ import annotations

import struct
from typing import cast

from pymodbus.constants import ModbusPlusOperation
from pymodbus.datastore import ModbusSlaveContext
from pymodbus.device import ModbusControlBlock
from pymodbus.pdu.pdu import ModbusPDU, pack_bitstring


_MCB = ModbusControlBlock()


class DiagnosticBase(ModbusPDU):
    """DiagnosticBase."""

    function_code = 0x08
    sub_function_code: int = 9999
    rtu_frame_size = 8

    def __init__(self, message: bytes | int | list | tuple | None = 0, dev_id: int = 1, transaction_id: int = 0) -> None:
        """Initialize a diagnostic request."""
        super().__init__(transaction_id=transaction_id, dev_id=dev_id)
        self.message: bytes | int | list | tuple | None = message

    def encode(self) -> bytes:
        """Encode a diagnostic response."""
        packet = struct.pack(">H", self.sub_function_code)
        if self.message is not None:
            if isinstance(self.message, bytes):
                packet += self.message
                return packet
            if isinstance(self.message, int):
                packet += struct.pack(">H", self.message)
                return packet
            if isinstance(self.message, (list, tuple)):
                for piece in self.message:
                    packet += struct.pack(">H", piece)
                return packet
            raise TypeError(f"UNKNOWN DIAG message type: {type(self.message)}")
        return packet

    def decode(self, data: bytes) -> None:
        """Decode a diagnostic request."""
        (self.sub_function_code, ) = struct.unpack(">H", data[:2])
        data = data[2:]
        if self.sub_function_code == ReturnQueryDataRequest.sub_function_code:
            self.message = data
        elif (data_len := len(data)):
            if data_len % 2:
                data_len += 1
                data += b"0"
            if (word_len := data_len // 2) == 1:
                (self.message,) = struct.unpack(">H", data)
            else:
                self.message = struct.unpack(">" + "H" * word_len, data)

    def get_response_pdu_size(self) -> int:
        """Get response pdu size.

        Func_code (1 byte) + Sub function code (2 byte) + Data (2 * N bytes)
        """
        return 1 + 2 + 2

    async def update_datastore(self, _context: ModbusSlaveContext) -> ModbusPDU:
        """Implement dummy."""
        response = {
            DiagnosticBase.sub_function_code: DiagnosticBase,
            ReturnQueryDataResponse.sub_function_code: ReturnQueryDataResponse,
            RestartCommunicationsOptionResponse.sub_function_code: RestartCommunicationsOptionResponse,
        }[self.sub_function_code]
        return response(message=self.message, dev_id=self.dev_id, transaction_id=self.transaction_id)


class ReturnQueryDataRequest(DiagnosticBase):
    """ReturnQueryDataRequest."""

    sub_function_code = 0x0000


class ReturnQueryDataResponse(DiagnosticBase):
    """ReturnQueryDataResponse."""

    sub_function_code = 0x0000


class RestartCommunicationsOptionRequest(DiagnosticBase):
    """RestartCommunicationsOptionRequest."""

    sub_function_code = 0x0001


class RestartCommunicationsOptionResponse(DiagnosticBase):
    """RestartCommunicationsOptionResponse."""

    sub_function_code = 0x0001


class ReturnDiagnosticRegisterRequest(DiagnosticBase):
    """ReturnDiagnosticRegisterRequest."""

    sub_function_code = 0x0002

    async def update_datastore(self, _context: ModbusSlaveContext) -> ModbusPDU:
        """update_datastore the diagnostic request on the given device."""
        register = pack_bitstring(_MCB.getDiagnosticRegister())
        return ReturnDiagnosticRegisterResponse(message=register, dev_id=self.dev_id, transaction_id=self.transaction_id)


class ReturnDiagnosticRegisterResponse(DiagnosticBase):
    """ReturnDiagnosticRegisterResponse."""

    sub_function_code = 0x0002


class ChangeAsciiInputDelimiterRequest(DiagnosticBase):
    """ChangeAsciiInputDelimiterRequest."""

    sub_function_code = 0x0003

    async def update_datastore(self, _context: ModbusSlaveContext) -> ModbusPDU:
        """update_datastore the diagnostic request on the given device."""
        char = (cast(int, self.message) & 0xFF00) >> 8
        _MCB.Delimiter = char
        return ChangeAsciiInputDelimiterResponse(message=self.message, dev_id=self.dev_id, transaction_id=self.transaction_id)


class ChangeAsciiInputDelimiterResponse(DiagnosticBase):
    """ChangeAsciiInputDelimiterResponse."""

    sub_function_code = 0x0003


class ForceListenOnlyModeRequest(DiagnosticBase):
    """ForceListenOnlyModeRequest."""

    sub_function_code = 0x0004

    async def update_datastore(self, _context: ModbusSlaveContext) -> ModbusPDU:
        """update_datastore the diagnostic request on the given device."""
        _MCB.ListenOnly = True
        return ForceListenOnlyModeResponse(dev_id=self.dev_id, transaction_id=self.transaction_id)


class ForceListenOnlyModeResponse(DiagnosticBase):
    """ForceListenOnlyModeResponse.

    This does not send a response
    """

    sub_function_code = 0x0004

    def __init__(self, dev_id=1, transaction_id=0):
        """Initialize to block a return response."""
        DiagnosticBase.__init__(self, dev_id=dev_id, transaction_id=transaction_id)
        self.message = []


class ClearCountersRequest(DiagnosticBase):
    """ClearCountersRequest."""

    sub_function_code = 0x000A

    async def update_datastore(self, _context: ModbusSlaveContext) -> ModbusPDU:
        """update_datastore the diagnostic request on the given device."""
        _MCB.reset()
        return ClearCountersResponse(dev_id=self.dev_id, transaction_id=self.transaction_id)


class ClearCountersResponse(DiagnosticBase):
    """ClearCountersResponse."""

    sub_function_code = 0x000A


class ReturnBusMessageCountRequest(DiagnosticBase):
    """ReturnBusMessageCountRequest."""

    sub_function_code = 0x000B

    async def update_datastore(self, _context: ModbusSlaveContext) -> ModbusPDU:
        """update_datastore the diagnostic request on the given device."""
        count = _MCB.Counter.BusMessage
        return ReturnBusMessageCountResponse(message=count, dev_id=self.dev_id, transaction_id=self.transaction_id)


class ReturnBusMessageCountResponse(DiagnosticBase):
    """ReturnBusMessageCountResponse."""

    sub_function_code = 0x000B


class ReturnBusCommunicationErrorCountRequest(DiagnosticBase):
    """ReturnBusCommunicationErrorCountRequest."""

    sub_function_code = 0x000C

    async def update_datastore(self, _context: ModbusSlaveContext) -> ModbusPDU:
        """update_datastore the diagnostic request on the given device."""
        count = _MCB.Counter.BusCommunicationError
        return ReturnBusCommunicationErrorCountResponse(message=count, dev_id=self.dev_id, transaction_id=self.transaction_id)


class ReturnBusCommunicationErrorCountResponse(DiagnosticBase):
    """ReturnBusCommunicationErrorCountResponse."""

    sub_function_code = 0x000C


class ReturnBusExceptionErrorCountRequest(DiagnosticBase):
    """ReturnBusExceptionErrorCountRequest."""

    sub_function_code = 0x000D

    async def update_datastore(self, _context: ModbusSlaveContext) -> ModbusPDU:
        """update_datastore the diagnostic request on the given device."""
        count = _MCB.Counter.BusExceptionError
        return ReturnBusExceptionErrorCountResponse(message=count, dev_id=self.dev_id, transaction_id=self.transaction_id)


class ReturnBusExceptionErrorCountResponse(DiagnosticBase):
    """ReturnBusExceptionErrorCountResponse."""

    sub_function_code = 0x000D


class ReturnSlaveMessageCountRequest(DiagnosticBase):
    """ReturnSlaveMessageCountRequest."""

    sub_function_code = 0x000E

    async def update_datastore(self, _context: ModbusSlaveContext) -> ModbusPDU:
        """update_datastore the diagnostic request on the given device."""
        count = _MCB.Counter.SlaveMessage
        return ReturnSlaveMessageCountResponse(message=count, dev_id=self.dev_id, transaction_id=self.transaction_id)


class ReturnSlaveMessageCountResponse(DiagnosticBase):
    """ReturnSlaveMessageCountResponse."""

    sub_function_code = 0x000E


class ReturnSlaveNoResponseCountRequest(DiagnosticBase):
    """ReturnSlaveNoResponseCountRequest."""

    sub_function_code = 0x000F

    async def update_datastore(self, _context: ModbusSlaveContext) -> ModbusPDU:
        """update_datastore the diagnostic request on the given device."""
        count = _MCB.Counter.SlaveNoResponse
        return ReturnSlaveNoResponseCountResponse(message=count, dev_id=self.dev_id, transaction_id=self.transaction_id)


class ReturnSlaveNoResponseCountResponse(DiagnosticBase):
    """ReturnSlaveNoResponseCountResponse."""

    sub_function_code = 0x000F


class ReturnSlaveNAKCountRequest(DiagnosticBase):
    """ReturnSlaveNAKCountRequest."""

    sub_function_code = 0x0010

    async def update_datastore(self, _context: ModbusSlaveContext) -> ModbusPDU:
        """update_datastore the diagnostic request on the given device."""
        count = _MCB.Counter.SlaveNAK
        return ReturnSlaveNAKCountResponse(message=count, dev_id=self.dev_id, transaction_id=self.transaction_id)


class ReturnSlaveNAKCountResponse(DiagnosticBase):
    """ReturnSlaveNAKCountResponse."""

    sub_function_code = 0x0010


class ReturnSlaveBusyCountRequest(DiagnosticBase):
    """ReturnSlaveBusyCountRequest."""

    sub_function_code = 0x0011

    async def update_datastore(self, _context: ModbusSlaveContext) -> ModbusPDU:
        """update_datastore the diagnostic request on the given device."""
        count = _MCB.Counter.SLAVE_BUSY
        return ReturnSlaveBusyCountResponse(message=count, dev_id=self.dev_id, transaction_id=self.transaction_id)


class ReturnSlaveBusyCountResponse(DiagnosticBase):
    """ReturnSlaveBusyCountResponse."""

    sub_function_code = 0x0011


class ReturnSlaveBusCharacterOverrunCountRequest(DiagnosticBase):
    """ReturnSlaveBusCharacterOverrunCountRequest."""

    sub_function_code = 0x0012

    async def update_datastore(self, _context: ModbusSlaveContext) -> ModbusPDU:
        """update_datastore the diagnostic request on the given device."""
        count = _MCB.Counter.BusCharacterOverrun
        return ReturnSlaveBusCharacterOverrunCountResponse(message=count, dev_id=self.dev_id, transaction_id=self.transaction_id)


class ReturnSlaveBusCharacterOverrunCountResponse(DiagnosticBase):
    """ReturnSlaveBusCharacterOverrunCountResponse."""

    sub_function_code = 0x0012


class ReturnIopOverrunCountRequest(DiagnosticBase):
    """ReturnIopOverrunCountRequest."""

    sub_function_code = 0x0013

    async def update_datastore(self, _context: ModbusSlaveContext) -> ModbusPDU:
        """update_datastore the diagnostic request on the given device."""
        count = _MCB.Counter.BusCharacterOverrun
        return ReturnIopOverrunCountResponse(message=count, dev_id=self.dev_id, transaction_id=self.transaction_id)


class ReturnIopOverrunCountResponse(DiagnosticBase):
    """ReturnIopOverrunCountResponse."""

    sub_function_code = 0x0013


class ClearOverrunCountRequest(DiagnosticBase):
    """ClearOverrunCountRequest."""

    sub_function_code = 0x0014

    async def update_datastore(self, _context: ModbusSlaveContext) -> ModbusPDU:
        """update_datastore the diagnostic request on the given device."""
        _MCB.Counter.BusCharacterOverrun = 0x0000
        return ClearOverrunCountResponse(dev_id=self.dev_id, transaction_id=self.transaction_id)


class ClearOverrunCountResponse(DiagnosticBase):
    """ClearOverrunCountResponse."""

    sub_function_code = 0x0014


class GetClearModbusPlusRequest(DiagnosticBase):
    """GetClearModbusPlusRequest."""

    sub_function_code = 0x0015

    def get_response_pdu_size(self):
        """Return size of the respaonse.

        Func_code (1 byte) + Sub function code (2 byte) + Operation (2 byte) + Data (108 bytes)
        """
        data = 2 + 108 if self.message == ModbusPlusOperation.GET_STATISTICS else 0
        return 1 + 2 + 2 + 2 + data

    async def update_datastore(self, _context: ModbusSlaveContext) -> ModbusPDU:
        """update_datastore the diagnostic request on the given device."""
        message: int | list | None = None  # the clear operation does not return info
        if self.message == ModbusPlusOperation.CLEAR_STATISTICS:
            _MCB.Plus.reset()
            message = self.message
        else:
            message = [self.message]
            message += _MCB.Plus.encode()
        return GetClearModbusPlusResponse(message=message, dev_id=self.dev_id, transaction_id=self.transaction_id)

    def encode(self):
        """Encode a diagnostic response."""
        packet = struct.pack(">H", self.sub_function_code)
        packet += struct.pack(">H", self.message)
        return packet


class GetClearModbusPlusResponse(DiagnosticBase):
    """GetClearModbusPlusResponse."""

    sub_function_code = 0x0015
