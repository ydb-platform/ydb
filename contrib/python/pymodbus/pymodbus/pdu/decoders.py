"""Modbus Request/Response Decoders."""
from __future__ import annotations

import pymodbus.pdu.bit_message as bit_msg
import pymodbus.pdu.diag_message as diag_msg
import pymodbus.pdu.file_message as file_msg
import pymodbus.pdu.mei_message as mei_msg
import pymodbus.pdu.other_message as o_msg
import pymodbus.pdu.pdu as base
import pymodbus.pdu.register_message as reg_msg
from pymodbus.exceptions import MessageRegisterException, ModbusException
from pymodbus.logging import Log


class DecodePDU:
    """Decode pdu requests/responses (server/client)."""

    _pdu_class_table: set[tuple[type[base.ModbusPDU], type[base.ModbusPDU]]] = {
        (reg_msg.ReadHoldingRegistersRequest, reg_msg.ReadHoldingRegistersResponse),
        (bit_msg.ReadDiscreteInputsRequest, bit_msg.ReadDiscreteInputsResponse),
        (reg_msg.ReadInputRegistersRequest, reg_msg.ReadInputRegistersResponse),
        (bit_msg.ReadCoilsRequest, bit_msg.ReadCoilsResponse),
        (bit_msg.WriteMultipleCoilsRequest, bit_msg.WriteMultipleCoilsResponse),
        (reg_msg.WriteMultipleRegistersRequest, reg_msg.WriteMultipleRegistersResponse),
        (reg_msg.WriteSingleRegisterRequest, reg_msg.WriteSingleRegisterResponse),
        (bit_msg.WriteSingleCoilRequest, bit_msg.WriteSingleCoilResponse),
        (reg_msg.ReadWriteMultipleRegistersRequest, reg_msg.ReadWriteMultipleRegistersResponse),
        (diag_msg.DiagnosticBase, diag_msg.DiagnosticBase),
        (o_msg.ReadExceptionStatusRequest, o_msg.ReadExceptionStatusResponse),
        (o_msg.GetCommEventCounterRequest, o_msg.GetCommEventCounterResponse),
        (o_msg.GetCommEventLogRequest, o_msg.GetCommEventLogResponse),
        (o_msg.ReportSlaveIdRequest, o_msg.ReportSlaveIdResponse),
        (file_msg.ReadFileRecordRequest, file_msg.ReadFileRecordResponse),
        (file_msg.WriteFileRecordRequest, file_msg.WriteFileRecordResponse),
        (reg_msg.MaskWriteRegisterRequest, reg_msg.MaskWriteRegisterResponse),
        (file_msg.ReadFifoQueueRequest, file_msg.ReadFifoQueueResponse),
        (mei_msg.ReadDeviceInformationRequest, mei_msg.ReadDeviceInformationResponse),
    }

    _pdu_sub_class_table: set[tuple[type[base.ModbusPDU], type[base.ModbusPDU]]] = {
        (diag_msg.ReturnQueryDataRequest, diag_msg.ReturnQueryDataResponse),
        (diag_msg.RestartCommunicationsOptionRequest, diag_msg.RestartCommunicationsOptionResponse),
        (diag_msg.ReturnDiagnosticRegisterRequest, diag_msg.ReturnDiagnosticRegisterResponse),
        (diag_msg.ChangeAsciiInputDelimiterRequest, diag_msg.ChangeAsciiInputDelimiterResponse),
        (diag_msg.ForceListenOnlyModeRequest, diag_msg.ForceListenOnlyModeResponse),
        (diag_msg.ClearCountersRequest, diag_msg.ClearCountersResponse),
        (diag_msg.ReturnBusMessageCountRequest, diag_msg.ReturnBusMessageCountResponse),
        (diag_msg.ReturnBusCommunicationErrorCountRequest, diag_msg.ReturnBusCommunicationErrorCountResponse),
        (diag_msg.ReturnBusExceptionErrorCountRequest, diag_msg.ReturnBusExceptionErrorCountResponse),
        (diag_msg.ReturnSlaveMessageCountRequest, diag_msg.ReturnSlaveMessageCountResponse),
        (diag_msg.ReturnSlaveNoResponseCountRequest, diag_msg.ReturnSlaveNoResponseCountResponse),
        (diag_msg.ReturnSlaveNAKCountRequest, diag_msg.ReturnSlaveNAKCountResponse),
        (diag_msg.ReturnSlaveBusyCountRequest, diag_msg.ReturnSlaveBusyCountResponse),
        (diag_msg.ReturnSlaveBusCharacterOverrunCountRequest, diag_msg.ReturnSlaveBusCharacterOverrunCountResponse),
        (diag_msg.ReturnIopOverrunCountRequest, diag_msg.ReturnIopOverrunCountResponse),
        (diag_msg.ClearOverrunCountRequest, diag_msg.ClearOverrunCountResponse),
        (diag_msg.GetClearModbusPlusRequest, diag_msg.GetClearModbusPlusResponse),
        (mei_msg.ReadDeviceInformationRequest, mei_msg.ReadDeviceInformationResponse),
    }

    def __init__(self, is_server: bool) -> None:
        """Initialize function_tables."""
        inx = 0 if is_server else 1
        self.lookup: dict[int, type[base.ModbusPDU]] = {cl[inx].function_code: cl[inx] for cl in self._pdu_class_table}
        self.sub_lookup: dict[int, dict[int, type[base.ModbusPDU]]] = {}
        for f in self._pdu_sub_class_table:
            if (function_code := f[inx].function_code) not in self.sub_lookup:
                self.sub_lookup[function_code] = {f[inx].sub_function_code: f[inx]}
            else:
                self.sub_lookup[function_code][f[inx].sub_function_code] = f[inx]

    def lookupPduClass(self, data: bytes) -> type[base.ModbusPDU] | None:
        """Use `function_code` to determine the class of the PDU."""
        func_code = int(data[1])
        if func_code & 0x80:
            return base.ExceptionResponse
        if func_code == 0x2B:  # mei message, sub_function_code is 1 byte
            sub_func_code = int(data[2])
            return self.sub_lookup[func_code].get(sub_func_code, None)
        if func_code == 0x08:  # diag message,  sub_function_code is 2 bytes
            sub_func_code = int(data[3])
            return self.sub_lookup[func_code].get(sub_func_code, None)
        return self.lookup.get(func_code, None)

    def register(self, custom_class: type[base.ModbusPDU]) -> None:
        """Register a function and sub function class with the decoder."""
        if not issubclass(custom_class, base.ModbusPDU):
            raise MessageRegisterException(
                f'"{custom_class.__class__.__name__}" is Not a valid Modbus Message'
                ". Class needs to be derived from "
                "`pymodbus.pdu.ModbusPDU` "
            )
        self.lookup[custom_class.function_code] = custom_class
        if custom_class.sub_function_code >= 0:
            if custom_class.function_code not in self.sub_lookup:
                self.sub_lookup[custom_class.function_code] = {}
            self.sub_lookup[custom_class.function_code][
                custom_class.sub_function_code
            ] = custom_class

    def decode(self, frame: bytes) -> base.ModbusPDU | None:
        """Decode a frame."""
        try:
            if (function_code := int(frame[0])) > 0x80:
                pdu_exp = base.ExceptionResponse(function_code & 0x7F)
                pdu_exp.decode(frame[1:])
                return pdu_exp
            if not (pdu_type := self.lookup.get(function_code, None)):
                Log.debug("decode PDU failed for function code {}", function_code)
                raise ModbusException(f"Unknown response {function_code}")
            pdu = pdu_type()
            pdu.decode(frame[1:])
            Log.debug("decoded PDU function_code({} sub {}) -> {} ", pdu.function_code, pdu.sub_function_code, str(pdu))

            if pdu.sub_function_code >= 0:
                lookup = self.sub_lookup.get(pdu.function_code, {})
                if subtype := lookup.get(pdu.sub_function_code, None):
                    pdu.__class__ = subtype
            return pdu
        except (ModbusException, ValueError, IndexError) as exc:
            Log.warning("Unable to decode frame {}", exc)
        return None
