"""Modbus Client Common."""
from __future__ import annotations

import struct
from abc import abstractmethod
from enum import Enum
from typing import Generic, Literal, TypeVar, cast

import pymodbus.pdu.bit_message as pdu_bit
import pymodbus.pdu.diag_message as pdu_diag
import pymodbus.pdu.file_message as pdu_file_msg
import pymodbus.pdu.mei_message as pdu_mei
import pymodbus.pdu.other_message as pdu_other_msg
import pymodbus.pdu.register_message as pdu_reg
from pymodbus.constants import ModbusStatus
from pymodbus.exceptions import ModbusException
from pymodbus.pdu.pdu import ModbusPDU, pack_bitstring, unpack_bitstring


T = TypeVar("T", covariant=False)


class ModbusClientMixin(Generic[T]):  # pylint: disable=too-many-public-methods
    """**ModbusClientMixin**.

    This is an interface class to facilitate the sending requests/receiving responses like read_coils.
    execute() allows to make a call with non-standard or user defined function codes (remember to add a PDU
    in the transport class to interpret the request/response).

    Simple modbus message call::

        response = client.read_coils(1, 10)
        # or
        response = await client.read_coils(1, 10)

    Advanced modbus message call::

        request = ReadCoilsRequest(1,10)
        response = client.execute(False, request)
        # or
        request = ReadCoilsRequest(1,10)
        response = await client.execute(False, request)

    .. tip::
        All methods can be used directly (synchronous) or
        with await <method> (asynchronous) depending on the client used.
    """

    def __init__(self):
        """Initialize."""

    @abstractmethod
    def execute(self, no_response_expected: bool, request: ModbusPDU) -> T:
        """Execute request."""

    def read_coils(self, address: int, *, count: int = 1, slave: int = 1, no_response_expected: bool = False) -> T:
        """Read coils (code 0x01).

        :param address: Start address to read from
        :param count: (optional) Number of coils to read
        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        reads from 1 to 2000 contiguous in a remote device (slave).

        Coils are addressed as 0-N (Note some device manuals uses 1-N, assuming 1==0).
        """
        return self.execute(no_response_expected, pdu_bit.ReadCoilsRequest(address=address, count=count, dev_id=slave))

    def read_discrete_inputs(self,
                             address: int,
                             *,
                             count: int = 1,
                             slave: int = 1,
                             no_response_expected: bool = False) -> T:
        """Read discrete inputs (code 0x02).

        :param address: Start address to read from
        :param count: (optional) Number of coils to read
        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        read from 1 to 2000(0x7d0) discrete inputs (bits) in a remote device.

        Discrete Inputs are addressed as 0-N (Note some device manuals uses 1-N, assuming 1==0).
        """
        pdu = pdu_bit.ReadDiscreteInputsRequest(address=address, count=count, dev_id=slave)
        return self.execute(no_response_expected, pdu)

    def read_holding_registers(self,
                               address: int,
                               *,
                               count: int = 1,
                               slave: int = 1,
                               no_response_expected: bool = False) -> T:
        """Read holding registers (code 0x03).

        :param address: Start address to read from
        :param count: (optional) Number of registers to read
        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        This function is used to read the contents of a contiguous block
        of holding registers in a remote device. The Request specifies the
        starting register address and the number of registers.

        Registers are addressed starting at zero.
        Therefore devices that specify 1-16 are addressed as 0-15.
        """
        return self.execute(no_response_expected, pdu_reg.ReadHoldingRegistersRequest(address=address, count=count, dev_id=slave))

    def read_input_registers(self,
                             address: int,
                             *,
                             count: int = 1,
                             slave: int = 1,
                             no_response_expected: bool = False) -> T:
        """Read input registers (code 0x04).

        :param address: Start address to read from
        :param count: (optional) Number of coils to read
        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        This function is used to read from 1 to approx. 125 contiguous
        input registers in a remote device. The Request specifies the
        starting register address and the number of registers.

        Registers are addressed starting at zero.
        Therefore devices that specify 1-16 are addressed as 0-15.
        """
        return self.execute(no_response_expected, pdu_reg.ReadInputRegistersRequest(address=address, count=count, dev_id=slave))

    def write_coil(self, address: int, value: bool, *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Write single coil (code 0x05).

        :param address: Address to write to
        :param value: Boolean to write
        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        write ON/OFF to a single coil in a remote device.

        Coils are addressed as 0-N (Note some device manuals uses 1-N, assuming 1==0).
        """
        pdu = pdu_bit.WriteSingleCoilRequest(address=address, bits=[value], dev_id=slave)
        return self.execute(no_response_expected, pdu)

    def write_register(self, address: int, value: int, *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Write register (code 0x06).

        :param address: Address to write to
        :param value: Value to write
        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        This function is used to write a single holding register in a remote device.

        The Request specifies the address of the register to be written.

        Registers are addressed starting at zero. Therefore register
        numbered 1 is addressed as 0.
        """
        return self.execute(no_response_expected, pdu_reg.WriteSingleRegisterRequest(address=address, registers=[value], dev_id=slave))

    def read_exception_status(self, *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Read Exception Status (code 0x07).

        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        This function is used to read the contents of eight Exception Status outputs in a remote device.

        The function provides a simple method for
        accessing this information, because the Exception Output references are
        known (no output reference is needed in the function).
        """
        return self.execute(no_response_expected, pdu_other_msg.ReadExceptionStatusRequest(dev_id=slave))

    def diag_query_data(self, msg: bytes, *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Diagnose query data (code 0x08 sub 0x00).

        :param msg: Message to be returned
        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        The data passed in the request data field is to be returned (looped back)
        in the response. The entire response message should be identical to the
        request.
        """
        return self.execute(no_response_expected, pdu_diag.ReturnQueryDataRequest(msg, dev_id=slave))

    def diag_restart_communication(self, toggle: bool, *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Diagnose restart communication (code 0x08 sub 0x01).

        :param toggle: True if toggled.
        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        The remote device serial line port must be initialized and restarted, and
        all of its communications event counters are cleared. If the port is
        currently in Listen Only Mode, no response is returned. This function is
        the only one that brings the port out of Listen Only Mode. If the port is
        not currently in Listen Only Mode, a normal response is returned. This
        occurs before the restart is update_datastored.
        """
        msg = ModbusStatus.ON if toggle else ModbusStatus.OFF
        return self.execute(no_response_expected, pdu_diag.RestartCommunicationsOptionRequest(message=msg, dev_id=slave))

    def diag_read_diagnostic_register(self, *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Diagnose read diagnostic register (code 0x08 sub 0x02).

        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        The contents of the remote device's 16-bit diagnostic register are returned in the response.
        """
        return self.execute(no_response_expected, pdu_diag.ReturnDiagnosticRegisterRequest(dev_id=slave))

    def diag_change_ascii_input_delimeter(self, *, delimiter: int = 0x0a, slave: int = 1, no_response_expected: bool = False) -> T:
        """Diagnose change ASCII input delimiter (code 0x08 sub 0x03).

        :param delimiter: char to replace LF
        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        The character passed in the request becomes the end of
        message delimiter for future messages (replacing the default LF
        character). This function is useful in cases of a Line Feed is not
        required at the end of ASCII messages.
        """
        return self.execute(no_response_expected, pdu_diag.ChangeAsciiInputDelimiterRequest(message=delimiter, dev_id=slave))

    def diag_force_listen_only(self, *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Diagnose force listen only (code 0x08 sub 0x04).

        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        Forces the addressed remote device to its Listen Only Mode for MODBUS communications.

        This isolates it from the other devices on the network,
        allowing them to continue communicating without interruption from the
        addressed remote device. No response is returned.
        """
        return self.execute(no_response_expected, pdu_diag.ForceListenOnlyModeRequest(dev_id=slave))

    def diag_clear_counters(self, *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Diagnose clear counters (code 0x08 sub 0x0A).

        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        Clear ll counters and the diagnostic register. Also, counters are cleared upon power-up
        """
        return self.execute(no_response_expected, pdu_diag.ClearCountersRequest(dev_id=slave))

    def diag_read_bus_message_count(self, *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Diagnose read bus message count (code 0x08 sub 0x0B).

        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        The response data field returns the quantity of messages that the
        remote device has detected on the communications systems since its last
        restart, clear counters operation, or power-up
        """
        return self.execute(no_response_expected, pdu_diag.ReturnBusMessageCountRequest(dev_id=slave))

    def diag_read_bus_comm_error_count(self, *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Diagnose read Bus Communication Error Count (code 0x08 sub 0x0C).

        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        The response data field returns the quantity of CRC errors encountered
        by the remote device since its last restart, clear counter operation, or
        power-up
        """
        return self.execute(no_response_expected, pdu_diag.ReturnBusCommunicationErrorCountRequest(dev_id=slave))

    def diag_read_bus_exception_error_count(self, *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Diagnose read Bus Exception Error Count (code 0x08 sub 0x0D).

        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        The response data field returns the quantity of modbus exception
        responses returned by the remote device since its last restart,
        clear counters operation, or power-up
        """
        return self.execute(no_response_expected, pdu_diag.ReturnBusExceptionErrorCountRequest(dev_id=slave))

    def diag_read_slave_message_count(self, *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Diagnose read Slave Message Count (code 0x08 sub 0x0E).

        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        The response data field returns the quantity of messages addressed to the
        remote device, that the remote device has processed since
        its last restart, clear counters operation, or power-up
        """
        return self.execute(no_response_expected, pdu_diag.ReturnSlaveMessageCountRequest(dev_id=slave))

    def diag_read_slave_no_response_count(self, *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Diagnose read Slave No Response Count (code 0x08 sub 0x0F).

        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        The response data field returns the quantity of messages addressed to the
        remote device, that the remote device has processed since
        its last restart, clear counters operation, or power-up.
        """
        return self.execute(no_response_expected, pdu_diag.ReturnSlaveNoResponseCountRequest(dev_id=slave))

    def diag_read_slave_nak_count(self, *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Diagnose read Slave NAK Count (code 0x08 sub 0x10).

        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        The response data field returns the quantity of messages addressed to the
        remote device for which it returned a Negative ACKNOWLEDGE (NAK) exception
        response, since its last restart, clear counters operation, or power-up.
        Exception responses are described and listed in section 7 .
        """
        return self.execute(no_response_expected, pdu_diag.ReturnSlaveNAKCountRequest(dev_id=slave))

    def diag_read_slave_busy_count(self, *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Diagnose read Slave Busy Count (code 0x08 sub 0x11).

        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        The response data field returns the quantity of messages addressed to the
        remote device for which it returned a Slave Device Busy exception response,
        since its last restart, clear counters operation, or power-up.
        """
        return self.execute(no_response_expected, pdu_diag.ReturnSlaveBusyCountRequest(dev_id=slave))

    def diag_read_bus_char_overrun_count(self, *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Diagnose read Bus Character Overrun Count (code 0x08 sub 0x12).

        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        The response data field returns the quantity of messages addressed to the
        remote device that it could not handle due to a character overrun condition,
        since its last restart, clear counters operation, or power-up. A character
        overrun is caused by data characters arriving at the port faster than they
        can be stored, or by the loss of a character due to a hardware malfunction.
        """
        return self.execute(no_response_expected, pdu_diag.ReturnSlaveBusCharacterOverrunCountRequest(dev_id=slave))

    def diag_read_iop_overrun_count(self, *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Diagnose read Iop overrun count (code 0x08 sub 0x13).

        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        An IOP overrun is caused by data characters arriving at the port
        faster than they can be stored, or by the loss of a character due
        to a hardware malfunction.  This function is specific to the 884.
        """
        return self.execute(no_response_expected, pdu_diag.ReturnIopOverrunCountRequest(dev_id=slave))

    def diag_clear_overrun_counter(self, *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Diagnose Clear Overrun Counter and Flag (code 0x08 sub 0x14).

        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        An error flag should be cleared, but nothing else in the
        specification mentions is, so it is ignored.
        """
        return self.execute(no_response_expected, pdu_diag.ClearOverrunCountRequest(dev_id=slave))

    def diag_getclear_modbus_response(self, *, data: int = 0, slave: int = 1, no_response_expected: bool = False) -> T:
        """Diagnose Get/Clear modbus plus (code 0x08 sub 0x15).

        :param data: "Get Statistics" or "Clear Statistics"
        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        In addition to the Function code (08) and Subfunction code
        (00 15 hex) in the query, a two-byte Operation field is used
        to specify either a "Get Statistics" or a "Clear Statistics"
        operation.  The two operations are exclusive - the "Get"
        operation cannot clear the statistics, and the "Clear"
        operation does not return statistics prior to clearing
        them. Statistics are also cleared on power-up of the slave
        device.
        """
        return self.execute(no_response_expected, pdu_diag.GetClearModbusPlusRequest(message=data, dev_id=slave))

    def diag_get_comm_event_counter(self, *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Diagnose get event counter (code 0x0B).

        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        This function is used to get a status word and an event count from the remote device.

        By fetching the current count before and after a series of messages, a
        client can determine whether the messages were handled normally by the
        remote device.

        The device's event counter is incremented once for each successful
        message completion. It is not incremented for exception responses,
        poll commands, or fetch event counter commands.

        The event counter can be reset by means of the Diagnostics function
        Restart Communications or Clear Counters and Diagnostic Register.
        """
        return self.execute(no_response_expected, pdu_other_msg.GetCommEventCounterRequest(dev_id=slave))

    def diag_get_comm_event_log(self, *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Diagnose get event counter (code 0x0C).

        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        This function is used to get a status word.

        Event count, message count, and a field of event bytes from the remote device.

        The status word and event counts are identical to that returned by
        the Get Communications Event Counter function.

        The message counter contains the quantity of messages processed by the
        remote device since its last restart, clear counters operation, or
        power-up. This count is identical to that returned by the Diagnostic
        function Return Bus Message Count.

        The event bytes field contains 0-64 bytes, with each byte corresponding
        to the status of one MODBUS send or receive operation for the remote
        device. The remote device enters the events into the field in
        chronological order. Byte 0 is the most recent event. Each new byte
        flushes the oldest byte from the field.
        """
        return self.execute(no_response_expected, pdu_other_msg.GetCommEventLogRequest(dev_id=slave))

    def write_coils(
        self,
        address: int,
        values: list[bool],
        *,
        slave: int = 1,
        no_response_expected: bool = False
    ) -> T:
        """Write coils (code 0x0F).

        :param address: Start address to write to
        :param values: List of booleans to write, or a single boolean to write
        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        write ON/OFF to multiple coils in a remote device.

        Coils are addressed as 0-N (Note some device manuals uses 1-N, assuming 1==0).
        """
        pdu = pdu_bit.WriteMultipleCoilsRequest(address=address, bits=values, dev_id=slave)
        return self.execute(no_response_expected, pdu)

    def write_registers(
        self,
        address: int,
        values: list[int],
        *,
        slave: int = 1,
        no_response_expected: bool = False
    ) -> T:
        """Write registers (code 0x10).

        :param address: Start address to write to
        :param values: List of values to write
        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        This function is used to write a block of contiguous registers
        (1 to approx. 120 registers) in a remote device.
        """
        return self.execute(no_response_expected, pdu_reg.WriteMultipleRegistersRequest(address=address, registers=values,dev_id=slave))

    def report_slave_id(self, *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Report slave ID (code 0x11).

        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        This function is used to read the description of the type, the current status
        and other information specific to a remote device.
        """
        return self.execute(no_response_expected, pdu_other_msg.ReportSlaveIdRequest(dev_id=slave))

    def read_file_record(self, records: list[pdu_file_msg.FileRecord], *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Read file record (code 0x14).

        :param records: List of FileRecord (Reference type, File number, Record Number)
        :param slave: device id
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        This function is used to perform a file record read. All request
        data lengths are provided in terms of number of bytes and all record
        lengths are provided in terms of registers.

        A file is an organization of records. Each file contains 10000 records,
        addressed 0000 to 9999 decimal or 0x0000 to 0x270f. For example, record
        12 is addressed as 12. The function can read multiple groups of
        references. The groups can be separating (non-contiguous), but the
        references within each group must be sequential. Each group is defined
        in a separate "sub-request" field that contains seven bytes::

            The reference type: 1 byte
            The file number: 2 bytes
            The starting record number within the file: 2 bytes
            The length of the record to be read: 2 bytes

        The quantity of registers to be read, combined with all other fields
        in the expected response, must not exceed the allowable length of the
        MODBUS PDU: 235 bytes.
        """
        return self.execute(no_response_expected, pdu_file_msg.ReadFileRecordRequest(records, dev_id=slave))

    def write_file_record(self, records: list[pdu_file_msg.FileRecord], *, slave: int = 1, no_response_expected: bool = False) -> T:
        """Write file record (code 0x15).

        :param records: List of File_record (Reference type, File number, Record Number, Record Length, Record Data)
        :param slave: (optional) Device id
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        This function is used to perform a file record write. All
        request data lengths are provided in terms of number of bytes
        and all record lengths are provided in terms of the number of 16
        bit words.
        """
        return self.execute(no_response_expected, pdu_file_msg.WriteFileRecordRequest(records=records, dev_id=slave))

    def mask_write_register(
        self,
        *,
        address: int = 0x0000,
        and_mask: int = 0xFFFF,
        or_mask: int = 0x0000,
        slave: int = 1,
        no_response_expected: bool = False
    ) -> T:
        """Mask write register (code 0x16).

        :param address: The mask pointer address (0x0000 to 0xffff)
        :param and_mask: The and bitmask to apply to the register address
        :param or_mask: The or bitmask to apply to the register address
        :param slave: (optional) device id
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        This function is used to modify the contents of a specified holding register
        using a combination of an AND mask, an OR mask, and the register's current contents.

        The function can be used to set or clear individual bits in the register.
        """
        return self.execute(no_response_expected, pdu_reg.MaskWriteRegisterRequest(address=address, and_mask=and_mask, or_mask=or_mask, dev_id=slave))

    def readwrite_registers(
        self,
        *,
        read_address: int = 0,
        read_count: int = 0,
        write_address: int = 0,
        address: int | None = None,
        values: list[int] | None = None,
        slave: int = 1,
        no_response_expected: bool = False
    ) -> T:
        """Read/Write registers (code 0x17).

        :param read_address: The address to start reading from
        :param read_count: The number of registers to read from address
        :param write_address: The address to start writing to
        :param address: (optional) use as read/write address
        :param values: List of values to write, or a single value to write
        :param slave: (optional) Modbus slave ID
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        This function performs a combination of one read operation and one
        write operation in a single MODBUS transaction. The write
        operation is performed before the read.

        Holding registers are addressed starting at zero. Therefore holding
        registers 1-16 are addressed in the PDU as 0-15.
        """
        if not values:
            values = []
        if address:
            read_address = address
            write_address = address
        return self.execute(no_response_expected, pdu_reg.ReadWriteMultipleRegistersRequest( read_address=read_address, read_count=read_count, write_address=write_address, write_registers=values,dev_id=slave))

    def read_fifo_queue(self, *, address: int = 0x0000, slave: int = 1, no_response_expected: bool = False) -> T:
        """Read FIFO queue (code 0x18).

        :param address: The address to start reading from
        :param slave: (optional) device id
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        This function allows to read the contents of a First-In-First-Out
        (FIFO) queue of register in a remote device. The function returns a
        count of the registers in the queue, followed by the queued data.
        Up to 32 registers can be read: the count, plus up to 31 queued data
        registers.

        The queue count register is returned first, followed by the queued data
        registers.  The function reads the queue contents, but does not clear
        them.
        """
        return self.execute(no_response_expected, pdu_file_msg.ReadFifoQueueRequest(address, dev_id=slave))

    # code 0x2B sub 0x0D: CANopen General Reference Request and Response, NOT IMPLEMENTED

    def read_device_information(self, *, read_code: int | None = None,
                                object_id: int = 0x00,
                                slave: int = 1,
                                no_response_expected: bool = False) -> T:
        """Read FIFO queue (code 0x2B sub 0x0E).

        :param read_code: The device information read code
        :param object_id: The object to read from
        :param slave: (optional) Device id
        :param no_response_expected: (optional) The client will not expect a response to the request
        :raises ModbusException:

        This function allows reading the identification and additional
        information relative to the physical and functional description of a
        remote device, only.

        The Read Device Identification interface is modeled as an address space
        composed of a set of addressable data elements. The data elements are
        called objects and an object Id identifies them.
        """
        return self.execute(no_response_expected, pdu_mei.ReadDeviceInformationRequest(read_code, object_id, dev_id=slave))

    # ------------------
    # Converter methods
    # ------------------

    class DATATYPE(Enum):
        """Datatype enum (name and internal data), used for convert_* calls."""

        INT16 = ("h", 1)
        UINT16 = ("H", 1)
        INT32 = ("i", 2)
        UINT32 = ("I", 2)
        INT64 = ("q", 4)
        UINT64 = ("Q", 4)
        FLOAT32 = ("f", 2)
        FLOAT64 = ("d", 4)
        STRING = ("s", 0)
        BITS = ("bits", 0)

    @classmethod
    def convert_from_registers(
        cls, registers: list[int], data_type: DATATYPE, word_order: Literal["big", "little"] = "big", string_encoding: str = "utf-8"
    ) -> int | float | str | list[bool] | list[int] | list[float]:
        """Convert registers to int/float/str.

        :param registers: list of registers received from e.g. read_holding_registers()
        :param data_type: data type to convert to
        :param word_order: "big"/"little" order of words/registers
        :param string_encoding: The encoding with which to decode the bytearray, only used when data_type=DATATYPE.STRING
        :returns: scalar or array of "data_type"
        :raises ModbusException: when size of registers is not a multiple of data_type
        :raises ParameterException: when the specified string encoding is not supported
        """
        if not (data_len := data_type.value[1]):
            byte_list = bytearray()
            if word_order == "little":
                registers.reverse()
            for x in registers:
                byte_list.extend(int.to_bytes(x, 2, "big"))
            if data_type == cls.DATATYPE.STRING:
                trailing_nulls_begin = len(byte_list)
                while trailing_nulls_begin > 0 and not byte_list[trailing_nulls_begin - 1]:
                    trailing_nulls_begin -= 1
                byte_list = byte_list[:trailing_nulls_begin]
                return byte_list.decode(string_encoding)
            return unpack_bitstring(byte_list)
        if (reg_len := len(registers)) % data_len:
            raise ModbusException(
                f"Registers illegal size ({len(registers)}) expected multiple of {data_len}!"
            )

        result = []
        for i in range(0, reg_len, data_len):
            regs = registers[i:i+data_len]
            if word_order == "little":
                regs.reverse()
            byte_list = bytearray()
            for x in regs:
                byte_list.extend(int.to_bytes(x, 2, "big"))
            result.append(struct.unpack(f">{data_type.value[0]}", byte_list)[0])
        return result if len(result) != 1 else result[0]

    @classmethod
    def convert_to_registers(
        cls, value: int | float | str | list[bool] | list[int] | list[float], data_type: DATATYPE, word_order: Literal["big", "little"] = "big", string_encoding: str = "utf-8"
    ) -> list[int]:
        """Convert int/float/str to registers (16/32/64 bit).

        :param value: value to be converted
        :param data_type: data type to convert from
        :param word_order: "big"/"little" order of words/registers
        :param string_encoding: The encoding with which to encode the bytearray, only used when data_type=DATATYPE.STRING
        :returns: List of registers, can be used directly in e.g. write_registers()
        :raises TypeError: when there is a mismatch between data_type and value
        :raises ParameterException: when the specified string encoding is not supported
        """
        if data_type == cls.DATATYPE.BITS:
            if not isinstance(value, list):
                raise TypeError(f"Value should be list of bool but is {type(value)}.")
            if (missing := len(value) % 16):
                value = value + [False] * (16 - missing)
            byte_list = pack_bitstring(cast(list[bool], value))
        elif data_type == cls.DATATYPE.STRING:
            if not isinstance(value, str):
                raise TypeError(f"Value should be string but is {type(value)}.")
            byte_list = value.encode(string_encoding)
            if len(byte_list) % 2:
                byte_list += b"\x00"
        else:
            if not isinstance(value, list):
                value = cast(list[int], [value])
            byte_list = bytearray()
            for v in value:
                byte_list.extend(struct.pack(f">{data_type.value[0]}", v))
        regs = [
            int.from_bytes(byte_list[x : x + 2], "big")
            for x in range(0, len(byte_list), 2)
        ]
        if word_order == "little":
            regs.reverse()
        return regs
