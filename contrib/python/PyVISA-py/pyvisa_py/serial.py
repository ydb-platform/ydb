# -*- coding: utf-8 -*-
"""Serial Session implementation using PySerial.


:copyright: 2014-2024 by PyVISA-py Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import sys
from typing import Any, List, Tuple

from pyvisa import attributes, constants, rname
from pyvisa.constants import (
    BufferOperation,
    ResourceAttribute,
    SerialTermination,
    StatusCode,
)

from . import common
from .common import LOGGER
from .sessions import Session, UnknownAttribute

try:
    import serial
    from serial.tools.list_ports import comports
except ImportError as e:
    Session.register_unavailable(
        constants.InterfaceType.asrl,
        "INSTR",
        "Please install PySerial (>=3.0) to use this resource type.\n%s" % e,
    )
    raise

IS_WIN = sys.platform == "win32"


def to_state(boolean_input: bool) -> constants.LineState:
    """Convert a boolean input into a LineState value."""
    if boolean_input:
        return constants.LineState.asserted
    return constants.LineState.unasserted


@Session.register(constants.InterfaceType.asrl, "INSTR")
class SerialSession(Session):
    """A serial Session that uses PySerial to do the low level communication."""

    # Override parsed to take into account the fact that this class is only used
    # for a specific kind of resource
    parsed: rname.ASRLInstr

    @staticmethod
    def list_resources() -> List[str]:
        return [
            "ASRL%s::INSTR" % (port[0][3:] if IS_WIN else port[0])
            for port in comports()
        ]

    @classmethod
    def get_low_level_info(cls) -> str:
        try:
            ver = serial.VERSION
        except AttributeError:
            ver = "N/A"

        return "via PySerial (%s)" % ver

    def after_parsing(self) -> None:
        self.interface = serial.serial_for_url(
            ("COM" if IS_WIN else "") + self.parsed.board,
            timeout=self.timeout,
            write_timeout=self.timeout,
        )

        for name in (
            "ASRL_END_IN",
            "ASRL_END_OUT",
            "SEND_END_EN",
            "TERMCHAR",
            "TERMCHAR_EN",
            "SUPPRESS_END_EN",
        ):
            attribute = getattr(constants, "VI_ATTR_" + name)
            self.attrs[attribute] = attributes.AttributesByID[attribute].default

    def _get_timeout(self, attribute: ResourceAttribute) -> Tuple[int, StatusCode]:
        if self.interface:
            self.timeout = self.interface.timeout
        return super(SerialSession, self)._get_timeout(attribute)

    def _set_timeout(self, attribute: ResourceAttribute, value: int) -> StatusCode:
        status = super(SerialSession, self)._set_timeout(attribute, value)
        if self.interface:
            self.interface.timeout = self.timeout
            self.interface.write_timeout = self.timeout
        return status

    def close(self) -> StatusCode:
        self.interface.close()
        return StatusCode.success

    def read(self, count: int) -> Tuple[bytes, StatusCode]:
        """Reads data from device or interface synchronously.

        Corresponds to viRead function of the VISA library.

        Parameters
        -----------
        count : int
            Number of bytes to be read.

        Returns
        -------
        bytes
            Data read from the device
        StatusCode
            Return value of the library call.

        """
        end_in, _ = self.get_attribute(ResourceAttribute.asrl_end_in)
        suppress_end_en, _ = self.get_attribute(ResourceAttribute.suppress_end_enabled)

        reader = lambda: self.interface.read(1)  # noqa: E731

        if end_in == SerialTermination.none:
            checker = lambda current: False  # noqa: E731

        elif end_in == SerialTermination.last_bit:
            mask = 2**self.interface.bytesize
            checker = lambda current: bool(current[-1] & mask)  # noqa: E731

        elif end_in == SerialTermination.termination_char:
            end_char, _ = self.get_attribute(ResourceAttribute.termchar)

            checker = lambda current: current[-1] == end_char  # noqa: E731

        else:
            raise ValueError("Unknown value for VI_ATTR_ASRL_END_IN: %s" % end_in)

        return self._read(
            reader,
            count,
            checker,
            suppress_end_en,
            None,
            False,
            serial.SerialTimeoutException,
        )

    def write(self, data: bytes) -> Tuple[int, StatusCode]:
        """Writes data to device or interface synchronously.

        Corresponds to viWrite function of the VISA library.

        Parameters
        ----------
        data : bytes
            Data to be written.

        Returns
        -------
        int
            Number of bytes actually transferred
        StatusCode
            Return value of the library call.

        """
        LOGGER.debug("Serial.write %r" % data)
        send_end, _ = self.get_attribute(ResourceAttribute.send_end_enabled)
        end_out, _ = self.get_attribute(ResourceAttribute.asrl_end_out)
        data_bits, _ = self.get_attribute(constants.ResourceAttribute.asrl_data_bits)

        if end_out == SerialTermination.none:
            pass
        elif end_out == SerialTermination.last_bit:
            data = b"".join(common.iter_bytes(data, data_bits, send_end))
        elif end_out == SerialTermination.termination_char:
            term_char, _ = self.get_attribute(ResourceAttribute.termchar)
            data = b"".join(common.iter_bytes(data, data_bits, send_end=None))
            data = data + common.int_to_byte(term_char)
        elif end_out == SerialTermination.termination_break:
            data = b"".join(common.iter_bytes(data, data_bits, send_end=None))
        else:
            raise ValueError("Unknown value for VI_ATTR_ASRL_END_OUT: %s" % end_out)

        try:
            count = self.interface.write(data)

            if end_out == SerialTermination.termination_break:
                LOGGER.debug("Serial.sendBreak")
                self.interface.sendBreak()

            return count, StatusCode.success

        except serial.SerialTimeoutException:
            return 0, StatusCode.error_timeout

    def flush(self, mask: BufferOperation) -> StatusCode:
        """Flush the specified buffers.

        The buffers can be associated with formatted I/O operations and/or
        serial communication.

        Corresponds to viFlush function of the VISA library.

        Parameters
        ----------
        mask : constants.BufferOperation
            Specifies the action to be taken with flushing the buffer.
            The values can be combined using the | operator. However multiple
            operations on a single buffer cannot be combined.

        Returns
        -------
        constants.StatusCode
            Return value of the library call.

        """
        if mask & BufferOperation.discard_read_buffer:
            self.interface.reset_input_buffer()
        if (
            mask & BufferOperation.flush_write_buffer
            or mask & BufferOperation.flush_transmit_buffer
        ):
            self.interface.flush()
        if (
            mask & BufferOperation.discard_write_buffer
            or mask & BufferOperation.discard_transmit_buffer
        ):
            self.interface.reset_output_buffer()

        return StatusCode.success

    def _get_attribute(  # noqa: C901
        self, attribute: constants.ResourceAttribute
    ) -> Tuple[Any, StatusCode]:
        """Get the value for a given VISA attribute for this session.

        Use to implement custom logic for attributes.

        Parameters
        ----------
        attribute : ResourceAttribute
            Attribute for which the state query is made

        Returns
        -------
        Any
            State of the queried attribute for a specified resource
        StatusCode
            Return value of the library call.

        """
        if attribute == constants.VI_ATTR_ASRL_ALLOW_TRANSMIT:
            raise NotImplementedError

        elif attribute == constants.VI_ATTR_ASRL_AVAIL_NUM:
            return self.interface.inWaiting(), StatusCode.success

        elif attribute == constants.VI_ATTR_ASRL_BAUD:
            return self.interface.baudrate, StatusCode.success

        elif attribute == constants.VI_ATTR_ASRL_BREAK_LEN:
            raise NotImplementedError

        elif attribute == constants.VI_ATTR_ASRL_BREAK_STATE:
            raise NotImplementedError

        elif attribute == constants.VI_ATTR_ASRL_CONNECTED:
            raise NotImplementedError

        elif attribute == constants.VI_ATTR_ASRL_CTS_STATE:
            return to_state(self.interface.getCTS()), StatusCode.success

        elif attribute == constants.VI_ATTR_ASRL_DATA_BITS:
            return self.interface.bytesize, StatusCode.success

        elif attribute == constants.VI_ATTR_ASRL_DCD_STATE:
            raise NotImplementedError

        elif attribute == constants.VI_ATTR_ASRL_DISCARD_NULL:
            raise NotImplementedError

        elif attribute == constants.VI_ATTR_ASRL_DSR_STATE:
            return to_state(self.interface.getDSR()), StatusCode.success

        elif attribute == constants.VI_ATTR_ASRL_DTR_STATE:
            raise NotImplementedError

        elif attribute == constants.VI_ATTR_ASRL_FLOW_CNTRL:
            return (
                (
                    self.interface.xonxoff * constants.VI_ASRL_FLOW_XON_XOFF
                    | self.interface.rtscts * constants.VI_ASRL_FLOW_RTS_CTS
                    | self.interface.dsrdtr * constants.VI_ASRL_FLOW_DTR_DSR
                ),
                StatusCode.success,
            )

        elif attribute == constants.VI_ATTR_ASRL_PARITY:
            parity = self.interface.parity
            if parity == serial.PARITY_NONE:
                return constants.Parity.none, StatusCode.success
            elif parity == serial.PARITY_EVEN:
                return constants.Parity.even, StatusCode.success
            elif parity == serial.PARITY_ODD:
                return constants.Parity.odd, StatusCode.success
            elif parity == serial.PARITY_MARK:
                return constants.Parity.mark, StatusCode.success
            elif parity == serial.PARITY_SPACE:
                return constants.Parity.space, StatusCode.success

            raise Exception("Unknown parity value: %r" % parity)

        elif attribute == constants.VI_ATTR_ASRL_RI_STATE:
            raise NotImplementedError

        elif attribute == constants.VI_ATTR_ASRL_RTS_STATE:
            raise NotImplementedError

        elif attribute == constants.VI_ATTR_ASRL_STOP_BITS:
            bits = self.interface.stopbits
            if bits == serial.STOPBITS_ONE:
                return constants.StopBits.one, StatusCode.success
            elif bits == serial.STOPBITS_ONE_POINT_FIVE:
                return constants.StopBits.one_and_a_half, StatusCode.success
            elif bits == serial.STOPBITS_TWO:
                return constants.StopBits.two, StatusCode.success

            raise Exception("Unknown bits value: %r" % bits)

        elif attribute == constants.VI_ATTR_ASRL_XOFF_CHAR:
            raise NotImplementedError

        elif attribute == constants.VI_ATTR_INTF_TYPE:
            return constants.InterfaceType.asrl, StatusCode.success

        raise UnknownAttribute(attribute)

    def _set_attribute(  # noqa: C901
        self, attribute: constants.ResourceAttribute, attribute_state: Any
    ) -> StatusCode:
        """Sets the state of an attribute.

        Corresponds to viSetAttribute function of the VISA library.

        Parameters
        ----------
        attribute : constants.ResourceAttribute
            Attribute for which the state is to be modified. (Attributes.*)
        attribute_state : Any
            The state of the attribute to be set for the specified object.

        Returns
        -------
        StatusCode
            Return value of the library call.

        """
        if attribute == constants.VI_ATTR_ASRL_ALLOW_TRANSMIT:
            raise NotImplementedError

        elif attribute == constants.VI_ATTR_ASRL_BAUD:
            self.interface.baudrate = attribute_state
            return StatusCode.success

        elif attribute == constants.VI_ATTR_ASRL_BREAK_LEN:
            raise NotImplementedError

        elif attribute == constants.VI_ATTR_ASRL_BREAK_STATE:
            raise NotImplementedError

        elif attribute == constants.VI_ATTR_ASRL_CONNECTED:
            raise NotImplementedError

        elif attribute == constants.VI_ATTR_ASRL_DATA_BITS:
            self.interface.bytesize = attribute_state
            return StatusCode.success

        elif attribute == constants.VI_ATTR_ASRL_DCD_STATE:
            raise NotImplementedError

        elif attribute == constants.VI_ATTR_ASRL_DISCARD_NULL:
            raise NotImplementedError

        elif attribute == constants.VI_ATTR_ASRL_DSR_STATE:
            raise NotImplementedError

        elif attribute == constants.VI_ATTR_ASRL_DTR_STATE:
            raise NotImplementedError

        elif attribute == constants.VI_ATTR_ASRL_FLOW_CNTRL:
            if not isinstance(attribute_state, int):
                return StatusCode.error_nonsupported_attribute_state

            if not 0 <= attribute_state < 8:
                return StatusCode.error_nonsupported_attribute_state

            try:
                self.interface.xonxoff = bool(
                    attribute_state & constants.VI_ASRL_FLOW_XON_XOFF
                )
                self.interface.rtscts = bool(
                    attribute_state & constants.VI_ASRL_FLOW_RTS_CTS
                )
                self.interface.dsrdtr = bool(
                    attribute_state & constants.VI_ASRL_FLOW_DTR_DSR
                )
                return StatusCode.success
            except Exception:
                return StatusCode.error_nonsupported_attribute_state

        elif attribute == constants.VI_ATTR_ASRL_PARITY:
            if attribute_state == constants.Parity.none:
                self.interface.parity = serial.PARITY_NONE
                return StatusCode.success

            elif attribute_state == constants.Parity.even:
                self.interface.parity = serial.PARITY_EVEN
                return StatusCode.success

            elif attribute_state == constants.Parity.odd:
                self.interface.parity = serial.PARITY_ODD
                return StatusCode.success

            elif attribute_state == serial.PARITY_MARK:
                self.interface.parity = serial.PARITY_MARK
                return StatusCode.success

            elif attribute_state == constants.Parity.space:
                self.interface.parity = serial.PARITY_SPACE
                return StatusCode.success

            return StatusCode.error_nonsupported_attribute_state

        elif attribute == constants.VI_ATTR_ASRL_RI_STATE:
            raise NotImplementedError

        elif attribute == constants.VI_ATTR_ASRL_RTS_STATE:
            raise NotImplementedError

        elif attribute == constants.VI_ATTR_ASRL_STOP_BITS:
            if attribute_state == constants.StopBits.one:
                self.interface.stopbits = serial.STOPBITS_ONE
                return StatusCode.success

            if attribute_state == constants.StopBits.one_and_a_half:
                self.interface.stopbits = serial.STOPBITS_ONE_POINT_FIVE
                return StatusCode.success

            if attribute_state == constants.StopBits.two:
                self.interface.stopbits = serial.STOPBITS_TWO
                return StatusCode.success

            return StatusCode.error_nonsupported_attribute_state

        elif attribute == constants.VI_ATTR_ASRL_XOFF_CHAR:
            raise NotImplementedError

        raise UnknownAttribute(attribute)
