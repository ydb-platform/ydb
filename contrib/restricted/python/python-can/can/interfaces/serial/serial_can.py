"""
A text based interface. For example use over serial ports like
"/dev/ttyS1" or "/dev/ttyUSB0" on Linux machines or "COM1" on Windows.
The interface is a simple implementation that has been used for
recording CAN traces.

See the interface documentation for the format being used.
"""

import io
import logging
import struct
from collections.abc import Sequence
from typing import Any, Optional, cast

from can import (
    BusABC,
    CanInitializationError,
    CanInterfaceNotImplementedError,
    CanOperationError,
    CanProtocol,
    CanTimeoutError,
    Message,
)
from can.typechecking import AutoDetectedConfig

logger = logging.getLogger("can.serial")

try:
    import serial.tools.list_ports
except ImportError:
    logger.warning(
        "You won't be able to use the serial can backend without "
        "the `pyserial` package installed!"
    )
    serial = None


CAN_ERR_FLAG = 0x20000000
CAN_RTR_FLAG = 0x40000000
CAN_EFF_FLAG = 0x80000000
CAN_ID_MASK_EXT = 0x1FFFFFFF
CAN_ID_MASK_STD = 0x7FF


class SerialBus(BusABC):
    """
    Enable basic can communication over a serial device.

    .. note:: See :meth:`~_recv_internal` for some special semantics.

    """

    def __init__(
        self,
        channel: str,
        baudrate: int = 115200,
        timeout: float = 0.1,
        rtscts: bool = False,
        **kwargs: Any,
    ) -> None:
        """
        :param channel:
            The serial device to open. For example "/dev/ttyS1" or
            "/dev/ttyUSB0" on Linux or "COM1" on Windows systems.

        :param baudrate:
            Baud rate of the serial device in bit/s (default 115200).

            .. warning::
                Some serial port implementations don't care about the baudrate.

        :param timeout:
            Timeout for the serial device in seconds (default 0.1).

        :param rtscts:
            turn hardware handshake (RTS/CTS) on and off

        :raises ~can.exceptions.CanInitializationError:
            If the given parameters are invalid.
        :raises ~can.exceptions.CanInterfaceNotImplementedError:
            If the serial module is not installed.
        """

        if not serial:
            raise CanInterfaceNotImplementedError("the serial module is not installed")

        if not channel:
            raise TypeError("Must specify a serial port.")

        self.channel_info = f"Serial interface: {channel}"
        self._can_protocol = CanProtocol.CAN_20

        try:
            self._ser = serial.serial_for_url(
                channel, baudrate=baudrate, timeout=timeout, rtscts=rtscts
            )
        except ValueError as error:
            raise CanInitializationError(
                "could not create the serial device"
            ) from error

        super().__init__(channel, **kwargs)

    def shutdown(self) -> None:
        """
        Close the serial interface.
        """
        super().shutdown()
        self._ser.close()

    def send(self, msg: Message, timeout: Optional[float] = None) -> None:
        """
        Send a message over the serial device.

        :param msg:
            Message to send.

            .. note:: If the timestamp is a float value it will be converted
                      to an integer.

        :param timeout:
            This parameter will be ignored. The timeout value of the channel is
            used instead.

        """
        # Pack timestamp
        try:
            timestamp = struct.pack("<I", int(msg.timestamp * 1000))
        except struct.error:
            raise ValueError(f"Timestamp is out of range: {msg.timestamp}") from None

        # Pack arbitration ID
        if msg.is_extended_id:
            arbitration_id = msg.arbitration_id & CAN_ID_MASK_EXT
            arbitration_id |= CAN_EFF_FLAG
        else:
            arbitration_id = msg.arbitration_id & CAN_ID_MASK_STD

        if msg.is_error_frame:
            arbitration_id |= CAN_ERR_FLAG
        if msg.is_remote_frame:
            arbitration_id |= CAN_RTR_FLAG

        arbitration_id_bytes = struct.pack("<I", arbitration_id)

        # Assemble message
        byte_msg = bytearray()
        byte_msg.append(0xAA)
        byte_msg += timestamp
        byte_msg.append(msg.dlc)
        byte_msg += arbitration_id_bytes
        byte_msg += msg.data
        byte_msg.append(0xBB)

        # Write to serial device
        try:
            self._ser.write(byte_msg)
        except serial.PortNotOpenError as error:
            raise CanOperationError("writing to closed port") from error
        except serial.SerialTimeoutException as error:
            raise CanTimeoutError() from error

    def _recv_internal(
        self, timeout: Optional[float]
    ) -> tuple[Optional[Message], bool]:
        """
        Read a message from the serial device.

        :param timeout:

            .. warning::
                This parameter will be ignored. The timeout value of the channel is used.

        :returns:
            Received message and :obj:`False` (because no filtering as taken place).
        """
        try:
            rx_byte = self._ser.read()
            if rx_byte and ord(rx_byte) == 0xAA:
                s = self._ser.read(4)
                timestamp = struct.unpack("<I", s)[0]
                dlc = ord(self._ser.read())
                if dlc > 8:
                    raise ValueError("received DLC may not exceed 8 bytes")

                s = self._ser.read(4)
                arbitration_id = struct.unpack("<I", s)[0]
                is_extended_id = bool(arbitration_id & CAN_EFF_FLAG)
                is_error_frame = bool(arbitration_id & CAN_ERR_FLAG)
                is_remote_frame = bool(arbitration_id & CAN_RTR_FLAG)

                if is_extended_id:
                    arbitration_id = arbitration_id & CAN_ID_MASK_EXT
                else:
                    arbitration_id = arbitration_id & CAN_ID_MASK_STD

                data = self._ser.read(dlc)

                delimiter_byte = ord(self._ser.read())
                if delimiter_byte == 0xBB:
                    # received message data okay
                    msg = Message(
                        # TODO: We are only guessing that they are milliseconds
                        timestamp=timestamp / 1000,
                        arbitration_id=arbitration_id,
                        dlc=dlc,
                        data=data,
                        is_extended_id=is_extended_id,
                        is_error_frame=is_error_frame,
                        is_remote_frame=is_remote_frame,
                    )
                    return msg, False

                else:
                    raise CanOperationError(
                        f"invalid delimiter byte while reading message: {delimiter_byte}"
                    )

            else:
                return None, False

        except serial.SerialException as error:
            raise CanOperationError("could not read from serial") from error

    def fileno(self) -> int:
        try:
            return cast("int", self._ser.fileno())
        except io.UnsupportedOperation:
            raise NotImplementedError(
                "fileno is not implemented using current CAN bus on this platform"
            ) from None
        except Exception as exception:
            raise CanOperationError("Cannot fetch fileno") from exception

    @staticmethod
    def _detect_available_configs() -> Sequence[AutoDetectedConfig]:
        configs: list[AutoDetectedConfig] = []
        if serial is None:
            return configs

        for port in serial.tools.list_ports.comports():
            configs.append({"interface": "serial", "channel": port.device})
        return configs
