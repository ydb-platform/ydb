"""
Interface for slcan compatible interfaces (win32/linux).
"""

import io
import logging
import time
import warnings
from queue import SimpleQueue
from typing import Any, Optional, Union, cast

from can import BitTiming, BitTimingFd, BusABC, CanProtocol, Message, typechecking
from can.exceptions import (
    CanInitializationError,
    CanInterfaceNotImplementedError,
    CanOperationError,
    error_check,
)
from can.util import (
    CAN_FD_DLC,
    check_or_adjust_timing_clock,
    deprecated_args_alias,
    len2dlc,
)

logger = logging.getLogger(__name__)

try:
    import serial
except ImportError:
    logger.warning(
        "You won't be able to use the slcan can backend without "
        "the serial module installed!"
    )
    serial = None


class slcanBus(BusABC):
    """
    slcan interface
    """

    # the supported bitrates and their commands
    _BITRATES = {
        10000: "S0",
        20000: "S1",
        50000: "S2",
        100000: "S3",
        125000: "S4",
        250000: "S5",
        500000: "S6",
        750000: "S7",
        1000000: "S8",
        83300: "S9",
    }
    _DATA_BITRATES = {
        0: "",
        2000000: "Y2",
        5000000: "Y5",
    }

    _SLEEP_AFTER_SERIAL_OPEN = 2  # in seconds

    _OK = b"\r"
    _ERROR = b"\a"

    LINE_TERMINATOR = b"\r"

    @deprecated_args_alias(
        deprecation_start="4.5.0",
        deprecation_end="5.0.0",
        ttyBaudrate="tty_baudrate",
    )
    def __init__(
        self,
        channel: typechecking.ChannelStr,
        tty_baudrate: int = 115200,
        bitrate: Optional[int] = None,
        timing: Optional[Union[BitTiming, BitTimingFd]] = None,
        sleep_after_open: float = _SLEEP_AFTER_SERIAL_OPEN,
        rtscts: bool = False,
        listen_only: bool = False,
        timeout: float = 0.001,
        **kwargs: Any,
    ) -> None:
        """
        :param str channel:
            port of underlying serial or usb device (e.g. ``/dev/ttyUSB0``, ``COM8``, ...)
            Must not be empty. Can also end with ``@115200`` (or similarly) to specify the baudrate.
        :param int tty_baudrate:
            baudrate of underlying serial or usb device (Ignored if set via the ``channel`` parameter)
        :param bitrate:
            Bitrate in bit/s
        :param timing:
            Optional :class:`~can.BitTiming` instance to use for custom bit timing setting.
            If this argument is set then it overrides the bitrate and btr arguments. The
            `f_clock` value of the timing instance must be set to 8_000_000 (8MHz)
            for standard CAN.
            CAN FD and the :class:`~can.BitTimingFd` class have partial support according to the non-standard
            slcan protocol implementation in the CANABLE 2.0 firmware: currently only data rates of 2M and 5M.
        :param poll_interval:
            Poll interval in seconds when reading messages
        :param sleep_after_open:
            Time to wait in seconds after opening serial connection
        :param rtscts:
            turn hardware handshake (RTS/CTS) on and off
        :param listen_only:
            If True, open interface/channel in listen mode with ``L`` command.
            Otherwise, the (default) ``O`` command is still used. See ``open`` method.
        :param timeout:
            Timeout for the serial or usb device in seconds (default 0.001)

        :raise ValueError: if both ``bitrate`` and ``btr`` are set or the channel is invalid
        :raise CanInterfaceNotImplementedError: if the serial module is missing
        :raise CanInitializationError: if the underlying serial connection could not be established
        """
        self._listen_only = listen_only

        if serial is None:
            raise CanInterfaceNotImplementedError("The serial module is not installed")

        btr: Optional[str] = kwargs.get("btr", None)
        if btr is not None:
            warnings.warn(
                "The 'btr' argument is deprecated since python-can v4.5.0 "
                "and scheduled for removal in v5.0.0. "
                "Use the 'timing' argument instead.",
                DeprecationWarning,
                stacklevel=1,
            )

        if not channel:  # if None or empty
            raise ValueError("Must specify a serial port.")
        if "@" in channel:
            (channel, baudrate) = channel.split("@")
            tty_baudrate = int(baudrate)

        with error_check(exception_type=CanInitializationError):
            self.serialPortOrig = serial.serial_for_url(
                channel,
                baudrate=tty_baudrate,
                rtscts=rtscts,
                timeout=timeout,
            )

        self._queue: SimpleQueue[str] = SimpleQueue()
        self._buffer = bytearray()
        self._can_protocol = CanProtocol.CAN_20

        time.sleep(sleep_after_open)

        with error_check(exception_type=CanInitializationError):
            if isinstance(timing, BitTiming):
                timing = check_or_adjust_timing_clock(timing, valid_clocks=[8_000_000])
                self.set_bitrate_reg(f"{timing.btr0:02X}{timing.btr1:02X}")
            elif isinstance(timing, BitTimingFd):
                self.set_bitrate(timing.nom_bitrate, timing.data_bitrate)
            else:
                if bitrate is not None and btr is not None:
                    raise ValueError("Bitrate and btr mutually exclusive.")
                if bitrate is not None:
                    self.set_bitrate(bitrate)
                if btr is not None:
                    self.set_bitrate_reg(btr)
            self.open()

        super().__init__(channel, **kwargs)

    def set_bitrate(self, bitrate: int, data_bitrate: Optional[int] = None) -> None:
        """
        :param bitrate:
            Bitrate in bit/s
        :param data_bitrate:
            Data Bitrate in bit/s for FD frames

        :raise ValueError: if ``bitrate`` is not among the possible values
        """
        if bitrate in self._BITRATES:
            bitrate_code = self._BITRATES[bitrate]
        else:
            bitrates = ", ".join(str(k) for k in self._BITRATES.keys())
            raise ValueError(f"Invalid bitrate, choose one of {bitrates}.")

        # If data_bitrate is None, we set it to 0 which means no data bitrate
        if data_bitrate is None:
            data_bitrate = 0

        if data_bitrate in self._DATA_BITRATES:
            dbitrate_code = self._DATA_BITRATES[data_bitrate]
        else:
            dbitrates = ", ".join(str(k) for k in self._DATA_BITRATES.keys())
            raise ValueError(f"Invalid data bitrate, choose one of {dbitrates}.")

        self.close()
        self._write(bitrate_code)
        self._write(dbitrate_code)
        self.open()

    def set_bitrate_reg(self, btr: str) -> None:
        """
        :param btr:
            BTR register value to set custom can speed as a string `xxyy` where
            xx is the BTR0 value in hex and yy is the BTR1 value in hex.
        """
        self.close()
        self._write("s" + btr)
        self.open()

    def _write(self, string: str) -> None:
        with error_check("Could not write to serial device"):
            self.serialPortOrig.write(string.encode() + self.LINE_TERMINATOR)
            self.serialPortOrig.flush()

    def _read(self, timeout: Optional[float]) -> Optional[str]:
        _timeout = serial.Timeout(timeout)

        with error_check("Could not read from serial device"):
            while True:
                # Due to accessing `serialPortOrig.in_waiting` too often will reduce the performance.
                # We read the `serialPortOrig.in_waiting` only once here.
                in_waiting = self.serialPortOrig.in_waiting
                for _ in range(max(1, in_waiting)):
                    new_byte = self.serialPortOrig.read(1)
                    if new_byte:
                        self._buffer.extend(new_byte)
                    else:
                        break

                    if new_byte in (self._ERROR, self._OK):
                        string = self._buffer.decode()
                        self._buffer.clear()
                        return string

                if _timeout.expired():
                    break

            return None

    def flush(self) -> None:
        self._buffer.clear()
        with error_check("Could not flush"):
            self.serialPortOrig.reset_input_buffer()

    def open(self) -> None:
        if self._listen_only:
            self._write("L")
        else:
            self._write("O")

    def close(self) -> None:
        self._write("C")

    def _recv_internal(
        self, timeout: Optional[float]
    ) -> tuple[Optional[Message], bool]:
        canId = None
        remote = False
        extended = False
        data = None
        isFd = False
        fdBrs = False

        if self._queue.qsize():
            string: Optional[str] = self._queue.get_nowait()
        else:
            string = self._read(timeout)

        if not string:
            pass
        elif string[0] in (
            "T",
            "x",  # x is an alternative extended message identifier for CANDapter
        ):
            # extended frame
            canId = int(string[1:9], 16)
            dlc = int(string[9])
            extended = True
            data = bytearray.fromhex(string[10 : 10 + dlc * 2])
        elif string[0] == "t":
            # normal frame
            canId = int(string[1:4], 16)
            dlc = int(string[4])
            data = bytearray.fromhex(string[5 : 5 + dlc * 2])
        elif string[0] == "r":
            # remote frame
            canId = int(string[1:4], 16)
            dlc = int(string[4])
            remote = True
        elif string[0] == "R":
            # remote extended frame
            canId = int(string[1:9], 16)
            dlc = int(string[9])
            extended = True
            remote = True
        elif string[0] == "d":
            # FD standard frame
            canId = int(string[1:4], 16)
            dlc = int(string[4], 16)
            isFd = True
            data = bytearray.fromhex(string[5 : 5 + CAN_FD_DLC[dlc] * 2])
        elif string[0] == "D":
            # FD extended frame
            canId = int(string[1:9], 16)
            dlc = int(string[9], 16)
            extended = True
            isFd = True
            data = bytearray.fromhex(string[10 : 10 + CAN_FD_DLC[dlc] * 2])
        elif string[0] == "b":
            # FD with bitrate switch
            canId = int(string[1:4], 16)
            dlc = int(string[4], 16)
            isFd = True
            fdBrs = True
            data = bytearray.fromhex(string[5 : 5 + CAN_FD_DLC[dlc] * 2])
        elif string[0] == "B":
            # FD extended with bitrate switch
            canId = int(string[1:9], 16)
            dlc = int(string[9], 16)
            extended = True
            isFd = True
            fdBrs = True
            data = bytearray.fromhex(string[10 : 10 + CAN_FD_DLC[dlc] * 2])

        if canId is not None:
            msg = Message(
                arbitration_id=canId,
                is_extended_id=extended,
                timestamp=time.time(),  # Better than nothing...
                is_remote_frame=remote,
                is_fd=isFd,
                bitrate_switch=fdBrs,
                dlc=CAN_FD_DLC[dlc],
                data=data,
            )
            return msg, False
        return None, False

    def send(self, msg: Message, timeout: Optional[float] = None) -> None:
        if timeout != self.serialPortOrig.write_timeout:
            self.serialPortOrig.write_timeout = timeout
        if msg.is_remote_frame:
            if msg.is_extended_id:
                sendStr = f"R{msg.arbitration_id:08X}{msg.dlc:d}"
            else:
                sendStr = f"r{msg.arbitration_id:03X}{msg.dlc:d}"
        elif msg.is_fd:
            fd_dlc = len2dlc(msg.dlc)
            if msg.bitrate_switch:
                if msg.is_extended_id:
                    sendStr = f"B{msg.arbitration_id:08X}{fd_dlc:X}"
                else:
                    sendStr = f"b{msg.arbitration_id:03X}{fd_dlc:X}"
                sendStr += msg.data.hex().upper()
            else:
                if msg.is_extended_id:
                    sendStr = f"D{msg.arbitration_id:08X}{fd_dlc:X}"
                else:
                    sendStr = f"d{msg.arbitration_id:03X}{fd_dlc:X}"
                sendStr += msg.data.hex().upper()
        else:
            if msg.is_extended_id:
                sendStr = f"T{msg.arbitration_id:08X}{msg.dlc:d}"
            else:
                sendStr = f"t{msg.arbitration_id:03X}{msg.dlc:d}"
            sendStr += msg.data.hex().upper()
        self._write(sendStr)

    def shutdown(self) -> None:
        super().shutdown()
        self.close()
        with error_check("Could not close serial socket"):
            self.serialPortOrig.close()

    def fileno(self) -> int:
        try:
            return cast("int", self.serialPortOrig.fileno())
        except io.UnsupportedOperation:
            raise NotImplementedError(
                "fileno is not implemented using current CAN bus on this platform"
            ) from None
        except Exception as exception:
            raise CanOperationError("Cannot fetch fileno") from exception

    def get_version(
        self, timeout: Optional[float]
    ) -> tuple[Optional[int], Optional[int]]:
        """Get HW and SW version of the slcan interface.

        :param timeout:
            seconds to wait for version or None to wait indefinitely

        :returns: tuple (hw_version, sw_version)
            WHERE
            int hw_version is the hardware version or None on timeout
            int sw_version is the software version or None on timeout
        """
        _timeout = serial.Timeout(timeout)
        cmd = "V"
        self._write(cmd)

        while True:
            if string := self._read(_timeout.time_left()):
                if string[0] == cmd:
                    # convert ASCII coded version
                    hw_version = int(string[1:3])
                    sw_version = int(string[3:5])
                    return hw_version, sw_version
                else:
                    self._queue.put_nowait(string)
            if _timeout.expired():
                break
        return None, None

    def get_serial_number(self, timeout: Optional[float]) -> Optional[str]:
        """Get serial number of the slcan interface.

        :param timeout:
            seconds to wait for serial number or :obj:`None` to wait indefinitely

        :return:
            :obj:`None` on timeout or a :class:`str` object.
        """
        _timeout = serial.Timeout(timeout)
        cmd = "N"
        self._write(cmd)

        while True:
            if string := self._read(_timeout.time_left()):
                if string[0] == cmd:
                    serial_number = string[1:-1]
                    return serial_number
                else:
                    self._queue.put_nowait(string)
            if _timeout.expired():
                break
        return None
