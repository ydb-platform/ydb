"""
This interface is for Windows only, otherwise use SocketCAN.
"""

import logging
from ctypes import byref
from typing import Optional, Union

from can import (
    BitTiming,
    BitTimingFd,
    BusABC,
    CanInitializationError,
    CanOperationError,
    CanProtocol,
    Message,
)
from can.util import check_or_adjust_timing_clock

from .serial_selector import find_serial_devices
from .usb2canabstractionlayer import (
    IS_ERROR_FRAME,
    IS_ID_TYPE,
    IS_REMOTE_FRAME,
    CanalError,
    CanalMsg,
    Usb2CanAbstractionLayer,
)

# Set up logging
log = logging.getLogger("can.usb2can")


def message_convert_tx(msg):
    message_tx = CanalMsg()

    length = msg.dlc
    message_tx.sizeData = length

    message_tx.id = msg.arbitration_id

    for i in range(length):
        message_tx.data[i] = msg.data[i]

    message_tx.flags = 0x80000000

    if msg.is_error_frame:
        message_tx.flags |= IS_ERROR_FRAME

    if msg.is_remote_frame:
        message_tx.flags |= IS_REMOTE_FRAME

    if msg.is_extended_id:
        message_tx.flags |= IS_ID_TYPE

    return message_tx


def message_convert_rx(message_rx):
    """convert the message from the CANAL type to pythoncan type"""
    is_extended_id = bool(message_rx.flags & IS_ID_TYPE)
    is_remote_frame = bool(message_rx.flags & IS_REMOTE_FRAME)
    is_error_frame = bool(message_rx.flags & IS_ERROR_FRAME)

    return Message(
        timestamp=message_rx.timestamp,
        is_remote_frame=is_remote_frame,
        is_extended_id=is_extended_id,
        is_error_frame=is_error_frame,
        arbitration_id=message_rx.id,
        dlc=message_rx.sizeData,
        data=message_rx.data[: message_rx.sizeData],
    )


class Usb2canBus(BusABC):
    """Interface to a USB2CAN Bus.

    This interface only works on Windows.
    Please use socketcan on Linux.

    :param channel:
        The device's serial number. If not provided, Windows Management Instrumentation
        will be used to identify the first such device.

    :param bitrate:
        Bitrate of channel in bit/s. Values will be limited to a maximum of 1000 Kb/s.
        Default is 500 Kbs

    :param timing:
        Optional :class:`~can.BitTiming` instance to use for custom bit timing setting.
        If this argument is set then it overrides the bitrate argument. The
        `f_clock` value of the timing instance must be set to 32_000_000 (32MHz)
        for standard CAN.
        CAN FD and the :class:`~can.BitTimingFd` class are not supported.

    :param flags:
        Flags to directly pass to open function of the usb2can abstraction layer.

    :param dll:
        Path to the DLL with the CANAL API to load
        Defaults to 'usb2can.dll'

    :param serial:
        Alias for `channel` that is provided for legacy reasons.
        If both `serial` and `channel` are set, `serial` will be used and
        channel will be ignored.

    """

    def __init__(
        self,
        channel: Optional[str] = None,
        dll: str = "usb2can.dll",
        flags: int = 0x00000008,
        bitrate: int = 500000,
        timing: Optional[Union[BitTiming, BitTimingFd]] = None,
        serial: Optional[str] = None,
        **kwargs,
    ):
        self.can = Usb2CanAbstractionLayer(dll)

        # get the serial number of the device
        device_id = serial or channel

        # search for a serial number if the device_id is None or empty
        if not device_id:
            devices = find_serial_devices()
            if not devices:
                raise CanInitializationError("could not automatically find any device")
            device_id = devices[0]

        self.channel_info = f"USB2CAN device {device_id}"

        if isinstance(timing, BitTiming):
            timing = check_or_adjust_timing_clock(timing, valid_clocks=[32_000_000])
            connector = (
                f"{device_id};"
                "0;"
                f"{timing.tseg1};"
                f"{timing.tseg2};"
                f"{timing.sjw};"
                f"{timing.brp}"
            )
        elif isinstance(timing, BitTimingFd):
            raise NotImplementedError(
                f"CAN FD is not supported by {self.__class__.__name__}."
            )
        else:
            # convert to kb/s and cap: max rate is 1000 kb/s
            baudrate = min(int(bitrate // 1000), 1000)
            connector = f"{device_id};{baudrate}"

        self._can_protocol = CanProtocol.CAN_20
        self.handle = self.can.open(connector, flags)

        super().__init__(channel=channel, **kwargs)

    def send(self, msg, timeout=None):
        tx = message_convert_tx(msg)

        if timeout:
            status = self.can.blocking_send(self.handle, byref(tx), int(timeout * 1000))
        else:
            status = self.can.send(self.handle, byref(tx))

        if status != CanalError.SUCCESS:
            raise CanOperationError("could not send message", error_code=status)

    def _recv_internal(self, timeout):
        messagerx = CanalMsg()

        if timeout == 0:
            status = self.can.receive(self.handle, byref(messagerx))

        else:
            time = 0 if timeout is None else int(timeout * 1000)
            status = self.can.blocking_receive(self.handle, byref(messagerx), time)

        if status == CanalError.SUCCESS:
            rx = message_convert_rx(messagerx)
        elif status in (
            CanalError.RCV_EMPTY,
            CanalError.TIMEOUT,
            CanalError.FIFO_EMPTY,
        ):
            rx = None
        else:
            raise CanOperationError("could not receive message", error_code=status)

        return rx, False

    def shutdown(self):
        """
        Shuts down connection to the device safely.

        :raise cam.CanOperationError: is closing the connection did not work
        """
        super().shutdown()
        status = self.can.close(self.handle)

        if status != CanalError.SUCCESS:
            raise CanOperationError("could not shut down bus", error_code=status)

    @staticmethod
    def _detect_available_configs():
        return Usb2canBus.detect_available_configs()

    @staticmethod
    def detect_available_configs(serial_matcher: Optional[str] = None):
        """
        Uses the *Windows Management Instrumentation* to identify serial devices.

        :param serial_matcher:
            search string for automatic detection of the device serial
        """
        if serial_matcher is None:
            channels = find_serial_devices()
        else:
            channels = find_serial_devices(serial_matcher)

        return [{"interface": "usb2can", "channel": c} for c in channels]
