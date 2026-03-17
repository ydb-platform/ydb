"""
Enable basic CAN over a PCAN USB device.
"""

import logging
import platform
import time
import warnings
from typing import Any, Optional, Union

from packaging import version

from can import (
    BitTiming,
    BitTimingFd,
    BusABC,
    BusState,
    CanError,
    CanInitializationError,
    CanOperationError,
    CanProtocol,
    Message,
)
from can.util import check_or_adjust_timing_clock, dlc2len, len2dlc

from .basic import (
    FEATURE_FD_CAPABLE,
    IS_LINUX,
    IS_WINDOWS,
    PCAN_ALLOW_ECHO_FRAMES,
    PCAN_ALLOW_ERROR_FRAMES,
    PCAN_API_VERSION,
    PCAN_ATTACHED_CHANNELS,
    PCAN_BAUD_500K,
    PCAN_BITRATES,
    PCAN_BUSOFF_AUTORESET,
    PCAN_CHANNEL_AVAILABLE,
    PCAN_CHANNEL_CONDITION,
    PCAN_CHANNEL_FEATURES,
    PCAN_CHANNEL_IDENTIFYING,
    PCAN_CHANNEL_NAMES,
    PCAN_DEVICE_NUMBER,
    PCAN_DICT_STATUS,
    PCAN_ERROR_BUSHEAVY,
    PCAN_ERROR_BUSLIGHT,
    PCAN_ERROR_ILLDATA,
    PCAN_ERROR_OK,
    PCAN_ERROR_QRCVEMPTY,
    PCAN_FD_PARAMETER_LIST,
    PCAN_LANBUS1,
    PCAN_LISTEN_ONLY,
    PCAN_MESSAGE_BRS,
    PCAN_MESSAGE_ECHO,
    PCAN_MESSAGE_ERRFRAME,
    PCAN_MESSAGE_ESI,
    PCAN_MESSAGE_EXTENDED,
    PCAN_MESSAGE_FD,
    PCAN_MESSAGE_RTR,
    PCAN_MESSAGE_STANDARD,
    PCAN_NONEBUS,
    PCAN_PARAMETER_OFF,
    PCAN_PARAMETER_ON,
    PCAN_PCCBUS1,
    PCAN_PCIBUS1,
    PCAN_RECEIVE_EVENT,
    PCAN_TYPE_ISA,
    PCAN_USBBUS1,
    VALID_PCAN_CAN_CLOCKS,
    VALID_PCAN_FD_CLOCKS,
    PCANBasic,
    TPCANBaudrate,
    TPCANChannelInformation,
    TPCANHandle,
    TPCANMsg,
    TPCANMsgFD,
)

# Set up logging
log = logging.getLogger("can.pcan")

MIN_PCAN_API_VERSION = version.parse("4.2.0")

try:
    # use the "uptime" library if available
    import uptime

    # boottime() and fromtimestamp() are timezone offset, so the difference is not.
    if uptime.boottime() is None:
        boottimeEpoch = 0
    else:
        boottimeEpoch = uptime.boottime().timestamp()
except ImportError:
    log.warning(
        "uptime library not available, timestamps are relative to boot time and not to Epoch UTC",
    )
    boottimeEpoch = 0

HAS_EVENTS = False

if IS_WINDOWS:
    try:
        # Try builtin Python 3 Windows API
        from _overlapped import CreateEvent
        from _winapi import INFINITE, WAIT_OBJECT_0, WaitForSingleObject

        HAS_EVENTS = True
    except ImportError:
        pass

elif IS_LINUX:
    try:
        import select

        HAS_EVENTS = True
    except Exception:
        pass


class PcanBus(BusABC):
    def __init__(
        self,
        channel: str = "PCAN_USBBUS1",
        device_id: Optional[int] = None,
        state: BusState = BusState.ACTIVE,
        timing: Optional[Union[BitTiming, BitTimingFd]] = None,
        bitrate: int = 500000,
        receive_own_messages: bool = False,
        **kwargs: Any,
    ):
        """A PCAN USB interface to CAN.

        On top of the usual :class:`~can.Bus` methods provided,
        the PCAN interface includes the :meth:`flash`
        and :meth:`status` methods.

        :param str channel:
            The can interface name. An example would be 'PCAN_USBBUS1'.
            Alternatively the value can be an int with the numerical value.
            Default is 'PCAN_USBBUS1'

        :param int device_id:
            Select the PCAN interface based on its ID. The device ID is a 8/32bit
            value that can be configured for each PCAN device. If you set the
            device_id parameter, it takes precedence over the channel parameter.
            The constructor searches all connected interfaces and initializes the
            first one that matches the parameter value. If no device is found,
            an exception is raised.

        :param can.bus.BusState state:
            BusState of the channel.
            Default is ACTIVE

        :param timing:
            An instance of :class:`~can.BitTiming` or :class:`~can.BitTimingFd`
            to specify the bit timing parameters for the PCAN interface. If this parameter
            is provided, it takes precedence over all other timing-related parameters.
            If this parameter is not provided, the bit timing parameters can be specified
            using the `bitrate` parameter for standard CAN or the `fd`, `f_clock`,
            `f_clock_mhz`, `nom_brp`, `nom_tseg1`, `nom_tseg2`, `nom_sjw`, `data_brp`,
            `data_tseg1`, `data_tseg2`, and `data_sjw` parameters for CAN FD.
            Note that the `f_clock` value of the `timing` instance must be 8_000_000
            for standard CAN or any of the following values for CAN FD: 20_000_000,
            24_000_000, 30_000_000, 40_000_000, 60_000_000, 80_000_000.

        :param int bitrate:
            Bitrate of channel in bit/s.
            Default is 500 kbit/s.
            Ignored if using CanFD.

        :param receive_own_messages:
            Enable self-reception of sent messages.

        :param bool fd:
            Should the Bus be initialized in CAN-FD mode.

        :param int f_clock:
            Clock rate in Hz.
            Any of the following:
            20000000, 24000000, 30000000, 40000000, 60000000, 80000000.
            Ignored if not using CAN-FD.
            Pass either f_clock or f_clock_mhz.

        :param int f_clock_mhz:
            Clock rate in MHz.
            Any of the following:
            20, 24, 30, 40, 60, 80.
            Ignored if not using CAN-FD.
            Pass either f_clock or f_clock_mhz.

        :param int nom_brp:
            Clock prescaler for nominal time quantum.
            In the range (1..1024)
            Ignored if not using CAN-FD.

        :param int nom_tseg1:
            Time segment 1 for nominal bit rate,
            that is, the number of quanta from (but not including)
            the Sync Segment to the sampling point.
            In the range (1..256).
            Ignored if not using CAN-FD.

        :param int nom_tseg2:
            Time segment 2 for nominal bit rate,
            that is, the number of quanta from the sampling
            point to the end of the bit.
            In the range (1..128).
            Ignored if not using CAN-FD.

        :param int nom_sjw:
            Synchronization Jump Width for nominal bit rate.
            Decides the maximum number of time quanta
            that the controller can resynchronize every bit.
            In the range (1..128).
            Ignored if not using CAN-FD.

        :param int data_brp:
            Clock prescaler for fast data time quantum.
            In the range (1..1024)
            Ignored if not using CAN-FD.

        :param int data_tseg1:
            Time segment 1 for fast data bit rate,
            that is, the number of quanta from (but not including)
            the Sync Segment to the sampling point.
            In the range (1..32).
            Ignored if not using CAN-FD.

        :param int data_tseg2:
            Time segment 2 for fast data bit rate,
            that is, the number of quanta from the sampling
            point to the end of the bit.
            In the range (1..16).
            Ignored if not using CAN-FD.

        :param int data_sjw:
            Synchronization Jump Width for fast data bit rate.
            Decides the maximum number of time quanta
            that the controller can resynchronize every bit.
            In the range (1..16).
            Ignored if not using CAN-FD.

        :param bool auto_reset:
            Enable automatic recovery in bus off scenario.
            Resetting the driver takes ~500ms during which
            it will not be responsive.
        """
        self.m_objPCANBasic = PCANBasic()

        if device_id is not None:
            channel = self._find_channel_by_dev_id(device_id)

            if channel is None:
                err_msg = f"Cannot find a channel with ID {device_id:08x}"
                raise ValueError(err_msg)

        is_fd = isinstance(timing, BitTimingFd) if timing else kwargs.get("fd", False)
        self._can_protocol = CanProtocol.CAN_FD if is_fd else CanProtocol.CAN_20
        self.channel_info = str(channel)

        hwtype = PCAN_TYPE_ISA
        ioport = 0x02A0
        interrupt = 11

        if not isinstance(channel, int):
            channel = PCAN_CHANNEL_NAMES[channel]

        self.m_PcanHandle = channel

        self.check_api_version()

        if state in [BusState.ACTIVE, BusState.PASSIVE]:
            self.state = state
        else:
            raise ValueError("BusState must be Active or Passive")

        if isinstance(timing, BitTiming):
            timing = check_or_adjust_timing_clock(timing, VALID_PCAN_CAN_CLOCKS)
            pcan_bitrate = TPCANBaudrate(timing.btr0 << 8 | timing.btr1)
            result = self.m_objPCANBasic.Initialize(
                self.m_PcanHandle, pcan_bitrate, hwtype, ioport, interrupt
            )
        elif is_fd:
            if isinstance(timing, BitTimingFd):
                timing = check_or_adjust_timing_clock(
                    timing, sorted(VALID_PCAN_FD_CLOCKS, reverse=True)
                )
                # We dump the timing parameters into the kwargs because they have equal names
                # as the kwargs parameters and this saves us one additional code path
                kwargs.update(timing)

            clock_param = "f_clock" if "f_clock" in kwargs else "f_clock_mhz"
            fd_parameters_values = [
                f"{key}={kwargs[key]}"
                for key in (clock_param, *PCAN_FD_PARAMETER_LIST)
                if key in kwargs
            ]

            self.fd_bitrate = ", ".join(fd_parameters_values).encode("ascii")

            result = self.m_objPCANBasic.InitializeFD(
                self.m_PcanHandle, self.fd_bitrate
            )

        else:
            pcan_bitrate = PCAN_BITRATES.get(bitrate, PCAN_BAUD_500K)
            result = self.m_objPCANBasic.Initialize(
                self.m_PcanHandle, pcan_bitrate, hwtype, ioport, interrupt
            )

        if result != PCAN_ERROR_OK:
            raise PcanCanInitializationError(self._get_formatted_error(result))

        result = self.m_objPCANBasic.SetValue(
            self.m_PcanHandle, PCAN_ALLOW_ERROR_FRAMES, PCAN_PARAMETER_ON
        )

        if result != PCAN_ERROR_OK:
            if platform.system() != "Darwin":
                raise PcanCanInitializationError(self._get_formatted_error(result))
            else:
                # TODO Remove Filter when MACCan actually supports it:
                #  https://github.com/mac-can/PCBUSB-Library/
                log.debug(
                    "Ignoring error. PCAN_ALLOW_ERROR_FRAMES is still unsupported by OSX Library PCANUSB v0.11.2"
                )

        if receive_own_messages:
            result = self.m_objPCANBasic.SetValue(
                self.m_PcanHandle, PCAN_ALLOW_ECHO_FRAMES, PCAN_PARAMETER_ON
            )

            if result != PCAN_ERROR_OK:
                raise PcanCanInitializationError(self._get_formatted_error(result))

        if kwargs.get("auto_reset", False):
            result = self.m_objPCANBasic.SetValue(
                self.m_PcanHandle, PCAN_BUSOFF_AUTORESET, PCAN_PARAMETER_ON
            )

            if result != PCAN_ERROR_OK:
                raise PcanCanInitializationError(self._get_formatted_error(result))

        if HAS_EVENTS:
            if IS_WINDOWS:
                self._recv_event = CreateEvent(None, 0, 0, None)
                result = self.m_objPCANBasic.SetValue(
                    self.m_PcanHandle, PCAN_RECEIVE_EVENT, self._recv_event
                )
            elif IS_LINUX:
                result, self._recv_event = self.m_objPCANBasic.GetValue(
                    self.m_PcanHandle, PCAN_RECEIVE_EVENT
                )

            if result != PCAN_ERROR_OK:
                raise PcanCanInitializationError(self._get_formatted_error(result))

        super().__init__(
            channel=channel,
            state=state,
            bitrate=bitrate,
            **kwargs,
        )

    def _find_channel_by_dev_id(self, device_id):
        """
        Iterate over all possible channels to find a channel that matches the device
        ID. This method is somewhat brute force, but the Basic API only offers a
        suitable API call since V4.4.0.

        :param device_id: The device_id for which to search for
        :return: The name of a PCAN channel that matches the device ID, or None if
            no channel can be found.
        """
        for ch_name, ch_handle in PCAN_CHANNEL_NAMES.items():
            err, cur_dev_id = self.m_objPCANBasic.GetValue(
                ch_handle, PCAN_DEVICE_NUMBER
            )
            if err != PCAN_ERROR_OK:
                continue

            if cur_dev_id == device_id:
                return ch_name

        return None

    def _get_formatted_error(self, error):
        """
        Gets the text using the GetErrorText API function.
        If the function call succeeds, the translated error is returned. If it fails,
        a text describing the current error is returned. Multiple errors may
        be present in which case their individual messages are included in the
        return string, one line per error.
        """

        def bits(n):
            """
            Iterate over all the set bits in `n`, returning the masked bits at
            the set indices
            """
            while n:
                # Create a mask to mask the lowest set bit in n
                mask = ~n + 1
                masked_value = n & mask
                yield masked_value
                # Toggle the lowest set bit
                n ^= masked_value

        stsReturn = self.m_objPCANBasic.GetErrorText(error, 0x9)
        if stsReturn[0] != PCAN_ERROR_OK:
            strings = []

            for b in bits(error):
                stsReturn = self.m_objPCANBasic.GetErrorText(b, 0x9)
                if stsReturn[0] != PCAN_ERROR_OK:
                    text = f"An error occurred. Error-code's text ({error:X}h) couldn't be retrieved"
                else:
                    text = stsReturn[1].decode("utf-8", errors="replace")

                strings.append(text)

            complete_text = "\n".join(strings)
        else:
            complete_text = stsReturn[1].decode("utf-8", errors="replace")

        return complete_text

    def get_api_version(self):
        error, value = self.m_objPCANBasic.GetValue(PCAN_NONEBUS, PCAN_API_VERSION)
        if error != PCAN_ERROR_OK:
            raise CanInitializationError("Failed to read pcan basic api version")

        # fix https://github.com/hardbyte/python-can/issues/1642
        version_string = value.decode("ascii").replace(",", ".").replace(" ", "")

        return version.parse(version_string)

    def check_api_version(self):
        apv = self.get_api_version()
        if apv < MIN_PCAN_API_VERSION:
            log.warning(
                f"Minimum version of pcan api is {MIN_PCAN_API_VERSION}."
                f" Installed version is {apv}. Consider upgrade of pcan basic package"
            )

    def status(self):
        """
        Query the PCAN bus status.

        :rtype: int
        :return: The status code. See values in **basic.PCAN_ERROR_**
        """
        return self.m_objPCANBasic.GetStatus(self.m_PcanHandle)

    def status_is_ok(self):
        """
        Convenience method to check that the bus status is OK
        """
        status = self.status()
        return status == PCAN_ERROR_OK

    def reset(self):
        """
        Command the PCAN driver to reset the bus after an error.
        """
        status = self.m_objPCANBasic.Reset(self.m_PcanHandle)
        return status == PCAN_ERROR_OK

    def get_device_number(self):
        """
        Return the PCAN device number.

        :rtype: int
        :return: PCAN device number
        """
        error, value = self.m_objPCANBasic.GetValue(
            self.m_PcanHandle, PCAN_DEVICE_NUMBER
        )
        if error != PCAN_ERROR_OK:
            return None
        return value

    def set_device_number(self, device_number):
        """
        Set the PCAN device number.

        :param device_number: new PCAN device number
        :rtype: bool
        :return: True if device number set successfully
        """
        try:
            if (
                self.m_objPCANBasic.SetValue(
                    self.m_PcanHandle, PCAN_DEVICE_NUMBER, int(device_number)
                )
                != PCAN_ERROR_OK
            ):
                raise ValueError()
        except ValueError:
            log.error("Invalid value '%s' for device number.", device_number)
            return False
        return True

    def _recv_internal(
        self, timeout: Optional[float]
    ) -> tuple[Optional[Message], bool]:
        end_time = time.time() + timeout if timeout is not None else None

        while True:
            if self._can_protocol is CanProtocol.CAN_FD:
                result, pcan_msg, pcan_timestamp = self.m_objPCANBasic.ReadFD(
                    self.m_PcanHandle
                )
            else:
                result, pcan_msg, pcan_timestamp = self.m_objPCANBasic.Read(
                    self.m_PcanHandle
                )

            if result == PCAN_ERROR_OK:
                # message received
                break

            if result == PCAN_ERROR_QRCVEMPTY:
                # receive queue is empty, wait or return on timeout

                if end_time is None:
                    time_left: Optional[float] = None
                    timed_out = False
                else:
                    time_left = max(0.0, end_time - time.time())
                    timed_out = time_left == 0.0

                if timed_out:
                    return None, False

                if not HAS_EVENTS:
                    # polling mode
                    time.sleep(0.001)
                    continue

                if IS_WINDOWS:
                    # Windows with event
                    if time_left is None:
                        time_left_ms = INFINITE
                    else:
                        time_left_ms = int(time_left * 1000)
                    _ret = WaitForSingleObject(self._recv_event, time_left_ms)
                    if _ret == WAIT_OBJECT_0:
                        continue

                elif IS_LINUX:
                    # Linux with event
                    recv, _, _ = select.select([self._recv_event], [], [], time_left)
                    if self._recv_event in recv:
                        continue

            elif result & (PCAN_ERROR_BUSLIGHT | PCAN_ERROR_BUSHEAVY):
                log.warning(self._get_formatted_error(result))

            elif result == PCAN_ERROR_ILLDATA:
                # When there is an invalid frame on CAN bus (in our case CAN FD), PCAN first reports result PCAN_ERROR_ILLDATA
                # and then it sends the error frame. If the PCAN_ERROR_ILLDATA is not ignored, python-can throws an exception.
                # So we ignore any PCAN_ERROR_ILLDATA results here.
                pass

            else:
                raise PcanCanOperationError(self._get_formatted_error(result))

            return None, False

        is_extended_id = bool(pcan_msg.MSGTYPE & PCAN_MESSAGE_EXTENDED.value)
        is_remote_frame = bool(pcan_msg.MSGTYPE & PCAN_MESSAGE_RTR.value)
        is_fd = bool(pcan_msg.MSGTYPE & PCAN_MESSAGE_FD.value)
        is_rx = not bool(pcan_msg.MSGTYPE & PCAN_MESSAGE_ECHO.value)
        bitrate_switch = bool(pcan_msg.MSGTYPE & PCAN_MESSAGE_BRS.value)
        error_state_indicator = bool(pcan_msg.MSGTYPE & PCAN_MESSAGE_ESI.value)
        is_error_frame = bool(pcan_msg.MSGTYPE & PCAN_MESSAGE_ERRFRAME.value)

        if self._can_protocol is CanProtocol.CAN_FD:
            dlc = dlc2len(pcan_msg.DLC)
            timestamp = boottimeEpoch + (pcan_timestamp.value / (1000.0 * 1000.0))
        else:
            dlc = pcan_msg.LEN
            timestamp = boottimeEpoch + (
                (
                    pcan_timestamp.micros
                    + 1000 * pcan_timestamp.millis
                    + 0x100000000 * 1000 * pcan_timestamp.millis_overflow
                )
                / (1000.0 * 1000.0)
            )

        rx_msg = Message(
            channel=self.channel_info,
            timestamp=timestamp,
            arbitration_id=pcan_msg.ID,
            is_extended_id=is_extended_id,
            is_remote_frame=is_remote_frame,
            is_error_frame=is_error_frame,
            dlc=dlc,
            data=pcan_msg.DATA[:dlc],
            is_fd=is_fd,
            is_rx=is_rx,
            bitrate_switch=bitrate_switch,
            error_state_indicator=error_state_indicator,
        )

        return rx_msg, False

    def send(self, msg, timeout=None):
        msgType = (
            PCAN_MESSAGE_EXTENDED.value
            if msg.is_extended_id
            else PCAN_MESSAGE_STANDARD.value
        )
        if msg.is_remote_frame:
            msgType |= PCAN_MESSAGE_RTR.value
        if msg.is_error_frame:
            msgType |= PCAN_MESSAGE_ERRFRAME.value
        if msg.is_fd:
            msgType |= PCAN_MESSAGE_FD.value
        if msg.bitrate_switch:
            msgType |= PCAN_MESSAGE_BRS.value
        if msg.error_state_indicator:
            msgType |= PCAN_MESSAGE_ESI.value

        if self._can_protocol is CanProtocol.CAN_FD:
            # create a TPCANMsg message structure
            CANMsg = TPCANMsgFD()

            # configure the message. ID, Length of data, message type and data
            CANMsg.ID = msg.arbitration_id
            CANMsg.DLC = len2dlc(msg.dlc)
            CANMsg.MSGTYPE = msgType

            # copy data
            CANMsg.DATA[: msg.dlc] = msg.data[: msg.dlc]

            log.debug("Data: %s", msg.data)
            log.debug("Type: %s", type(msg.data))

            result = self.m_objPCANBasic.WriteFD(self.m_PcanHandle, CANMsg)

        else:
            # create a TPCANMsg message structure
            CANMsg = TPCANMsg()

            # configure the message. ID, Length of data, message type and data
            CANMsg.ID = msg.arbitration_id
            CANMsg.LEN = msg.dlc
            CANMsg.MSGTYPE = msgType

            # if a remote frame will be sent, data bytes are not important.
            if not msg.is_remote_frame:
                # copy data
                CANMsg.DATA[: CANMsg.LEN] = msg.data[: CANMsg.LEN]

            log.debug("Data: %s", msg.data)
            log.debug("Type: %s", type(msg.data))

            result = self.m_objPCANBasic.Write(self.m_PcanHandle, CANMsg)

        if result != PCAN_ERROR_OK:
            raise PcanCanOperationError(
                "Failed to send: " + self._get_formatted_error(result)
            )

    def flash(self, flash):
        """
        Turn on or off flashing of the device's LED for physical
        identification purposes.
        """
        self.m_objPCANBasic.SetValue(
            self.m_PcanHandle, PCAN_CHANNEL_IDENTIFYING, bool(flash)
        )

    def shutdown(self):
        super().shutdown()
        if HAS_EVENTS and IS_LINUX:
            self.m_objPCANBasic.SetValue(self.m_PcanHandle, PCAN_RECEIVE_EVENT, 0)

        self.m_objPCANBasic.Uninitialize(self.m_PcanHandle)

    @property
    def fd(self) -> bool:
        class_name = self.__class__.__name__
        warnings.warn(
            f"The {class_name}.fd property is deprecated and superseded by {class_name}.protocol. "
            "It is scheduled for removal in python-can version 5.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._can_protocol is CanProtocol.CAN_FD

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, new_state):
        # declare here, which is called by __init__()
        self._state = new_state  # pylint: disable=attribute-defined-outside-init

        if new_state is BusState.ACTIVE:
            self.m_objPCANBasic.SetValue(
                self.m_PcanHandle, PCAN_LISTEN_ONLY, PCAN_PARAMETER_OFF
            )

        elif new_state is BusState.PASSIVE:
            # When this mode is set, the CAN controller does not take part on active events (eg. transmit CAN messages)
            # but stays in a passive mode (CAN monitor), in which it can analyse the traffic on the CAN bus used by a
            # PCAN channel. See also the Philips Data Sheet "SJA1000 Stand-alone CAN controller".
            self.m_objPCANBasic.SetValue(
                self.m_PcanHandle, PCAN_LISTEN_ONLY, PCAN_PARAMETER_ON
            )

    @staticmethod
    def _detect_available_configs():
        channels = []
        try:
            library_handle = PCANBasic()
        except OSError:
            return channels

        interfaces = []

        if platform.system() != "Darwin":
            res, value = library_handle.GetValue(PCAN_NONEBUS, PCAN_ATTACHED_CHANNELS)
            if res != PCAN_ERROR_OK:
                return interfaces
            channel_information: list[TPCANChannelInformation] = list(value)
            for channel in channel_information:
                # find channel name in PCAN_CHANNEL_NAMES by value
                channel_name = next(
                    _channel_name
                    for _channel_name, channel_id in PCAN_CHANNEL_NAMES.items()
                    if channel_id.value == channel.channel_handle
                )
                channel_config = {
                    "interface": "pcan",
                    "channel": channel_name,
                    "supports_fd": bool(channel.device_features & FEATURE_FD_CAPABLE),
                    "controller_number": channel.controller_number,
                    "device_features": channel.device_features,
                    "device_id": channel.device_id,
                    "device_name": channel.device_name.decode("latin-1"),
                    "device_type": channel.device_type,
                    "channel_condition": channel.channel_condition,
                }
                interfaces.append(channel_config)
            return interfaces

        for i in range(16):
            interfaces.append(
                {
                    "id": TPCANHandle(PCAN_PCIBUS1.value + i),
                    "name": "PCAN_PCIBUS" + str(i + 1),
                }
            )
        for i in range(16):
            interfaces.append(
                {
                    "id": TPCANHandle(PCAN_USBBUS1.value + i),
                    "name": "PCAN_USBBUS" + str(i + 1),
                }
            )
        for i in range(2):
            interfaces.append(
                {
                    "id": TPCANHandle(PCAN_PCCBUS1.value + i),
                    "name": "PCAN_PCCBUS" + str(i + 1),
                }
            )
        for i in range(16):
            interfaces.append(
                {
                    "id": TPCANHandle(PCAN_LANBUS1.value + i),
                    "name": "PCAN_LANBUS" + str(i + 1),
                }
            )
        for i in interfaces:
            try:
                error, value = library_handle.GetValue(i["id"], PCAN_CHANNEL_CONDITION)
                if error != PCAN_ERROR_OK or value != PCAN_CHANNEL_AVAILABLE:
                    continue
                has_fd = False
                error, value = library_handle.GetValue(i["id"], PCAN_CHANNEL_FEATURES)
                if error == PCAN_ERROR_OK:
                    has_fd = bool(value & FEATURE_FD_CAPABLE)
                channels.append(
                    {"interface": "pcan", "channel": i["name"], "supports_fd": has_fd}
                )
            except AttributeError:  # Ignore if this fails for some interfaces
                pass
        return channels

    def status_string(self) -> Optional[str]:
        """
        Query the PCAN bus status.

        :return: The status description, if any was found.
        """
        try:
            return PCAN_DICT_STATUS[self.status()]
        except KeyError:
            return None


class PcanError(CanError):
    """A generic error on a PCAN bus."""


class PcanCanOperationError(CanOperationError, PcanError):
    """Like :class:`can.exceptions.CanOperationError`, but specific to Pcan."""


class PcanCanInitializationError(CanInitializationError, PcanError):
    """Like :class:`can.exceptions.CanInitializationError`, but specific to Pcan."""
