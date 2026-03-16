"""
Intrepid Control Systems (ICS) neoVI interface module.

python-ics is a Python wrapper around the API provided by Intrepid Control
Systems for communicating with their neoVI range of devices.

Implementation references:
* https://github.com/intrepidcs/python_ics
"""

import functools
import logging
import os
import tempfile
from collections import Counter, defaultdict, deque
from datetime import datetime
from functools import partial
from itertools import cycle
from threading import Event
from warnings import warn

from can import BusABC, CanProtocol, Message

from ...exceptions import (
    CanError,
    CanInitializationError,
    CanOperationError,
    CanTimeoutError,
)

logger = logging.getLogger(__name__)

try:
    import ics
except ImportError as ie:
    logger.warning(
        "You won't be able to use the ICS neoVI can backend without the "
        "python-ics module installed!: %s",
        ie,
    )
    ics = None


try:
    from filelock import FileLock
except ImportError as ie:
    logger.warning(
        "Using ICS neoVI can backend without the "
        "filelock module installed may cause some issues!: %s",
        ie,
    )

    class FileLock:
        """Dummy file lock that does not actually do anything"""

        def __init__(self, lock_file, timeout=-1):
            self._lock_file = lock_file
            self.timeout = timeout

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return None


# Use inter-process mutex to prevent concurrent device open.
# When neoVI server is enabled, there is an issue with concurrent device open.
open_lock = FileLock(os.path.join(tempfile.gettempdir(), "neovi.lock"))
description_id = cycle(range(1, 0x8000))

ICS_EPOCH = datetime.fromisoformat("2007-01-01")
ICS_EPOCH_DELTA = (ICS_EPOCH - datetime.fromisoformat("1970-01-01")).total_seconds()


class ICSApiError(CanError):
    """
    Indicates an error with the ICS API.
    """

    # A critical error which affects operation or accuracy.
    ICS_SPY_ERR_CRITICAL = 0x10
    # An error which is not understood.
    ICS_SPY_ERR_QUESTION = 0x20
    # An important error which may be critical depending on the application
    ICS_SPY_ERR_EXCLAMATION = 0x30
    # An error which probably does not need attention.
    ICS_SPY_ERR_INFORMATION = 0x40

    def __init__(
        self,
        error_code: int,
        description_short: str,
        description_long: str,
        severity: int,
        restart_needed: int,
    ):
        super().__init__(f"{description_short}. {description_long}", error_code)
        self.description_short = description_short
        self.description_long = description_long
        self.severity = severity
        self.restart_needed = restart_needed == 1

    def __reduce__(self):
        return type(self), (
            self.error_code,
            self.description_short,
            self.description_long,
            self.severity,
            self.restart_needed,
        )

    @property
    def error_number(self) -> int:
        """Deprecated. Renamed to :attr:`can.CanError.error_code`."""
        warn(
            "ICSApiError::error_number has been replaced by ICSApiError.error_code in python-can 4.0"
            "and will be remove in version 5.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.error_code

    @property
    def is_critical(self) -> bool:
        return self.severity == self.ICS_SPY_ERR_CRITICAL


class ICSInitializationError(ICSApiError, CanInitializationError):
    pass


class ICSOperationError(ICSApiError, CanOperationError):
    pass


def check_if_bus_open(func):
    """
    Decorator that checks if the bus is open before executing the function.

    If the bus is not open, it raises a CanOperationError.
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        """
        Wrapper function that checks if the bus is open before executing the function.

        :raises CanOperationError: If the bus is not open.
        """
        if self._is_shutdown:
            raise CanOperationError("Cannot operate on a closed bus")
        return func(self, *args, **kwargs)

    return wrapper


class NeoViBus(BusABC):
    """
    The CAN Bus implemented for the python_ics interface
    https://github.com/intrepidcs/python_ics
    """

    def __init__(self, channel, can_filters=None, **kwargs):
        """
        :param channel:
            The channel ids to create this bus with.
            Can also be a single integer, netid name or a comma separated
            string.
        :type channel: int or str or list(int) or list(str)
        :param list can_filters:
            See :meth:`can.BusABC.set_filters` for details.
        :param bool receive_own_messages:
            If transmitted messages should also be received by this bus.
        :param bool use_system_timestamp:
            Use system timestamp for can messages instead of the hardware time
            stamp
        :param str serial:
            Serial to connect (optional, will use the first found if not
            supplied)
        :param int bitrate:
            Channel bitrate in bit/s. (optional, will enable the auto bitrate
            feature if not supplied)
        :param bool fd:
            If CAN-FD frames should be supported.
        :param int data_bitrate:
            Which bitrate to use for data phase in CAN FD.
            Defaults to arbitration bitrate.
        :param override_library_name:
            Absolute path or relative path to the library including filename.

        :raise ImportError:
            If *python-ics* is not available
        :raise CanInitializationError:
            If the bus could not be set up.
            May or may not be a :class:`~ICSInitializationError`.
        """
        if ics is None:
            raise ImportError("Please install python-ics")

        super().__init__(
            channel=channel,
            can_filters=can_filters,
            **kwargs,
        )

        logger.info(f"CAN Filters: {can_filters}")
        logger.info(f"Got configuration of: {kwargs}")

        if "override_library_name" in kwargs:
            ics.override_library_name(kwargs.get("override_library_name"))

        if isinstance(channel, (list, tuple)):
            self.channels = channel
        elif isinstance(channel, int):
            self.channels = [channel]
        else:
            # Assume comma separated string of channels
            self.channels = [ch.strip() for ch in channel.split(",")]
        self.channels = [NeoViBus.channel_to_netid(ch) for ch in self.channels]

        type_filter = kwargs.get("type_filter")
        serial = kwargs.get("serial")
        self.dev = self._find_device(type_filter, serial)

        is_fd = kwargs.get("fd", False)
        self._can_protocol = CanProtocol.CAN_FD if is_fd else CanProtocol.CAN_20

        with open_lock:
            ics.open_device(self.dev)

        try:
            if "bitrate" in kwargs:
                for channel in self.channels:
                    ics.set_bit_rate(self.dev, kwargs.get("bitrate"), channel)

            if is_fd:
                if "data_bitrate" in kwargs:
                    for channel in self.channels:
                        ics.set_fd_bit_rate(
                            self.dev, kwargs.get("data_bitrate"), channel
                        )
        except ics.RuntimeError as re:
            logger.error(re)
            err = ICSInitializationError(*ics.get_last_api_error(self.dev))
            try:
                self.shutdown()
            finally:
                raise err

        self._use_system_timestamp = bool(kwargs.get("use_system_timestamp", False))
        self._receive_own_messages = kwargs.get("receive_own_messages", True)

        self.channel_info = (
            f"{self.dev.Name} {self.get_serial_number(self.dev)} CH:{self.channels}"
        )
        logger.info(f"Using device: {self.channel_info}")

        self.rx_buffer = deque()
        self.message_receipts = defaultdict(Event)

    @staticmethod
    def channel_to_netid(channel_name_or_id):
        try:
            channel = int(channel_name_or_id)
        except ValueError:
            netid = f"NETID_{channel_name_or_id.upper()}"
            if hasattr(ics, netid):
                channel = getattr(ics, netid)
            else:
                raise ValueError(
                    "channel must be an integer or a valid ICS channel name"
                ) from None
        return channel

    @staticmethod
    def get_serial_number(device):
        """Decode (if needed) and return the ICS device serial string

        :param device: ics device
        :return: ics device serial string
        :rtype: str
        """
        if int("0A0000", 36) < device.SerialNumber < int("ZZZZZZ", 36):
            return ics.base36enc(device.SerialNumber)
        else:
            return str(device.SerialNumber)

    def shutdown(self):
        super().shutdown()
        ics.close_device(self.dev)

    @staticmethod
    def _detect_available_configs():
        """Detect all configurations/channels that this interface could
        currently connect with.

        :rtype: Iterator[dict]
        :return: an iterable of dicts, each being a configuration suitable
                 for usage in the interface's bus constructor.
        """
        if ics is None:
            return []

        try:
            devices = ics.find_devices()
        except Exception as e:
            logger.debug("Failed to detect configs: %s", e)
            return []

        # TODO: add the channel(s)
        return [
            {"interface": "neovi", "serial": NeoViBus.get_serial_number(device)}
            for device in devices
        ]

    def _find_device(self, type_filter=None, serial=None):
        """Returns the first matching device or raises an error.

        :raise CanInitializationError:
            If not matching device could be found
        """
        if type_filter is not None:
            devices = ics.find_devices(type_filter)
        else:
            devices = ics.find_devices()

        for device in devices:
            if serial is None or self.get_serial_number(device) == str(serial):
                return device

        msg = ["No device"]

        if type_filter is not None:
            msg.append(f"with type {type_filter}")
        if serial is not None:
            msg.append(f"with serial {serial}")
        msg.append("found.")
        raise CanInitializationError(" ".join(msg))

    @check_if_bus_open
    def _process_msg_queue(self, timeout=0.1):
        try:
            messages, errors = ics.get_messages(self.dev, False, timeout)
        except ics.RuntimeError:
            return
        for ics_msg in messages:
            channel = ics_msg.NetworkID | (ics_msg.NetworkID2 << 8)
            if channel not in self.channels:
                continue

            is_tx = bool(ics_msg.StatusBitField & ics.SPY_STATUS_TX_MSG)

            if is_tx:
                if bool(ics_msg.StatusBitField & ics.SPY_STATUS_GLOBAL_ERR):
                    continue

                receipt_key = (ics_msg.ArbIDOrHeader, ics_msg.DescriptionID)
                if ics_msg.DescriptionID and receipt_key in self.message_receipts:
                    self.message_receipts[receipt_key].set()
                if not self._receive_own_messages:
                    continue

            self.rx_buffer.append(ics_msg)
        if errors:
            logger.warning("%d error(s) found", errors)

            for msg, count in Counter(ics.get_error_messages(self.dev)).items():
                error = ICSApiError(*msg)
                if count > 1:
                    logger.warning(f"{error} (Repeated {count} times)")
                else:
                    logger.warning(error)

    def _get_timestamp_for_msg(self, ics_msg):
        if self._use_system_timestamp:
            # This is the system time stamp.
            # TimeSystem is loaded with the value received from the timeGetTime
            # call in the WIN32 multimedia API.
            #
            # The timeGetTime accuracy is up to 1 millisecond. See the WIN32
            # API documentation for more information.
            #
            # This timestamp is useful for time comparing with other system
            # events or data which is not synced with the neoVI timestamp.
            #
            # Currently, TimeSystem2 is not used.
            return ics_msg.TimeSystem
        else:
            # This is the hardware time stamp.
            return ics.get_timestamp_for_msg(self.dev, ics_msg) + ICS_EPOCH_DELTA

    def _ics_msg_to_message(self, ics_msg):
        is_fd = ics_msg.Protocol == ics.SPY_PROTOCOL_CANFD

        message_from_ics = partial(
            Message,
            timestamp=self._get_timestamp_for_msg(ics_msg),
            arbitration_id=ics_msg.ArbIDOrHeader,
            is_extended_id=bool(ics_msg.StatusBitField & ics.SPY_STATUS_XTD_FRAME),
            is_remote_frame=bool(ics_msg.StatusBitField & ics.SPY_STATUS_REMOTE_FRAME),
            is_error_frame=bool(ics_msg.StatusBitField2 & ics.SPY_STATUS2_ERROR_FRAME),
            channel=ics_msg.NetworkID | (ics_msg.NetworkID2 << 8),
            dlc=ics_msg.NumberBytesData,
            is_fd=is_fd,
            is_rx=not bool(ics_msg.StatusBitField & ics.SPY_STATUS_TX_MSG),
        )

        if is_fd:
            if ics_msg.ExtraDataPtrEnabled:
                data = ics_msg.ExtraDataPtr[: ics_msg.NumberBytesData]
            else:
                data = ics_msg.Data[: ics_msg.NumberBytesData]

            return message_from_ics(
                data=data,
                error_state_indicator=bool(
                    ics_msg.StatusBitField3 & ics.SPY_STATUS3_CANFD_ESI
                ),
                bitrate_switch=bool(
                    ics_msg.StatusBitField3 & ics.SPY_STATUS3_CANFD_BRS
                ),
            )
        else:
            return message_from_ics(
                data=ics_msg.Data[: ics_msg.NumberBytesData],
            )

    def _recv_internal(self, timeout=0.1):
        if not self.rx_buffer:
            self._process_msg_queue(timeout=timeout)
        try:
            ics_msg = self.rx_buffer.popleft()
            msg = self._ics_msg_to_message(ics_msg)
        except IndexError:
            return None, False
        return msg, False

    @check_if_bus_open
    def send(self, msg, timeout=0):
        """Transmit a message to the CAN bus.

        :param Message msg: A message object.

        :param float timeout:
            If > 0, wait up to this many seconds for message to be ACK'ed.
            If timeout is exceeded, an exception will be raised.
            None blocks indefinitely.

        :raises ValueError:
            if the message is invalid
        :raises can.CanTimeoutError:
            if sending timed out
        :raises CanOperationError:
            If the bus is closed or the message could otherwise not be sent.
            May or may not be a :class:`~ICSOperationError`.
        """
        if not ics.validate_hobject(self.dev):
            raise CanOperationError("bus not open")

        # Check for valid DLC to avoid passing extra long data to the driver
        if msg.is_fd:
            if msg.dlc > 64:
                raise ValueError(
                    f"DLC was {msg.dlc} but it should be <= 64 for CAN FD frames"
                )
        elif msg.dlc > 8:
            raise ValueError(
                f"DLC was {msg.dlc} but it should be <= 8 for normal CAN frames"
            )

        message = ics.SpyMessage()

        flag0 = 0
        if msg.is_extended_id:
            flag0 |= ics.SPY_STATUS_XTD_FRAME
        if msg.is_remote_frame:
            flag0 |= ics.SPY_STATUS_REMOTE_FRAME

        flag3 = 0
        if msg.is_fd:
            message.Protocol = ics.SPY_PROTOCOL_CANFD
            if msg.bitrate_switch:
                flag3 |= ics.SPY_STATUS3_CANFD_BRS
            if msg.error_state_indicator:
                flag3 |= ics.SPY_STATUS3_CANFD_ESI

        message.ArbIDOrHeader = msg.arbitration_id
        msg_data = msg.data[: msg.dlc]
        message.NumberBytesData = msg.dlc
        message.Data = tuple(msg_data[:8])
        if msg.is_fd and len(msg_data) > 8:
            message.ExtraDataPtrEnabled = 1
            message.ExtraDataPtr = tuple(msg_data)
        message.StatusBitField = flag0
        message.StatusBitField2 = 0
        message.StatusBitField3 = flag3
        if msg.channel is not None:
            network_id = msg.channel
        elif len(self.channels) == 1:
            network_id = self.channels[0]
        else:
            raise ValueError("msg.channel must be set when using multiple channels.")

        message.NetworkID, message.NetworkID2 = int(network_id & 0xFF), int(
            (network_id >> 8) & 0xFF
        )

        if timeout != 0:
            msg_desc_id = next(description_id)
            message.DescriptionID = msg_desc_id
            receipt_key = (msg.arbitration_id, msg_desc_id)
            self.message_receipts[receipt_key].clear()

        try:
            ics.transmit_messages(self.dev, message)
        except ics.RuntimeError:
            raise ICSOperationError(*ics.get_last_api_error(self.dev)) from None

        # If timeout is set, wait for ACK
        # This requires a notifier for the bus or
        # some other thread calling recv periodically
        if timeout != 0:
            got_receipt = self.message_receipts[receipt_key].wait(timeout)
            # We no longer need this receipt, so no point keeping it in memory
            del self.message_receipts[receipt_key]
            if not got_receipt:
                raise CanTimeoutError("Transmit timeout")
