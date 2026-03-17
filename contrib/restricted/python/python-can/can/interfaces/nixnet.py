"""
NI-XNET interface module.

Implementation references:
    NI-XNET Hardware and Software Manual: https://www.ni.com/pdf/manuals/372840h.pdf
    NI-XNET Python implementation: https://github.com/ni/nixnet-python

Authors: Javier Rubio Giménez <jvrr20@gmail.com>, Jose A. Escobar <joseleescobar@hotmail.com>
"""

import logging
import os
import time
import warnings
from queue import SimpleQueue
from types import ModuleType
from typing import Any, Optional, Union

import can.typechecking
from can import BitTiming, BitTimingFd, BusABC, CanProtocol, Message
from can.exceptions import (
    CanInitializationError,
    CanInterfaceNotImplementedError,
    CanOperationError,
)
from can.util import check_or_adjust_timing_clock, deprecated_args_alias

logger = logging.getLogger(__name__)

nixnet: Optional[ModuleType] = None
try:
    import nixnet  # type: ignore
    import nixnet.constants  # type: ignore
    import nixnet.system  # type: ignore
    import nixnet.types  # type: ignore
except Exception as exc:
    logger.warning("Could not import nixnet: %s", exc)


class NiXNETcanBus(BusABC):
    """
    The CAN Bus implemented for the NI-XNET interface.
    """

    @deprecated_args_alias(
        deprecation_start="4.2.0",
        deprecation_end="5.0.0",
        brs=None,
        log_errors=None,
    )
    def __init__(
        self,
        channel: str = "CAN1",
        bitrate: int = 500_000,
        timing: Optional[Union[BitTiming, BitTimingFd]] = None,
        can_filters: Optional[can.typechecking.CanFilters] = None,
        receive_own_messages: bool = False,
        can_termination: bool = False,
        fd: bool = False,
        fd_bitrate: Optional[int] = None,
        poll_interval: float = 0.001,
        **kwargs: Any,
    ) -> None:
        """
        :param str channel:
            Name of the object to open (e.g. 'CAN0')

        :param int bitrate:
            Bitrate in bits/s

        :param timing:
            Optional :class:`~can.BitTiming` or :class:`~can.BitTimingFd` instance
            to use for custom bit timing setting. The `f_clock` value of the timing
            instance must be set to 40_000_000 (40MHz).
            If this parameter is provided, it takes precedence over all other
            timing-related parameters like `bitrate`, `fd_bitrate` and `fd`.

        :param list can_filters:
            See :meth:`can.BusABC.set_filters`.

        :param receive_own_messages:
            Enable self-reception of sent messages.

        :param poll_interval:
            Poll interval in seconds.

        :raises ~can.exceptions.CanInitializationError:
            If starting communication fails
        """
        if os.name != "nt" and not kwargs.get("_testing", False):
            raise CanInterfaceNotImplementedError(
                f"The NI-XNET interface is only supported on Windows, "
                f'but you are running "{os.name}"'
            )

        if nixnet is None:
            raise CanInterfaceNotImplementedError("The NI-XNET API has not been loaded")

        self.nixnet = nixnet

        self._rx_queue = SimpleQueue()  # type: ignore[var-annotated]
        self.channel = channel
        self.channel_info = "NI-XNET: " + channel

        self.poll_interval = poll_interval

        is_fd = isinstance(timing, BitTimingFd) if timing else fd
        self._can_protocol = CanProtocol.CAN_FD if is_fd else CanProtocol.CAN_20

        # Set database for the initialization
        database_name = ":can_fd_brs:" if is_fd else ":memory:"

        try:
            # We need two sessions for this application,
            # one to send frames and another to receive them
            self._session_send = nixnet.session.FrameOutStreamSession(
                channel, database_name=database_name
            )
            self._session_receive = nixnet.session.FrameInStreamSession(
                channel, database_name=database_name
            )
            self._interface = self._session_send.intf

            # set interface properties
            self._interface.can_lstn_only = kwargs.get("listen_only", False)
            self._interface.echo_tx = receive_own_messages
            self._interface.bus_err_to_in_strm = True

            if isinstance(timing, BitTimingFd):
                timing = check_or_adjust_timing_clock(timing, [40_000_000])
                custom_nom_baud_rate = (  # nxPropSession_IntfBaudRate64
                    0xA0000000
                    + (timing.nom_tq << 32)
                    + (timing.nom_sjw - 1 << 16)
                    + (timing.nom_tseg1 - 1 << 8)
                    + (timing.nom_tseg2 - 1)
                )
                custom_data_baud_rate = (  # nxPropSession_IntfCanFdBaudRate64
                    0xA0000000
                    + (timing.data_tq << 13)
                    + (timing.data_tseg1 - 1 << 8)
                    + (timing.data_tseg2 - 1 << 4)
                    + (timing.data_sjw - 1)
                )
                self._interface.baud_rate = custom_nom_baud_rate
                self._interface.can_fd_baud_rate = custom_data_baud_rate
            elif isinstance(timing, BitTiming):
                timing = check_or_adjust_timing_clock(timing, [40_000_000])
                custom_baud_rate = (  # nxPropSession_IntfBaudRate64
                    0xA0000000
                    + (timing.tq << 32)
                    + (timing.sjw - 1 << 16)
                    + (timing.tseg1 - 1 << 8)
                    + (timing.tseg2 - 1)
                )
                self._interface.baud_rate = custom_baud_rate
            else:
                # See page 1017 of NI-XNET Hardware and Software Manual
                # to set custom can configuration
                if bitrate:
                    self._interface.baud_rate = bitrate

                if is_fd:
                    # See page 951 of NI-XNET Hardware and Software Manual
                    # to set custom can configuration
                    self._interface.can_fd_baud_rate = fd_bitrate or bitrate

            _can_termination = (
                nixnet.constants.CanTerm.ON
                if can_termination
                else nixnet.constants.CanTerm.OFF
            )
            self._interface.can_term = _can_termination

            # self._session_receive.queue_size = 512
            # Once that all the parameters have been set, we start the sessions
            self._session_send.start()
            self._session_receive.start()

        except nixnet.errors.XnetError as error:
            raise CanInitializationError(
                f"{error.args[0]} ({error.error_type})", error.error_code
            ) from None

        self._is_filtered = False
        super().__init__(
            channel=channel,
            can_filters=can_filters,
            bitrate=bitrate,
            **kwargs,
        )

    @property
    def fd(self) -> bool:
        class_name = self.__class__.__name__
        warnings.warn(
            f"The {class_name}.fd property is deprecated and superseded by "
            f"{class_name}.protocol. It is scheduled for removal in python-can version 5.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._can_protocol is CanProtocol.CAN_FD

    def _recv_internal(
        self, timeout: Optional[float]
    ) -> tuple[Optional[Message], bool]:
        end_time = time.perf_counter() + timeout if timeout is not None else None

        while True:
            # try to read all available frames
            for frame in self._session_receive.frames.read(1024, timeout=0):
                self._rx_queue.put_nowait(frame)

            if self._rx_queue.qsize():
                break

            # check for timeout
            if end_time is not None and time.perf_counter() > end_time:
                return None, False

            # Wait a short time until we try to read again
            time.sleep(self.poll_interval)

        can_frame = self._rx_queue.get_nowait()

        # Timestamp should be converted from raw frame format(100ns increment
        # from(12:00 a.m. January 1 1601 Coordinated Universal Time (UTC))
        # to epoch time(number of seconds from January 1, 1970 (midnight UTC/GMT))
        timestamp = can_frame.timestamp * 1e-7 - 11_644_473_600
        if can_frame.type is self.nixnet.constants.FrameType.CAN_BUS_ERROR:
            msg = Message(
                timestamp=timestamp,
                channel=self.channel,
                is_error_frame=True,
            )
        else:
            msg = Message(
                timestamp=timestamp,
                channel=self.channel,
                is_remote_frame=can_frame.type
                is self.nixnet.constants.FrameType.CAN_REMOTE,
                is_error_frame=False,
                is_fd=(
                    can_frame.type is self.nixnet.constants.FrameType.CANFD_DATA
                    or can_frame.type is self.nixnet.constants.FrameType.CANFDBRS_DATA
                ),
                bitrate_switch=(
                    can_frame.type is self.nixnet.constants.FrameType.CANFDBRS_DATA
                ),
                is_extended_id=can_frame.identifier.extended,
                # Get identifier from CanIdentifier structure
                arbitration_id=can_frame.identifier.identifier,
                dlc=len(can_frame.payload),
                data=can_frame.payload,
                is_rx=not can_frame.echo,
            )
        return msg, False

    def send(self, msg: Message, timeout: Optional[float] = None) -> None:
        """
        Send a message using NI-XNET.

        :param can.Message msg:
            Message to send

        :param float timeout:
            Max time to wait for the device to be ready in seconds, None if time is infinite

        :raises can.exceptions.CanOperationError:
            If writing to transmit buffer fails.
            It does not wait for message to be ACKed currently.
        """
        if timeout is None:
            timeout = self.nixnet.constants.TIMEOUT_INFINITE

        if msg.is_remote_frame:
            type_message = self.nixnet.constants.FrameType.CAN_REMOTE
        elif msg.is_error_frame:
            type_message = self.nixnet.constants.FrameType.CAN_BUS_ERROR
        elif msg.is_fd:
            if msg.bitrate_switch:
                type_message = self.nixnet.constants.FrameType.CANFDBRS_DATA
            else:
                type_message = self.nixnet.constants.FrameType.CANFD_DATA
        else:
            type_message = self.nixnet.constants.FrameType.CAN_DATA

        can_frame = self.nixnet.types.CanFrame(
            self.nixnet.types.CanIdentifier(msg.arbitration_id, msg.is_extended_id),
            type=type_message,
            payload=msg.data,
        )

        try:
            self._session_send.frames.write([can_frame], timeout)
        except self.nixnet.errors.XnetError as error:
            raise CanOperationError(
                f"{error.args[0]} ({error.error_type})", error.error_code
            ) from None

    def reset(self) -> None:
        """
        Resets network interface. Stops network interface, then resets the CAN
        chip to clear the CAN error counters (clear error passive state).
        Resetting includes clearing all entries from read and write queues.
        """
        self._session_send.flush()
        self._session_receive.flush()

        self._session_send.stop()
        self._session_receive.stop()

        self._session_send.start()
        self._session_receive.start()

    def shutdown(self) -> None:
        """Close object."""
        super().shutdown()
        if hasattr(self, "_session_send"):
            self._session_send.flush()
            self._session_send.stop()
            self._session_send.close()

        if hasattr(self, "_session_receive"):
            self._session_receive.flush()
            self._session_receive.stop()
            self._session_receive.close()

    @staticmethod
    def _detect_available_configs() -> list[can.typechecking.AutoDetectedConfig]:
        configs = []

        try:
            with nixnet.system.System() as nixnet_system:  # type: ignore[union-attr]
                for interface in nixnet_system.intf_refs_can:
                    channel = str(interface)
                    logger.debug(
                        "Found channel index %d: %s", interface.port_num, channel
                    )
                    configs.append(
                        {
                            "interface": "nixnet",
                            "channel": channel,
                            "can_term_available": interface.can_term_cap
                            is nixnet.constants.CanTermCap.YES,  # type: ignore[union-attr]
                            "supports_fd": interface.can_tcvr_cap
                            is nixnet.constants.CanTcvrCap.HS,  # type: ignore[union-attr]
                        }
                    )
        except Exception as error:
            logger.debug("An error occured while searching for configs: %s", str(error))

        return configs  # type: ignore
