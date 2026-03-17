import logging
import time
from collections import deque
from collections.abc import Sequence
from ctypes import c_ubyte
from typing import Any, Optional, Union

import canalystii as driver

from can import BitTiming, BitTimingFd, BusABC, CanProtocol, Message
from can.exceptions import CanTimeoutError
from can.typechecking import CanFilters
from can.util import check_or_adjust_timing_clock, deprecated_args_alias

logger = logging.getLogger(__name__)


class CANalystIIBus(BusABC):
    @deprecated_args_alias(
        deprecation_start="4.2.0", deprecation_end="5.0.0", bit_timing="timing"
    )
    def __init__(
        self,
        channel: Union[int, Sequence[int], str] = (0, 1),
        device: int = 0,
        bitrate: Optional[int] = None,
        timing: Optional[Union[BitTiming, BitTimingFd]] = None,
        can_filters: Optional[CanFilters] = None,
        rx_queue_size: Optional[int] = None,
        **kwargs: dict[str, Any],
    ):
        """

        :param channel:
            Optional channel number, list/tuple of multiple channels, or comma
            separated string of channels. Default is to configure both
            channels.
        :param device:
            Optional USB device number. Default is 0 (first device found).
        :param bitrate:
            CAN bitrate in bits/second. Required unless the bit_timing argument is set.
        :param timing:
            Optional :class:`~can.BitTiming` instance to use for custom bit timing setting.
            If this argument is set then it overrides the bitrate argument. The
            `f_clock` value of the timing instance must be set to 8_000_000 (8MHz)
            for standard CAN.
            CAN FD and the :class:`~can.BitTimingFd` class are not supported.
        :param can_filters:
            Optional filters for received CAN messages.
        :param rx_queue_size:
            If set, software received message queue can only grow to this many
            messages (for all channels) before older messages are dropped
        """
        if not (bitrate or timing):
            raise ValueError("Either bitrate or timing argument is required")

        # Do this after the error handling
        super().__init__(
            channel=channel,
            can_filters=can_filters,
            **kwargs,
        )
        if isinstance(channel, str):
            # Assume comma separated string of channels
            self.channels = [int(ch.strip()) for ch in channel.split(",")]
        elif isinstance(channel, int):
            self.channels = [channel]
        else:  # Sequence[int]
            self.channels = list(channel)

        self.channel_info = f"CANalyst-II: device {device}, channels {self.channels}"
        self.rx_queue: deque[tuple[int, driver.Message]] = deque(maxlen=rx_queue_size)
        self.device = driver.CanalystDevice(device_index=device)
        self._can_protocol = CanProtocol.CAN_20

        for single_channel in self.channels:
            if isinstance(timing, BitTiming):
                timing = check_or_adjust_timing_clock(timing, valid_clocks=[8_000_000])
                self.device.init(
                    single_channel, timing0=timing.btr0, timing1=timing.btr1
                )
            elif isinstance(timing, BitTimingFd):
                raise NotImplementedError(
                    f"CAN FD is not supported by {self.__class__.__name__}."
                )
            else:
                self.device.init(single_channel, bitrate=bitrate)

    # Delay to use between each poll for new messages
    #
    # The timeout is deliberately kept low to avoid the possibility of
    # a hardware buffer overflow. This value was determined
    # experimentally, but the ideal value will depend on the specific
    # system.
    RX_POLL_DELAY = 0.020

    def send(self, msg: Message, timeout: Optional[float] = None) -> None:
        """Send a CAN message to the bus

        :param msg: message to send
        :param timeout: timeout (in seconds) to wait for the TX queue to clear.
        If set to ``None`` (default) the function returns immediately.

        Note: Due to limitations in the device firmware and protocol, the
        timeout will not trigger if there are problems with CAN arbitration,
        but only if the device is overloaded with a backlog of too many
        messages to send.
        """
        raw_message = driver.Message(
            msg.arbitration_id,
            0,  # timestamp
            1,  # time_flag
            0,  # send_type
            msg.is_remote_frame,
            msg.is_extended_id,
            msg.dlc,
            (c_ubyte * 8)(*msg.data),
        )

        if msg.channel is not None:
            channel = msg.channel
        elif len(self.channels) == 1:
            channel = self.channels[0]
        else:
            raise ValueError(
                "Message channel must be set when using multiple channels."
            )

        send_result = self.device.send(channel, [raw_message], timeout)
        if timeout is not None and not send_result:
            raise CanTimeoutError(f"Send timed out after {timeout} seconds")

    def _recv_from_queue(self) -> tuple[Message, bool]:
        """Return a message from the internal receive queue"""
        channel, raw_msg = self.rx_queue.popleft()

        # Protocol timestamps are in units of 100us, convert to seconds as
        # float
        timestamp = raw_msg.timestamp * 100e-6

        return (
            Message(
                channel=channel,
                timestamp=timestamp,
                arbitration_id=raw_msg.can_id,
                is_extended_id=raw_msg.extended,
                is_remote_frame=raw_msg.remote,
                dlc=raw_msg.data_len,
                data=bytes(raw_msg.data),
            ),
            False,
        )

    def poll_received_messages(self) -> None:
        """Poll new messages from the device into the rx queue but don't
        return any message to the caller

        Calling this function isn't necessary as polling the device is done
        automatically when calling recv(). This function is for the situation
        where an application needs to empty the hardware receive buffer without
        consuming any message.
        """
        for channel in self.channels:
            self.rx_queue.extend(
                (channel, raw_msg) for raw_msg in self.device.receive(channel)
            )

    def _recv_internal(
        self, timeout: Optional[float] = None
    ) -> tuple[Optional[Message], bool]:
        """

        :param timeout: float in seconds
        :return:
        """

        if self.rx_queue:
            return self._recv_from_queue()

        deadline = None
        while deadline is None or time.time() < deadline:
            if deadline is None and timeout is not None:
                deadline = time.time() + timeout

            self.poll_received_messages()

            if self.rx_queue:
                return self._recv_from_queue()

            # If blocking on a timeout, add a sleep before we loop again
            # to reduce CPU usage.
            if deadline is None or deadline - time.time() > 0.050:
                time.sleep(self.RX_POLL_DELAY)

        return (None, False)

    def flush_tx_buffer(self, channel: Optional[int] = None) -> None:
        """Flush the TX buffer of the device.

        :param channel:
            Optional channel number to flush. If set to None, all initialized
            channels are flushed.

        Note that because of protocol limitations this function returning
        doesn't mean that messages have been sent, it may also mean they
        failed to send.
        """
        if channel:
            self.device.flush_tx_buffer(channel, float("infinity"))
        else:
            for ch in self.channels:
                self.device.flush_tx_buffer(ch, float("infinity"))

    def shutdown(self) -> None:
        super().shutdown()
        for channel in self.channels:
            self.device.stop(channel)
        self.device = None
