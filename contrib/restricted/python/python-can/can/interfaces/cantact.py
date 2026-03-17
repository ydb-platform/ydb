"""
Interface for CANtact devices from Linklayer Labs
"""

import logging
import time
from collections.abc import Sequence
from typing import Any, Optional, Union
from unittest.mock import Mock

from can import BitTiming, BitTimingFd, BusABC, CanProtocol, Message

from ..exceptions import (
    CanInitializationError,
    CanInterfaceNotImplementedError,
    error_check,
)
from ..typechecking import AutoDetectedConfig
from ..util import check_or_adjust_timing_clock, deprecated_args_alias

logger = logging.getLogger(__name__)

try:
    import cantact
except ImportError:
    cantact = None
    logger.warning(
        "The CANtact module is not installed. Install it using `pip install cantact`"
    )


class CantactBus(BusABC):
    """CANtact interface"""

    @staticmethod
    def _detect_available_configs() -> Sequence[AutoDetectedConfig]:
        try:
            interface = cantact.Interface()
        except (NameError, SystemError, AttributeError):
            logger.debug(
                "Could not import or instantiate cantact, so no configurations are available"
            )
            return []

        channels: list[AutoDetectedConfig] = []
        for i in range(0, interface.channel_count()):
            channels.append({"interface": "cantact", "channel": f"ch:{i}"})
        return channels

    @deprecated_args_alias(
        deprecation_start="4.2.0", deprecation_end="5.0.0", bit_timing="timing"
    )
    def __init__(
        self,
        channel: int,
        bitrate: int = 500_000,
        poll_interval: float = 0.01,
        monitor: bool = False,
        timing: Optional[Union[BitTiming, BitTimingFd]] = None,
        **kwargs: Any,
    ) -> None:
        """
        :param int channel:
            Channel number (zero indexed, labeled on multi-channel devices)
        :param int bitrate:
            Bitrate in bits/s
        :param bool monitor:
            If true, operate in listen-only monitoring mode
        :param timing:
            Optional :class:`~can.BitTiming` instance to use for custom bit timing setting.
            If this argument is set then it overrides the bitrate argument. The
            `f_clock` value of the timing instance must be set to 24_000_000 (24MHz)
            for standard CAN.
            CAN FD and the :class:`~can.BitTimingFd` class are not supported.
        """

        if kwargs.get("_testing", False):
            self.interface = MockInterface()
        else:
            if cantact is None:
                raise CanInterfaceNotImplementedError(
                    "The CANtact module is not installed. "
                    "Install it using `python -m pip install cantact`"
                )
            with error_check(
                "Cannot create the cantact.Interface", CanInitializationError
            ):
                self.interface = cantact.Interface()

        self.channel = int(channel)
        self.channel_info = f"CANtact: ch:{channel}"
        self._can_protocol = CanProtocol.CAN_20

        # Configure the interface
        with error_check("Cannot setup the cantact.Interface", CanInitializationError):
            if isinstance(timing, BitTiming):
                timing = check_or_adjust_timing_clock(timing, valid_clocks=[24_000_000])

                # use custom bit timing
                self.interface.set_bit_timing(
                    int(channel),
                    int(timing.brp),
                    int(timing.tseg1),
                    int(timing.tseg2),
                    int(timing.sjw),
                )
            elif isinstance(timing, BitTimingFd):
                raise NotImplementedError(
                    f"CAN FD is not supported by {self.__class__.__name__}."
                )
            else:
                # use bitrate
                self.interface.set_bitrate(int(channel), int(bitrate))

            self.interface.set_enabled(int(channel), True)
            self.interface.set_monitor(int(channel), monitor)
            self.interface.start()

        super().__init__(
            channel=channel,
            bitrate=bitrate,
            poll_interval=poll_interval,
            **kwargs,
        )

    def _recv_internal(
        self, timeout: Optional[float]
    ) -> tuple[Optional[Message], bool]:
        if timeout is None:
            raise TypeError(
                f"{self.__class__.__name__} expects a numeric `timeout` value."
            )

        with error_check("Cannot receive message"):
            frame = self.interface.recv(int(timeout * 1000))
        if frame is None:
            # timeout occurred
            return None, False

        msg = Message(
            arbitration_id=frame["id"],
            is_extended_id=frame["extended"],
            timestamp=frame["timestamp"],
            is_remote_frame=frame["rtr"],
            dlc=frame["dlc"],
            data=frame["data"][: frame["dlc"]],
            channel=frame["channel"],
            is_rx=(not frame["loopback"]),  # received if not loopback frame
        )
        return msg, False

    def send(self, msg: Message, timeout: Optional[float] = None) -> None:
        with error_check("Cannot send message"):
            self.interface.send(
                self.channel,
                msg.arbitration_id,
                bool(msg.is_extended_id),
                bool(msg.is_remote_frame),
                msg.dlc,
                msg.data,
            )

    def shutdown(self) -> None:
        super().shutdown()
        with error_check("Cannot shutdown interface"):
            self.interface.stop()


def mock_recv(timeout: int) -> Optional[dict[str, Any]]:
    if timeout > 0:
        return {
            "id": 0x123,
            "extended": False,
            "timestamp": time.time(),
            "loopback": False,
            "rtr": False,
            "dlc": 8,
            "data": [1, 2, 3, 4, 5, 6, 7, 8],
            "channel": 0,
        }
    else:
        # simulate timeout when timeout = 0
        return None


class MockInterface:
    """
    Mock interface to replace real interface when testing.
    This allows for tests to run without actual hardware.
    """

    start = Mock()
    set_bitrate = Mock()
    set_bit_timing = Mock()
    set_enabled = Mock()
    set_monitor = Mock()
    stop = Mock()
    send = Mock()
    channel_count = Mock(return_value=1)

    recv = Mock(side_effect=mock_recv)
