from collections.abc import Sequence
from typing import Callable, Optional, Union

import can.interfaces.ixxat.canlib_vcinpl as vcinpl
import can.interfaces.ixxat.canlib_vcinpl2 as vcinpl2
from can import (
    BusABC,
    BusState,
    CyclicSendTaskABC,
    Message,
)


class IXXATBus(BusABC):
    """The CAN Bus implemented for the IXXAT interface.

    Based on the C implementation of IXXAT, two different dlls are provided by IXXAT, one to work with CAN,
    the other with CAN-FD.

    This class only delegates to related implementation (in calib_vcinpl or canlib_vcinpl2)
    class depending on fd user option.
    """

    def __init__(
        self,
        channel: int,
        can_filters=None,
        receive_own_messages: bool = False,
        unique_hardware_id: Optional[int] = None,
        extended: bool = True,
        fd: bool = False,
        rx_fifo_size: Optional[int] = None,
        tx_fifo_size: Optional[int] = None,
        bitrate: int = 500000,
        data_bitrate: int = 2000000,
        sjw_abr: Optional[int] = None,
        tseg1_abr: Optional[int] = None,
        tseg2_abr: Optional[int] = None,
        sjw_dbr: Optional[int] = None,
        tseg1_dbr: Optional[int] = None,
        tseg2_dbr: Optional[int] = None,
        ssp_dbr: Optional[int] = None,
        **kwargs,
    ):
        """
        :param channel:
            The Channel id to create this bus with.

        :param can_filters:
            See :meth:`can.BusABC.set_filters`.

        :param receive_own_messages:
            Enable self-reception of sent messages.

        :param unique_hardware_id:
            UniqueHardwareId to connect (optional, will use the first found if not supplied)

        :param extended:
            Default True, enables the capability to use extended IDs.

        :param fd:
            Default False, enables CAN-FD usage.

        :param rx_fifo_size:
            Receive fifo size (default 1024 for fd, else 16)

        :param tx_fifo_size:
            Transmit fifo size (default 128 for fd, else 16)

        :param bitrate:
            Channel bitrate in bit/s

        :param data_bitrate:
            Channel bitrate in bit/s (only in CAN-Fd if baudrate switch enabled).

        :param sjw_abr:
            Bus timing value sample jump width (arbitration). Only takes effect with fd enabled.

        :param tseg1_abr:
            Bus timing value tseg1 (arbitration). Only takes effect with fd enabled.

        :param tseg2_abr:
            Bus timing value tseg2 (arbitration). Only takes effect with fd enabled.

        :param sjw_dbr:
            Bus timing value sample jump width (data). Only takes effect with fd and baudrate switch enabled.

        :param tseg1_dbr:
            Bus timing value tseg1 (data). Only takes effect with fd and bitrate switch enabled.

        :param tseg2_dbr:
            Bus timing value tseg2 (data). Only takes effect with fd and bitrate switch enabled.

        :param ssp_dbr:
            Secondary sample point (data). Only takes effect with fd and bitrate switch enabled.

        """
        if fd:
            if rx_fifo_size is None:
                rx_fifo_size = 1024
            if tx_fifo_size is None:
                tx_fifo_size = 128
            self.bus = vcinpl2.IXXATBus(
                channel=channel,
                can_filters=can_filters,
                receive_own_messages=receive_own_messages,
                unique_hardware_id=unique_hardware_id,
                extended=extended,
                rx_fifo_size=rx_fifo_size,
                tx_fifo_size=tx_fifo_size,
                bitrate=bitrate,
                data_bitrate=data_bitrate,
                sjw_abr=sjw_abr,
                tseg1_abr=tseg1_abr,
                tseg2_abr=tseg2_abr,
                sjw_dbr=sjw_dbr,
                tseg1_dbr=tseg1_dbr,
                tseg2_dbr=tseg2_dbr,
                ssp_dbr=ssp_dbr,
                **kwargs,
            )
        else:
            if rx_fifo_size is None:
                rx_fifo_size = 16
            if tx_fifo_size is None:
                tx_fifo_size = 16
            self.bus = vcinpl.IXXATBus(
                channel=channel,
                can_filters=can_filters,
                receive_own_messages=receive_own_messages,
                unique_hardware_id=unique_hardware_id,
                extended=extended,
                rx_fifo_size=rx_fifo_size,
                tx_fifo_size=tx_fifo_size,
                bitrate=bitrate,
                **kwargs,
            )

        super().__init__(channel=channel, **kwargs)
        self._can_protocol = self.bus.protocol

    def flush_tx_buffer(self):
        """Flushes the transmit buffer on the IXXAT"""
        return self.bus.flush_tx_buffer()

    def _recv_internal(self, timeout):
        """Read a message from IXXAT device."""
        return self.bus._recv_internal(timeout)

    def send(self, msg: Message, timeout: Optional[float] = None) -> None:
        return self.bus.send(msg, timeout)

    def _send_periodic_internal(
        self,
        msgs: Union[Sequence[Message], Message],
        period: float,
        duration: Optional[float] = None,
        autostart: bool = True,
        modifier_callback: Optional[Callable[[Message], None]] = None,
    ) -> CyclicSendTaskABC:
        return self.bus._send_periodic_internal(
            msgs, period, duration, autostart, modifier_callback
        )

    def shutdown(self) -> None:
        super().shutdown()
        self.bus.shutdown()

    @property
    def state(self) -> BusState:
        """
        Return the current state of the hardware
        """
        return self.bus.state

    @staticmethod
    def _detect_available_configs() -> Sequence[vcinpl.AutoDetectedIxxatConfig]:
        return vcinpl._detect_available_configs()
