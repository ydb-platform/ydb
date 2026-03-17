import logging
from threading import Event

from can import (
    BusABC,
    BusState,
    CanError,
    CanInitializationError,
    CanOperationError,
    CanProtocol,
    Message,
)

from .constants import *
from .exceptions import UcanException
from .structures import *
from .ucan import UcanServer

log = logging.getLogger("can.systec")


class Ucan(UcanServer):
    """
    Wrapper around UcanServer to read messages with timeout using events.
    """

    def __init__(self):
        super().__init__()
        self._msg_received_event = Event()

    def can_msg_received_event(self, channel):
        self._msg_received_event.set()

    def read_can_msg(self, channel, count, timeout):
        self._msg_received_event.clear()
        if self.get_msg_pending(channel, PendingFlags.PENDING_FLAG_RX_DLL) == 0:
            if not self._msg_received_event.wait(timeout):
                return None, False
        return super().read_can_msg(channel, 1)


class UcanBus(BusABC):
    """
    The CAN Bus implemented for the SYSTEC interface.
    """

    BITRATES = {
        10000: Baudrate.BAUD_10kBit,
        20000: Baudrate.BAUD_20kBit,
        50000: Baudrate.BAUD_50kBit,
        100000: Baudrate.BAUD_100kBit,
        125000: Baudrate.BAUD_125kBit,
        250000: Baudrate.BAUD_250kBit,
        500000: Baudrate.BAUD_500kBit,
        800000: Baudrate.BAUD_800kBit,
        1000000: Baudrate.BAUD_1MBit,
    }

    def __init__(self, channel, can_filters=None, **kwargs):
        """
        :param int channel:
            The Channel id to create this bus with.

        :param list can_filters:
            See :meth:`can.BusABC.set_filters`.

        Backend Configuration

        :param int bitrate:
            Channel bitrate in bit/s.
            Default is 500000.

        :param int device_number:
            The device number of the USB-CAN.
            Valid values: 0 through 254. Special value 255 is reserved to detect the first connected device (should only
            be used, in case only one module is connected to the computer).
            Default is 255.

        :param can.bus.BusState state:
            BusState of the channel.
            Default is ACTIVE.

        :param bool receive_own_messages:
            If messages transmitted should also be received back.
            Default is False.

        :param int rx_buffer_entries:
            The maximum number of entries in the receive buffer.
            Default is 4096.

        :param int tx_buffer_entries:
            The maximum number of entries in the transmit buffer.
            Default is 4096.

        :raises ValueError:
            If invalid input parameter were passed.

        :raises ~can.exceptions.CanInterfaceNotImplementedError:
            If the platform is not supported.

        :raises ~can.exceptions.CanInitializationError:
            If hardware or CAN interface initialization failed.
        """
        try:
            self._ucan = Ucan()
        except CanError as error:
            raise error
        except Exception as exception:
            raise CanInitializationError(
                "The SYSTEC ucan library has not been initialized."
            ) from exception

        self.channel = int(channel)
        self._can_protocol = CanProtocol.CAN_20

        device_number = int(kwargs.get("device_number", ANY_MODULE))

        # configuration options
        bitrate = kwargs.get("bitrate", 500000)
        if bitrate not in self.BITRATES:
            raise ValueError(f"Invalid bitrate {bitrate}")

        state = kwargs.get("state", BusState.ACTIVE)
        if state is BusState.ACTIVE or state is BusState.PASSIVE:
            self._state = state
        else:
            raise ValueError("BusState must be Active or Passive")

        # get parameters
        self._params = {
            "mode": Mode.MODE_NORMAL
            | (Mode.MODE_TX_ECHO if kwargs.get("receive_own_messages") else 0)
            | (Mode.MODE_LISTEN_ONLY if state is BusState.PASSIVE else 0),
            "BTR": self.BITRATES[bitrate],
        }
        # get extra parameters
        if kwargs.get("rx_buffer_entries"):
            self._params["rx_buffer_entries"] = int(kwargs.get("rx_buffer_entries"))
        if kwargs.get("tx_buffer_entries"):
            self._params["tx_buffer_entries"] = int(kwargs.get("tx_buffer_entries"))

        try:
            self._ucan.init_hardware(device_number=device_number)
            self._ucan.init_can(self.channel, **self._params)
            hw_info_ex, _, _ = self._ucan.get_hardware_info()
            self.channel_info = (
                f"{self._ucan.get_product_code_message(hw_info_ex.product_code)}, "
                f"S/N {hw_info_ex.serial}, "
                f"CH {self.channel}, "
                f"BTR {self._ucan.get_baudrate_message(self.BITRATES[bitrate])}"
            )
        except UcanException as exception:
            raise CanInitializationError() from exception

        self._is_filtered = False

        super().__init__(
            channel=channel,
            can_filters=can_filters,
            **kwargs,
        )

    def _recv_internal(self, timeout):
        try:
            message, _ = self._ucan.read_can_msg(self.channel, 1, timeout)
        except UcanException as exception:
            raise CanOperationError() from exception

        if not message:
            return None, False

        msg = Message(
            timestamp=float(message[0].time) / 1000.0,
            is_remote_frame=bool(message[0].frame_format & MsgFrameFormat.MSG_FF_RTR),
            is_extended_id=bool(message[0].frame_format & MsgFrameFormat.MSG_FF_EXT),
            arbitration_id=message[0].id,
            dlc=len(message[0].data),
            data=message[0].data,
        )
        return msg, self._is_filtered

    def send(self, msg, timeout=None):
        """
        Sends one CAN message.

        When a transmission timeout is set the firmware tries to send
        a message within this timeout. If it could not be sent the firmware sets
        the "auto delete" state. Within this state all transmit CAN messages for
        this channel will be deleted automatically for not blocking the other channel.

        :param can.Message msg:
            The CAN message.

        :param float timeout:
            Transmit timeout in seconds (value 0 switches off the "auto delete")

        :raises ~can.exceptions.CanOperationError:
            If the message could not be sent.
        """
        try:
            if timeout is not None and timeout >= 0:
                self._ucan.set_tx_timeout(self.channel, int(timeout * 1000))

            message = CanMsg(
                msg.arbitration_id,
                MsgFrameFormat.MSG_FF_STD
                | (MsgFrameFormat.MSG_FF_EXT if msg.is_extended_id else 0)
                | (MsgFrameFormat.MSG_FF_RTR if msg.is_remote_frame else 0),
                msg.data,
                msg.dlc,
            )
            self._ucan.write_can_msg(self.channel, [message])
        except UcanException as exception:
            raise CanOperationError() from exception

    @staticmethod
    def _detect_available_configs():
        configs = []
        try:
            # index, is_used, hw_info_ex, init_info
            for _, _, hw_info_ex, _ in Ucan.enumerate_hardware():
                configs.append(
                    {
                        "interface": "systec",
                        "channel": Channel.CHANNEL_CH0,
                        "device_number": hw_info_ex.device_number,
                    }
                )
                if Ucan.check_support_two_channel(hw_info_ex):
                    configs.append(
                        {
                            "interface": "systec",
                            "channel": Channel.CHANNEL_CH1,
                            "device_number": hw_info_ex.device_number,
                        }
                    )
        except Exception:
            log.warning("The SYSTEC ucan library has not been initialized.")
        return configs

    def _apply_filters(self, filters):
        try:
            if filters and len(filters) == 1:
                can_id = filters[0]["can_id"]
                can_mask = filters[0]["can_mask"]
                self._ucan.set_acceptance(self.channel, can_mask, can_id)
                self._is_filtered = True
                log.info("Hardware filtering on ID 0x%X, mask 0x%X", can_id, can_mask)
            else:
                self._ucan.set_acceptance(self.channel)
                self._is_filtered = False
                log.info("Hardware filtering has been disabled")
        except UcanException as exception:
            raise CanOperationError() from exception

    def flush_tx_buffer(self):
        """
        Flushes the transmit buffer.

        :raises ~can.exceptions.CanError:
            If flushing of the transmit buffer failed.
        """
        log.info("Flushing transmit buffer")
        try:
            self._ucan.reset_can(self.channel, ResetFlags.RESET_ONLY_TX_BUFF)
        except UcanException as exception:
            raise CanOperationError() from exception

    @staticmethod
    def create_filter(extended, from_id, to_id, rtr_only, rtr_too):
        """
        Calculates AMR and ACR using CAN-ID as parameter.

        :param bool extended:
            if True parameters from_id and to_id contains 29-bit CAN-ID

        :param int from_id:
            first CAN-ID which should be received

        :param int to_id:
            last CAN-ID which should be received

        :param bool rtr_only:
            if True only RTR-Messages should be received, and rtr_too will be ignored

        :param bool rtr_too:
            if True CAN data frames and RTR-Messages should be received

        :return: Returns list with one filter containing a "can_id", a "can_mask" and "extended" key.
        """
        return [
            {
                "can_id": Ucan.calculate_acr(
                    extended, from_id, to_id, rtr_only, rtr_too
                ),
                "can_mask": Ucan.calculate_amr(
                    extended, from_id, to_id, rtr_only, rtr_too
                ),
                "extended": extended,
            }
        ]

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, new_state):
        if self._state is not BusState.ERROR and (
            new_state is BusState.ACTIVE or new_state is BusState.PASSIVE
        ):
            try:
                # close the CAN channel
                self._ucan.shutdown(self.channel, False)
                # set mode
                if new_state is BusState.ACTIVE:
                    self._params["mode"] &= ~Mode.MODE_LISTEN_ONLY
                else:
                    self._params["mode"] |= Mode.MODE_LISTEN_ONLY
                # reinitialize CAN channel
                self._ucan.init_can(self.channel, **self._params)
            except UcanException as exception:
                raise CanOperationError() from exception

    def shutdown(self):
        """
        Shuts down all CAN interfaces and hardware interface.
        """
        super().shutdown()
        try:
            self._ucan.shutdown()
        except Exception as exception:
            log.error(exception)
