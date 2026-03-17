"""Neousys CAN bus driver"""

#
# This kind of interface can be found for example on Neousys POC-551VTC
# One needs to have correct drivers and DLL (Share object for Linux) from Neousys
#
# https://www.neousys-tech.com/en/support-service/resources/category/299-poc-551vtc-driver
#
# Beware this is only tested on Linux kernel higher than v5.3. This should be drop in
# with Windows but you have to replace with correct named DLL
#

# pylint: disable=too-few-public-methods
# pylint: disable=too-many-instance-attributes
# pylint: disable=wrong-import-position

import logging
import platform
import queue
from ctypes import (
    CFUNCTYPE,
    POINTER,
    Structure,
    byref,
    c_ubyte,
    c_uint,
    c_ushort,
    sizeof,
)
from time import time

try:
    from ctypes import WinDLL
except ImportError:
    from ctypes import CDLL

from can import (
    BusABC,
    CanInitializationError,
    CanInterfaceNotImplementedError,
    CanOperationError,
    CanProtocol,
    Message,
)

logger = logging.getLogger(__name__)


class NeousysCanSetup(Structure):
    """C CAN Setup struct"""

    _fields_ = [
        ("bitRate", c_uint),
        ("recvConfig", c_uint),
        ("recvId", c_uint),
        ("recvMask", c_uint),
    ]


class NeousysCanMsg(Structure):
    """C CAN Message struct"""

    _fields_ = [
        ("id", c_uint),
        ("flags", c_ushort),
        ("extra", c_ubyte),
        ("len", c_ubyte),
        ("data", c_ubyte * 8),
    ]


# valid:2~16, sum of the Synchronization, Propagation, and
#             Phase Buffer 1 segments, measured in time quanta.
# valid:1~8, the Phase Buffer 2 segment in time quanta.
# valid:1~4, Resynchronization Jump Width in time quanta
# valid:1~1023, CAN_CLK divider used to determine time quanta
class NeousysCanBitClk(Structure):
    """C CAN BIT Clock struct"""

    _fields_ = [
        ("syncPropPhase1Seg", c_ushort),
        ("phase2Seg", c_ushort),
        ("jumpWidth", c_ushort),
        ("quantumPrescaler", c_ushort),
    ]


NEOUSYS_CAN_MSG_CALLBACK = CFUNCTYPE(None, POINTER(NeousysCanMsg), c_uint)
NEOUSYS_CAN_STATUS_CALLBACK = CFUNCTYPE(None, c_uint)

NEOUSYS_CAN_MSG_EXTENDED_ID = 0x0004
NEOUSYS_CAN_MSG_REMOTE_FRAME = 0x0040
NEOUSYS_CAN_MSG_DATA_NEW = 0x0080
NEOUSYS_CAN_MSG_DATA_LOST = 0x0100

NEOUSYS_CAN_MSG_USE_ID_FILTER = 0x00000008
NEOUSYS_CAN_MSG_USE_DIR_FILTER = (
    0x00000010 | NEOUSYS_CAN_MSG_USE_ID_FILTER
)  # only accept the direction specified in the message type
NEOUSYS_CAN_MSG_USE_EXT_FILTER = (
    0x00000020 | NEOUSYS_CAN_MSG_USE_ID_FILTER
)  # filters on only extended identifiers

NEOUSYS_CAN_STATUS_BUS_OFF = 0x00000080
NEOUSYS_CAN_STATUS_EWARN = (
    0x00000040  # can controller error level has reached warning level.
)
NEOUSYS_CAN_STATUS_EPASS = (
    0x00000020  # can controller error level has reached error passive level.
)
NEOUSYS_CAN_STATUS_LEC_STUFF = 0x00000001  # a bit stuffing error has occurred.
NEOUSYS_CAN_STATUS_LEC_FORM = 0x00000002  # a formatting error has occurred.
NEOUSYS_CAN_STATUS_LEC_ACK = 0x00000003  # an acknowledge error has occurred.
NEOUSYS_CAN_STATUS_LEC_BIT1 = (
    0x00000004  # the bus remained a bit level of 1 for longer than is allowed.
)
NEOUSYS_CAN_STATUS_LEC_BIT0 = (
    0x00000005  # the bus remained a bit level of 0 for longer than is allowed.
)
NEOUSYS_CAN_STATUS_LEC_CRC = 0x00000006  # a crc error has occurred.
NEOUSYS_CAN_STATUS_LEC_MASK = (
    0x00000007  # this is the mask for the can last error code (lec).
)

NEOUSYS_CANLIB = None

try:
    if platform.system() == "Windows":
        NEOUSYS_CANLIB = WinDLL("./WDT_DIO.dll")
    else:
        NEOUSYS_CANLIB = CDLL("libwdt_dio.so")
    logger.info("Loaded Neousys WDT_DIO Can driver")
except OSError as error:
    logger.info("Cannot load Neousys CAN bus dll or shared object: %s", error)


class NeousysBus(BusABC):
    """Neousys CAN bus Class"""

    def __init__(self, channel, device=0, bitrate=500000, **kwargs):
        """
        :param channel: channel number
        :param device: device number
        :param bitrate: bit rate.
        """
        super().__init__(channel, **kwargs)

        if NEOUSYS_CANLIB is None:
            raise CanInterfaceNotImplementedError("Neousys WDT_DIO Can driver missing")

        self.channel = channel
        self.device = device
        self.channel_info = f"Neousys Can: device {self.device}, channel {self.channel}"
        self._can_protocol = CanProtocol.CAN_20

        self.queue = queue.Queue()

        # Init with accept all and wanted bitrate
        self.init_config = NeousysCanSetup(bitrate, NEOUSYS_CAN_MSG_USE_ID_FILTER, 0, 0)

        self._neousys_recv_cb = NEOUSYS_CAN_MSG_CALLBACK(self._neousys_recv_cb)
        self._neousys_status_cb = NEOUSYS_CAN_STATUS_CALLBACK(self._neousys_status_cb)

        if NEOUSYS_CANLIB.CAN_RegisterReceived(0, self._neousys_recv_cb) == 0:
            raise CanInitializationError("Neousys CAN bus Setup receive callback")

        if NEOUSYS_CANLIB.CAN_RegisterStatus(0, self._neousys_status_cb) == 0:
            raise CanInitializationError("Neousys CAN bus Setup status callback")

        if (
            NEOUSYS_CANLIB.CAN_Setup(
                channel, byref(self.init_config), sizeof(self.init_config)
            )
            == 0
        ):
            raise CanInitializationError("Neousys CAN bus Setup Error")

        if NEOUSYS_CANLIB.CAN_Start(channel) == 0:
            raise CanInitializationError("Neousys CAN bus Start Error")

    def send(self, msg, timeout=None) -> None:
        """
        :param msg: message to send
        :param timeout: timeout is not used here
        """

        tx_msg = NeousysCanMsg(
            msg.arbitration_id, 0, 0, msg.dlc, (c_ubyte * 8)(*msg.data)
        )

        if NEOUSYS_CANLIB.CAN_Send(self.channel, byref(tx_msg), sizeof(tx_msg)) == 0:
            raise CanOperationError("Neousys Can can't send message")

    def _recv_internal(self, timeout):
        try:
            return self.queue.get(block=True, timeout=timeout), False
        except queue.Empty:
            return None, False

    def _neousys_recv_cb(self, msg, sizeof_msg) -> None:
        """
        :param msg: struct CAN_MSG
        :param sizeof_msg: message number
        """
        msg_bytes = bytearray(msg.contents.data)
        remote_frame = bool(msg.contents.flags & NEOUSYS_CAN_MSG_REMOTE_FRAME)
        extended_frame = bool(msg.contents.flags & NEOUSYS_CAN_MSG_EXTENDED_ID)

        if msg.contents.flags & NEOUSYS_CAN_MSG_DATA_LOST:
            logger.error("_neousys_recv_cb flag CAN_MSG_DATA_LOST")

        msg = Message(
            timestamp=time(),
            arbitration_id=msg.contents.id,
            is_remote_frame=remote_frame,
            is_extended_id=extended_frame,
            channel=self.channel,
            dlc=msg.contents.len,
            data=msg_bytes[: msg.contents.len],
        )

        # Reading happens in Callback function and
        # with Python-CAN it happens polling
        # so cache stuff in array to for poll
        try:
            self.queue.put(msg)
        except queue.Full:
            raise CanOperationError("Neousys message Queue is full") from None

    def _neousys_status_cb(self, status) -> None:
        """
        :param status: BUS Status
        """
        logger.info("%s _neousys_status_cb: %d", self.init_config, status)

    def shutdown(self):
        super().shutdown()
        NEOUSYS_CANLIB.CAN_Stop(self.channel)

    @staticmethod
    def _detect_available_configs():
        if NEOUSYS_CANLIB is None:
            return []
        else:
            # There is only one channel
            return [{"interface": "neousys", "channel": 0}]
