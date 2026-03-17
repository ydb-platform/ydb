"""
The main module of the socketcan interface containing most user-facing classes and methods
along some internal methods.

At the end of the file the usage of the internal methods is shown.
"""

import ctypes
import ctypes.util
import errno
import logging
import select
import socket
import struct
import threading
import time
import warnings
from collections.abc import Sequence
from typing import Callable, Optional, Union

import can
from can import BusABC, CanProtocol, Message
from can.broadcastmanager import (
    LimitedDurationCyclicSendTaskABC,
    ModifiableCyclicTaskABC,
    RestartableCyclicTaskABC,
)
from can.interfaces.socketcan import constants
from can.interfaces.socketcan.utils import find_available_interfaces, pack_filters
from can.typechecking import CanFilters

log = logging.getLogger(__name__)
log_tx = log.getChild("tx")
log_rx = log.getChild("rx")

try:
    from socket import CMSG_SPACE

    CMSG_SPACE_available = True
except ImportError:
    CMSG_SPACE_available = False
    log.error("socket.CMSG_SPACE not available on this platform")


# Constants needed for precise handling of timestamps
RECEIVED_TIMESTAMP_STRUCT = struct.Struct("@ll")
RECEIVED_ANCILLARY_BUFFER_SIZE = (
    CMSG_SPACE(RECEIVED_TIMESTAMP_STRUCT.size) if CMSG_SPACE_available else 0
)


# Setup BCM struct
def bcm_header_factory(
    fields: list[tuple[str, Union[type[ctypes.c_uint32], type[ctypes.c_long]]]],
    alignment: int = 8,
):
    curr_stride = 0
    results: list[
        tuple[
            str, Union[type[ctypes.c_uint8], type[ctypes.c_uint32], type[ctypes.c_long]]
        ]
    ] = []
    pad_index = 0
    for field in fields:
        field_alignment = ctypes.alignment(field[1])
        field_size = ctypes.sizeof(field[1])

        # If the current stride index isn't a multiple of the alignment
        # requirements of this field, then we must add padding bytes until we
        # are aligned
        while curr_stride % field_alignment != 0:
            results.append((f"pad_{pad_index}", ctypes.c_uint8))
            pad_index += 1
            curr_stride += 1

        # Now can it fit?
        # Example: If this is 8 bytes and the type requires 4 bytes alignment
        # then we can only fit when we're starting at 0. Otherwise, we will
        # split across 2 strides.
        #
        # | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
        results.append(field)
        curr_stride += field_size

    # Add trailing padding to align to a multiple of the largest scalar member
    # in the structure
    while curr_stride % alignment != 0:
        results.append((f"pad_{pad_index}", ctypes.c_uint8))
        pad_index += 1
        curr_stride += 1

    return type("BcmMsgHead", (ctypes.Structure,), {"_fields_": results})


# The fields definition is taken from the C struct definitions in
# <linux/can/bcm.h>
#
#     struct bcm_timeval {
#     	long tv_sec;
#     	long tv_usec;
#     };
#
#     /**
#      * struct bcm_msg_head - head of messages to/from the broadcast manager
#      * @opcode:    opcode, see enum below.
#      * @flags:     special flags, see below.
#      * @count:     number of frames to send before changing interval.
#      * @ival1:     interval for the first @count frames.
#      * @ival2:     interval for the following frames.
#      * @can_id:    CAN ID of frames to be sent or received.
#      * @nframes:   number of frames appended to the message head.
#      * @frames:    array of CAN frames.
#      */
#     struct bcm_msg_head {
#     	__u32 opcode;
#     	__u32 flags;
#     	__u32 count;
#     	struct bcm_timeval ival1, ival2;
#     	canid_t can_id;
#     	__u32 nframes;
#     	struct can_frame frames[0];
#     };
BcmMsgHead = bcm_header_factory(
    fields=[
        ("opcode", ctypes.c_uint32),
        ("flags", ctypes.c_uint32),
        ("count", ctypes.c_uint32),
        ("ival1_tv_sec", ctypes.c_long),
        ("ival1_tv_usec", ctypes.c_long),
        ("ival2_tv_sec", ctypes.c_long),
        ("ival2_tv_usec", ctypes.c_long),
        ("can_id", ctypes.c_uint32),
        ("nframes", ctypes.c_uint32),
    ]
)


# struct module defines a binary packing format:
# https://docs.python.org/3/library/struct.html#struct-format-strings
# The 32bit can id is directly followed by the 8bit data link count
# The data field is aligned on an 8 byte boundary, hence we add padding
# which aligns the data field to an 8 byte boundary.
CAN_FRAME_HEADER_STRUCT = struct.Struct("=IBB1xB")


def build_can_frame(msg: Message) -> bytes:
    """CAN frame packing/unpacking (see 'struct can_frame' in <linux/can.h>)
    /**
    * struct can_frame - Classical CAN frame structure (aka CAN 2.0B)
    * @can_id:   CAN ID of the frame and CAN_*_FLAG flags, see canid_t definition
    * @len:      CAN frame payload length in byte (0 .. 8)
    * @can_dlc:  deprecated name for CAN frame payload length in byte (0 .. 8)
    * @__pad:    padding
    * @__res0:   reserved / padding
    * @len8_dlc: optional DLC value (9 .. 15) at 8 byte payload length
    *            len8_dlc contains values from 9 .. 15 when the payload length is
    *            8 bytes but the DLC value (see ISO 11898-1) is greater then 8.
    *            CAN_CTRLMODE_CC_LEN8_DLC flag has to be enabled in CAN driver.
    * @data:     CAN frame payload (up to 8 byte)
    */
    struct can_frame {
        canid_t can_id;  /* 32 bit CAN_ID + EFF/RTR/ERR flags */
        union {
            /* CAN frame payload length in byte (0 .. CAN_MAX_DLEN)
            * was previously named can_dlc so we need to carry that
            * name for legacy support
            */
            __u8 len;
            __u8 can_dlc; /* deprecated */
        } __attribute__((packed)); /* disable padding added in some ABIs */
        __u8 __pad; /* padding */
        __u8 __res0; /* reserved / padding */
        __u8 len8_dlc; /* optional DLC for 8 byte payload length (9 .. 15) */
        __u8 data[CAN_MAX_DLEN] __attribute__((aligned(8)));
    };

    /*
    * defined bits for canfd_frame.flags
    *
    * The use of struct canfd_frame implies the FD Frame (FDF) bit to
    * be set in the CAN frame bitstream on the wire. The FDF bit switch turns
    * the CAN controllers bitstream processor into the CAN FD mode which creates
    * two new options within the CAN FD frame specification:
    *
    * Bit Rate Switch - to indicate a second bitrate is/was used for the payload
    * Error State Indicator - represents the error state of the transmitting node
    *
    * As the CANFD_ESI bit is internally generated by the transmitting CAN
    * controller only the CANFD_BRS bit is relevant for real CAN controllers when
    * building a CAN FD frame for transmission. Setting the CANFD_ESI bit can make
    * sense for virtual CAN interfaces to test applications with echoed frames.
    *
    * The struct can_frame and struct canfd_frame intentionally share the same
    * layout to be able to write CAN frame content into a CAN FD frame structure.
    * When this is done the former differentiation via CAN_MTU / CANFD_MTU gets
    * lost. CANFD_FDF allows programmers to mark CAN FD frames in the case of
    * using struct canfd_frame for mixed CAN / CAN FD content (dual use).
    * Since the introduction of CAN XL the CANFD_FDF flag is set in all CAN FD
    * frame structures provided by the CAN subsystem of the Linux kernel.
    */
    #define CANFD_BRS 0x01 /* bit rate switch (second bitrate for payload data) */
    #define CANFD_ESI 0x02 /* error state indicator of the transmitting node */
    #define CANFD_FDF 0x04 /* mark CAN FD for dual use of struct canfd_frame */

    /**
    * struct canfd_frame - CAN flexible data rate frame structure
    * @can_id: CAN ID of the frame and CAN_*_FLAG flags, see canid_t definition
    * @len:    frame payload length in byte (0 .. CANFD_MAX_DLEN)
    * @flags:  additional flags for CAN FD
    * @__res0: reserved / padding
    * @__res1: reserved / padding
    * @data:   CAN FD frame payload (up to CANFD_MAX_DLEN byte)
    */
    struct canfd_frame {
        canid_t can_id;  /* 32 bit CAN_ID + EFF/RTR/ERR flags */
        __u8    len;     /* frame payload length in byte */
        __u8    flags;   /* additional flags for CAN FD */
        __u8    __res0;  /* reserved / padding */
        __u8    __res1;  /* reserved / padding */
        __u8    data[CANFD_MAX_DLEN] __attribute__((aligned(8)));
    };
    """
    can_id = _compose_arbitration_id(msg)

    flags = 0

    # The socketcan code identify the received FD frame by the packet length.
    # So, padding to the data length is performed according to the message type (Classic / FD)
    if msg.is_fd:
        flags |= constants.CANFD_FDF
        max_len = constants.CANFD_MAX_DLEN
    else:
        max_len = constants.CAN_MAX_DLEN

    if msg.bitrate_switch:
        flags |= constants.CANFD_BRS
    if msg.error_state_indicator:
        flags |= constants.CANFD_ESI

    data = bytes(msg.data).ljust(max_len, b"\x00")

    if msg.is_remote_frame:
        data_len = msg.dlc
    else:
        data_len = min(i for i in can.util.CAN_FD_DLC if i >= len(msg.data))
    header = CAN_FRAME_HEADER_STRUCT.pack(can_id, data_len, flags, msg.dlc)
    return header + data


def build_bcm_header(
    opcode: int,
    flags: int,
    count: int,
    ival1_seconds: int,
    ival1_usec: int,
    ival2_seconds: int,
    ival2_usec: int,
    can_id: int,
    nframes: int,
) -> bytes:
    result = BcmMsgHead(
        opcode=opcode,
        flags=flags,
        count=count,
        ival1_tv_sec=ival1_seconds,
        ival1_tv_usec=ival1_usec,
        ival2_tv_sec=ival2_seconds,
        ival2_tv_usec=ival2_usec,
        can_id=can_id,
        nframes=nframes,
    )
    return ctypes.string_at(ctypes.addressof(result), ctypes.sizeof(result))


def build_bcm_tx_delete_header(can_id: int, flags: int) -> bytes:
    opcode = constants.CAN_BCM_TX_DELETE
    return build_bcm_header(opcode, flags, 0, 0, 0, 0, 0, can_id, 1)


def build_bcm_transmit_header(
    can_id: int,
    count: int,
    initial_period: float,
    subsequent_period: float,
    msg_flags: int,
    nframes: int = 1,
) -> bytes:
    opcode = constants.CAN_BCM_TX_SETUP

    flags = msg_flags | constants.SETTIMER | constants.STARTTIMER

    if initial_period > 0:
        # Note `TX_COUNTEVT` creates the message TX_EXPIRED when count expires
        flags |= constants.TX_COUNTEVT

    def split_time(value: float) -> tuple[int, int]:
        """Given seconds as a float, return whole seconds and microseconds"""
        seconds = int(value)
        microseconds = int(1e6 * (value - seconds))
        return seconds, microseconds

    ival1_seconds, ival1_usec = split_time(initial_period)
    ival2_seconds, ival2_usec = split_time(subsequent_period)

    return build_bcm_header(
        opcode,
        flags,
        count,
        ival1_seconds,
        ival1_usec,
        ival2_seconds,
        ival2_usec,
        can_id,
        nframes,
    )


def build_bcm_update_header(can_id: int, msg_flags: int, nframes: int = 1) -> bytes:
    return build_bcm_header(
        constants.CAN_BCM_TX_SETUP, msg_flags, 0, 0, 0, 0, 0, can_id, nframes
    )


def is_frame_fd(frame: bytes):
    # According to the SocketCAN implementation the frame length
    # should indicate if the message is FD or not (not the flag value)
    return len(frame) == constants.CANFD_MTU


def dissect_can_frame(frame: bytes) -> tuple[int, int, int, bytes]:
    can_id, data_len, flags, len8_dlc = CAN_FRAME_HEADER_STRUCT.unpack_from(frame)

    if data_len not in can.util.CAN_FD_DLC:
        data_len = min(i for i in can.util.CAN_FD_DLC if i >= data_len)

    can_dlc = data_len

    if not is_frame_fd(frame):
        # Flags not valid in non-FD frames
        flags = 0

        if (
            data_len == constants.CAN_MAX_DLEN
            and constants.CAN_MAX_DLEN < len8_dlc <= constants.CAN_MAX_RAW_DLC
        ):
            can_dlc = len8_dlc

    return can_id, can_dlc, flags, frame[8 : 8 + data_len]


def create_bcm_socket(channel: str) -> socket.socket:
    """create a broadcast manager socket and connect to the given interface"""
    s = socket.socket(constants.PF_CAN, socket.SOCK_DGRAM, constants.CAN_BCM)
    s.connect((channel,))
    return s


def send_bcm(bcm_socket: socket.socket, data: bytes) -> int:
    """
    Send raw frame to a BCM socket and handle errors.
    """
    try:
        return bcm_socket.send(data)
    except OSError as error:
        base = f"Couldn't send CAN BCM frame due to OS Error: {error.strerror}"

        if error.errno == errno.EINVAL:
            specific_message = " You are probably referring to a non-existing frame."
        elif error.errno == errno.ENETDOWN:
            specific_message = " The CAN interface appears to be down."
        elif error.errno == errno.EBADF:
            specific_message = " The CAN socket appears to be closed."
        else:
            specific_message = ""

        raise can.CanOperationError(base + specific_message, error.errno) from error


def _compose_arbitration_id(message: Message) -> int:
    can_id = message.arbitration_id
    if message.is_extended_id:
        log.debug("sending an extended id type message")
        can_id |= constants.CAN_EFF_FLAG
    if message.is_remote_frame:
        log.debug("requesting a remote frame")
        can_id |= constants.CAN_RTR_FLAG
    if message.is_error_frame:
        log.debug("sending error frame")
        can_id |= constants.CAN_ERR_FLAG
    return can_id


class CyclicSendTask(
    LimitedDurationCyclicSendTaskABC, ModifiableCyclicTaskABC, RestartableCyclicTaskABC
):
    """
    A SocketCAN cyclic send task supports:

        - setting of a task duration
        - modifying the data
        - stopping then subsequent restarting of the task
    """

    def __init__(
        self,
        bcm_socket: socket.socket,
        task_id: int,
        messages: Union[Sequence[Message], Message],
        period: float,
        duration: Optional[float] = None,
        autostart: bool = True,
    ) -> None:
        """Construct and :meth:`~start` a task.

        :param bcm_socket: An open BCM socket on the desired CAN channel.
        :param task_id:
            The identifier used to uniquely reference particular cyclic send task
            within Linux BCM.
        :param messages:
            The messages to be sent periodically.
        :param period:
            The rate in seconds at which to send the messages.
        :param duration:
            Approximate duration in seconds to send the messages for.
        """
        # The following are assigned by LimitedDurationCyclicSendTaskABC:
        #   - self.messages
        #   - self.period
        #   - self.duration
        super().__init__(messages, period, duration)

        self.bcm_socket = bcm_socket
        self.task_id = task_id
        if autostart:
            self._tx_setup(self.messages)

    def _tx_setup(
        self,
        messages: Sequence[Message],
        raise_if_task_exists: bool = True,
    ) -> None:
        # Create a low level packed frame to pass to the kernel
        body = bytearray()
        self.flags = constants.CAN_FD_FRAME if messages[0].is_fd else 0

        if self.duration:
            count = int(self.duration / self.period)
            ival1 = self.period
            ival2 = 0.0
        else:
            count = 0
            ival1 = 0.0
            ival2 = self.period

        if raise_if_task_exists:
            self._check_bcm_task()

        header = build_bcm_transmit_header(
            self.task_id, count, ival1, ival2, self.flags, nframes=len(messages)
        )
        for message in messages:
            body += build_can_frame(message)
        log.debug("Sending BCM command")
        send_bcm(self.bcm_socket, header + body)

    def _check_bcm_task(self) -> None:
        # Do a TX_READ on a task ID, and check if we get EINVAL. If so,
        # then we are referring to a CAN message with an existing ID
        check_header = build_bcm_header(
            opcode=constants.CAN_BCM_TX_READ,
            flags=0,
            count=0,
            ival1_seconds=0,
            ival1_usec=0,
            ival2_seconds=0,
            ival2_usec=0,
            can_id=self.task_id,
            nframes=0,
        )
        log.debug(
            "Reading properties of (cyclic) transmission task id=%d", self.task_id
        )
        try:
            self.bcm_socket.send(check_header)
        except OSError as error:
            if error.errno != errno.EINVAL:
                raise can.CanOperationError("failed to check", error.errno) from error
            else:
                log.debug("Invalid argument - transmission task not known to kernel")
        else:
            # No exception raised - transmission task with this ID exists in kernel.
            # Existence of an existing transmission task might not be a problem!
            raise can.CanOperationError(
                f"A periodic task for task ID {self.task_id} is already in progress "
                "by the SocketCAN Linux layer"
            )

    def stop(self) -> None:
        """Stop a task by sending TX_DELETE message to Linux kernel.

        This will delete the entry for the transmission of the CAN-message
        with the specified ``task_id`` identifier. The message length
        for the command TX_DELETE is {[bcm_msg_head]} (only the header).
        """
        log.debug("Stopping periodic task")

        stopframe = build_bcm_tx_delete_header(self.task_id, self.flags)
        send_bcm(self.bcm_socket, stopframe)

    def modify_data(self, messages: Union[Sequence[Message], Message]) -> None:
        """Update the contents of the periodically sent CAN messages by
        sending TX_SETUP message to Linux kernel.

        The number of new cyclic messages to be sent must be equal to the
        original number of messages originally specified for this task.

        .. note:: The messages must all have the same
                  :attr:`~can.Message.arbitration_id` like the first message.

        :param messages:
            The messages with the new :attr:`can.Message.data`.
        """
        messages = self._check_and_convert_messages(messages)
        self._check_modified_messages(messages)

        self.messages = messages

        body = bytearray()
        header = build_bcm_update_header(
            can_id=self.task_id, msg_flags=self.flags, nframes=len(messages)
        )
        for message in messages:
            body += build_can_frame(message)
        log.debug("Sending BCM command")
        send_bcm(self.bcm_socket, header + body)

    def start(self) -> None:
        """Restart a periodic task by sending TX_SETUP message to Linux kernel.

        It verifies presence of the particular BCM task through sending TX_READ
        message to Linux kernel prior to scheduling.

        :raises ValueError:
            If the task referenced by ``task_id`` is already running.
        """
        self._tx_setup(self.messages, raise_if_task_exists=False)


class MultiRateCyclicSendTask(CyclicSendTask):
    """Exposes more of the full power of the TX_SETUP opcode."""

    def __init__(
        self,
        channel: socket.socket,
        task_id: int,
        messages: Sequence[Message],
        count: int,
        initial_period: float,
        subsequent_period: float,
    ):
        super().__init__(channel, task_id, messages, subsequent_period)

        # Create a low level packed frame to pass to the kernel
        header = build_bcm_transmit_header(
            self.task_id,
            count,
            initial_period,
            subsequent_period,
            self.flags,
            nframes=len(messages),
        )

        body = bytearray()
        for message in messages:
            body += build_can_frame(message)

        log.info("Sending BCM TX_SETUP command")
        send_bcm(self.bcm_socket, header + body)


def create_socket() -> socket.socket:
    """Creates a raw CAN socket. The socket will
    be returned unbound to any interface.
    """
    sock = socket.socket(constants.PF_CAN, socket.SOCK_RAW, constants.CAN_RAW)

    log.info("Created a socket")

    return sock


def bind_socket(sock: socket.socket, channel: str = "can0") -> None:
    """
    Binds the given socket to the given interface.

    :param sock:
        The socket to be bound
    :param channel:
        The channel / interface to bind to
    :raises OSError:
        If the specified interface isn't found.
    """
    log.debug("Binding socket to channel=%s", channel)
    sock.bind((channel,))
    log.debug("Bound socket.")


def capture_message(
    sock: socket.socket, get_channel: bool = False
) -> Optional[Message]:
    """
    Captures a message from given socket.

    :param sock:
        The socket to read a message from.
    :param get_channel:
        Find out which channel the message comes from.

    :return: The received message, or None on failure.
    """
    # Fetching the Arb ID, DLC and Data
    try:
        cf, ancillary_data, msg_flags, addr = sock.recvmsg(
            constants.CANFD_MTU, RECEIVED_ANCILLARY_BUFFER_SIZE
        )
        if get_channel:
            channel = addr[0] if isinstance(addr, tuple) else addr
        else:
            channel = None
    except OSError as error:
        raise can.CanOperationError(
            f"Error receiving: {error.strerror}", error.errno
        ) from error

    can_id, can_dlc, flags, data = dissect_can_frame(cf)

    # Fetching the timestamp
    assert len(ancillary_data) == 1, "only requested a single extra field"
    cmsg_level, cmsg_type, cmsg_data = ancillary_data[0]
    assert (
        cmsg_level == socket.SOL_SOCKET and cmsg_type == constants.SO_TIMESTAMPNS
    ), "received control message type that was not requested"
    # see https://man7.org/linux/man-pages/man3/timespec.3.html -> struct timespec for details
    seconds, nanoseconds = RECEIVED_TIMESTAMP_STRUCT.unpack_from(cmsg_data)
    if nanoseconds >= 1e9:
        raise can.CanOperationError(
            f"Timestamp nanoseconds field was out of range: {nanoseconds} not less than 1e9"
        )
    timestamp = seconds + nanoseconds * 1e-9

    # EXT, RTR, ERR flags -> boolean attributes
    #   /* special address description flags for the CAN_ID */
    #   #define CAN_EFF_FLAG 0x80000000U /* EFF/SFF is set in the MSB */
    #   #define CAN_RTR_FLAG 0x40000000U /* remote transmission request */
    #   #define CAN_ERR_FLAG 0x20000000U /* error frame */
    is_extended_frame_format = bool(can_id & constants.CAN_EFF_FLAG)
    is_remote_transmission_request = bool(can_id & constants.CAN_RTR_FLAG)
    is_error_frame = bool(can_id & constants.CAN_ERR_FLAG)
    is_fd = len(cf) == constants.CANFD_MTU
    bitrate_switch = bool(flags & constants.CANFD_BRS)
    error_state_indicator = bool(flags & constants.CANFD_ESI)

    # Section 4.7.1: MSG_DONTROUTE: set when the received frame was created on the local host.
    is_rx = not bool(msg_flags & socket.MSG_DONTROUTE)

    if is_extended_frame_format:
        # log.debug("CAN: Extended")
        # TODO does this depend on SFF or EFF?
        arbitration_id = can_id & 0x1FFFFFFF
    else:
        # log.debug("CAN: Standard")
        arbitration_id = can_id & 0x000007FF

    msg = Message(
        timestamp=timestamp,
        channel=channel,
        arbitration_id=arbitration_id,
        is_extended_id=is_extended_frame_format,
        is_remote_frame=is_remote_transmission_request,
        is_error_frame=is_error_frame,
        is_fd=is_fd,
        is_rx=is_rx,
        bitrate_switch=bitrate_switch,
        error_state_indicator=error_state_indicator,
        dlc=can_dlc,
        data=data,
    )

    return msg


class SocketcanBus(BusABC):  # pylint: disable=abstract-method
    """A SocketCAN interface to CAN.

    It implements :meth:`can.BusABC._detect_available_configs` to search for
    available interfaces.
    """

    def __init__(
        self,
        channel: str = "",
        receive_own_messages: bool = False,
        local_loopback: bool = True,
        fd: bool = False,
        can_filters: Optional[CanFilters] = None,
        ignore_rx_error_frames=False,
        **kwargs,
    ) -> None:
        """Creates a new socketcan bus.

        If setting some socket options fails, an error will be printed
        but no exception will be thrown. This includes enabling:

            - that own messages should be received,
            - CAN-FD frames and
            - error frames.

        :param channel:
            The can interface name with which to create this bus.
            An example channel would be 'vcan0' or 'can0'.
            An empty string '' will receive messages from all channels.
            In that case any sent messages must be explicitly addressed to a
            channel using :attr:`can.Message.channel`.
        :param receive_own_messages:
            If transmitted messages should also be received by this bus.
        :param local_loopback:
            If local loopback should be enabled on this bus.
            Please note that local loopback does not mean that messages sent
            on a socket will be readable on the same socket, they will only
            be readable on other open sockets on the same machine. More info
            can be read on the socketcan documentation:
            See https://www.kernel.org/doc/html/latest/networking/can.html#socketcan-local-loopback1
        :param fd:
            If CAN-FD frames should be supported.
        :param can_filters:
            See :meth:`can.BusABC.set_filters`.
        :param ignore_rx_error_frames:
            If incoming error frames should be discarded.
        """
        self.socket = create_socket()
        self.channel = channel
        self.channel_info = f"socketcan channel '{channel}'"
        self._bcm_sockets: dict[str, socket.socket] = {}
        self._is_filtered = False
        self._task_id = 0
        self._task_id_guard = threading.Lock()
        self._can_protocol = CanProtocol.CAN_FD if fd else CanProtocol.CAN_20

        # set the local_loopback parameter
        try:
            self.socket.setsockopt(
                constants.SOL_CAN_RAW,
                constants.CAN_RAW_LOOPBACK,
                1 if local_loopback else 0,
            )
        except OSError as error:
            log.error("Could not set local loopback flag(%s)", error)

        # set the receive_own_messages parameter
        try:
            self.socket.setsockopt(
                constants.SOL_CAN_RAW,
                constants.CAN_RAW_RECV_OWN_MSGS,
                1 if receive_own_messages else 0,
            )
        except OSError as error:
            log.error("Could not receive own messages (%s)", error)

        # enable CAN-FD frames if desired
        if fd:
            try:
                self.socket.setsockopt(
                    constants.SOL_CAN_RAW, constants.CAN_RAW_FD_FRAMES, 1
                )
            except OSError as error:
                log.error("Could not enable CAN-FD frames (%s)", error)

        if not ignore_rx_error_frames:
            # enable error frames
            try:
                self.socket.setsockopt(
                    constants.SOL_CAN_RAW, constants.CAN_RAW_ERR_FILTER, 0x1FFFFFFF
                )
            except OSError as error:
                log.error("Could not enable error frames (%s)", error)

        # enable nanosecond resolution timestamping
        # we can always do this since
        #  1) it is guaranteed to be at least as precise as without
        #  2) it is available since Linux 2.6.22, and CAN support was only added afterward
        #     so this is always supported by the kernel
        self.socket.setsockopt(socket.SOL_SOCKET, constants.SO_TIMESTAMPNS, 1)

        try:
            bind_socket(self.socket, channel)
            kwargs.update(
                {
                    "receive_own_messages": receive_own_messages,
                    "fd": fd,
                    "local_loopback": local_loopback,
                }
            )
        except OSError as error:
            log.error("Could not access SocketCAN device %s (%s)", channel, error)
            raise
        super().__init__(
            channel=channel,
            can_filters=can_filters,
            **kwargs,
        )

    def shutdown(self) -> None:
        """Stops all active periodic tasks and closes the socket."""
        super().shutdown()
        for channel, bcm_socket in self._bcm_sockets.items():
            log.debug("Closing bcm socket for channel %s", channel)
            bcm_socket.close()
        log.debug("Closing raw can socket")
        self.socket.close()

    def _recv_internal(
        self, timeout: Optional[float]
    ) -> tuple[Optional[Message], bool]:
        try:
            # get all sockets that are ready (can be a list with a single value
            # being self.socket or an empty list if self.socket is not ready)
            ready_receive_sockets, _, _ = select.select([self.socket], [], [], timeout)
        except OSError as error:
            # something bad happened (e.g. the interface went down)
            raise can.CanOperationError(
                f"Failed to receive: {error.strerror}", error.errno
            ) from error

        if ready_receive_sockets:  # not empty
            get_channel = self.channel == ""
            msg = capture_message(self.socket, get_channel)
            if msg and not msg.channel and self.channel:
                # Default to our own channel
                msg.channel = self.channel
            return msg, self._is_filtered

        # socket wasn't readable or timeout occurred
        return None, self._is_filtered

    def send(self, msg: Message, timeout: Optional[float] = None) -> None:
        """Transmit a message to the CAN bus.

        :param msg: A message object.
        :param timeout:
            Wait up to this many seconds for the transmit queue to be ready.
            If not given, the call may fail immediately.

        :raises ~can.exceptions.CanError:
            if the message could not be written.
        """
        log.debug("We've been asked to write a message to the bus")
        logger_tx = log.getChild("tx")
        logger_tx.debug("sending: %s", msg)

        started = time.time()
        # If no timeout is given, poll for availability
        if timeout is None:
            timeout = 0
        time_left = timeout
        data = build_can_frame(msg)

        while time_left >= 0:
            # Wait for write availability
            ready = select.select([], [self.socket], [], time_left)[1]
            if not ready:
                # Timeout
                break
            channel = str(msg.channel) if msg.channel else None
            sent = self._send_once(data, channel)
            if sent == len(data):
                return
            # Not all data were sent, try again with remaining data
            data = data[sent:]
            time_left = timeout - (time.time() - started)

        raise can.CanOperationError("Transmit buffer full")

    def _send_once(self, data: bytes, channel: Optional[str] = None) -> int:
        try:
            if self.channel == "" and channel:
                # Message must be addressed to a specific channel
                sent = self.socket.sendto(data, (channel,))
            else:
                sent = self.socket.send(data)
        except OSError as error:
            raise can.CanOperationError(
                f"Failed to transmit: {error.strerror}", error.errno
            ) from error
        return sent

    def _send_periodic_internal(
        self,
        msgs: Union[Sequence[Message], Message],
        period: float,
        duration: Optional[float] = None,
        autostart: bool = True,
        modifier_callback: Optional[Callable[[Message], None]] = None,
    ) -> can.broadcastmanager.CyclicSendTaskABC:
        """Start sending messages at a given period on this bus.

        The Linux kernel's Broadcast Manager SocketCAN API is used to schedule
        periodic sending of CAN messages. The wrapping 32-bit counter (see
        :meth:`~_get_next_task_id()`) designated to distinguish different
        :class:`CyclicSendTask` within BCM provides flexibility to schedule
        CAN messages sending with the same CAN ID, but different CAN data.

        :param msgs:
            The message(s) to be sent periodically.
        :param period:
            The rate in seconds at which to send the messages.
        :param duration:
            Approximate duration in seconds to continue sending messages. If
            no duration is provided, the task will continue indefinitely.
        :param autostart:
            If True (the default) the sending task will immediately start after creation.
            Otherwise, the task has to be started by calling the
            tasks :meth:`~can.RestartableCyclicTaskABC.start` method on it.

        :raises ValueError:
            If task identifier passed to :class:`CyclicSendTask` can't be used
            to schedule new task in Linux BCM.

        :return:
            A :class:`CyclicSendTask` task instance. This can be used to modify the data,
            pause/resume the transmission and to stop the transmission.

        .. note::

            Note the duration before the messages stop being sent may not
            be exactly the same as the duration specified by the user. In
            general the message will be sent at the given rate until at
            least *duration* seconds.
        """
        if modifier_callback is None:
            msgs = LimitedDurationCyclicSendTaskABC._check_and_convert_messages(  # pylint: disable=protected-access
                msgs
            )

            msgs_channel = str(msgs[0].channel) if msgs[0].channel else None
            bcm_socket = self._get_bcm_socket(msgs_channel or self.channel)
            task_id = self._get_next_task_id()
            task = CyclicSendTask(
                bcm_socket, task_id, msgs, period, duration, autostart=autostart
            )
            return task

        # fallback to thread based cyclic task
        warnings.warn(
            f"{self.__class__.__name__} falls back to a thread-based cyclic task, "
            "when the `modifier_callback` argument is given.",
            stacklevel=3,
        )
        return BusABC._send_periodic_internal(
            self,
            msgs=msgs,
            period=period,
            duration=duration,
            autostart=autostart,
            modifier_callback=modifier_callback,
        )

    def _get_next_task_id(self) -> int:
        with self._task_id_guard:
            self._task_id = (self._task_id + 1) % (2**32 - 1)
            return self._task_id

    def _get_bcm_socket(self, channel: str) -> socket.socket:
        if channel not in self._bcm_sockets:
            self._bcm_sockets[channel] = create_bcm_socket(self.channel)
        return self._bcm_sockets[channel]

    def _apply_filters(self, filters: Optional[can.typechecking.CanFilters]) -> None:
        try:
            self.socket.setsockopt(
                constants.SOL_CAN_RAW, constants.CAN_RAW_FILTER, pack_filters(filters)
            )
        except OSError as error:
            # fall back to "software filtering" (= not in kernel)
            self._is_filtered = False
            log.error(
                "Setting filters failed; falling back to software filtering (not in kernel): %s",
                error,
            )
        else:
            self._is_filtered = True

    def fileno(self) -> int:
        return self.socket.fileno()

    @staticmethod
    def _detect_available_configs() -> list[can.typechecking.AutoDetectedConfig]:
        return [
            {"interface": "socketcan", "channel": channel}
            for channel in find_available_interfaces()
        ]
