"""
Python implementation of HiSLIP protocol.  Based on the HiSLIP spec:

http://www.ivifoundation.org/downloads/Class%20Specifications/IVI-6.1_HiSLIP-1.1-2024-02-24.pdf
"""

import socket
import struct
import time
from typing import Dict, Optional, Tuple

PORT = 4880

MESSAGETYPE_STR: Dict[int, str] = {
    0: "Initialize",
    1: "InitializeResponse",
    2: "FatalError",
    3: "Error",
    4: "AsyncLock",
    5: "AsyncLockResponse",
    6: "Data",
    7: "DataEnd",
    8: "DeviceClearComplete",
    9: "DeviceClearAcknowledge",
    10: "AsyncRemoteLocalControl",
    11: "AsyncRemoteLocalResponse",
    12: "Trigger",
    13: "Interrupted",
    14: "AsyncInterrupted",
    15: "AsyncMaxMsgSize",
    16: "AsyncMaxMsgSizeResponse",
    17: "AsyncInitialize",
    18: "AsyncInitializeResponse",
    19: "AsyncDeviceClear",
    20: "AsyncServiceRequest",
    21: "AsyncStatusQuery",
    22: "AsyncStatusResponse",
    23: "AsyncDeviceClearAcknowledge",
    24: "AsyncLockInfo",
    25: "AsyncLockInfoResponse",
    26: "GetDescriptors",
    27: "GetDescriptorsResponse",
    28: "StartTLS",
    29: "AsyncStartTLS",
    30: "AsyncStartTLSResponse",
    31: "EndTLS",
    32: "AsyncEndTLS",
    33: "AsyncEndTLSResponse",
    34: "GetSaslMechanismList",
    35: "GetSaslMechanismListResponse",
    36: "AuthenticationStart",
    37: "AuthenticationExchange",
    38: "AuthenticationResult",
    # reserved for future use         39-127 inclusive
    # VendorSpecific                  128-255 inclusive
}
MESSAGETYPE: Dict[str, int] = {value: key for (key, value) in MESSAGETYPE_STR.items()}

FATALERRORMESSAGE: Dict[int, str] = {
    0: "Unidentified error",
    1: "Poorly formed message header",
    2: "Attempt to use connection without both channels established",
    3: "Invalid Initialization sequence",
    4: "Server refused connection due to maximum number of clients exceeded",
    5: "Secure connection failed",
    # 6-127:   reserved for HiSLIP extensions
    # 128-255: device defined errors
}
FATALERRORCODE: Dict[str, int] = {
    value: key for (key, value) in FATALERRORMESSAGE.items()
}

ERRORMESSAGE: Dict[int, str] = {
    0: "Unidentified error",
    1: "Unrecognized Message Type",
    2: "Unrecognized control code",
    3: "Unrecognized Vendor Defined Message",
    4: "Message too large",
    5: "Authentication failed",
    # 6-127:   Reserved
    # 128-255: Device defined errors
}
ERRORCODE: Dict[str, int] = {value: key for (key, value) in ERRORMESSAGE.items()}

LOCKCONTROLCODE: Dict[str, int] = {
    "release": 0,
    "request": 1,
}

LOCKRESPONSE: Dict[int, str] = {
    0: "failure",
    1: "success",  # or "success exclusive"
    2: "success shared",
    3: "error",
}

REMOTELOCALCONTROLCODE: Dict[str, int] = {
    "disableRemote": 0,
    "enableRemote": 1,
    "disableAndGTL": 2,
    "enableAndGotoRemote": 3,
    "enableAndLockoutLocal": 4,
    "enableAndGTRLLO": 5,
    "justGTL": 6,
}

HEADER_FORMAT = "!2sBBIQ"
# !  = network order,
# 2s = prologue ('HS'),
# B  = message type (unsigned byte),
# B  = control code (unsigned byte),
# I  = message parameter (unsigned int),
# Q  = payload length (unsigned long long)
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

DEFAULT_MAX_MSG_SIZE = 1 << 20  # from VISA spec


#########################################################################################


def receive_flush(sock: socket.socket, recv_len: int) -> None:
    """
    receive exactly 'recv_len' bytes from 'sock'.
    no explicit timeout is specified, since it is assumed
    that a call to select indicated that data is available.
    received data is thrown away and nothing is returned
    """
    # limit the size of the recv_buffer to something moderate
    # in order to limit the impact on virtual memory
    recv_buffer = bytearray(min(1 << 20, recv_len))
    bytes_recvd = 0

    while bytes_recvd < recv_len:
        request_size = min(len(recv_buffer), recv_len - bytes_recvd)
        data_len = sock.recv_into(recv_buffer, request_size)
        bytes_recvd += data_len


def receive_exact(sock: socket.socket, recv_len: int) -> bytes:
    """
    receive exactly 'recv_len' bytes from 'sock'.
    no explicit timeout is specified, since it is assumed
    that a call to select indicated that data is available.
    returns a bytearray containing the received data.
    """
    recv_buffer = bytearray(recv_len)
    receive_exact_into(sock, recv_buffer)
    return recv_buffer


def receive_exact_into(sock: socket.socket, recv_buffer: bytes) -> None:
    """
    receive data from 'sock' to exactly fill 'recv_buffer'.
    no explicit timeout is specified, since it is assumed
    that a call to select indicated that data is available.
    """
    view = memoryview(recv_buffer)
    recv_len = len(recv_buffer)
    bytes_recvd = 0

    while bytes_recvd < recv_len:
        request_size = recv_len - bytes_recvd
        data_len = sock.recv_into(view, request_size)
        if data_len == 0:
            raise RuntimeError("Connection was dropped by server.")
        bytes_recvd += data_len
        view = view[data_len:]

    if bytes_recvd > recv_len:
        raise MemoryError("socket.recv_into scribbled past end of recv_buffer")


def send_msg(
    sock: socket.socket,
    msg_type: str,
    control_code: int,
    message_parameter: Optional[int],
    payload: bytes = b"",
) -> None:
    """Send a message on sock w/ payload."""
    msg = bytearray(
        struct.pack(
            HEADER_FORMAT,
            b"HS",
            MESSAGETYPE[msg_type],
            control_code,
            message_parameter or 0,
            len(payload),
        )
    )
    # txdecode(msg, payload)  # uncomment for debugging
    msg.extend(payload)
    sock.sendall(msg)


class RxHeader:
    """Generic base class for receiving messages.

    specific protocol responses subclass this class.
    """

    def __init__(
        self,
        sock: socket.socket,
        expected_message_type: Optional[str] = None,
    ) -> None:
        """receive and decode the HiSLIP message header"""
        self.header = receive_exact(sock, HEADER_SIZE)
        # rxdecode(self.header)  # uncomment for debugging
        (
            prologue,
            msg_type,
            self.control_code,
            self.message_parameter,
            self.payload_length,
        ) = struct.unpack(HEADER_FORMAT, self.header)

        if prologue != b"HS":
            # XXX we should send a 'Fatal Error' to the server, close the
            # sockets, then raise an exception
            raise RuntimeError("protocol synchronization error")

        if msg_type not in MESSAGETYPE_STR:
            # XXX we should send 'Unrecognized message type' to the
            #     server and discard this packet plus any payload.
            raise RuntimeError("unrecognized message type: %d" % msg_type)

        self.msg_type = MESSAGETYPE_STR[msg_type]

        if expected_message_type is not None and self.msg_type != expected_message_type:
            # XXX we should send an 'Error: Unidentified Error' to the server
            # and discard this packet plus any payload
            payload = (
                (": " + str(receive_exact(sock, self.payload_length)))
                if self.payload_length > 0
                else ""
            )
            raise RuntimeError(
                "expected message type '%s', received '%s%s'"
                % (expected_message_type, self.msg_type, payload)
            )

        if self.msg_type == "DataEnd" or self.msg_type == "Data":
            assert self.control_code == 0
            self.message_id = self.message_parameter


class InitializeResponse(RxHeader):
    def __init__(self, sock: socket.socket) -> None:
        super().__init__(sock, "InitializeResponse")
        assert self.payload_length == 0
        self.overlap = bool(self.control_code)
        self.version, self.session_id = struct.unpack("!4xHH8x", self.header)


class AsyncInitializeResponse(RxHeader):
    def __init__(self, sock: socket.socket) -> None:
        super().__init__(sock, "AsyncInitializeResponse")
        assert self.control_code == 0
        assert self.payload_length == 0
        self.vendor_id = struct.unpack("!4x4s8x", self.header)


class AsyncMaxMsgSizeResponse(RxHeader):
    def __init__(self, sock: socket.socket) -> None:
        super().__init__(sock, "AsyncMaxMsgSizeResponse")
        assert self.control_code == 0
        assert self.message_parameter == 0
        assert self.payload_length == 8
        payload = receive_exact(sock, self.payload_length)
        self.max_msg_size = struct.unpack("!Q", payload)[0]


class AsyncDeviceClearAcknowledge(RxHeader):
    def __init__(self, sock: socket.socket) -> None:
        super().__init__(sock, "AsyncDeviceClearAcknowledge")
        self.feature_bitmap = self.control_code
        assert self.message_parameter == 0
        assert self.payload_length == 0


class AsyncInterrupted(RxHeader):
    def __init__(self, sock: socket.socket) -> None:
        super().__init__(sock, "AsyncInterrupted")
        assert self.control_code == 0
        self.message_id = self.message_parameter
        assert self.payload_length == 0


class AsyncLockInfoResponse(RxHeader):
    def __init__(self, sock: socket.socket) -> None:
        super().__init__(sock, "AsyncLockInfoResponse")
        self.exclusive_lock = self.control_code  # 0: no lock, 1: lock granted
        self.clients_holding_locks = self.message_parameter
        assert self.payload_length == 0


class AsyncLockResponse(RxHeader):
    def __init__(self, sock: socket.socket) -> None:
        super().__init__(sock, "AsyncLockResponse")
        self.lock_response = LOCKRESPONSE[self.control_code]
        assert self.message_parameter == 0
        assert self.payload_length == 0


class AsyncRemoteLocalResponse(RxHeader):
    def __init__(self, sock: socket.socket) -> None:
        super().__init__(sock, "AsyncRemoteLocalResponse")
        assert self.control_code == 0
        assert self.message_parameter == 0
        assert self.payload_length == 0


class AsyncServiceRequest(RxHeader):
    def __init__(self, sock: socket.socket) -> None:
        super().__init__(sock, "AsyncServiceRequest")
        self.server_status = self.control_code
        assert self.message_parameter == 0
        assert self.payload_length == 0


class AsyncStatusResponse(RxHeader):
    def __init__(self, sock: socket.socket) -> None:
        super().__init__(sock, "AsyncStatusResponse")
        self.server_status = self.control_code
        assert self.message_parameter == 0
        assert self.payload_length == 0


class DeviceClearAcknowledge(RxHeader):
    def __init__(self, sock: socket.socket) -> None:
        super().__init__(sock, "DeviceClearAcknowledge")
        self.feature_bitmap = self.control_code
        assert self.message_parameter == 0
        assert self.payload_length == 0


class Interrupted(RxHeader):
    def __init__(self, sock: socket.socket) -> None:
        super().__init__(sock, "Interrupted")
        assert self.control_code == 0
        self.message_id = self.message_parameter
        assert self.payload_length == 0


class Error(RxHeader):
    def __init__(self, sock: socket.socket) -> None:
        super().__init__(sock, "Error")
        self.error_code = ERRORMESSAGE[self.control_code]
        assert self.message_parameter == 0
        self.error_message = receive_exact(sock, self.payload_length)


class FatalError(RxHeader):
    def __init__(self, sock: socket.socket) -> None:
        super().__init__(sock, "FatalError")
        self.error_code = FATALERRORMESSAGE[self.control_code]
        assert self.message_parameter == 0
        self.error_message = receive_exact(sock, self.payload_length)


class Instrument:
    """
    this is the principal export from this module.  it opens up a HiSLIP
    connection to the instrument at the specified IP address.
    """

    def __init__(
        self,
        ip_addr: str,
        open_timeout: Optional[float] = 0.0,
        timeout: Optional[float] = None,
        port: int = PORT,
        sub_address: str = "hislip0",
    ) -> None:
        # init transaction:
        #     C->S: Initialize
        #     S->C: InitializeResponse
        #     C->S: AsyncInitialize
        #     S->C: AsyncInitializeResponse

        timeout = timeout or 5.0

        # open the synchronous socket and send an initialize packet
        self._sync = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # The VISA spec does not allow to tune the socket timeout when opening
        # a connection. ``open_timeout`` only applies to attempt to acquire a
        # lock.
        self._sync.settimeout(5.0)
        self._sync.connect((ip_addr, port))
        self._sync.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        init = self.initialize(sub_address=sub_address.encode("ascii"))
        if init.overlap != 0:
            print("**** prefer overlap = %d" % init.overlap)
        # We set the user timeout once we managed to initialize the connection.
        self._sync.settimeout(timeout)

        # open the asynchronous socket and send an initialize packet
        self._async = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._async.connect((ip_addr, port))
        self._async.settimeout(5.0)
        self._async.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._async_init = self.async_initialize(session_id=init.session_id)
        # We set the user timeout once we managed to initialize the connection.
        self._async.settimeout(timeout)

        # initialize variables
        self.max_msg_size = DEFAULT_MAX_MSG_SIZE
        self.keepalive = False
        self.timeout = timeout
        self._rmt = 0
        self._message_id = 0xFFFF_FF00
        self._last_message_id: Optional[int] = None
        self._msg_type: str = ""
        self._payload_remaining: int = 0

    # ================ #
    # MEMBER FUNCTIONS #
    # ================ #

    def close(self) -> None:
        self._sync.close()
        self._async.close()

    @property
    def timeout(self) -> float:
        """Timeout value in seconds for both the sync and async sockets"""
        return self._timeout

    @timeout.setter
    def timeout(self, val: float) -> None:
        """Timeout value in seconds for both the sync and async sockets"""
        self._timeout = val
        self._sync.settimeout(self._timeout)
        self._async.settimeout(self._timeout)

    @property
    def max_msg_size(self) -> int:
        """Maximum HiSLIP message size in bytes."""
        return self._max_msg_size

    @max_msg_size.setter
    def max_msg_size(self, size: int) -> None:
        self._max_msg_size = self.async_maximum_message_size(size)

    @property
    def last_message_id(self) -> Optional[int]:
        return self._last_message_id

    @last_message_id.setter
    def last_message_id(self, message_id: Optional[int]) -> None:
        """Re-set last message id and related attributes"""
        self._last_message_id = message_id
        self._rmt = 0
        self._payload_remaining = 0
        self._msg_type = ""

    @property
    def keepalive(self) -> bool:
        """Status of the TCP keepalive.

        Keepalive is on/off for both the sync and async sockets

        If a connection is dropped as a result of “keepalives”, the error code
        VI_ERROR_CONN_LOST is returned to current and subsequent I/O
        calls on the session.

        """
        return self._keepalive

    @keepalive.setter
    def keepalive(self, keepalive: bool) -> None:
        self._keepalive = bool(keepalive)
        self._sync.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, bool(keepalive))
        self._async.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, bool(keepalive))

    def send(self, data: bytes) -> int:
        """Send the data on the synchronous channel.

        More than one packet may be necessary in order
        to not exceed max_payload_size.
        """
        # print(f"send({data=})")  # uncomment for debugging
        data_view = memoryview(data)
        num_bytes_to_send = len(data)
        max_payload_size = self._max_msg_size - HEADER_SIZE

        # send the data in chunks of max_payload_size bytes at a time
        while num_bytes_to_send > 0:
            if num_bytes_to_send <= max_payload_size:
                assert len(data_view) == num_bytes_to_send
                self._send_data_end_packet(data_view)
                bytes_sent = num_bytes_to_send
            else:
                self._send_data_packet(data_view[:max_payload_size])
                bytes_sent = max_payload_size

            data_view = data_view[bytes_sent:]
            num_bytes_to_send -= bytes_sent

        return len(data)

    def receive(self, max_len: int = 4096) -> bytes:
        """Receive data on the synchronous channel.

        Terminate after max_len bytes or after receiving a DataEnd message
        """

        # print(f"receive({max_len=})")  # uncomment for debugging

        # receive data, terminating after len(recv_buffer) bytes or
        # after receiving a DataEnd message.
        #
        # note the use of receive_exact_into (which calls socket.recv_into),
        # avoiding unnecessary copies.
        #
        recv_buffer = bytearray(max_len)
        view = memoryview(recv_buffer)
        bytes_recvd = 0

        while bytes_recvd < max_len:
            if self._payload_remaining <= 0:
                if self._msg_type == "DataEnd":
                    # truncate to the actual number of bytes received
                    recv_buffer = recv_buffer[:bytes_recvd]
                    break
                self._msg_type, self._payload_remaining = self._next_data_header()

            request_size = min(self._payload_remaining, max_len - bytes_recvd)
            receive_exact_into(self._sync, view[:request_size])
            self._payload_remaining -= request_size
            bytes_recvd += request_size
            view = view[request_size:]

        if bytes_recvd > max_len:
            raise MemoryError("scribbled past end of recv_buffer")

        # if there is no data remaining, set the RMT flag
        if self._payload_remaining == 0 and self._msg_type == "DataEnd":
            #
            # From IEEE Std 488.2: Response Message Terminator.
            #
            # RMT is the new-line accompanied by END sent from the server
            # to the client at the end of a response. Note that with HiSLIP
            # this is implied by the DataEND message.
            #
            self._rmt = 1

        return recv_buffer

    def _next_data_header(self) -> Tuple[str, int]:
        """
        receive the next data header (either Data or DataEnd), check the
        message_id, and return the msg_type and payload_length.
        """
        while True:
            header = RxHeader(self._sync)

            if header.msg_type in ("Data", "DataEnd"):
                # When receiving Data messages if the MessageID is not 0xffff ffff,
                # then verify that the MessageID indicated in the Data message is
                # the MessageID that the client sent to the server with the most
                # recent Data, DataEND or Trigger message.
                #
                # If the MessageIDs do not match, the client shall clear any Data
                # responses already buffered and discard the offending Data message

                if (
                    header.message_parameter == 0xFFFF_FFFF
                    or header.message_parameter == self.last_message_id
                ):
                    break

            # we're out of sync.  flush this message and continue.
            receive_flush(self._sync, header.payload_length)

        return header.msg_type, header.payload_length

    def device_clear(self) -> None:
        feature = self.async_device_clear()
        # Abandon pending messages and wait for in-process synchronous messages
        # to complete.
        time.sleep(0.1)
        # Indicate to server that synchronous channel is cleared out.
        self.device_clear_complete(feature)
        # reset messageID and resume normal opreation
        self._message_id = 0xFFFF_FF00

    def initialize(
        self,
        version: tuple = (1, 0),
        vendor_id: bytes = b"xx",
        sub_address: bytes = b"hislip0",
    ) -> InitializeResponse:
        """
        perform an Initialize transaction.
        returns the InitializeResponse header.
        """
        major, minor = version
        header = struct.pack(
            "!2sBBBB2sQ",
            b"HS",
            MESSAGETYPE["Initialize"],
            0,
            major,
            minor,
            vendor_id,
            len(sub_address),
        )
        # txdecode(header, sub_address)  # uncomment for debugging
        self._sync.sendall(header + sub_address)
        return InitializeResponse(self._sync)

    def async_initialize(self, session_id: int) -> AsyncInitializeResponse:
        """
        perform an AsyncInitialize transaction.
        returns the AsyncInitializeResponse header.
        """
        send_msg(self._async, "AsyncInitialize", 0, session_id)
        return AsyncInitializeResponse(self._async)

    def async_maximum_message_size(self, size: int) -> int:
        """
        perform an AsyncMaxMsgSize transaction.
        returns the max_msg_size from the AsyncMaxMsgSizeResponse packet.
        """
        # maximum_message_size transaction:
        #     C->S: AsyncMaxMsgSize
        #     S->C: AsyncMaxMsgSizeResponse
        payload = struct.pack("!Q", size)
        send_msg(self._async, "AsyncMaxMsgSize", 0, 0, payload)
        response = AsyncMaxMsgSizeResponse(self._async)
        return response.max_msg_size

    def async_lock_info(self) -> int:
        """
        perform an AsyncLockInfo transaction.
        returns the exclusive_lock from the AsyncLockInfoResponse packet.
        """
        # async_lock_info transaction:
        #     C->S: AsyncLockInfo
        #     S->C: AsyncLockInfoResponse
        send_msg(self._async, "AsyncLockInfo", 0, 0)
        response = AsyncLockInfoResponse(self._async)
        return response.exclusive_lock

    def async_lock_request(self, timeout: float, lock_string: str = "") -> str:
        """
        perform an AsyncLock request transaction.
        returns the lock_response from the AsyncLockResponse packet.
        """
        # async_lock transaction:
        #     C->S: AsyncLock
        #     S->C: AsyncLockResponse
        ctrl_code = LOCKCONTROLCODE["request"]
        timeout_ms = int(1e3 * timeout)
        send_msg(self._async, "AsyncLock", ctrl_code, timeout_ms, lock_string.encode())
        response = AsyncLockResponse(self._async)
        return response.lock_response

    def async_lock_release(self) -> str:
        """
        perform an AsyncLock release transaction.
        returns the lock_response from the AsyncLockResponse packet.
        """
        # async_lock transaction:
        #     C->S: AsyncLock
        #     S->C: AsyncLockResponse
        ctrl_code = LOCKCONTROLCODE["release"]
        send_msg(self._async, "AsyncLock", ctrl_code, self.last_message_id)
        response = AsyncLockResponse(self._async)
        return response.lock_response

    def async_remote_local_control(self, remotelocalcontrol: str) -> None:
        """
        perform an AsyncRemoteLocalControl transaction.
        """
        # remote_local transaction:
        #     C->S: AsyncRemoteLocalControl
        #     S->C: AsyncRemoteLocalResponse
        ctrl_code = REMOTELOCALCONTROLCODE[remotelocalcontrol]
        send_msg(
            self._async, "AsyncRemoteLocalControl", ctrl_code, self.last_message_id
        )
        AsyncRemoteLocalResponse(self._async)

    def async_status_query(self) -> int:
        """
        perform an AsyncStatusQuery transaction.
        returns the server_status from the AsyncStatusResponse packet.
        """
        # async_status_query transaction:
        #     C->S: AsyncStatusQuery
        #     S->C: AsyncStatusResponse
        send_msg(self._async, "AsyncStatusQuery", self._rmt, self._message_id)
        self._rmt = 0
        response = AsyncStatusResponse(self._async)
        return response.server_status

    def async_device_clear(self) -> int:
        """
        perform an AsyncDeviceClear transaction.
        returns the feature_bitmap from the AsyncDeviceClearAcknowledge packet.
        """
        send_msg(self._async, "AsyncDeviceClear", 0, 0)
        response = AsyncDeviceClearAcknowledge(self._async)
        return response.feature_bitmap

    def device_clear_complete(self, feature_bitmap: int) -> int:
        """
        perform a DeviceClear transaction.
        returns the feature_bitmap from the DeviceClearAcknowledge packet.
        """
        send_msg(self._sync, "DeviceClearComplete", feature_bitmap, 0)
        response = DeviceClearAcknowledge(self._sync)
        return response.feature_bitmap

    def trigger(self) -> None:
        """send a Trigger packet on the sync channel"""
        send_msg(self._sync, "Trigger", self._rmt, self._message_id)
        self.last_message_id = self._message_id
        self._message_id = (self._message_id + 2) & 0xFFFF_FFFF

    def _send_data_packet(self, payload: bytes) -> None:
        """send a Data packet on the sync channel"""
        send_msg(self._sync, "Data", self._rmt, self._message_id, payload)
        self.last_message_id = self._message_id
        self._message_id = (self._message_id + 2) & 0xFFFF_FFFF

    def _send_data_end_packet(self, payload: bytes) -> None:
        """send a DataEnd packet on the sync channel"""
        send_msg(self._sync, "DataEnd", self._rmt, self._message_id, payload)
        self.last_message_id = self._message_id
        self._message_id = (self._message_id + 2) & 0xFFFF_FFFF

    def fatal_error(self, error: str, error_message: str = "") -> None:
        err_msg = (error_message or error).encode()
        send_msg(self._sync, "FatalError", FATALERRORCODE[error], 0, err_msg)

    def error(self, error: str, error_message: str = "") -> None:
        err_msg = (error_message or error).encode()
        send_msg(self._sync, "Error", ERRORCODE[error], 0, err_msg)


# the following two routines are only used for debugging.
# they are commented out because their f-strings use a feature
# that is a syntax error in Python versions < 3.7

# def rxdecode(header):
#     (
#         prologue,
#         msg_type,
#         control_code,
#         message_parameter,
#         payload_length,
#     ) = struct.unpack(HEADER_FORMAT, header)
#
#     msg_type = MESSAGETYPE_STR[msg_type]
#     print(
#         f"Rx: {prologue=}, "
#         f"{msg_type=}, "
#         f"{control_code=}, "
#         f"{message_parameter=}, "
#         f"{payload_length=}"
#     )


# def txdecode(header, payload=b""):
#     (
#         prologue,
#         msg_type,
#         control_code,
#         message_parameter,
#         payload_length,
#     ) = struct.unpack(HEADER_FORMAT, header)
#
#     msg_type = MESSAGETYPE_STR[msg_type]
#     print(
#         f"Tx: {prologue=}, "
#         f"{msg_type=}, "
#         f"{control_code=}, "
#         f"{message_parameter=}, "
#         f"{payload_length=}, "
#         f"{len(payload)=}, "
#         f"{bytes(payload[:20]).decode('iso-8859-1')!r}"
#     )
