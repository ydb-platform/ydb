import errno
import logging
import platform
import select
import socket
import struct
import time
import warnings
from typing import Any, Optional, Union

import can
from can import BusABC, CanProtocol, Message
from can.typechecking import AutoDetectedConfig

from .utils import is_msgpack_installed, pack_message, unpack_message

is_linux = platform.system() == "Linux"
if is_linux:
    from fcntl import ioctl

log = logging.getLogger(__name__)


# see socket.getaddrinfo()
IPv4_ADDRESS_INFO = tuple[str, int]  # address, port
IPv6_ADDRESS_INFO = tuple[str, int, int, int]  # address, port, flowinfo, scope_id
IP_ADDRESS_INFO = Union[IPv4_ADDRESS_INFO, IPv6_ADDRESS_INFO]

# Additional constants for the interaction with Unix kernels
SO_TIMESTAMPNS = 35
SIOCGSTAMP = 0x8906

# Additional constants for the interaction with the Winsock API
WSAEINVAL = 10022


class UdpMulticastBus(BusABC):
    """A virtual interface for CAN communications between multiple processes using UDP over Multicast IP.

    It supports IPv4 and IPv6, specified via the channel (which really is just a multicast IP address as a
    string). You can also specify the port and the IPv6 *hop limit*/the IPv4 *time to live* (TTL).

    This bus does not support filtering based on message IDs on the kernel level but instead provides it in
    user space (in Python) as a fallback.

    Both default addresses should allow for multi-host CAN networks in a normal local area network (LAN) where
    multicast is enabled.

    .. note::
        The auto-detection of available interfaces (see) is implemented using heuristic that checks if the
        required socket operations are available. It then returns two configurations, one based on
        the :attr:`~UdpMulticastBus.DEFAULT_GROUP_IPv6` address and another one based on
        the :attr:`~UdpMulticastBus.DEFAULT_GROUP_IPv4` address.

    .. warning::
        The parameter `receive_own_messages` is currently unsupported and setting it to `True` will raise an
        exception.

    .. warning::
        This interface does not make guarantees on reliable delivery and message ordering, and also does not
        implement rate limiting or ID arbitration/prioritization under high loads. Please refer to the section
        :ref:`virtual_interfaces_doc` for more information on this and a comparison to alternatives.

    :param channel: A multicast IPv4 address (in `224.0.0.0/4`) or an IPv6 address (in `ff00::/8`).
                    This defines which version of IP is used. See
                    `Wikipedia ("Multicast address") <https://en.wikipedia.org/wiki/Multicast_address>`__
                    for more details on the addressing schemes.
                    Defaults to :attr:`~UdpMulticastBus.DEFAULT_GROUP_IPv6`.
    :param port: The IP port to read from and write to.
    :param hop_limit: The hop limit in IPv6 or in IPv4 the time to live (TTL).
    :param receive_own_messages: If transmitted messages should also be received by this bus.
                                 CURRENTLY UNSUPPORTED.
    :param fd:
        If CAN-FD frames should be supported. If set to false, an error will be raised upon sending such a
        frame and such received frames will be ignored.
    :param can_filters: See :meth:`~can.BusABC.set_filters`.

    :raises RuntimeError: If the *msgpack*-dependency is not available. It should be installed on all
                          non Windows platforms via the `setup.py` requirements.
    :raises NotImplementedError: If the `receive_own_messages` is passed as `True`.
    """

    #: An arbitrary IPv6 multicast address with "site-local" scope, i.e. only to be routed within the local
    #: physical network and not beyond it. It should allow for multi-host CAN networks in a normal IPv6 LAN.
    #: This is the default channel and should work with most modern routers if multicast is allowed.
    DEFAULT_GROUP_IPv6 = "ff15:7079:7468:6f6e:6465:6d6f:6d63:6173"

    #: An arbitrary IPv4 multicast address with "administrative" scope, i.e. only to be routed within
    #: administrative organizational boundaries and not beyond it.
    #: It should allow for multi-host CAN networks in a normal IPv4 LAN.
    #: This is provided as a default fallback channel if IPv6 is (still) not supported.
    DEFAULT_GROUP_IPv4 = "239.74.163.2"

    def __init__(
        self,
        channel: str = DEFAULT_GROUP_IPv6,
        port: int = 43113,
        hop_limit: int = 1,
        receive_own_messages: bool = False,
        fd: bool = True,
        **kwargs: Any,
    ) -> None:
        is_msgpack_installed()

        if receive_own_messages:
            raise can.CanInterfaceNotImplementedError(
                "receiving own messages is not yet implemented"
            )

        super().__init__(
            channel,
            **kwargs,
        )

        self._multicast = GeneralPurposeUdpMulticastBus(channel, port, hop_limit)
        self._can_protocol = CanProtocol.CAN_FD if fd else CanProtocol.CAN_20

    @property
    def is_fd(self) -> bool:
        class_name = self.__class__.__name__
        warnings.warn(
            f"The {class_name}.is_fd property is deprecated and superseded by "
            f"{class_name}.protocol. It is scheduled for removal in python-can version 5.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._can_protocol is CanProtocol.CAN_FD

    def _recv_internal(
        self, timeout: Optional[float]
    ) -> tuple[Optional[Message], bool]:
        result = self._multicast.recv(timeout)
        if not result:
            return None, False

        data, _, timestamp = result
        try:
            can_message = unpack_message(
                data, replace={"timestamp": timestamp}, check=True
            )
        except Exception as exception:
            raise can.CanOperationError(
                "could not unpack received message"
            ) from exception

        if self._can_protocol is not CanProtocol.CAN_FD and can_message.is_fd:
            return None, False

        return can_message, False

    def send(self, msg: can.Message, timeout: Optional[float] = None) -> None:
        if self._can_protocol is not CanProtocol.CAN_FD and msg.is_fd:
            raise can.CanOperationError(
                "cannot send FD message over bus with CAN FD disabled"
            )

        data = pack_message(msg)
        self._multicast.send(data, timeout)

    def fileno(self) -> int:
        """Provides the internally used file descriptor of the socket or `-1` if not available."""
        return self._multicast.fileno()

    def shutdown(self) -> None:
        """Close all sockets and free up any resources.

        Never throws errors and only logs them.
        """
        super().shutdown()
        self._multicast.shutdown()

    @staticmethod
    def _detect_available_configs() -> list[AutoDetectedConfig]:
        if hasattr(socket, "CMSG_SPACE"):
            return [
                {
                    "interface": "udp_multicast",
                    "channel": UdpMulticastBus.DEFAULT_GROUP_IPv6,
                },
                {
                    "interface": "udp_multicast",
                    "channel": UdpMulticastBus.DEFAULT_GROUP_IPv4,
                },
            ]

        # else, this interface cannot be used
        return []


class GeneralPurposeUdpMulticastBus:
    """A general purpose send and receive handler for multicast over IP/UDP.

    However, it raises CAN-specific exceptions for convenience.
    """

    def __init__(
        self, group: str, port: int, hop_limit: int, max_buffer: int = 4096
    ) -> None:
        self.group = group
        self.port = port
        self.hop_limit = hop_limit
        self.max_buffer = max_buffer

        # `False` will always work, no matter the setup. This might be changed by _create_socket().
        self.timestamp_nanosecond = False

        # Look up multicast group address in name server and find out IP version of the first suitable target
        # and then get the address family of it (socket.AF_INET or socket.AF_INET6)
        connection_candidates = socket.getaddrinfo(
            group, self.port, type=socket.SOCK_DGRAM
        )
        sock = None
        for connection_candidate in connection_candidates:
            address_family: socket.AddressFamily = connection_candidate[0]
            self.ip_version = 4 if address_family == socket.AF_INET else 6
            try:
                sock = self._create_socket(address_family)
            except OSError as error:
                log.info(
                    "could not connect to the multicast IP network of candidate %s; reason: %s",
                    connection_candidates,
                    error,
                )
        if sock is not None:
            self._socket = sock
        else:
            raise can.CanInitializationError(
                "could not connect to a multicast IP network"
            )

        # used in recv()
        self.received_timestamp_struct = "@ll"
        self.received_timestamp_struct_size = struct.calcsize(
            self.received_timestamp_struct
        )
        if self.timestamp_nanosecond:
            self.received_ancillary_buffer_size = socket.CMSG_SPACE(
                self.received_timestamp_struct_size
            )
        else:
            self.received_ancillary_buffer_size = 0

        # used by send()
        self._send_destination = (self.group, self.port)
        self._last_send_timeout: Optional[float] = None

    def _create_socket(self, address_family: socket.AddressFamily) -> socket.socket:
        """Creates a new socket. This might fail and raise an exception!

        :param address_family: whether this is of type `socket.AF_INET` or `socket.AF_INET6`

        :raises can.CanInitializationError:
            if the socket could not be opened or configured correctly; in this case, it is
            guaranteed to be closed/cleaned up
        """
        # create the UDP socket
        # this might already fail but then there is nothing to clean up
        sock = socket.socket(address_family, socket.SOCK_DGRAM)

        # configure the socket
        try:
            # set hop limit / TTL
            ttl_as_binary = struct.pack("@I", self.hop_limit)
            if self.ip_version == 4:
                sock.setsockopt(
                    socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl_as_binary
                )
            else:
                sock.setsockopt(
                    socket.IPPROTO_IPV6, socket.IPV6_MULTICAST_HOPS, ttl_as_binary
                )

            # Allow multiple programs to access that address + port
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            # Option not supported on Windows.
            if hasattr(socket, "SO_REUSEPORT"):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

            # set how to receive timestamps
            try:
                sock.setsockopt(socket.SOL_SOCKET, SO_TIMESTAMPNS, 1)
            except OSError as error:
                if (
                    error.errno == errno.ENOPROTOOPT
                    or error.errno == errno.EINVAL
                    or error.errno == WSAEINVAL
                ):  # It is unavailable on macOS (ENOPROTOOPT) or windows(EINVAL/WSAEINVAL)
                    self.timestamp_nanosecond = False
                else:
                    raise error
            else:
                self.timestamp_nanosecond = True

            # Bind it to the port (on any interface)
            sock.bind(("", self.port))

            # Join the multicast group
            group_as_binary = socket.inet_pton(address_family, self.group)
            if self.ip_version == 4:
                request = group_as_binary + struct.pack("@I", socket.INADDR_ANY)
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, request)
            else:
                request = group_as_binary + struct.pack("@I", 0)
                sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_JOIN_GROUP, request)

            return sock

        except OSError as error:
            # clean up the incompletely configured but opened socket
            try:
                sock.close()
            except OSError as close_error:
                # ignore but log any failures in here
                log.warning("Could not close partly configured socket: %s", close_error)

            # still raise the error
            raise can.CanInitializationError(
                "could not create or configure socket"
            ) from error

    def send(self, data: bytes, timeout: Optional[float] = None) -> None:
        """Send data to all group members. This call blocks.

        :param timeout: the timeout in seconds after which an Exception is raised is sending has failed
        :param data: the data to be sent
        :raises can.CanOperationError: if an error occurred while writing to the underlying socket
        :raises can.CanTimeoutError: if the timeout ran out before sending was completed
        """
        if timeout != self._last_send_timeout:
            self._last_send_timeout = timeout
            # this applies to all blocking calls on the socket, but sending is the only one that is blocking
            self._socket.settimeout(timeout)

        try:
            bytes_sent = self._socket.sendto(data, self._send_destination)
            if bytes_sent < len(data):
                raise TimeoutError()
        except TimeoutError:
            raise can.CanTimeoutError() from None
        except OSError as error:
            raise can.CanOperationError("failed to send via socket") from error

    def recv(
        self, timeout: Optional[float] = None
    ) -> Optional[tuple[bytes, IP_ADDRESS_INFO, float]]:
        """
        Receive up to **max_buffer** bytes.

        :param timeout: the timeout in seconds after which `None` is returned if no data arrived
        :returns: `None` on timeout, or a 3-tuple comprised of:
            - received data,
            - the sender of the data, and
            - a timestamp in seconds
        """
        # get all sockets that are ready (can be a list with a single value
        # being self.socket or an empty list if self.socket is not ready)
        try:
            # get all sockets that are ready (can be a list with a single value
            # being self.socket or an empty list if self.socket is not ready)
            ready_receive_sockets, _, _ = select.select([self._socket], [], [], timeout)
        except OSError as exc:
            # something bad (not a timeout) happened (e.g. the interface went down)
            raise can.CanOperationError(
                f"Failed to wait for IP/UDP socket: {exc}"
            ) from exc

        if ready_receive_sockets:  # not empty
            # fetch timestamp; this is configured in _create_socket()
            if self.timestamp_nanosecond:
                # fetch data, timestamp & source address
                (
                    raw_message_data,
                    ancillary_data,
                    _,  # flags
                    sender_address,
                ) = self._socket.recvmsg(
                    self.max_buffer, self.received_ancillary_buffer_size
                )

                # Very similar to timestamp handling in can/interfaces/socketcan/socketcan.py -> capture_message()
                if len(ancillary_data) != 1:
                    raise can.CanOperationError(
                        "Only requested a single extra field but got a different amount"
                    )
                cmsg_level, cmsg_type, cmsg_data = ancillary_data[0]
                if cmsg_level != socket.SOL_SOCKET or cmsg_type != SO_TIMESTAMPNS:
                    raise can.CanOperationError(
                        "received control message type that was not requested"
                    )
                # see https://man7.org/linux/man-pages/man3/timespec.3.html -> struct timespec for details
                seconds, nanoseconds = struct.unpack(
                    self.received_timestamp_struct, cmsg_data
                )
                if nanoseconds >= 1e9:
                    raise can.CanOperationError(
                        f"Timestamp nanoseconds field was out of range: {nanoseconds} not less than 1e9"
                    )
                timestamp = seconds + nanoseconds * 1.0e-9
            else:
                # fetch data & source address
                (raw_message_data, sender_address) = self._socket.recvfrom(
                    self.max_buffer
                )

                if is_linux:
                    # This ioctl isn't supported on Darwin & Windows.
                    result_buffer = ioctl(
                        self._socket.fileno(),
                        SIOCGSTAMP,
                        bytes(self.received_timestamp_struct_size),
                    )
                    seconds, microseconds = struct.unpack(
                        self.received_timestamp_struct, result_buffer
                    )
                else:
                    # fallback to time.time_ns
                    now = time.time()

                    # Extract seconds and microseconds
                    seconds = int(now)
                    microseconds = int((now - seconds) * 1000000)

                if microseconds >= 1e6:
                    raise can.CanOperationError(
                        f"Timestamp microseconds field was out of range: {microseconds} not less than 1e6"
                    )
                timestamp = seconds + microseconds * 1e-6

            return raw_message_data, sender_address, timestamp

        # socket wasn't readable or timeout occurred
        return None

    def fileno(self) -> int:
        """Provides the internally used file descriptor of the socket or `-1` if not available."""
        return self._socket.fileno()

    def shutdown(self) -> None:
        """Close all sockets and free up any resources.

        Never throws errors and only logs them.
        """
        try:
            self._socket.close()
        except OSError as exception:
            log.error("could not close IP socket: %s", exception)
