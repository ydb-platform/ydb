import logging
import ipaddress
import socket
import struct
import time
import ssl
from enum import IntEnum
from typing import Union
from .constants import (
    A_DOIP_CTRL,
    TCP_DATA_UNSECURED,
    UDP_DISCOVERY,
    A_PROCESSING_TIME,
    LINK_LOCAL_MULTICAST_ADDRESS,
)
from .messages import *

logger = logging.getLogger("doipclient")


class Parser:
    """Implements state machine for DoIP transport layer.

    See Table 16 "Generic DoIP header structure" of ISO 13400-2:2019 (E). While TCP transport
    is reliable, the UDP broadcasts are not, so the state machine is a little more defensive
    than one might otherwise expect. When using TCP, reads from the socket aren't guaranteed
    to be exactly one DoIP message, so the running buffer needs to be maintained across reads
    """

    class ParserState(IntEnum):
        READ_PROTOCOL_VERSION = 1
        READ_INVERSE_PROTOCOL_VERSION = 2
        READ_PAYLOAD_TYPE = 3
        READ_PAYLOAD_SIZE = 4
        READ_PAYLOAD = 5

    def __init__(self):
        self.reset()

    def reset(self):
        self.rx_buffer = bytearray()
        self.protocol_version = None
        self.payload_type = None
        self.payload_size = None
        self.payload = bytearray()
        self._state = Parser.ParserState.READ_PROTOCOL_VERSION

    def push_bytes(self, data_bytes):
        self.rx_buffer += data_bytes

    def read_message(self, data_bytes):
        self.rx_buffer += data_bytes
        if self._state == Parser.ParserState.READ_PROTOCOL_VERSION:
            if len(self.rx_buffer) >= 1:
                self.payload = bytearray()
                self.payload_type = None
                self.payload_size = None
                self.protocol_version = int(self.rx_buffer.pop(0))
                self._state = Parser.ParserState.READ_INVERSE_PROTOCOL_VERSION

        if self._state == Parser.ParserState.READ_INVERSE_PROTOCOL_VERSION:
            if len(self.rx_buffer) >= 1:
                inverse_protocol_version = int(self.rx_buffer.pop(0))
                if inverse_protocol_version != (0xFF ^ self.protocol_version):
                    logger.warning(
                        "Bad DoIP Header - Inverse protocol version does not match. Ignoring."
                    )
                    # Bad protocol version inverse - shift the buffer forward
                    self.protocol_version = inverse_protocol_version
                else:
                    self._state = Parser.ParserState.READ_PAYLOAD_TYPE

        if self._state == Parser.ParserState.READ_PAYLOAD_TYPE:
            if len(self.rx_buffer) >= 2:
                self.payload_type = self.rx_buffer.pop(0) << 8
                self.payload_type |= self.rx_buffer.pop(0)
                self._state = Parser.ParserState.READ_PAYLOAD_SIZE

        if self._state == Parser.ParserState.READ_PAYLOAD_SIZE:
            if len(self.rx_buffer) >= 4:
                self.payload_size = self.rx_buffer.pop(0) << 24
                self.payload_size |= self.rx_buffer.pop(0) << 16
                self.payload_size |= self.rx_buffer.pop(0) << 8
                self.payload_size |= self.rx_buffer.pop(0)
                self._state = Parser.ParserState.READ_PAYLOAD

        if self._state == Parser.ParserState.READ_PAYLOAD:
            remaining_bytes = self.payload_size - len(self.payload)
            self.payload += self.rx_buffer[:remaining_bytes]
            self.rx_buffer = self.rx_buffer[remaining_bytes:]
            if len(self.payload) == self.payload_size:
                self._state = Parser.ParserState.READ_PROTOCOL_VERSION
                logger.debug(
                    "Received DoIP Message. Type: 0x{:X}, Payload Size: {} bytes, Payload: {}".format(
                        self.payload_type,
                        self.payload_size,
                        " ".join(f"{byte:02X}" for byte in self.payload),
                    )
                )
                try:
                    return payload_type_to_message[self.payload_type].unpack(
                        self.payload, self.payload_size
                    )
                except KeyError:
                    return ReservedMessage.unpack(
                        self.payload_type, self.payload, self.payload_size
                    )


class DoIPClient:
    """A Diagnostic over IP (DoIP) Client implementing the majority of ISO-13400-2:2019 (E).

    This is a basic DoIP client which was designed primarily for use with the python-udsoncan package for UDS communication
    with ECU's over automotive ethernet. Certain parts of the specification would require threaded operation to
    maintain the time-based state described by the ISO document. However, in practice these are rarely important,
    particularly for use with UDS - especially with scripts that tend to go through instructions as fast as possible.

    :param ecu_ip_address: This is the IP address of the target ECU. This should be a string representing an IPv4
        address like "192.168.1.1" or an IPv6 address like "2001:db8::". Like the logical_address, if you don't know the
        value for your ECU, utilize the get_entity() or await_vehicle_announcement() method.
    :type ecu_ip_address: str
    :param ecu_logical_address: The logical address of the target ECU. This should be an integer. According to the
        specification, the correct range is 0x0001 to 0x0DFF ("VM specific"). If you don't know the logical address,
        either use the get_entity() method OR the await_vehicle_announcement() method and power
        cycle the ECU - it should identify itself on bootup.
    :type ecu_logical_address: int
    :param tcp_port: The destination TCP port for DoIP data communication. By default this is 13400 for unsecure and
        3496 when using TLS.
    :type tcp_port: int, optional
    :param activation_type: The activation type to use on initial connection. Most ECU's require an activation request
        before they'll respond, and typically the default activation type will do. The type can be changed later using
        request_activation() method. Use `None` to disable activation at startup.
    :type activation_type: RoutingActivationRequest.ActivationType, optional
    :param protocol_version: The DoIP protocol version to use for communication. Represents the version of the ISO 13400
        specification to follow. 0x02 (2012) is probably correct for most ECU's at the time of writing, though technically
        this implementation is against 0x03 (2019).
    :type protocol_version: int
    :param client_logical_address: The logical address that this DoIP client will use to identify itself. Per the spec,
        this should be 0x0E00 to 0x0FFF. Can typically be left as default.
    :type client_logical_address: int
    :param client_ip_address: If specified, attempts to bind to this IP as the source for both UDP and TCP communication.
        Useful if you have multiple network adapters. Can be an IPv4 or IPv6 address just like `ecu_ip_address`, though
        the type should match.
    :type client_ip_address: str, optional
    :param use_secure: Enables TLS. If set to True, a default SSL context is used. For more control, a preconfigured
        SSL context can be passed directly. Untested. Should be combined with changing tcp_port to 3496.
    :type use_secure: Union[bool,ssl.SSLContext]
    :param auto_reconnect_tcp: Attempt to automatically reconnect TCP sockets that were closed by peer
    :type auto_reconnect_tcp: bool
    :param vm_specific: Optional 4 byte long int
    :type vm_specific: int, optional

    :raises ConnectionRefusedError: If the activation request fails
    :raises ValueError: If the IPAddress is neither an IPv4 nor an IPv6 address
    """

    def __init__(
        self,
        ecu_ip_address,
        ecu_logical_address,
        tcp_port=TCP_DATA_UNSECURED,
        udp_port=UDP_DISCOVERY,
        activation_type=RoutingActivationRequest.ActivationType.Default,
        protocol_version=0x02,
        client_logical_address=0x0E00,
        client_ip_address=None,
        use_secure=False,
        auto_reconnect_tcp=False,
        vm_specific=None,
    ):
        self._ecu_logical_address = ecu_logical_address
        self._client_logical_address = client_logical_address
        self._client_ip_address = client_ip_address
        self._use_secure = use_secure
        self._ecu_ip_address = ecu_ip_address
        self._tcp_port = tcp_port
        self._udp_port = udp_port
        self._activation_type = activation_type
        self._udp_parser = Parser()
        self._tcp_parser = Parser()
        self._protocol_version = protocol_version
        self._auto_reconnect_tcp = auto_reconnect_tcp
        self._tcp_close_detected = False
        self.vm_specific = vm_specific

        # Check the ECU IP type to determine socket family
        # Will raise ValueError if neither a valid IPv4, nor IPv6 address
        if type(ipaddress.ip_address(self._ecu_ip_address)) == ipaddress.IPv6Address:
            self._address_family = socket.AF_INET6
        else:
            self._address_family = socket.AF_INET

        self._connect()

        if self._activation_type is not None:
            result = self.request_activation(self._activation_type, disable_retry=True)
            if result.response_code != RoutingActivationResponse.ResponseCode.Success:
                raise ConnectionRefusedError(
                    f"Activation Request failed with code {result.response_code}"
                )

    class TransportType(IntEnum):
        TRANSPORT_UDP = 1
        TRANSPORT_TCP = 2

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    @staticmethod
    def _create_udp_socket(
        ipv6=False, udp_port=UDP_DISCOVERY, timeout=None, source_interface=None
    ):
        if ipv6:
            sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)

            # IPv6 version always uses link-local scope multicast address (FF02 16 ::1)
            sock.bind((LINK_LOCAL_MULTICAST_ADDRESS, udp_port))

            if source_interface is None:
                # 0 is the "default multicast interface" which is unlikely to be correct, but it will do
                interface_index = 0
            else:
                interface_index = socket.if_nametoindex(source_interface)

            # Join the group so that packets are delivered
            mc_addr = ipaddress.IPv6Address(LINK_LOCAL_MULTICAST_ADDRESS)
            join_data = struct.pack("16sI", mc_addr.packed, interface_index)
            # IPV6_JOIN_GROUP is also known as IPV6_ADD_MEMBERSHIP, though older Python for Windows doesn't have it
            # IPPROTO_IPV6 may be missing in older Windows builds
            try:
                from socket import IPPROTO_IPV6
            except ImportError:
                IPPROTO_IPV6 = 41
            sock.setsockopt(IPPROTO_IPV6, socket.IPV6_JOIN_GROUP, join_data)
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # IPv4, use INADDR_ANY to listen to all interfaces for broadcasts (not multicast)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.bind(("", udp_port))

        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if timeout is not None:
            sock.settimeout(timeout)

        return sock

    @staticmethod
    def _pack_doip(protocol_version, payload_type, payload_data):
        data_bytes = struct.pack(
            "!BBHL",
            protocol_version,
            0xFF ^ protocol_version,
            payload_type,
            len(payload_data),
        )
        data_bytes += payload_data

        return data_bytes

    @classmethod
    def await_vehicle_announcement(
        cls,
        udp_port=UDP_DISCOVERY,
        timeout=None,
        ipv6=False,
        source_interface=None,
        sock=None,
    ):
        """Receive Vehicle Announcement Message

        When an ECU first turns on, it's supposed to broadcast a Vehicle Announcement Message over UDP 3 times
        to assist DoIP clients in determining ECU IP's and Logical Addresses. Will use an IPv4 socket by default,
        though this can be overridden with the `ipv6` parameter.

        :param udp_port: The UDP port to listen on. Per the spec this should be 13400, but some VM's use a custom
            one.
        :type udp_port: int, optional
        :param timeout: Maximum amount of time to wait for message
        :type timeout: float, optional
        :param ipv6: Bool forcing IPV6 socket instead of IPV4 socket
        :type ipv6: bool, optional
        :param source_interface: Interface name (like "eth0") to bind to for use with IPv6. Defaults to None which
            will use the default interface (which may not be the one connected to the ECU). Does nothing for IPv4,
            which will bind to all interfaces uses INADDR_ANY.
        :type source_interface: str, optional
        :return: IP Address of ECU and VehicleAnnouncementMessage object
        :rtype: tuple
        :raises TimeoutError: If vehicle announcement not received in time
        """
        start_time = time.time()

        parser = Parser()

        if not sock:
            sock = cls._create_udp_socket(
                ipv6=ipv6,
                udp_port=udp_port,
                timeout=timeout,
                source_interface=source_interface,
            )

        while True:
            remaining = None
            if timeout:
                duration = time.time() - start_time
                if duration >= timeout:
                    raise TimeoutError(
                        "Timed out waiting for Vehicle Announcement broadcast"
                    )
                else:
                    remaining = timeout - duration
                    sock.settimeout(remaining)
            try:
                data, addr = sock.recvfrom(1024)
            except socket.timeout:
                raise TimeoutError(
                    "Timed out waiting for Vehicle Announcement broadcast"
                )
            # "Only one DoIP message shall be transmitted by any DoIP entity per datagram"
            # So, reset the parser after each UDP read
            parser.reset()
            result = parser.read_message(data)
            if result and type(result) == VehicleIdentificationResponse:
                return addr, result

    @classmethod
    def get_entity(
        cls, ecu_ip_address="255.255.255.255", protocol_version=0x02, eid=None, vin=None
    ):
        """Sends a VehicleIdentificationRequest and awaits a VehicleIdentificationResponse from the ECU,
        either with a specified VIN, EIN, or nothing. Equivalent to the request_vehicle_identification() method
        but can be called without instantiation.

        :param ecu_ip_address: This is the IP address of the target ECU for unicast. Defaults to broadcast if
        the address is not known.
        :type ecu_ip_address: str, optional
        :param protocol_version: The DoIP protocol version to use for communication. Represents the version of the ISO 13400
            specification to follow. 0x02 (2012) is probably correct for most ECU's at the time of writing, though technically
            this implementation is against 0x03 (2019).
        :type protocol_version: int, optional
        :param eid: EID of the Vehicle
        :type eid: bytes, optional
        :param vin: VIN of the Vehicle
        :type vin: str, optional
        :return: The vehicle identification response message
        :rtype: VehicleIdentificationResponse
        """

        # UDP_TEST_EQUIPMENT_REQUEST is dynamically assigned using udp_port=0
        sock = cls._create_udp_socket(udp_port=0, timeout=A_DOIP_CTRL)

        if eid:
            message = VehicleIdentificationRequestWithEID(eid)
        elif vin:
            message = VehicleIdentificationRequestWithVIN(vin)
        else:
            message = VehicleIdentificationRequest()

        payload_data = message.pack()
        payload_type = payload_message_to_type[type(message)]

        data_bytes = cls._pack_doip(protocol_version, payload_type, payload_data)
        logger.debug(
            "Sending DoIP Vehicle Identification Request: Type: 0x{:X}, Payload Size: {}, Payload: {}".format(
                payload_type,
                len(payload_data),
                " ".join(f"{byte:02X}" for byte in payload_data),
            )
        )
        sock.sendto(data_bytes, (ecu_ip_address, UDP_DISCOVERY))

        return cls.await_vehicle_announcement(timeout=A_DOIP_CTRL, sock=sock)

    def empty_rxqueue(self):
        """Implemented for compatibility with udsoncan library. Nothing useful to be done yet"""
        pass

    def empty_txqueue(self):
        """Implemented for compatibility with udsoncan library. Nothing useful to be done yet"""
        pass

    def read_doip(
        self, timeout=A_PROCESSING_TIME, transport=TransportType.TRANSPORT_TCP
    ):
        """Helper function to read from the DoIP socket.

        :param timeout: Maximum time allowed for response from ECU
        :type timeout: float, optional
        :param transport: The IP transport layer to read from, either UDP or TCP
        :type transport: DoIPClient.TransportType, optional
        :raises IOError: If DoIP layer fails with negative acknowledgement
        :raises TimeoutException: If ECU fails to respond in time
        """
        start_time = time.time()
        data = bytearray()
        while (time.time() - start_time) <= timeout:
            if transport == DoIPClient.TransportType.TRANSPORT_TCP:
                response = self._tcp_parser.read_message(data)
            else:
                response = self._udp_parser.read_message(data)
            data = bytearray()
            if type(response) == GenericDoIPNegativeAcknowledge:
                raise IOError(
                    f"DoIP Negative Acknowledge. NACK Code: {response.nack_code}"
                )
            elif type(response) == AliveCheckRequest:
                logger.warning("Responding to an alive check")
                self.send_doip_message(AliveCheckResponse(self._client_logical_address))
            elif response:
                # We got a response that might actually be interesting to the caller,
                # so return it.
                return response
            else:
                # There were no responses in the parser, so we need to read off the network
                # and feed that to the parser until we find another DoIP message

                if (
                    transport == DoIPClient.TransportType.TRANSPORT_TCP
                ) and self._tcp_close_detected:
                    # The caller is looking for TCP responses, but there were no messages
                    # returned from the parser and the socket has been closed (so no further
                    # responses are expected). It's safe to stop looking early and raise
                    # a TimeoutError
                    break
                else:
                    try:
                        if transport == DoIPClient.TransportType.TRANSPORT_TCP:
                            data = self._tcp_sock.recv(1024)
                            if len(data) == 0:
                                logger.debug("Peer has closed the connection.")
                                self._tcp_close_detected = True
                        else:
                            # "Only one DoIP message shall be transmitted by any DoIP entity
                            # per UDP datagram", so reset the UDP parser for each recv()
                            self._udp_parser.reset()
                            data = self._udp_sock.recv(1024)
                    except socket.timeout:
                        pass
        raise TimeoutError("ECU failed to respond in time")

    def _tcp_socket_check(self, first_timeout=0.010):
        """Helper function to service a TCP socket and check for disconnects.

        Called from send_doip() before and after TCP socket sends to detect if reconnect
        is needed.

        :param first_timeout: Timeout for the first recv() call. This should correspond to
            how long you expect the ECU to return an RST after sending to the
            socket if the connection was unexpectedly terminated. Too long
            and it hurts performance, too short and you run the risk of
            missing a socket reconnect opportunity. Normally <1ms, but
            allowing 10ms by default to be safe.
        :type first_timeout: float
        """
        original_timeout = self._tcp_sock.gettimeout()
        try:
            self._tcp_sock.settimeout(first_timeout)
            while True:
                data = self._tcp_sock.recv(1024)
                if len(data) == 0:
                    logger.debug("TCP Connection closed by ECU, attempting to reset")
                    self._tcp_close_detected = True
                    break
                else:
                    self._tcp_parser.push_bytes(data)
                # Subsequent reads, go to 0 timeout
                self._tcp_sock.settimeout(0)
        except (BlockingIOError, socket.timeout, ssl.SSLError):
            pass
        except (ConnectionResetError, BrokenPipeError):
            logger.debug("TCP Connection broken, attempting to reset")
            self._tcp_close_detected = True
        finally:
            self._tcp_sock.settimeout(original_timeout)

    def send_doip(
        self,
        payload_type,
        payload_data,
        transport=TransportType.TRANSPORT_TCP,
        disable_retry=False,
    ):
        """Helper function to send to the DoIP socket.

        Adds the correct DoIP header to the payload and sends to the socket.

        :param payload_type: The payload type (see Table 17 "Overview of DoIP payload types" in ISO-13400
        :type payload_type: int
        :param transport: The IP transport layer to send to, either UDP or TCP
        :type transport: DoIPClient.TransportType, optional
        :param disable_retry: Disables retry regardless of auto_reconnect_tcp flag. This is used by activation
            requests during connect/reconnect.
        :type disable_retry: bool, optional
        """

        retry = self._auto_reconnect_tcp and not disable_retry

        data_bytes = self._pack_doip(self._protocol_version, payload_type, payload_data)
        logger.debug(
            "Sending DoIP Message: Type: 0x{:X}, Payload Size: {}, Payload: {}".format(
                payload_type,
                len(payload_data),
                " ".join(f"{byte:02X}" for byte in payload_data),
            )
        )

        # The ECU is well within its rights to have closed the socket since we last sent it data -
        # particularly if the tester has been quiet for a while. For TCP there's two possibilities
        # 1) The ECU closed the connection properly, and there's a FIN/RST waiting to be read
        # 2) The ECU force closed the connection - we won't find that out until we try to write
        #    something and the ECU responds with an RST because the session isn't valid anymore.
        #
        # For (1) we could easily let the state machine go without a special case, but then we'd
        # be pushing a packet that the ECU would have to ignore (if they closed they have no way
        # to respond). So, we'll handle before the Tx, but we won't allow it to block.

        if retry:
            self._tcp_socket_check(first_timeout=0)

        remaining = len(data_bytes)
        attempted_reconnect = False

        # In general, the entire DoIP message should fit in one TCP packet, but it's good practice
        # to loop until the whole packet has been written, in case the OS write buffers get backed up
        while remaining > 0:
            if transport == DoIPClient.TransportType.TRANSPORT_TCP:
                if retry and self._tcp_close_detected:
                    if not attempted_reconnect:
                        logger.warning("TCP reconnecting")
                        self.reconnect()
                        attempted_reconnect = True
                    else:
                        logger.warning(
                            "TCP needs reconnection, but we already attempted once. Send will fail."
                        )

                remaining -= self._tcp_sock.send(data_bytes[-remaining:])

                if retry and not self._tcp_close_detected:
                    self._tcp_socket_check()
                    if self._tcp_close_detected:
                        remaining = len(data_bytes)

            else:
                remaining -= self._udp_sock.sendto(
                    data_bytes[-remaining:], (self._ecu_ip_address, self._udp_port)
                )

    def send_doip_message(
        self,
        doip_message,
        transport=TransportType.TRANSPORT_TCP,
        disable_retry=False,
    ):
        """Helper function to send an unpacked message to the DoIP socket.

        Packs the given message and adds the correct DoIP header before sending to the socket

        :param doip_message: DoIP message object
        :type doip_message: object
        :param transport: The IP transport layer to send to, either UDP or TCP
        :type transport: DoIPClient.TransportType, optional
        :param disable_retry: Disables retry regardless of auto_reconnect_tcp flag. This is used by activation
            requests during connect/reconnect.
        :type disable_retry: bool, optional
        """
        payload_type = payload_message_to_type[type(doip_message)]
        payload_data = doip_message.pack()
        self.send_doip(
            payload_type, payload_data, transport=transport, disable_retry=disable_retry
        )

    def request_activation(
        self, activation_type, vm_specific=None, disable_retry=False
    ):
        """Requests a given activation type from the ECU for this connection using payload type 0x0005

        :param activation_type: The type of activation to request - see Table 47 ("Routing
            activation request activation types") of ISO-13400, but should generally be 0 (default)
            or 1 (regulatory diagnostics)
        :type activation_type: RoutingActivationRequest.ActivationType
        :param vm_specific: Optional 4 byte long int
        :type vm_specific: int, optional
        :param disable_retry: Disables retry regardless of auto_reconnect_tcp flag. This is used by activation
            requests during connect/reconnect.
        :type disable_retry: bool, optional
        :return: The resulting activation response object
        :rtype: RoutingActivationResponse
        :raises ValueError: vm_specific is invalid or out of range
        """
        message = RoutingActivationRequest(
            self._client_logical_address,
            activation_type,
            vm_specific=(
                self._validate_vm_specific_value(vm_specific)
                if vm_specific
                else self.vm_specific
            ),
        )
        self.send_doip_message(message, disable_retry=disable_retry)
        while True:
            result = self.read_doip()
            if isinstance(result, RoutingActivationResponse):
                return result
            if result:
                logger.warning(
                    "Received unexpected DoIP message type {}. Ignoring".format(
                        type(result)
                    )
                )

    def request_vehicle_identification(self, eid=None, vin=None):
        """Sends a VehicleIdentificationRequest and awaits a VehicleIdentificationResponse from the ECU, either with a specified VIN, EIN,
        or nothing.
        :param eid: EID of the Vehicle
        :type eid: bytes, optional
        :param vin: VIN of the Vehicle
        :type vin: str, optional
        :return: The vehicle identification response message
        :rtype: VehicleIdentificationResponse
        """
        if eid:
            message = VehicleIdentificationRequestWithEID(eid)
        elif vin:
            message = VehicleIdentificationRequestWithVIN(vin)
        else:
            message = VehicleIdentificationRequest()
        self.send_doip_message(
            message, transport=DoIPClient.TransportType.TRANSPORT_UDP
        )
        while True:
            result = self.read_doip(transport=DoIPClient.TransportType.TRANSPORT_UDP)
            if type(result) == VehicleIdentificationResponse:
                return result
            elif result:
                logger.warning(
                    "Received unexpected DoIP message type {}. Ignoring".format(
                        type(result)
                    )
                )

    def request_alive_check(self):
        """Request that the ECU send an alive check response

        :return: Alive Check Response object
        :rtype: AliveCheckResopnse
        """
        message = AliveCheckRequest()
        self.send_doip_message(
            message, transport=DoIPClient.TransportType.TRANSPORT_TCP
        )
        while True:
            result = self.read_doip(transport=DoIPClient.TransportType.TRANSPORT_TCP)
            if type(result) == AliveCheckResponse:
                return result
            elif result:
                logger.warning(
                    "Received unexpected DoIP message type {}. Ignoring".format(
                        type(result)
                    )
                )

    def request_diagnostic_power_mode(self):
        """Request that the ECU send a Diagnostic Power Mode response

        :return: Diagnostic Power Mode Response object
        :rtype: DiagnosticPowerModeResponse
        """
        message = DiagnosticPowerModeRequest()
        self.send_doip_message(
            message, transport=DoIPClient.TransportType.TRANSPORT_UDP
        )
        while True:
            result = self.read_doip(transport=DoIPClient.TransportType.TRANSPORT_UDP)
            if type(result) == DiagnosticPowerModeResponse:
                return result
            elif result:
                logger.warning(
                    "Received unexpected DoIP message type {}. Ignoring".format(
                        type(result)
                    )
                )

    def request_entity_status(self):
        """Request that the ECU send a DoIP Entity Status Response

        :return: DoIP Entity Status Response
        :rtype: EntityStatusResponse
        """
        message = DoipEntityStatusRequest()
        self.send_doip_message(
            message, transport=DoIPClient.TransportType.TRANSPORT_UDP
        )
        while True:
            result = self.read_doip(transport=DoIPClient.TransportType.TRANSPORT_UDP)
            if type(result) == EntityStatusResponse:
                return result
            elif result:
                logger.warning(
                    "Received unexpected DoIP message type {}. Ignoring".format(
                        type(result)
                    )
                )

    def send_diagnostic(self, diagnostic_payload, timeout=A_PROCESSING_TIME):
        """Send a raw diagnostic payload (ie: UDS) to the ECU.

        :param diagnostic_payload: UDS payload to transmit to the ECU
        :type diagnostic_payload: bytearray
        :raises IOError: DoIP negative acknowledgement received
        """
        self.send_diagnostic_to_address(
            self._ecu_logical_address, diagnostic_payload, timeout
        )

    def send_diagnostic_to_address(
        self, address, diagnostic_payload, timeout=A_PROCESSING_TIME
    ):
        """Send a raw diagnostic payload (ie: UDS) to the specified address.

        :param address: The logical address to send the diagnostic payload to
        :type address: int
        :param diagnostic_payload: UDS payload to transmit to the ECU
        :type diagnostic_payload: bytearray
        :raises IOError: DoIP negative acknowledgement received
        """
        message = DiagnosticMessage(
            self._client_logical_address, address, diagnostic_payload
        )
        self.send_doip_message(message)
        start_time = time.time()
        while True:
            ellapsed_time = time.time() - start_time
            if timeout and ellapsed_time > timeout:
                raise TimeoutError("Timed out waiting for diagnostic response")
            if timeout:
                result = self.read_doip(timeout=(timeout - ellapsed_time))
            else:
                result = self.read_doip()
            if type(result) == DiagnosticMessageNegativeAcknowledgement:
                raise IOError(
                    "Diagnostic request rejected with negative acknowledge code: {}".format(
                        result.nack_code
                    )
                )
            elif type(result) == DiagnosticMessagePositiveAcknowledgement:
                return
            elif result:
                logger.warning(
                    "Received unexpected DoIP message type {}. Ignoring".format(
                        type(result)
                    )
                )

    def receive_diagnostic(self, timeout=None):
        """Receive a raw diagnostic payload (ie: UDS) from the ECU.

        :return: Raw UDS payload
        :rtype: bytearray
        :raises TimeoutError: No diagnostic response received in time
        """
        start_time = time.time()
        while True:
            ellapsed_time = time.time() - start_time
            if timeout and ellapsed_time > timeout:
                raise TimeoutError("Timed out waiting for diagnostic response")
            if timeout:
                result = self.read_doip(timeout=(timeout - ellapsed_time))
            else:
                result = self.read_doip()
            if type(result) == DiagnosticMessage:
                return result.user_data
            elif result:
                logger.warning(
                    "Received unexpected DoIP message type {}. Ignoring".format(
                        type(result)
                    )
                )

    def _connect(self):
        """Helper to establish socket communication"""
        self._tcp_sock = socket.socket(self._address_family, socket.SOCK_STREAM)
        self._tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        self._tcp_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        if self._client_ip_address is not None:
            self._tcp_sock.bind((self._client_ip_address, 0))
        self._tcp_sock.connect((self._ecu_ip_address, self._tcp_port))
        self._tcp_sock.settimeout(A_PROCESSING_TIME)
        self._tcp_close_detected = False

        self._udp_sock = socket.socket(self._address_family, socket.SOCK_DGRAM)
        self._udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._udp_sock.settimeout(A_PROCESSING_TIME)
        if self._client_ip_address is not None:
            self._udp_sock.bind((self._client_ip_address, 0))

        if self._use_secure:
            if isinstance(self._use_secure, ssl.SSLContext):
                ssl_context = self._use_secure
            else:
                ssl_context = ssl.create_default_context()
            self._wrap_socket(ssl_context)

    def _wrap_socket(self, ssl_context):
        """Wrap the underlying socket in a SSL context."""
        self._tcp_sock = ssl_context.wrap_socket(self._tcp_sock)

    def close(self):
        """Close the DoIP client"""
        self._tcp_sock.close()
        self._udp_sock.close()

    def reconnect(self, close_delay=A_PROCESSING_TIME):
        """Attempts to re-establish the connection. Useful after an ECU reset

        :param close_delay: Time to wait between closing and re-opening socket
        :type close_delay: float, optional
        """
        # Close the sockets
        self.close()
        # Reset the parser state machines
        self._udp_parser = Parser()
        self._tcp_parser = Parser()
        # Allow the ECU time time to cleanup the DoIP session/socket before re-establishing
        time.sleep(close_delay)
        self._connect()
        if self._activation_type is not None:
            result = self.request_activation(self._activation_type, disable_retry=True)
            if result.response_code != RoutingActivationResponse.ResponseCode.Success:
                raise ConnectionRefusedError(
                    f"Activation Request failed with code {result.response_code}"
                )

    @staticmethod
    def _validate_vm_specific_value(value):
        """Validate the VM specific value (must be > 0 and <= 0xffffffff) or None.
        If the conditions are not fulfilled, raises an exception.

        :param value: The value to check.
        :type value: int, optional
        :return: The input value if valid.
        :rtype: int or None
        :raises ValueError: If the value is invalid or out of range.
        """
        if not isinstance(value, int) and value is not None:
            raise ValueError("Invalid vm_specific type must be int or None")
        if isinstance(value, int) and (value < 0 or value > 0xFFFFFFFF):
            raise ValueError("Invalid vm_specific value must be > 0 and <= 0xffffffff")
        return value

    @property
    def vm_specific(self):
        """Get the optional OEM specific field value if set.

        :return: vm_specific value
        :rtype: int, optional
        """
        return self._vm_specific

    @vm_specific.setter
    def vm_specific(self, value):
        """Set the optional OEM specific field value. If you do not need to send this item, set it to None.

        :param value: The vm_specific value (must be > 0 and <= 0xffffffff) or None
        :type value: int, optional
        :raises ValueError: Value is invalid or out of range
        """
        self._vm_specific = self._validate_vm_specific_value(value)
