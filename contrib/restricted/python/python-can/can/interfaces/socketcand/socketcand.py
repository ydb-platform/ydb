"""
Interface to socketcand
see https://github.com/linux-can/socketcand

Authors: Marvin Seiler, Gerrit Telkamp

Copyright (C) 2021  DOMOLOGIC GmbH
http://www.domologic.de
"""

import logging
import os
import select
import socket
import time
import traceback
import urllib.parse as urlparselib
import xml.etree.ElementTree as ET
from collections import deque

import can

log = logging.getLogger(__name__)

DEFAULT_SOCKETCAND_DISCOVERY_ADDRESS = ""
DEFAULT_SOCKETCAND_DISCOVERY_PORT = 42000


def detect_beacon(timeout_ms: int = 3100) -> list[can.typechecking.AutoDetectedConfig]:
    """
    Detects socketcand servers

    This is what :meth:`can.detect_available_configs` ends up calling to search
    for available socketcand servers with a default timeout of 3100ms
    (socketcand sends a beacon packet every 3000ms).

    Using this method directly allows for adjusting the timeout. Extending
    the timeout beyond the default time period could be useful if UDP
    packet loss is a concern.

    :param timeout_ms:
        Timeout in milliseconds to wait for socketcand beacon packets

    :return:
        See :meth:`~can.detect_available_configs`
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(
            (DEFAULT_SOCKETCAND_DISCOVERY_ADDRESS, DEFAULT_SOCKETCAND_DISCOVERY_PORT)
        )
        log.info(
            "Listening on for socketcand UDP advertisement on %s:%s",
            DEFAULT_SOCKETCAND_DISCOVERY_ADDRESS,
            DEFAULT_SOCKETCAND_DISCOVERY_PORT,
        )

        now = time.time() * 1000
        end_time = now + timeout_ms
        while (time.time() * 1000) < end_time:
            try:
                # get all sockets that are ready (can be a list with a single value
                # being self.socket or an empty list if self.socket is not ready)
                ready_receive_sockets, _, _ = select.select([sock], [], [], 1)

                if not ready_receive_sockets:
                    log.debug("No advertisement received")
                    continue

                msg = sock.recv(1024).decode("utf-8")
                root = ET.fromstring(msg)
                if root.tag != "CANBeacon":
                    log.debug("Unexpected message received over UDP")
                    continue

                det_devs = []
                det_host = None
                det_port = None
                for child in root:
                    if child.tag == "Bus":
                        bus_name = child.attrib["name"]
                        det_devs.append(bus_name)
                    elif child.tag == "URL":
                        url = urlparselib.urlparse(child.text)
                        det_host = url.hostname
                        det_port = url.port

                if not det_devs:
                    log.debug(
                        "Got advertisement, but no SocketCAN devices advertised by socketcand"
                    )
                    continue

                if (det_host is None) or (det_port is None):
                    det_host = None
                    det_port = None
                    log.debug(
                        "Got advertisement, but no SocketCAN URL advertised by socketcand"
                    )
                    continue

                log.info(f"Found SocketCAN devices: {det_devs}")
                return [
                    {
                        "interface": "socketcand",
                        "host": det_host,
                        "port": det_port,
                        "channel": channel,
                    }
                    for channel in det_devs
                ]

            except ET.ParseError:
                log.debug("Unexpected message received over UDP")
                continue

            except Exception as exc:
                # something bad happened (e.g. the interface went down)
                log.error(f"Failed to detect beacon: {exc}  {traceback.format_exc()}")
                raise OSError(
                    f"Failed to detect beacon: {exc} {traceback.format_exc()}"
                ) from exc

        return []


def convert_ascii_message_to_can_message(ascii_msg: str) -> can.Message:
    if not ascii_msg.endswith(" >"):
        log.warning(f"Missing ending character in ascii message: {ascii_msg}")
        return None

    if ascii_msg.startswith("< frame "):
        # frame_string = ascii_msg.removeprefix("< frame ").removesuffix(" >")
        frame_string = ascii_msg[8:-2]
        parts = frame_string.split(" ", 3)
        can_id, timestamp = int(parts[0], 16), float(parts[1])
        is_ext = len(parts[0]) != 3

        data = bytearray.fromhex(parts[2])
        can_dlc = len(data)
        can_message = can.Message(
            timestamp=timestamp,
            arbitration_id=can_id,
            data=data,
            dlc=can_dlc,
            is_extended_id=is_ext,
            is_rx=True,
        )
        return can_message

    if ascii_msg.startswith("< error "):
        frame_string = ascii_msg[8:-2]
        parts = frame_string.split(" ", 3)
        can_id, timestamp = int(parts[0], 16), float(parts[1])
        is_ext = len(parts[0]) != 3

        # socketcand sends no data in the error message so we don't have information
        # about the error details, therefore the can frame is created with one
        # data byte set to zero
        data = bytearray([0])
        can_dlc = len(data)
        can_message = can.Message(
            timestamp=timestamp,
            arbitration_id=can_id & 0x1FFFFFFF,
            is_error_frame=True,
            data=data,
            dlc=can_dlc,
            is_extended_id=True,
            is_rx=True,
        )
        return can_message

    log.warning(f"Could not parse ascii message: {ascii_msg}")
    return None


def convert_can_message_to_ascii_message(can_message: can.Message) -> str:
    # Note: socketcan bus adds extended flag, remote_frame_flag & error_flag to id
    # not sure if that is necessary here
    can_id = can_message.arbitration_id
    if can_message.is_extended_id:
        can_id_string = f"{(can_id&0x1FFFFFFF):08X}"
    else:
        can_id_string = f"{(can_id&0x7FF):03X}"
    # Note: seems like we cannot add CANFD_BRS (bitrate_switch) and CANFD_ESI (error_state_indicator) flags
    data = can_message.data
    length = can_message.dlc
    bytes_string = " ".join(f"{x:x}" for x in data[0:length])
    return f"< send {can_id_string} {length:X} {bytes_string} >"


def connect_to_server(s, host, port):
    timeout_ms = 10000
    now = time.time() * 1000
    end_time = now + timeout_ms
    while now < end_time:
        try:
            s.connect((host, port))
            return
        except Exception as e:
            log.warning(f"Failed to connect to server: {type(e)} Message: {e}")
            now = time.time() * 1000
    raise TimeoutError(
        f"connect_to_server: Failed to connect server for {timeout_ms} ms"
    )


class SocketCanDaemonBus(can.BusABC):
    def __init__(self, channel, host, port, tcp_tune=False, can_filters=None, **kwargs):
        """Connects to a CAN bus served by socketcand.

        It implements :meth:`can.BusABC._detect_available_configs` to search for
        available interfaces.

        It will attempt to connect to the server for up to 10s, after which a
        TimeoutError exception will be thrown.

        If the handshake with the socketcand server fails, a CanError exception
        is thrown.

        :param channel:
            The can interface name served by socketcand.
            An example channel would be 'vcan0' or 'can0'.
        :param host:
            The host address of the socketcand server.
        :param port:
            The port of the socketcand server.
        :param tcp_tune:
            This tunes the TCP socket for low latency (TCP_NODELAY, and
            TCP_QUICKACK).
            This option is not available under windows.
        :param can_filters:
            See :meth:`can.BusABC.set_filters`.
        """
        self.__host = host
        self.__port = port

        self.__tcp_tune = tcp_tune
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        if self.__tcp_tune:
            if os.name == "nt":
                self.__tcp_tune = False
                log.warning("'tcp_tune' not available in Windows. Setting to False")
            else:
                self.__socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        self.__message_buffer = deque()
        self.__receive_buffer = ""  # i know string is not the most efficient here
        self.channel = channel
        self.channel_info = f"socketcand on {channel}@{host}:{port}"
        connect_to_server(self.__socket, self.__host, self.__port)
        self._expect_msg("< hi >")

        log.info(
            f"SocketCanDaemonBus: connected with address {self.__socket.getsockname()}"
        )
        self._tcp_send(f"< open {channel} >")
        self._expect_msg("< ok >")
        self._tcp_send("< rawmode >")
        self._expect_msg("< ok >")
        super().__init__(channel=channel, can_filters=can_filters, **kwargs)

    def _recv_internal(self, timeout):
        if len(self.__message_buffer) != 0:
            can_message = self.__message_buffer.popleft()
            return can_message, False

        try:
            # get all sockets that are ready (can be a list with a single value
            # being self.socket or an empty list if self.socket is not ready)
            ready_receive_sockets, _, _ = select.select(
                [self.__socket], [], [], timeout
            )
        except OSError as exc:
            # something bad happened (e.g. the interface went down)
            log.error(f"Failed to receive: {exc}")
            raise can.CanError(f"Failed to receive: {exc}") from exc

        try:
            if not ready_receive_sockets:
                # socket wasn't readable or timeout occurred
                log.debug("Socket not ready")
                return None, False

            ascii_msg = self.__socket.recv(1024).decode(
                "ascii"
            )  # may contain multiple messages
            if self.__tcp_tune:
                self.__socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_QUICKACK, 1)
            self.__receive_buffer += ascii_msg
            log.debug(f"Received Ascii Message: {ascii_msg}")
            buffer_view = self.__receive_buffer
            chars_processed_successfully = 0
            while True:
                if len(buffer_view) == 0:
                    break

                start = buffer_view.find("<")
                if start == -1:
                    log.warning(
                        f"Bad data: No opening < found => discarding entire buffer '{buffer_view}'"
                    )
                    chars_processed_successfully = len(self.__receive_buffer)
                    break
                end = buffer_view.find(">")
                if end == -1:
                    log.warning("Got incomplete message => waiting for more data")
                    if len(buffer_view) > 200:
                        log.warning(
                            "Incomplete message exceeds 200 chars => Discarding"
                        )
                        chars_processed_successfully = len(self.__receive_buffer)
                    break
                chars_processed_successfully += end + 1
                single_message = buffer_view[start : end + 1]
                parsed_can_message = convert_ascii_message_to_can_message(
                    single_message
                )
                if parsed_can_message is None:
                    log.warning(f"Invalid Frame: {single_message}")
                else:
                    parsed_can_message.channel = self.channel
                    self.__message_buffer.append(parsed_can_message)
                buffer_view = buffer_view[end + 1 :]

            self.__receive_buffer = self.__receive_buffer[chars_processed_successfully:]
            can_message = (
                None
                if len(self.__message_buffer) == 0
                else self.__message_buffer.popleft()
            )
            return can_message, False

        except Exception as exc:
            log.error(f"Failed to receive: {exc}  {traceback.format_exc()}")
            raise can.CanError(
                f"Failed to receive: {exc}  {traceback.format_exc()}"
            ) from exc

    def _tcp_send(self, msg: str):
        log.debug(f"Sending TCP Message: '{msg}'")
        self.__socket.sendall(msg.encode("ascii"))
        if self.__tcp_tune:
            self.__socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_QUICKACK, 1)

    def _expect_msg(self, msg):
        ascii_msg = self.__socket.recv(256).decode("ascii")
        if self.__tcp_tune:
            self.__socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_QUICKACK, 1)
        if not ascii_msg == msg:
            raise can.CanError(f"Expected '{msg}' got: '{ascii_msg}'")

    def send(self, msg, timeout=None):
        """Transmit a message to the CAN bus.

        :param msg: A message object.
        :param timeout: Ignored
        """
        ascii_msg = convert_can_message_to_ascii_message(msg)
        self._tcp_send(ascii_msg)

    def shutdown(self):
        """Stops all active periodic tasks and closes the socket."""
        super().shutdown()
        self.__socket.close()

    @staticmethod
    def _detect_available_configs() -> list[can.typechecking.AutoDetectedConfig]:
        try:
            return detect_beacon()
        except Exception as e:
            log.warning(f"Could not detect socketcand beacon: {e}")
            return []
