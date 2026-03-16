"""Modbus client async UDP communication."""
from __future__ import annotations

import socket
import time
from collections.abc import Callable

from pymodbus.client.base import ModbusBaseClient, ModbusBaseSyncClient
from pymodbus.exceptions import ConnectionException
from pymodbus.framer import FramerType
from pymodbus.logging import Log
from pymodbus.pdu import ModbusPDU
from pymodbus.transport import CommParams, CommType


DGRAM_TYPE = socket.SOCK_DGRAM


class AsyncModbusUdpClient(ModbusBaseClient):
    """**AsyncModbusUdpClient**.

    Fixed parameters:

    :param host: Host IP address or host name

    Optional parameters:

    :param framer: Framer name, default FramerType.SOCKET
    :param port: Port used for communication.
    :param name: Set communication name, used in logging
    :param source_address: source address of client,
    :param reconnect_delay: Minimum delay in seconds.milliseconds before reconnecting.
    :param reconnect_delay_max: Maximum delay in seconds.milliseconds before reconnecting.
    :param timeout: Timeout for connecting and receiving data, in seconds.
    :param retries: Max number of retries per request.
    :param trace_packet: Called with bytestream received/to be sent
    :param trace_pdu: Called with PDU received/to be sent
    :param trace_connect: Called when connected/disconnected

    .. tip::
        The trace methods allow to modify the datastream/pdu !

    .. tip::
        **reconnect_delay** doubles automatically with each unsuccessful connect, from
        **reconnect_delay** to **reconnect_delay_max**.
        Set `reconnect_delay=0` to avoid automatic reconnection.

    Example::

        from pymodbus.client import AsyncModbusUdpClient

        async def run():
            client = AsyncModbusUdpClient("localhost")

            await client.connect()
            ...
            client.close()

    Please refer to :ref:`Pymodbus internals` for advanced usage.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        host: str,
        *,
        framer: FramerType = FramerType.SOCKET,
        port: int = 502,
        name: str = "comm",
        source_address: tuple[str, int] | None = None,
        reconnect_delay: float = 0.1,
        reconnect_delay_max: float = 300,
        timeout: float = 3,
        retries: int = 3,
        trace_packet: Callable[[bool, bytes], bytes] | None = None,
        trace_pdu: Callable[[bool, ModbusPDU], ModbusPDU] | None = None,
        trace_connect: Callable[[bool], None] | None = None,
    ) -> None:
        """Initialize Asyncio Modbus UDP Client."""
        self.comm_params = CommParams(
            comm_type=CommType.UDP,
            host=host,
            port=port,
            comm_name=name,
            source_address=source_address,
            reconnect_delay=reconnect_delay,
            reconnect_delay_max=reconnect_delay_max,
            timeout_connect=timeout,
        )
        if framer not in [FramerType.SOCKET, FramerType.RTU, FramerType.ASCII]:
            raise TypeError("Only FramerType SOCKET/RTU/ASCII allowed.")
        ModbusBaseClient.__init__(
            self,
            framer,
            retries,
            self.comm_params,
            trace_packet,
            trace_pdu,
            trace_connect,
        )
        self.source_address = source_address


class ModbusUdpClient(ModbusBaseSyncClient):
    """**ModbusUdpClient**.

    Fixed parameters:

    :param host: Host IP address or host name

    Optional parameters:

    :param framer: Framer name, default FramerType.SOCKET
    :param port: Port used for communication.
    :param name: Set communication name, used in logging
    :param source_address: source address of client,
    :param reconnect_delay: Not used in the sync client
    :param reconnect_delay_max: Not used in the sync client
    :param timeout: Timeout for connecting and receiving data, in seconds.
    :param retries: Max number of retries per request.
    :param trace_packet: Called with bytestream received/to be sent
    :param trace_pdu: Called with PDU received/to be sent
    :param trace_connect: Called when connected/disconnected

    .. tip::
        The trace methods allow to modify the datastream/pdu !

    Example::

        from pymodbus.client import ModbusUdpClient

        async def run():
            client = ModbusUdpClient("localhost")

            client.connect()
            ...
            client.close()

    Please refer to :ref:`Pymodbus internals` for advanced usage.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        host: str,
        *,
        framer: FramerType = FramerType.SOCKET,
        port: int = 502,
        name: str = "comm",
        source_address: tuple[str, int] | None = None,
        reconnect_delay: float = 0.1,
        reconnect_delay_max: float = 300,
        timeout: float = 3,
        retries: int = 3,
        trace_packet: Callable[[bool, bytes], bytes] | None = None,
        trace_pdu: Callable[[bool, ModbusPDU], ModbusPDU] | None = None,
        trace_connect: Callable[[bool], None] | None = None,
    ) -> None:
        """Initialize Modbus UDP Client."""
        if framer not in [FramerType.SOCKET, FramerType.RTU, FramerType.ASCII]:
            raise TypeError("Only FramerType SOCKET/RTU/ASCII allowed.")
        self.comm_params = CommParams(
            comm_type=CommType.UDP,
            host=host,
            port=port,
            comm_name=name,
            source_address=source_address,
            reconnect_delay=reconnect_delay,
            reconnect_delay_max=reconnect_delay_max,
            timeout_connect=timeout,
        )
        super().__init__(
            framer,
            retries,
            self.comm_params,
            trace_packet,
            trace_pdu,
            trace_connect,
        )
        self.socket: socket.socket | None = None

    @property
    def connected(self) -> bool:
        """Connect internal."""
        return self.socket is not None

    def connect(self):
        """Connect to the modbus udp server.

        :meta private:
        """
        if self.socket:
            return True
        try:
            self.socket = socket.socket(-1, socket.SOCK_DGRAM)
            self.socket.settimeout(self.comm_params.timeout_connect)
        except OSError as exc:
            Log.error("Unable to create udp socket {}", exc)
            self.close()
        return self.socket is not None

    def close(self):
        """Close the underlying socket connection.

        :meta private:
        """
        self.socket = None

    def send(self, request: bytes, addr: tuple | None = None) -> int:
        """Send data on the underlying socket.

        :meta private:
        """
        _ = addr
        if not self.socket:
            raise ConnectionException(str(self))
        if request:
            return self.socket.sendto(
                request, (self.comm_params.host, self.comm_params.port)
            )
        return 0

    def recv(self, size: int | None) -> bytes:
        """Read data from the underlying descriptor.

        :meta private:
        """
        if not self.socket:
            raise ConnectionException(str(self))
        if size is None:
            size = 4096
        data = self.socket.recvfrom(size)[0]
        self.last_frame_end = round(time.time(), 6)
        return data

    def is_socket_open(self):
        """Check if socket is open.

        :meta private:
        """
        return True

    def __repr__(self):
        """Return string representation."""
        return (
            f"<{self.__class__.__name__} at {hex(id(self))} socket={self.socket}, "
            f"ipaddr={self.comm_params.host}, port={self.comm_params.port}, timeout={self.comm_params.timeout_connect}>"
        )
