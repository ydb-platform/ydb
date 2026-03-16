"""ModbusProtocol layer.

Contains pure transport methods needed to
- connect/listen,
- send/receive
- close/abort connections
for unix socket, tcp, tls and serial communications as well as a special
null modem option.

Contains high level methods like reconnect.

All transport differences are handled in transport, providing a unified
interface to upper layers.

Host/Port/SourceAddress explanation:
- SourceAddress (host, port):
- server (host, port): Listen on host:port
- server serial (comm_port, _): comm_port is device string
- client (host, port): Bind host:port to interface
- client serial: not used
- Host
- server: not used
- client: remote host to connect to (as host:port)
- client serial: host is comm_port device string
- Port
- server: not used
- client: remote port to connect to (as host:port)
- client serial: no used

Pyserial allow the comm_port to be a socket e.g. "socket://localhost:502",
this allows serial clients to connect to a tcp server with RTU framer.

Pymodbus allows this format for both server and client.
For clients the string is passed to pyserial,
but for servers it is used to start a modbus tcp server.
This allows for serial testing, without a serial cable.

Pymodbus offers nullmodem for clients/servers running in the same process
if <host> is set to NULLMODEM_HOST it will be automatically invoked.
This allows testing without actual network traffic and is a lot faster.

Class NullModem is a asyncio transport class,
that replaces the socket class or pyserial.

The class is designed to take care of differences between the different
transport mediums, and provide a neutral interface for the upper layers.
It basically provides a pipe, without caring about the actual data content.
"""
from __future__ import annotations

import asyncio
import dataclasses
import ssl
from abc import abstractmethod
from collections.abc import Callable, Coroutine
from contextlib import suppress
from enum import Enum
from functools import partial
from typing import Any

from pymodbus.logging import Log
from pymodbus.transport.serialtransport import create_serial_connection


NULLMODEM_HOST = "__pymodbus_nullmodem"


class CommType(int, Enum):
    """Type of transport."""

    TCP = 1
    TLS = 2
    UDP = 3
    SERIAL = 4


@dataclasses.dataclass
class CommParams:
    """Parameter class."""

    # generic
    comm_name: str | None = None
    comm_type: CommType | None = None
    reconnect_delay: float | None = None
    reconnect_delay_max: float = 0.0
    timeout_connect: float = 0.0
    host: str = "localhost" # On some machines this will now be ::1
    port: int = 0
    source_address: tuple[str, int] | None = None
    handle_local_echo: bool = False

    # tls
    sslctx: ssl.SSLContext | None = None

    # serial
    baudrate: int = -1
    bytesize: int = -1
    parity: str = ''
    stopbits: int = -1

    @classmethod
    def generate_ssl(
        cls,
        is_server: bool,
        certfile: str | None = None,
        keyfile: str | None = None,
        password: str | None = None,
        sslctx: ssl.SSLContext | None = None,
    ) -> ssl.SSLContext:
        """Generate sslctx from cert/key/password.

        MODBUS/TCP Security Protocol Specification demands TLSv2 at least
        """
        if sslctx:
            return sslctx
        new_sslctx = ssl.SSLContext(
            ssl.PROTOCOL_TLS_SERVER if is_server else ssl.PROTOCOL_TLS_CLIENT
        )
        new_sslctx.check_hostname = False
        new_sslctx.verify_mode = ssl.CERT_NONE
        new_sslctx.minimum_version = ssl.TLSVersion.TLSv1_2
        new_sslctx.maximum_version = ssl.TLSVersion.TLSv1_3
        if certfile:
            new_sslctx.load_cert_chain(
                certfile=certfile, keyfile=keyfile, password=password
            )
        return new_sslctx

    def copy(self) -> CommParams:
        """Create a copy."""
        return dataclasses.replace(self)


class ModbusProtocol(asyncio.BaseProtocol):
    """Protocol layer including transport."""

    def __init__(
        self,
        params: CommParams,
        is_server: bool,
        is_sync: bool = False
    ) -> None:
        """Initialize a transport instance.

        :param params: parameter dataclass
        :param is_server: true if object act as a server (listen/connect)
        :param is_sync: true if used with sync client
        """
        self.comm_params = params.copy()
        self.is_server = is_server
        self.is_closing = False

        self.transport: asyncio.BaseTransport = None  # type: ignore[assignment]
        self.recv_buffer: bytes = b""
        self.call_create: Callable[[], Coroutine[Any, Any, Any]] = None  # type: ignore[assignment]
        self.reconnect_task: asyncio.Task | None = None
        self.listener: ModbusProtocol | None = None
        self.is_listener: bool = False
        self.active_connections: dict[str, ModbusProtocol] = {}
        self.unique_id: str = str(id(self))
        self.reconnect_delay_current = 0.0
        self.sent_buffer: bytes = b""
        self.loop: asyncio.AbstractEventLoop
        if is_sync:
            return
        self.loop = asyncio.get_running_loop()
        if self.is_server:
            if self.comm_params.source_address is not None:
                host = self.comm_params.source_address[0]
                port = int(self.comm_params.source_address[1])
            else:
                # This behaviour isn't quite right.
                # It listens on any IPv4 address rather than the more natural default of any address (v6 or v4).
                host = "0.0.0.0" # Any IPv4 host
                port = 502 # Server will listen on standard modbus port
        else:
            host = self.comm_params.host
            port = int(self.comm_params.port)
        if self.comm_params.comm_type == CommType.SERIAL and NULLMODEM_HOST in host:
            host, port = NULLMODEM_HOST, int(host[9:].split(":")[1])
        if host == NULLMODEM_HOST:
            self.call_create = partial(self.create_nullmodem, port)
            return
        if (
            self.comm_params.comm_type == CommType.SERIAL
            and self.is_server
            and host.startswith("socket")
        ):
            # format is "socket://<host>:port"
            self.comm_params.comm_type = CommType.TCP
            parts = host.split(":")
            host, port = parts[1][2:], int(parts[2])
        self.init_setup_connect_listen(host, port)

    def init_setup_connect_listen(self, host: str, port: int) -> None:
        """Handle connect/listen handler."""
        if self.comm_params.comm_type == CommType.SERIAL:
            self.call_create = partial(create_serial_connection,
                self.loop,
                self.handle_new_connection,
                host,
                baudrate=self.comm_params.baudrate,
                bytesize=self.comm_params.bytesize,
                parity=self.comm_params.parity,
                stopbits=self.comm_params.stopbits,
                timeout=self.comm_params.timeout_connect,
            )
            return
        if self.comm_params.comm_type == CommType.UDP:
            if self.is_server:
                self.call_create = partial(self.loop.create_datagram_endpoint,
                    self.handle_new_connection,
                    local_addr=(host, port),
                )
            else:
                self.call_create = partial(self.loop.create_datagram_endpoint,
                    self.handle_new_connection,
                    remote_addr=(host, port),
                )
            return
        # TLS and TCP
        if self.is_server:
            self.call_create = partial(self.loop.create_server,
                self.handle_new_connection,
                host,
                port,
                ssl=self.comm_params.sslctx,
                reuse_address=True,
                start_serving=True,
            )
        else:
            self.call_create = partial(self.loop.create_connection,
                self.handle_new_connection,
                host,
                port,
                local_addr=self.comm_params.source_address,
                ssl=self.comm_params.sslctx,
            )

    async def connect(self) -> bool:
        """Handle generic connect and call on to specific transport connect."""
        Log.debug("Connecting {}", self.comm_params.comm_name)
        self.is_closing = False
        try:
            self.transport, _protocol = await asyncio.wait_for(
                self.call_create(),
                timeout=self.comm_params.timeout_connect,
            )
        except (asyncio.TimeoutError, OSError) as exc:  # pylint: disable=overlapping-except
            Log.warning("Failed to connect {}", exc)
            return False
        return bool(self.transport)

    async def listen(self) -> bool:
        """Handle generic listen and call on to specific transport listen."""
        Log.debug("Awaiting connections {}", self.comm_params.comm_name)
        self.is_closing = False
        self.is_listener = True
        try:
            self.transport = await self.call_create()
            if isinstance(self.transport, tuple):
                self.transport = self.transport[0]
        except OSError as exc:
            Log.warning("Failed to start server {}", exc)
            self.__close()
            return False
        return True

    # --------------------------------------- #
    # ModbusProtocol asyncio standard methods #
    # --------------------------------------- #
    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """Call from asyncio, when a connection is made.

        :param transport: socket etc. representing the connection.
        """
        Log.debug("Connected to {}", self.comm_params.comm_name)
        self.transport = transport
        self.reset_delay()
        self.callback_connected()

    def connection_lost(self, reason: Exception | None) -> None:
        """Call from asyncio, when the connection is lost or closed.

        :param reason: None or an exception object
        """
        if not self.transport or self.is_closing:
            return
        Log.debug("Connection lost {} due to {}", self.comm_params.comm_name, reason)
        self.__close()
        if self.is_listener:
            self.reconnect_task = asyncio.create_task(self.do_relisten())
            self.reconnect_task.set_name("transport relisten")
        elif not self.listener and self.comm_params.reconnect_delay:
            self.reconnect_task = asyncio.create_task(self.do_reconnect())
            self.reconnect_task.set_name("transport reconnect")
        self.callback_disconnected(reason)

    def data_received(self, data: bytes) -> None:
        """Call when some data is received.

        :param data: non-empty bytes object with incoming data.
        """
        self.datagram_received(data, None)

    def datagram_received(self, data: bytes, addr: tuple | None) -> None:
        """Receive datagram (UDP connections)."""
        if self.comm_params.handle_local_echo and self.sent_buffer:
            if data.startswith(self.sent_buffer):
                Log.debug(
                    "recv skipping (local_echo): {} addr={}",
                    self.sent_buffer,
                    ":hex",
                    addr,
                )
                data = data[len(self.sent_buffer) :]
                self.sent_buffer = b""
            elif self.sent_buffer.startswith(data):
                Log.debug(
                    "recv skipping (partial local_echo): {} addr={}", data, ":hex", addr
                )
                self.sent_buffer = self.sent_buffer[len(data) :]
                return
            else:
                Log.debug("did not receive local echo: {} addr={}", data, ":hex", addr)
                self.sent_buffer = b""
            if not data:
                return
        Log.debug(
            "recv: {} old_data: {} addr={}",
            data,
            ":hex",
            self.recv_buffer,
            ":hex",
            addr,
        )
        self.recv_buffer += data
        cut = self.callback_data(self.recv_buffer, addr=addr)
        self.recv_buffer = self.recv_buffer[cut:]
        if self.recv_buffer:
            Log.debug(
                "recv, unused data waiting for next packet: {}",
                self.recv_buffer,
                ":hex",
            )

    def eof_received(self) -> None:
        """Accept other end terminates connection."""
        Log.debug("-> transport: received eof")

    def error_received(self, exc):
        """Get error detected in UDP."""
        Log.debug("-> error_received {}", exc)

    # --------- #
    # callbacks #
    # --------- #
    @abstractmethod
    def callback_new_connection(self) -> ModbusProtocol:
        """Call when listener receive new connection request."""

    @abstractmethod
    def callback_connected(self) -> None:
        """Call when connection is succcesfull."""

    @abstractmethod
    def callback_disconnected(self, exc: Exception | None) -> None:
        """Call when connection is lost."""

    @abstractmethod
    def callback_data(self, data: bytes, addr: tuple | None = None) -> int:
        """Handle received data."""

    # ----------------------------------- #
    # Helper methods for external classes #
    # ----------------------------------- #
    def send(self, data: bytes, addr: tuple | None = None) -> None:
        """Send modbus message.

        :param data: non-empty bytes object with data to send.
        :param addr: optional addr, only used for UDP server.
        """
        if not self.transport:
            Log.error("Cancel send, because not connected!")
            return
        Log.debug("send: {}", data, ":hex")
        self.recv_buffer = b""
        if self.comm_params.handle_local_echo:
            self.sent_buffer += data
        if self.comm_params.comm_type == CommType.UDP:
            if addr:
                self.transport.sendto(data, addr=addr)  # type: ignore[attr-defined]
            else:
                self.transport.sendto(data)  # type: ignore[attr-defined]
        else:
            self.transport.write(data)  # type: ignore[attr-defined]

    def __close(self, reconnect: bool = False) -> None:
        """Close connection (internal).

        :param reconnect: (default false), try to reconnect
        """
        if self.transport:
            self.transport.close()
            self.transport = None  # type: ignore[assignment]
        self.recv_buffer = b""
        if self.is_listener:
            for _key, value in self.active_connections.items():
                value.listener = None
                value.callback_disconnected(None)
                value.close()
            self.active_connections = {}
            return
        if not reconnect and self.reconnect_task:
            self.reconnect_task.cancel()
            self.reconnect_task = None
            self.reconnect_delay_current = 0.0
        if self.listener:
            self.listener.active_connections.pop(self.unique_id)

    def close(self, reconnect: bool = False) -> None:
        """Close connection (external).

        :param reconnect: (default false), try to reconnect
        """
        if self.is_closing:
            return
        self.is_closing = True
        self.__close(reconnect=reconnect)

    def reset_delay(self) -> None:
        """Reset wait time before next reconnect to minimal period."""
        self.reconnect_delay_current = self.comm_params.reconnect_delay or 0.0

    def is_active(self) -> bool:
        """Return true if connected/listening."""
        return bool(self.transport)

    # ---------------- #
    # Internal methods #
    # ---------------- #
    async def create_nullmodem(
        self, port
    ) -> tuple[asyncio.Transport, asyncio.BaseProtocol]:
        """Bypass create_ and use null modem."""
        if self.is_server:
            # Listener object
            self.transport = NullModem.set_listener(port, self)
            return self.transport, self

        # connect object
        return NullModem.set_connection(port, self)

    def handle_new_connection(self) -> ModbusProtocol:
        """Handle incoming connect."""
        if not self.is_server:
            # Clients reuse the same object.
            return self

        new_protocol = self.callback_new_connection()
        self.active_connections[new_protocol.unique_id] = new_protocol
        new_protocol.listener = self
        return new_protocol

    async def do_relisten(self) -> None:
        """Handle reconnect as a task."""
        try:
            Log.debug("Wait 1s before reopening listener.")
            await asyncio.sleep(1)
            await self.listen()
        except asyncio.CancelledError:
            pass
        self.reconnect_task = None

    async def do_reconnect(self) -> None:
        """Handle reconnect as a task."""
        try:
            self.reconnect_delay_current = self.comm_params.reconnect_delay or 0.0
            while True:
                Log.debug(
                    "Wait {} {} ms before reconnecting.",
                    self.comm_params.comm_name,
                    self.reconnect_delay_current * 1000,
                )
                await asyncio.sleep(self.reconnect_delay_current)
                if await self.connect():
                    break
                self.reconnect_delay_current = min(
                    2 * self.reconnect_delay_current,
                    self.comm_params.reconnect_delay_max,
                )
        except asyncio.CancelledError:
            pass
        self.reconnect_task = None

    # ----------------- #
    # The magic methods #
    # ----------------- #
    async def __aenter__(self) -> ModbusProtocol:
        """Implement the client with async enter block."""
        return self

    async def __aexit__(self, _class, _value, _traceback) -> None:
        """Implement the client with async exit block."""
        self.close()

    def __str__(self) -> str:
        """Build a string representation of the connection."""
        return f"{self.__class__.__name__}({self.comm_params.comm_name})"


class NullModem(asyncio.DatagramTransport, asyncio.Transport):
    """ModbusProtocol layer.

    Contains methods to act as a null modem between 2 objects.
    (Allowing tests to be shortcut without actual network calls)
    """

    listeners: dict[int, ModbusProtocol] = {}
    connections: dict[NullModem, int] = {}

    def __init__(self, protocol: ModbusProtocol, listen: int | None = None) -> None:
        """Create half part of null modem."""
        asyncio.DatagramTransport.__init__(self)
        asyncio.Transport.__init__(self)
        self.protocol: ModbusProtocol = protocol
        self.other_modem: NullModem = None  # type: ignore[assignment]
        self.listen = listen
        self.manipulator: Callable[[bytes], list[bytes]] | None = None
        self._is_closing = False

    # -------------------------- #
    # external nullmodem methods #
    # -------------------------- #
    @classmethod
    def set_listener(cls, port: int, parent: ModbusProtocol) -> NullModem:
        """Register listener."""
        if port in cls.listeners:
            raise AssertionError(f"Port {port} already listening !")
        cls.listeners[port] = parent
        return NullModem(parent, listen=port)

    @classmethod
    def set_connection(
        cls, port: int, parent: ModbusProtocol
    ) -> tuple[NullModem, ModbusProtocol]:
        """Connect to listener."""
        if port not in cls.listeners:
            raise asyncio.TimeoutError(f"Port {port} not being listened on !")

        client_protocol = parent.handle_new_connection()
        server_protocol = NullModem.listeners[port].handle_new_connection()
        client_transport = NullModem(client_protocol)
        server_transport = NullModem(server_protocol)
        cls.connections[client_transport] = port
        cls.connections[server_transport] = -port
        client_transport.other_modem = server_transport
        server_transport.other_modem = client_transport
        client_protocol.connection_made(client_transport)
        server_protocol.connection_made(server_transport)
        return client_transport, client_protocol

    def set_manipulator(self, function: Callable[[bytes], list[bytes]]) -> None:
        """Register a manipulator."""
        self.manipulator = function

    @classmethod
    def is_dirty(cls):
        """Check if everything is closed."""
        dirty = False
        if cls.connections:
            Log.error(
                "NullModem_FATAL missing close on port {} connect()",
                [str(key) for key in cls.connections.values()],
            )
            dirty = True
        if cls.listeners:
            Log.error(
                "NullModem_FATAL missing close on port {} listen()",
                [str(value) for value in cls.listeners],
            )
            dirty = True
        return dirty

    # ---------------- #
    # external methods #
    # ---------------- #

    def close(self) -> None:
        """Close null modem."""
        if self._is_closing:
            return
        self._is_closing = True
        if self.listen:
            del self.listeners[self.listen]
            return
        if self.connections:
            with suppress(KeyError):
                del self.connections[self]
        if self.other_modem:
            self.other_modem.other_modem = None  # type: ignore[assignment]
            self.other_modem.close()
            self.other_modem = None  # type: ignore[assignment]
        if self.protocol:
            self.protocol.connection_lost(None)

    def sendto(self, data: bytes, _addr: Any = None) -> None:
        """Send datagrame."""
        self.write(data)

    def write(self, data: bytes) -> None:
        """Send data."""
        if not self.manipulator:
            if self.other_modem:
                self.other_modem.protocol.data_received(data)
            return
        data_manipulated = self.manipulator(data)
        for part in data_manipulated:
            self.other_modem.protocol.data_received(part)

    # ------------- #
    # Dummy methods #
    # ------------- #
    def abort(self) -> None:
        """Alias for closing the connection."""
        self.close()

    def can_write_eof(self) -> bool:
        """Allow to write eof."""
        return False

    def get_write_buffer_size(self) -> int:
        """Set write limit."""
        return 1024

    def get_write_buffer_limits(self) -> tuple[int, int]:
        """Set flush limits."""
        return (1, 1024)

    def set_write_buffer_limits(
        self, high: int | None = None, low: int | None = None
    ) -> None:
        """Set flush limits."""

    def write_eof(self) -> None:
        """Write eof."""

    def get_protocol(self) -> ModbusProtocol | asyncio.BaseProtocol:
        """Return current protocol."""
        return self.protocol

    def set_protocol(self, protocol: asyncio.BaseProtocol) -> None:
        """Set current protocol."""

    def is_closing(self) -> bool:
        """Return true if closing."""
        return self._is_closing

    def is_reading(self) -> bool:
        """Return true if read is active."""
        return True

    def pause_reading(self):
        """Pause receiver."""

    def resume_reading(self):
        """Resume receiver."""
