"""Modbus client async serial communication."""
from __future__ import annotations

import contextlib
import sys
import time
from collections.abc import Callable
from functools import partial

from pymodbus.client.base import ModbusBaseClient, ModbusBaseSyncClient
from pymodbus.exceptions import ConnectionException
from pymodbus.framer import FramerType
from pymodbus.logging import Log
from pymodbus.pdu import ModbusPDU
from pymodbus.transport import CommParams, CommType


with contextlib.suppress(ImportError):
    import serial


class AsyncModbusSerialClient(ModbusBaseClient):
    """**AsyncModbusSerialClient**.

    Fixed parameters:

    :param port: Serial port used for communication.

    Optional parameters:

    :param framer: Framer name, default FramerType.RTU
    :param baudrate: Bits per second.
    :param bytesize: Number of bits per byte 7-8.
    :param parity: 'E'ven, 'O'dd or 'N'one
    :param stopbits: Number of stop bits 1, 1.5, 2.
    :param handle_local_echo: Discard local echo from dongle.
    :param name: Set communication name, used in logging
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

        from pymodbus.client import AsyncModbusSerialClient

        async def run():
            client = AsyncModbusSerialClient("dev/serial0")

            await client.connect()
            ...
            client.close()

    Please refer to :ref:`Pymodbus internals` for advanced usage.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        port: str,
        *,
        framer: FramerType = FramerType.RTU,
        baudrate: int = 19200,
        bytesize: int = 8,
        parity: str = "N",
        stopbits: int = 1,
        handle_local_echo: bool = False,
        name: str = "comm",
        reconnect_delay: float = 0.1,
        reconnect_delay_max: float = 300,
        timeout: float = 3,
        retries: int = 3,
        trace_packet: Callable[[bool, bytes], bytes] | None = None,
        trace_pdu: Callable[[bool, ModbusPDU], ModbusPDU] | None = None,
        trace_connect: Callable[[bool], None] | None = None,
    ) -> None:
        """Initialize Asyncio Modbus Serial Client."""
        if "serial" not in sys.modules:  # pragma: no cover
            raise RuntimeError(
                "Serial client requires pyserial "
                'Please install with "pip install pyserial" and try again.'
            )
        if framer not in [FramerType.ASCII, FramerType.RTU]:
            raise TypeError("Only FramerType RTU/ASCII allowed.")
        self.comm_params = CommParams(
            comm_type=CommType.SERIAL,
            host=port,
            baudrate=baudrate,
            bytesize=bytesize,
            parity=parity,
            stopbits=stopbits,
            handle_local_echo=handle_local_echo,
            comm_name=name,
            reconnect_delay=reconnect_delay,
            reconnect_delay_max=reconnect_delay_max,
            timeout_connect=timeout,
        )
        ModbusBaseClient.__init__(
            self,
            framer,
            retries,
            self.comm_params,
            trace_packet,
            trace_pdu,
            trace_connect,
        )


class ModbusSerialClient(ModbusBaseSyncClient):
    """**ModbusSerialClient**.

    Fixed parameters:

    :param port: Serial port used for communication.

    Optional parameters:

    :param framer: Framer name, default FramerType.RTU
    :param baudrate: Bits per second.
    :param bytesize: Number of bits per byte 7-8.
    :param parity: 'E'ven, 'O'dd or 'N'one
    :param stopbits: Number of stop bits 0-2.
    :param handle_local_echo: Discard local echo from dongle.
    :param name: Set communication name, used in logging
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

        from pymodbus.client import ModbusSerialClient

        def run():
            client = ModbusSerialClient("dev/serial0")

            client.connect()
            ...
            client.close()

    Please refer to :ref:`Pymodbus internals` for advanced usage.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        port: str,
        *,
        framer: FramerType = FramerType.RTU,
        baudrate: int = 19200,
        bytesize: int = 8,
        parity: str = "N",
        stopbits: int = 1,
        handle_local_echo: bool = False,
        name: str = "comm",
        reconnect_delay: float = 0.1,
        reconnect_delay_max: float = 300,
        timeout: float = 3,
        retries: int = 3,
        trace_packet: Callable[[bool, bytes], bytes] | None = None,
        trace_pdu: Callable[[bool, ModbusPDU], ModbusPDU] | None = None,
        trace_connect: Callable[[bool], None] | None = None,
    ) -> None:
        """Initialize Modbus Serial Client."""
        if "serial" not in sys.modules:  # pragma: no cover
            raise RuntimeError(
                "Serial client requires pyserial "
                'Please install with "pip install pyserial" and try again.'
            )
        if framer not in [FramerType.ASCII, FramerType.RTU]:
            raise TypeError("Only RTU/ASCII allowed.")
        self.comm_params = CommParams(
            comm_type=CommType.SERIAL,
            host=port,
            baudrate=baudrate,
            bytesize=bytesize,
            parity=parity,
            stopbits=stopbits,
            handle_local_echo=handle_local_echo,
            comm_name=name,
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
        self.socket: serial.Serial | None = None
        self.last_frame_end = None
        self._t0 = float(1 + bytesize + stopbits) / baudrate

        # Check every 4 bytes / 2 registers if the reading is ready
        self._recv_interval = self._t0 * 4
        # Set a minimum of 1ms for high baudrates
        self._recv_interval = max(self._recv_interval, 0.001)

        self.inter_byte_timeout: float = 0
        self.silent_interval: float = 0
        if baudrate > 19200:
            self.silent_interval = 1.75 / 1000  # ms
        else:
            self.inter_byte_timeout = 1.5 * self._t0
            self.silent_interval = 3.5 * self._t0
        self.silent_interval = round(self.silent_interval, 6)

    @property
    def connected(self) -> bool:
        """Check if socket exists."""
        return self.socket is not None

    def connect(self) -> bool:
        """Connect to the modbus serial server."""
        if self.socket:
            return True
        try:
            self.socket = serial.serial_for_url(
                self.comm_params.host,
                timeout=self.comm_params.timeout_connect,
                bytesize=self.comm_params.bytesize,
                stopbits=self.comm_params.stopbits,
                baudrate=self.comm_params.baudrate,
                parity=self.comm_params.parity,
                exclusive=True,
            )
            self.socket.inter_byte_timeout = self.inter_byte_timeout
            self.last_frame_end = None
        # except serial.SerialException as msg:
        # pyserial raises undocumented exceptions like termios
        except Exception as msg:  # pylint: disable=broad-exception-caught
            Log.error("{}", msg)
            self.close()
        return self.socket is not None

    def close(self):
        """Close the underlying socket connection."""
        if self.socket:
            self.socket.close()
        self.socket = None

    def _in_waiting(self):
        """Return waiting bytes."""
        return getattr(self.socket, "in_waiting") if hasattr(self.socket, "in_waiting") else getattr(self.socket, "inWaiting")()

    def send(self, request: bytes, addr: tuple | None = None) -> int:
        """Send data on the underlying socket."""
        _ = addr
        if not self.socket:
            raise ConnectionException(str(self))
        if request:
            if waitingbytes := self._in_waiting():
                result = self.socket.read(waitingbytes)
                Log.warning("Cleanup recv buffer before send: {}", result, ":hex")
            if (size := self.socket.write(request)) is None:
                size = 0
            return size
        return 0

    def _wait_for_data(self) -> int:
        """Wait for data."""
        size = 0
        more_data = False
        condition = partial(
            lambda start, timeout: (time.time() - start) <= timeout,
            timeout=self.comm_params.timeout_connect,
        )
        start = time.time()
        while condition(start):
            available = self._in_waiting()
            if (more_data and not available) or (more_data and available == size):
                break
            if available and available != size:
                more_data = True
                size = available
            time.sleep(self._recv_interval)
        return size

    def recv(self, size: int | None) -> bytes:
        """Read data from the underlying descriptor."""
        if not self.socket:
            raise ConnectionException(str(self))
        if size is None:
            size = self._wait_for_data()
        if size > self._in_waiting():
            self._wait_for_data()
        result = self.socket.read(size)
        self.last_frame_end = round(time.time(), 6)
        return result

    def is_socket_open(self) -> bool:
        """Check if socket is open."""
        if self.socket:
            return self.socket.is_open
        return False

    def __repr__(self):
        """Return string representation."""
        return (
            f"<{self.__class__.__name__} at {hex(id(self))} socket={self.socket}, "
            f"framer={self.framer}, timeout={self.comm_params.timeout_connect}>"
        )
