"""asyncio serial support for modbus (based on pyserial)."""
from __future__ import annotations

import asyncio
import contextlib
import os
import sys


with contextlib.suppress(ImportError):
    import serial


class SerialTransport(asyncio.Transport):
    """An asyncio serial transport."""

    force_poll: bool = os.name == "nt"

    def __init__(self, loop, protocol, url, baudrate, bytesize, parity, stopbits, timeout) -> None:
        """Initialize."""
        super().__init__()
        if "serial" not in sys.modules:
            raise RuntimeError(
                "Serial client requires pyserial "
                'Please install with "pip install pyserial" and try again.'
            )
        self.async_loop = loop
        self.intern_protocol: asyncio.BaseProtocol = protocol
        self.sync_serial = serial.serial_for_url(url, exclusive=True,
            baudrate=baudrate, bytesize=bytesize, parity=parity, stopbits=stopbits, timeout=timeout
)
        self.intern_write_buffer: list[bytes] = []
        self.poll_task: asyncio.Task | None = None
        self._poll_wait_time = 0.0005
        self.sync_serial.timeout = 0
        self.sync_serial.write_timeout = 0

    def setup(self) -> None:
        """Prepare to read/write."""
        if self.force_poll:
            self.poll_task = asyncio.create_task(self.polling_task())
            self.poll_task.set_name("SerialTransport poll")
        else:
            self.async_loop.add_reader(self.sync_serial.fileno(), self.intern_read_ready)
        self.async_loop.call_soon(self.intern_protocol.connection_made, self)

    def close(self, exc: Exception | None = None) -> None:
        """Close the transport gracefully."""
        if not self.sync_serial:
            return
        self.flush()
        if self.poll_task:
            self.poll_task.cancel()
            self.poll_task = None
        else:
            self.async_loop.remove_reader(self.sync_serial.fileno())
            self.async_loop.remove_writer(self.sync_serial.fileno())
        self.sync_serial.close()
        self.sync_serial = None  # type: ignore[assignment]
        if exc:
            with contextlib.suppress(Exception):
                self.intern_protocol.connection_lost(exc)

    def write(self, data) -> None:
        """Write some data to the transport."""
        self.intern_write_buffer.append(data)
        if not self.force_poll:
            self.async_loop.add_writer(self.sync_serial.fileno(), self.intern_write_ready)

    def flush(self) -> None:
        """Clear output buffer and stops any more data being written."""
        if not self.poll_task:
            self.async_loop.remove_writer(self.sync_serial.fileno())
        self.intern_write_buffer.clear()

    # ------------------------------------------------
    # Dummy methods needed to please asyncio.Transport.
    # ------------------------------------------------
    @property
    def loop(self):
        """Return asyncio event loop."""
        return self.async_loop

    def get_protocol(self) -> asyncio.BaseProtocol:
        """Return protocol."""
        return self.intern_protocol

    def set_protocol(self, protocol: asyncio.BaseProtocol) -> None:
        """Set protocol."""
        self.intern_protocol = protocol

    def get_write_buffer_limits(self) -> tuple[int, int]:
        """Return buffer sizes."""
        return (1, 1024)

    def can_write_eof(self):
        """Return Serial do not support end-of-file."""
        return False

    def write_eof(self):
        """Write end of file marker."""

    def set_write_buffer_limits(self, high=None, low=None):
        """Set the high- and low-water limits for write flow control."""

    def get_write_buffer_size(self):
        """Return The number of bytes in the write buffer."""
        return len(self.intern_write_buffer)

    def is_reading(self) -> bool:
        """Return true if read is active."""
        return True

    def pause_reading(self):
        """Pause receiver."""

    def resume_reading(self):
        """Resume receiver."""

    def is_closing(self):
        """Return True if the transport is closing or closed."""
        return False

    def abort(self) -> None:
        """Alias for closing the connection."""
        self.close()

    # ------------------------------------------------

    def intern_read_ready(self) -> None:
        """Test if there are data waiting."""
        try:
            if data := self.sync_serial.read(1024):
                self.intern_protocol.data_received(data)  # type: ignore[attr-defined]
        except serial.SerialException as exc:
            self.close(exc=exc)

    def intern_write_ready(self) -> None:
        """Asynchronously write buffered data."""
        data = b"".join(self.intern_write_buffer)
        try:
            if (nlen := self.sync_serial.write(data)) and nlen < len(data):
                self.intern_write_buffer = [data[nlen:]]
                if not self.poll_task:
                    self.async_loop.add_writer(
                        self.sync_serial.fileno(), self.intern_write_ready
                    )
                return
            self.flush()
        except (BlockingIOError, InterruptedError):
            return
        except serial.SerialException as exc:
            self.close(exc=exc)

    async def polling_task(self):
        """Poll and try to read/write."""
        while self.sync_serial:
            await asyncio.sleep(self._poll_wait_time)
            while self.intern_write_buffer:
                self.intern_write_ready()
            if self.sync_serial.in_waiting:
                self.intern_read_ready()

async def create_serial_connection(
    loop, protocol_factory, url,
    baudrate=None,
    bytesize=None,
    parity=None,
    stopbits=None,
    timeout=None,
) -> tuple[asyncio.Transport, asyncio.BaseProtocol]:
    """Create a connection to a new serial port instance."""
    protocol = protocol_factory()
    transport = SerialTransport(loop, protocol, url,
                    baudrate,
                    bytesize,
                    parity,
                    stopbits,
                    timeout)
    loop.call_soon(transport.setup)
    return transport, protocol
