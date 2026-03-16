r"""
Synchronous (blocking) interface for telnetlib3.

This module provides a non-asyncio interface that wraps the async
telnetlib3 implementation. The asyncio event loop runs in a background
thread, and blocking methods wait on thread-safe futures.

Example client usage::

    from telnetlib3.sync import TelnetConnection

    with TelnetConnection('localhost', 6023) as conn:
        conn.write('hello\r\n')
        print(conn.readline())

Example server usage::

    from telnetlib3.sync import BlockingTelnetServer
    import threading

    def handler(conn):
        conn.write('Hello!\r\n')
        while line := conn.readline():
            conn.write(f'Echo: {line}')

    server = BlockingTelnetServer('localhost', 6023, handler=handler)
    server.serve_forever()
"""

from __future__ import annotations

# std imports
import time
import queue
import asyncio
import threading
import concurrent.futures
from typing import Any, Union, Callable, Optional

# local
# Import from submodules to avoid cyclic import
from .client import open_connection as _open_connection
from .server import Server
from .server import create_server as _create_server
from .stream_reader import TelnetReader
from .stream_writer import TelnetWriter

__all__ = ("TelnetConnection", "BlockingTelnetServer", "ServerConnection")


class TelnetConnection:
    r"""
    Blocking telnet client connection.

    Wraps async ``telnetlib3.open_connection()`` with blocking methods.
    The asyncio event loop runs in a daemon thread.

    :param host: Remote server hostname or IP address.
    :param port: Remote server port (default 23).
    :param timeout: Default timeout for operations in seconds.
    :param encoding: Character encoding (default 'utf8').
    :param connect_timeout: Timeout in seconds for the TCP connection to be
        established.  Passed to ``telnetlib3.open_connection()``.
    :param kwargs: Additional arguments passed to ``telnetlib3.open_connection()``.

    Example::

        with TelnetConnection('localhost', 6023) as conn:
            conn.write('hello\r\n')
            response = conn.readline()
    """

    def __init__(
        self,
        host: str,
        port: int = 23,
        timeout: Optional[float] = None,
        encoding: str = "utf8",
        **kwargs: Any,
    ):
        """Initialize connection parameters without connecting."""
        self._host = host
        self._port = port
        self._timeout = timeout
        self._encoding = encoding
        self._kwargs = kwargs

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._reader: Optional[TelnetReader] = None
        self._writer: Optional[TelnetWriter] = None
        self._connected = threading.Event()
        self._closed = False

    def connect(self) -> None:
        """
        Establish connection to the server.

        Blocks until connected or timeout expires.

        :raises RuntimeError: If already connected.
        :raises TimeoutError: If connection times out.
        :raises ConnectionError: If connection fails.
        :raises Exception: If connection fails for other reasons.
        """
        if self._thread is not None:
            raise RuntimeError("Already connected")

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

        future = asyncio.run_coroutine_threadsafe(self._async_connect(), self._loop)
        try:
            future.result(timeout=self._timeout)
        except concurrent.futures.TimeoutError as exc:
            self._cleanup()
            raise TimeoutError("Connection timed out") from exc
        except Exception:
            self._cleanup()
            raise

    def _run_loop(self) -> None:
        """Run event loop in background thread."""
        assert self._loop is not None
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    async def _async_connect(self) -> None:
        """Async connection coroutine."""
        kwargs = dict(self._kwargs)
        # Default to TelnetClient (not TelnetTerminalClient) -- the blocking API
        # is programmatic, not a terminal app, so it should use the cols/rows
        # parameters rather than reading the real terminal size.
        if "client_factory" not in kwargs:
            from .client import TelnetClient

            kwargs["client_factory"] = TelnetClient
        self._reader, self._writer = await _open_connection(
            self._host, self._port, encoding=self._encoding, **kwargs
        )
        self._connected.set()

    def _ensure_connected(self) -> None:
        """Raise if not connected."""
        if not self._connected.is_set():
            raise RuntimeError("Not connected")
        if self._closed:
            raise RuntimeError("Connection closed")

    def read(self, n: int = -1, timeout: Optional[float] = None) -> Union[str, bytes]:
        """
        Read up to n bytes/characters from the connection.

        Blocks until data is available or timeout expires.

        :param n: Maximum bytes to read (-1 for any available data).
        :param timeout: Timeout in seconds (uses default if None).
        :returns: Data read from connection.
        :raises TimeoutError: If timeout expires before data available.
        :raises EOFError: If connection closed.
        """
        self._ensure_connected()
        assert self._reader is not None
        assert self._loop is not None
        timeout = timeout if timeout is not None else self._timeout
        future = asyncio.run_coroutine_threadsafe(self._reader.read(n), self._loop)
        try:
            result: Union[str, bytes] = future.result(timeout=timeout)
            if not result:
                raise EOFError("Connection closed")
            return result
        except concurrent.futures.TimeoutError as exc:
            future.cancel()
            raise TimeoutError("Read timed out") from exc

    def read_some(self, timeout: Optional[float] = None) -> Union[str, bytes]:
        """
        Read some available data from the connection.

        Unlike :meth:`read` with ``n=-1``, this returns as soon as any data is
        available rather than waiting for EOF.

        :param timeout: Timeout in seconds (uses default if None).
        :returns: Data read from connection.
        """
        return self.read(self._reader._limit, timeout=timeout)

    def readline(self, timeout: Optional[float] = None) -> Union[str, bytes]:
        """
        Read one line from the connection.

        Blocks until a complete line is received or timeout expires.

        :param timeout: Timeout in seconds (uses default if None).
        :returns: Line including terminator.
        :raises TimeoutError: If timeout expires.
        :raises EOFError: If connection closed before line complete.
        """
        self._ensure_connected()
        assert self._reader is not None
        assert self._loop is not None
        timeout = timeout if timeout is not None else self._timeout
        future = asyncio.run_coroutine_threadsafe(self._reader.readline(), self._loop)
        try:
            result: Union[str, bytes] = future.result(timeout=timeout)
            if not result:
                raise EOFError("Connection closed")
            return result
        except concurrent.futures.TimeoutError as exc:
            future.cancel()
            raise TimeoutError("Readline timed out") from exc

    def read_until(
        self, match: Union[str, bytes], timeout: Optional[float] = None
    ) -> Union[str, bytes]:
        """
        Read until match is found.

        Like old telnetlib's read_until method.

        :param match: String or bytes to match.
        :param timeout: Timeout in seconds (uses default if None).
        :returns: Data up to and including match.
        :raises TimeoutError: If timeout expires before match found.
        :raises EOFError: If connection closed before match found.
        """
        self._ensure_connected()
        assert self._reader is not None
        assert self._loop is not None
        timeout = timeout if timeout is not None else self._timeout
        # readuntil expects bytes, encode if string
        if isinstance(match, str):
            match = match.encode(self._encoding or "utf-8")
        future = asyncio.run_coroutine_threadsafe(self._reader.readuntil(match), self._loop)
        try:
            result: Union[str, bytes] = future.result(timeout=timeout)
            return result
        except concurrent.futures.TimeoutError as exc:
            future.cancel()
            raise TimeoutError("Read until timed out") from exc
        except asyncio.IncompleteReadError as exc:
            raise EOFError("Connection closed before match found") from exc

    def write(self, data: Union[str, bytes]) -> None:
        """
        Write data to the connection.

        This method buffers data and returns immediately. Use :meth:`flush`
        to ensure data is sent.

        :param data: String or bytes to write.
        """
        self._ensure_connected()
        assert self._writer is not None
        assert self._loop is not None
        # writer may be TelnetWriter (bytes) or TelnetWriterUnicode (str)
        self._loop.call_soon_threadsafe(self._writer.write, data)  # type: ignore[arg-type]

    def flush(self, timeout: Optional[float] = None) -> None:
        """
        Flush buffered data to the connection.

        Blocks until all buffered data has been sent.

        :param timeout: Timeout in seconds (uses default if None).
        :raises TimeoutError: If timeout expires.
        """
        self._ensure_connected()
        assert self._writer is not None
        assert self._loop is not None
        timeout = timeout if timeout is not None else self._timeout
        coro = self._writer.drain()
        try:
            future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        except RuntimeError:
            coro.close()
            raise
        try:
            future.result(timeout=timeout)
        except concurrent.futures.TimeoutError as exc:
            future.cancel()
            raise TimeoutError("Flush timed out") from exc

    def close(self) -> None:
        """Close the connection and stop the event loop."""
        if self._closed:
            return
        self._closed = True
        self._cleanup()

    def _cleanup(self) -> None:
        """Clean up resources."""
        if self._writer and self._loop and self._loop.is_running():
            # Schedule proper async cleanup
            future = asyncio.run_coroutine_threadsafe(self._async_cleanup(), self._loop)
            try:
                future.result(timeout=2.0)
            except Exception:
                pass  # Cleanup should not raise
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        # Always close the loop if it exists and isn't closed
        if self._loop and not self._loop.is_closed():
            self._loop.close()

    async def _async_cleanup(self) -> None:
        """Async cleanup for writer."""
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass  # Cleanup should not raise

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        """
        Get extra information about the connection.

        After negotiation completes, provides access to negotiated values:

        - ``'TERM'``: Terminal type (e.g., 'xterm-256color')
        - ``'cols'``: Terminal width in columns
        - ``'rows'``: Terminal height in rows
        - ``'peername'``: Remote address tuple (host, port)
        - ``'LANG'``: Language/locale setting

        :param name: Information key.
        :param default: Default value if key not found.
        :returns: Information value or default.
        """
        self._ensure_connected()
        assert self._writer is not None
        return self._writer.get_extra_info(name, default)

    def wait_for(
        self,
        remote: Optional[dict[str, bool]] = None,
        local: Optional[dict[str, bool]] = None,
        pending: Optional[dict[str, bool]] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """
        Wait for telnet option negotiation states.

        This method blocks until the specified options reach their desired
        states, or timeout expires. This is not possible with the legacy
        telnetlib module.

        :param remote: Dict of options for remote (client WILL) state.
            Example: ``{'NAWS': True, 'TTYPE': True}``
        :param local: Dict of options for local (client DO) state.
            Example: ``{'BINARY': True, 'ECHO': True}``
        :param pending: Dict of options for pending negotiation state.
            Example: ``{'TTYPE': False}`` (wait for negotiation to complete)
        :param timeout: Timeout in seconds (uses default if None).
        :raises TimeoutError: If timeout expires before conditions met.

        Example - wait for terminal info before proceeding::

            conn = TelnetConnection('localhost', 6023)
            conn.connect()

            # Wait for NAWS and TTYPE negotiation to complete
            conn.wait_for(remote={'NAWS': True, 'TTYPE': True}, timeout=5.0)

            # Now terminal info is available
            term = conn.get_extra_info('TERM')
            cols = conn.get_extra_info('cols')
            rows = conn.get_extra_info('rows')
            print(f"Terminal: {term} ({cols}x{rows})")
        """
        self._ensure_connected()
        assert self._writer is not None
        assert self._loop is not None
        timeout = timeout if timeout is not None else self._timeout
        future = asyncio.run_coroutine_threadsafe(
            self._writer.wait_for(remote=remote, local=local, pending=pending), self._loop
        )
        try:
            future.result(timeout=timeout)
        except concurrent.futures.TimeoutError as exc:
            future.cancel()
            raise TimeoutError("Wait for negotiation timed out") from exc

    @property
    def writer(self) -> TelnetWriter:
        """
        Access the underlying TelnetWriter for advanced operations.

        This provides access to telnet protocol features not available
        in the legacy telnetlib:

        - Option state inspection (``writer.remote_option``, ``writer.local_option``)
        - Mode detection (``writer.mode`` - 'local', 'remote', 'kludge')
        - Protocol constants and negotiation methods

        :returns: The underlying TelnetWriter instance.
        """
        self._ensure_connected()
        assert self._writer is not None
        return self._writer

    def __enter__(self) -> "TelnetConnection":
        self.connect()
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


class BlockingTelnetServer:
    r"""
    Blocking telnet server.

    Wraps async ``telnetlib3.create_server()`` with a blocking interface.
    Each client connection can be handled in a separate thread.

    :param host: Address to bind to.
    :param port: Port to bind to (default 6023).
    :param handler: Function called for each client connection.
        Receives a :class:`TelnetConnection`-like object as argument.
    :param kwargs: Additional arguments passed to ``telnetlib3.create_server()``.

    Example with handler::

        def handle_client(conn):
            conn.write('Welcome!\r\n')
            while line := conn.readline():
                conn.write(f'Echo: {line}')

        server = BlockingTelnetServer('localhost', 6023, handler=handle_client)
        server.serve_forever()

    Example with manual accept loop::

        server = BlockingTelnetServer('localhost', 6023)
        server.start()
        while True:
            conn = server.accept()
            threading.Thread(target=handle_client, args=(conn,)).start()
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6023,
        handler: Optional[Callable[["ServerConnection"], None]] = None,
        **kwargs: Any,
    ):
        """Initialize server parameters without starting."""
        self._host = host
        self._port = port
        self._handler = handler
        self._kwargs = kwargs

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._server: Optional[Server] = None
        self._client_queue: queue.Queue[ServerConnection] = queue.Queue()
        self._started = threading.Event()
        self._shutdown = threading.Event()

    def start(self) -> None:
        """
        Start the server.

        Non-blocking. Use :meth:`accept` or :meth:`serve_forever` to handle clients.

        :raises RuntimeError: If already started.
        """
        if self._thread is not None:
            raise RuntimeError("Server already started")

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

        # Wait for server to be ready
        self._started.wait()

    def _run_loop(self) -> None:
        """Run event loop in background thread."""
        assert self._loop is not None
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._start_server())
        self._started.set()
        self._loop.run_forever()

    async def _start_server(self) -> None:
        """Start the async server."""
        assert self._loop is not None
        loop = self._loop  # Capture for closure

        async def shell(reader: TelnetReader, writer: TelnetWriter) -> None:
            """Shell that queues connections for sync handling."""
            conn = ServerConnection(reader, writer, loop)
            self._client_queue.put(conn)
            # Wait until the sync handler closes the connection
            await conn._wait_closed()

        self._server = await _create_server(self._host, self._port, shell=shell, **self._kwargs)

    def accept(self, timeout: Optional[float] = None) -> "ServerConnection":
        """
        Accept a client connection.

        Blocks until a client connects.

        :param timeout: Timeout in seconds (None for no timeout).
        :returns: Connection object for the client.
        :raises TimeoutError: If timeout expires.
        :raises RuntimeError: If server not started.
        """
        if not self._started.is_set():
            raise RuntimeError("Server not started")

        try:
            return self._client_queue.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError("Accept timed out") from None

    def serve_forever(self) -> None:
        """
        Serve clients forever.

        Blocks and handles each client in a new thread using the handler function provided at
        construction.

        :raises RuntimeError: If no handler was provided.
        """
        if self._handler is None:
            raise RuntimeError("No handler provided")

        self.start()

        while not self._shutdown.is_set():
            try:
                conn = self.accept(timeout=1.0)
            except TimeoutError:
                continue

            thread = threading.Thread(target=self._handle_client, args=(conn,), daemon=True)
            thread.start()

    def _handle_client(self, conn: "ServerConnection") -> None:
        """Handle a client in the handler function."""
        assert self._handler is not None
        try:
            self._handler(conn)
        finally:
            if not conn._closed:
                conn.close()

    def shutdown(self) -> None:
        """
        Shutdown the server.

        Stops accepting new connections and closes the server.
        """
        self._shutdown.set()
        if self._server and self._loop and self._loop.is_running():
            # Schedule proper async cleanup
            future = asyncio.run_coroutine_threadsafe(self._async_shutdown(), self._loop)
            try:
                future.result(timeout=2.0)
            except Exception:
                pass  # Cleanup should not raise
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        # Always close the loop if it exists and isn't closed
        if self._loop and not self._loop.is_closed():
            self._loop.close()

    async def _async_shutdown(self) -> None:
        """Async cleanup for server."""
        if self._server:
            self._server.close()
            try:
                await self._server.wait_closed()
            except Exception:
                pass  # Cleanup should not raise
        # Cancel all pending tasks to avoid "Task was destroyed but pending" warnings
        for task in asyncio.all_tasks(self._loop):
            if task is not asyncio.current_task():
                task.cancel()
        # Give cancelled tasks a chance to clean up
        await asyncio.sleep(0)


class ServerConnection:
    """
    Blocking interface for a server-side client connection.

    This is similar to :class:`TelnetConnection` but for server-side use.
    Created automatically when a client connects to :class:`BlockingTelnetServer`.

    Provides miniboa-compatible properties for easier migration:

    - :attr:`active` - Connection state (set to False to disconnect)
    - :attr:`address`, :attr:`port` - Client address info
    - :attr:`terminal_type`, :attr:`columns`, :attr:`rows` - Terminal info
    - :meth:`send` - Alias for :meth:`write`
    - :meth:`addrport` - Returns "IP:PORT" string
    - :meth:`idle`, :meth:`duration` - Timing information
    - :meth:`deactivate` - Set active=False to queue disconnection
    """

    def __init__(self, reader: TelnetReader, writer: TelnetWriter, loop: asyncio.AbstractEventLoop):
        """Initialize connection from reader/writer pair."""
        self._reader = reader
        self._writer = writer
        self._loop = loop
        self._closed = False
        self._close_event = asyncio.Event()
        self._connect_time = time.time()
        self._last_input_time = time.time()

    async def _wait_closed(self) -> None:
        """Wait for the connection to be closed (called from async shell)."""
        await self._close_event.wait()

    def read(self, n: int = -1, timeout: Optional[float] = None) -> Union[str, bytes]:
        """
        Read up to n bytes/characters from the connection.

        :param n: Maximum bytes to read (-1 for any available data).
        :param timeout: Timeout in seconds.
        :returns: Data read from connection.
        :raises RuntimeError: If connection already closed.
        :raises TimeoutError: If timeout expires.
        :raises EOFError: If connection closed.
        """
        if self._closed:
            raise RuntimeError("Connection closed")
        future = asyncio.run_coroutine_threadsafe(self._reader.read(n), self._loop)
        try:
            result = future.result(timeout=timeout)
            if not result:
                raise EOFError("Connection closed")
            self._last_input_time = time.time()
            return result
        except concurrent.futures.TimeoutError as exc:
            future.cancel()
            raise TimeoutError("Read timed out") from exc

    def read_some(self, timeout: Optional[float] = None) -> Union[str, bytes]:
        """
        Read some available data from the connection.

        Unlike :meth:`read` with ``n=-1``, this returns as soon as any data is
        available rather than waiting for EOF.

        :param timeout: Timeout in seconds.
        :returns: Data read from connection.
        """
        return self.read(self._reader._limit, timeout=timeout)

    def readline(self, timeout: Optional[float] = None) -> Union[str, bytes]:
        """
        Read one line from the connection.

        :param timeout: Timeout in seconds.
        :returns: Line including terminator.
        :raises RuntimeError: If connection already closed.
        :raises TimeoutError: If timeout expires.
        :raises EOFError: If connection closed.
        """
        if self._closed:
            raise RuntimeError("Connection closed")
        future = asyncio.run_coroutine_threadsafe(self._reader.readline(), self._loop)
        try:
            result = future.result(timeout=timeout)
            if not result:
                raise EOFError("Connection closed")
            self._last_input_time = time.time()
            return result
        except concurrent.futures.TimeoutError as exc:
            future.cancel()
            raise TimeoutError("Readline timed out") from exc

    def read_until(
        self, match: Union[str, bytes], timeout: Optional[float] = None
    ) -> Union[str, bytes]:
        """
        Read until match is found.

        :param match: String or bytes to match.
        :param timeout: Timeout in seconds.
        :returns: Data up to and including match.
        :raises RuntimeError: If connection already closed.
        :raises TimeoutError: If timeout expires.
        :raises EOFError: If connection closed.
        """
        if self._closed:
            raise RuntimeError("Connection closed")
        # readuntil expects bytes, encode if string
        if isinstance(match, str):
            match = match.encode("utf-8")
        future = asyncio.run_coroutine_threadsafe(self._reader.readuntil(match), self._loop)
        try:
            result = future.result(timeout=timeout)
            self._last_input_time = time.time()
            return result
        except concurrent.futures.TimeoutError as exc:
            future.cancel()
            raise TimeoutError("Read until timed out") from exc
        except asyncio.IncompleteReadError as exc:
            raise EOFError("Connection closed before match found") from exc

    def write(self, data: Union[str, bytes]) -> None:
        """
        Write data to the connection.

        :param data: String or bytes to write.
        :raises RuntimeError: If connection already closed.
        """
        if self._closed:
            raise RuntimeError("Connection closed")
        self._loop.call_soon_threadsafe(self._writer.write, data)  # type: ignore[arg-type]

    def flush(self, timeout: Optional[float] = None) -> None:
        """
        Flush buffered data to the connection.

        :param timeout: Timeout in seconds.
        :raises RuntimeError: If connection already closed.
        :raises TimeoutError: If timeout expires.
        """
        if self._closed:
            raise RuntimeError("Connection closed")
        coro = self._writer.drain()
        try:
            future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        except RuntimeError:
            coro.close()
            raise
        try:
            future.result(timeout=timeout)
        except concurrent.futures.TimeoutError as exc:
            future.cancel()
            raise TimeoutError("Flush timed out") from exc

    def close(self) -> None:
        """Close the connection."""
        if self._closed:
            return
        self._closed = True
        try:
            self._loop.call_soon_threadsafe(self._writer.close)
            self._loop.call_soon_threadsafe(self._close_event.set)
        except RuntimeError:
            pass  # Event loop already closed during shutdown

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        """
        Get extra information about the connection.

        After negotiation completes, provides access to negotiated values:

        - ``'TERM'``: Terminal type (e.g., 'xterm-256color')
        - ``'cols'``: Terminal width in columns
        - ``'rows'``: Terminal height in rows
        - ``'peername'``: Remote address tuple (host, port)

        :param name: Information key.
        :param default: Default value if key not found.
        :returns: Information value or default.
        """
        return self._writer.get_extra_info(name, default)

    def wait_for(
        self,
        remote: Optional[dict[str, bool]] = None,
        local: Optional[dict[str, bool]] = None,
        pending: Optional[dict[str, bool]] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """
        Wait for telnet option negotiation states.

        Blocks until the specified options reach their desired states.

        :param remote: Dict of options for remote state.
            Example: ``{'NAWS': True, 'TTYPE': True}``
        :param local: Dict of options for local state.
        :param pending: Dict of options for pending state.
        :param timeout: Timeout in seconds.
        :raises RuntimeError: If connection already closed.
        :raises TimeoutError: If timeout expires.

        Example::

            conn = server.accept()
            conn.wait_for(remote={'NAWS': True}, timeout=5.0)
            print(f"Window: {conn.columns}x{conn.rows}")
        """
        if self._closed:
            raise RuntimeError("Connection closed")
        future = asyncio.run_coroutine_threadsafe(
            self._writer.wait_for(remote=remote, local=local, pending=pending), self._loop
        )
        try:
            future.result(timeout=timeout)
        except concurrent.futures.TimeoutError as exc:
            future.cancel()
            raise TimeoutError("Wait for negotiation timed out") from exc

    @property
    def writer(self) -> TelnetWriter:
        """
        Access the underlying TelnetWriter for advanced operations.

        :returns: The underlying TelnetWriter instance.
        """
        return self._writer

    # Miniboa-compatible properties and methods

    @property
    def active(self) -> bool:
        """
        Connection health status (miniboa-compatible).

        Set to False to disconnect on next opportunity.
        """
        return not self._closed

    @active.setter
    def active(self, value: bool) -> None:
        if not value:
            self.close()

    @property
    def address(self) -> str:
        """Remote IP address of the connected client (miniboa-compatible)."""
        peername = self.get_extra_info("peername", ("", 0))
        return peername[0] if peername else ""

    @property
    def port(self) -> int:
        """Remote port number of the connected client (miniboa-compatible)."""
        peername = self.get_extra_info("peername", ("", 0))
        return peername[1] if peername else 0

    @property
    def terminal_type(self) -> str:
        """Client terminal type (miniboa-compatible)."""
        result: str = self.get_extra_info("TERM", "unknown")
        return result

    @property
    def columns(self) -> int:
        """Terminal width (miniboa-compatible)."""
        result: int = self.get_extra_info("cols", 80)
        return result

    @property
    def rows(self) -> int:
        """Terminal height (miniboa-compatible)."""
        result: int = self.get_extra_info("rows", 24)
        return result

    @property
    def connect_time(self) -> float:
        """Timestamp when connection was established (miniboa-compatible)."""
        return self._connect_time

    @property
    def last_input_time(self) -> float:
        """Timestamp of last input received (miniboa-compatible)."""
        return self._last_input_time

    def send(self, text: Union[str, bytes]) -> None:
        r"""
        Send text to the client (miniboa-compatible).

        Alias for :meth:`write`. Normalizes newlines to \r\n like miniboa.

        :param text: Text to send.
        """
        if isinstance(text, str):
            text = text.replace("\r\n", "\n").replace("\n", "\r\n")
        self.write(text)

    def addrport(self) -> str:
        """
        Return client's IP:PORT as string (miniboa-compatible).

        :returns: String in format "IP:PORT".
        """
        return f"{self.address}:{self.port}"

    def idle(self) -> float:
        """
        Seconds since last input received (miniboa-compatible).

        :returns: Idle time in seconds.
        """
        return time.time() - self._last_input_time

    def duration(self) -> float:
        """
        Seconds since connection was established (miniboa-compatible).

        :returns: Connection duration in seconds.
        """
        return time.time() - self._connect_time

    def deactivate(self) -> None:
        """
        Set connection to disconnect on next opportunity (miniboa-compatible).

        Same as setting ``active = False``.
        """
        self.active = False
