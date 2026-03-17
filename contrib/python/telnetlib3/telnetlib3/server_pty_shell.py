"""
PTY shell implementation for telnetlib3.

This module provides the ability to spawn PTY-connected programs (bash, tmux, nethack, etc.) for
each telnet connection, with proper terminal negotiation forwarding.
"""

from __future__ import annotations

# std imports
import os
import sys
import time
import codecs
import struct
import asyncio
import logging
from typing import Any, Dict, List, Tuple, Union, Callable, Optional, Awaitable, cast

# local
from .telopt import ECHO, NAWS, WONT
from .stream_reader import TelnetReader, TelnetReaderUnicode
from .stream_writer import TelnetWriter, TelnetWriterUnicode

__all__ = ("make_pty_shell", "pty_shell", "PTYSpawnError")

# Delay between termination signals (seconds)
_TERMINATE_DELAY = 0.1

# Debounce delay for NAWS updates (seconds)
_NAWS_DEBOUNCE = 0.2

# Idle delay before sending IAC GA (seconds)
_GA_IDLE = 0.5

# Polling interval for _wait_for_terminal_info (seconds)
_TERMINAL_INFO_POLL = 0.05


class PTYSpawnError(Exception):
    """Raised when PTY child process fails to exec."""


logger = logging.getLogger("telnetlib3.server_pty_shell")

# Synchronized Output sequences (DEC private mode 2026)
# https://gist.github.com/christianparpart/d8a62cc1ab659194337d73e399004036
_BSU = b"\x1b[?2026h"  # Begin Synchronized Update
_ESU = b"\x1b[?2026l"  # End Synchronized Update


def _platform_check() -> None:
    """Verify platform supports PTY operations."""
    if sys.platform == "win32":
        raise NotImplementedError("PTY support is not available on Windows")


class PTYSession:
    """Manages a PTY session lifecycle."""

    def __init__(
        self,
        reader: Union[TelnetReader, TelnetReaderUnicode],
        writer: Union[TelnetWriter, TelnetWriterUnicode],
        program: str,
        args: Optional[List[str]],
        *,
        preexec_fn: Optional[Callable[[], None]] = None,
        raw_mode: bool = False,
    ) -> None:
        """
        Initialize PTY session.

        :param reader: TelnetReader instance.
        :param writer: TelnetWriter instance.
        :param program: Path to program to execute.
        :param args: List of arguments for the program.
        :param preexec_fn: Optional callable to run in child before exec. Called with no arguments
            after fork but before _setup_child. Useful for test coverage tracking in the forked
            child process.
        :param raw_mode: If True, disable PTY echo and canonical mode. Use for programs that handle
            their own terminal I/O (e.g., blessed, curses, ucs-detect).
        """
        self.reader = reader
        self.writer = writer
        self.program = program
        self.args = args or []
        self.preexec_fn = preexec_fn
        self.raw_mode = raw_mode
        self.master_fd: Optional[int] = None
        self.child_pid: Optional[int] = None
        self._closing = False
        self._output_buffer = b""
        self._in_sync_update = False
        self._decoder: Optional[codecs.IncrementalDecoder] = None
        self._decoder_charset: Optional[str] = None
        self._naws_pending: Optional[Tuple[int, int]] = None
        self._naws_timer: Optional[asyncio.TimerHandle] = None
        self._ga_timer: Optional[asyncio.TimerHandle] = None

    def start(self) -> None:
        """
        Fork PTY, configure environment, and exec program.

        :raises PTYSpawnError: If the child process fails to exec.
        """
        import pty
        import fcntl

        _platform_check()

        env = self._build_environment()
        rows, cols = self._get_window_size()

        # Create pipe for exec error detection (ptyprocess pattern).
        # Child sets close-on-exec; successful exec closes pipe automatically.
        # If exec fails, child writes error through pipe before exiting.
        exec_err_pipe_read, exec_err_pipe_write = os.pipe()

        self.child_pid, self.master_fd = pty.fork()

        if self.child_pid == 0:
            # Child process
            os.close(exec_err_pipe_read)
            fcntl.fcntl(exec_err_pipe_write, fcntl.F_SETFD, fcntl.FD_CLOEXEC)

            # Coverage object from preexec_fn, saved before exec
            child_cov = None
            if self.preexec_fn is not None:
                try:
                    child_cov = self.preexec_fn()
                except Exception as e:
                    self._write_exec_error(exec_err_pipe_write, e)
                    os._exit(1)
            self._setup_child(env, rows, cols, exec_err_pipe_write, child_cov=child_cov)
        else:
            # Parent process
            os.close(exec_err_pipe_write)
            exec_err_data = os.read(exec_err_pipe_read, 4096)
            os.close(exec_err_pipe_read)

            if exec_err_data:
                self._handle_exec_error(exec_err_data)

            logger.debug(
                "forked PTY: program=%s pid=%d fd=%d", self.program, self.child_pid, self.master_fd
            )
            self._setup_parent()
            pid, status = os.waitpid(self.child_pid, os.WNOHANG)
            if pid:
                logger.warning("child already exited: status=%d", status)

    def _write_exec_error(self, pipe_fd: int, exc: Exception) -> None:
        """Write exception info to pipe for parent to read."""
        ename = type(exc).__name__
        msg = f"{ename}:{getattr(exc, 'errno', 0)}:{exc}"
        os.write(pipe_fd, msg.encode("utf-8", errors="replace"))
        os.close(pipe_fd)

    def _handle_exec_error(self, data: bytes) -> None:
        """Parse exec error from child and raise appropriate exception."""
        try:
            parts = data.decode("utf-8", errors="replace").split(":", 2)
            if len(parts) == 3:
                errclass, _errno_s, errmsg = parts
                raise PTYSpawnError(f"{errclass}: {errmsg}")
            raise PTYSpawnError(f"Exec failed: {data!r}")
        except PTYSpawnError:
            raise
        except Exception as exc:
            raise PTYSpawnError(f"Exec failed: {data!r}") from exc

    def _build_environment(self) -> Dict[str, str]:
        """Build environment dict from negotiated values."""
        env = os.environ.copy()

        term = self.writer.get_extra_info("TERM", "xterm")
        if term:
            # Terminfo entries are lowercase; telnet TTYPE may send uppercase
            env["TERM"] = term.lower()

        rows = self.writer.get_extra_info("rows")
        cols = self.writer.get_extra_info("cols")
        if rows:
            env["LINES"] = str(rows)
        if cols:
            env["COLUMNS"] = str(cols)

        lang = self.writer.get_extra_info("LANG")
        if lang:
            env["LANG"] = lang
            env["LC_ALL"] = lang
        else:
            charset = self.writer.get_extra_info("charset")
            if charset:
                env["LANG"] = f"en_US.{charset}"

        for key in ("DISPLAY", "USER", "COLORTERM", "HOME", "SHELL", "LOGNAME"):
            val = self.writer.get_extra_info(key)
            if val:
                env[key] = val

        return env

    def _get_window_size(self) -> Tuple[int, int]:
        """Get window size from negotiated values."""
        rows: int = self.writer.get_extra_info("rows", 25)
        cols: int = self.writer.get_extra_info("cols", 80)
        return rows, cols

    def _setup_child(
        self,
        env: Dict[str, str],
        rows: int,
        cols: int,
        exec_err_pipe: int,
        *,
        child_cov: Any = None,
    ) -> None:
        """Child process setup before exec."""
        # Note: pty.fork() already calls setsid() for the child, so we don't need to
        import fcntl
        import termios

        if rows and cols:
            winsize = struct.pack("HHHH", rows, cols, 0, 0)
            fcntl.ioctl(sys.stdout.fileno(), termios.TIOCSWINSZ, winsize)

        attrs = termios.tcgetattr(sys.stdin.fileno())

        if self.raw_mode:
            # Raw mode: disable echo and canonical mode for programs that handle
            # their own terminal I/O (blessed, curses, ucs-detect). This prevents
            # terminal responses from being echoed back through the PTY.
            attrs[3] &= ~(termios.ECHO | termios.ICANON)
        else:
            # Normal mode: keep ICANON for line editing but disable ECHO.
            # We sent WONT ECHO so the client does local echo; if the PTY
            # also echoed, every character would appear twice.
            attrs[3] &= ~termios.ECHO

        # Set VERASE to ^H (0x08) since many telnet clients send ^H for backspace
        # (default PTY ERASE is often ^? which won't work for those clients).
        attrs[6][termios.VERASE] = 8  # ^H
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, attrs)

        # Save coverage data before exec replaces the process
        if child_cov is not None:
            child_cov.stop()
            child_cov.save()

        argv = [self.program] + self.args
        try:
            os.execvpe(self.program, argv, env)
        except OSError as err:
            self._write_exec_error(exec_err_pipe, err)
            os._exit(os.EX_OSERR)

    def _setup_parent(self) -> None:
        """Parent process setup after fork."""
        import fcntl

        assert self.master_fd is not None
        flags = fcntl.fcntl(self.master_fd, fcntl.F_GETFL)
        fcntl.fcntl(self.master_fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
        self.writer.set_ext_callback(NAWS, self._on_naws)

    def _on_naws(self, rows: int, cols: int) -> None:
        """Handle NAWS updates by resizing PTY with debouncing."""
        self.writer.protocol.on_naws(rows, cols)
        self._schedule_naws_update(rows, cols)

    def _schedule_naws_update(self, rows: int, cols: int) -> None:
        """Schedule debounced NAWS update to avoid signal storms during rapid resize."""
        self._naws_pending = (rows, cols)
        if self._naws_timer is not None:
            self._naws_timer.cancel()
        loop = asyncio.get_event_loop()
        self._naws_timer = loop.call_later(_NAWS_DEBOUNCE, self._fire_naws_update)

    def _fire_naws_update(self) -> None:
        """Fire the pending NAWS update after debounce delay."""
        if self._naws_pending is not None:
            rows, cols = self._naws_pending
            self._naws_pending = None
            self._naws_timer = None
            self._set_window_size(rows, cols)

    def _set_window_size(self, rows: int, cols: int) -> None:
        """Set PTY window size and send SIGWINCH to child."""
        import fcntl
        import signal
        import termios

        if self.master_fd is None or self.child_pid is None:
            return
        winsize = struct.pack("HHHH", rows, cols, 0, 0)
        fcntl.ioctl(self.master_fd, termios.TIOCSWINSZ, winsize)
        try:
            os.killpg(os.getpgid(self.child_pid), signal.SIGWINCH)
        except ProcessLookupError:
            pass

    async def run(self) -> None:
        """Bridge loop between telnet and PTY."""
        import errno

        loop = asyncio.get_event_loop()
        pty_read_event = asyncio.Event()
        pty_data_queue: asyncio.Queue[bytes] = asyncio.Queue()

        assert self.child_pid is not None
        assert self.master_fd is not None
        pid, _ = os.waitpid(self.child_pid, os.WNOHANG)
        if pid:
            return

        master_fd = self.master_fd

        def pty_readable() -> None:
            """Callback when PTY has data to read."""
            # Drain available data to reduce tearing, but cap at 256KB to avoid
            # buffering forever on continuous output (e.g., cat large_file)
            chunks: list[bytes] = []
            total = 0
            max_batch = 262144  # 256KB
            while total < max_batch:
                try:
                    data = os.read(master_fd, 65536)
                    if data:
                        chunks.append(data)
                        total += len(data)
                    else:
                        self._closing = True
                        break
                except OSError as e:
                    if e.errno == errno.EAGAIN:
                        break  # No more data available
                    if e.errno == errno.EIO:
                        self._closing = True
                        break
                    logger.debug("PTY read error: %s", e)
                    self._closing = True
                    break
            if chunks:
                pty_data_queue.put_nowait(b"".join(chunks))
            pty_read_event.set()

        loop.add_reader(master_fd, pty_readable)

        try:
            await self._bridge_loop(pty_read_event, pty_data_queue)
        finally:
            try:
                loop.remove_reader(master_fd)
            except (ValueError, KeyError):
                pass

    async def _bridge_loop(
        self, pty_read_event: asyncio.Event, pty_data_queue: asyncio.Queue[bytes]
    ) -> None:
        """Main bridge loop transferring data between telnet and PTY."""
        while not self._closing and not self.writer.is_closing():
            telnet_task: asyncio.Task[Union[bytes, str]] = asyncio.create_task(
                self.reader.read(4096)
            )
            pty_task: asyncio.Task[bool] = asyncio.create_task(pty_read_event.wait())

            done, pending = await asyncio.wait(
                {telnet_task, pty_task}, return_when=asyncio.FIRST_COMPLETED
            )

            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            try:
                if telnet_task in done:
                    telnet_data = telnet_task.result()
                    if telnet_data:
                        self._write_to_pty(telnet_data)
                    else:
                        self._closing = True
                        continue

                if pty_task in done:
                    pty_task.result()
                    while not pty_data_queue.empty():
                        pty_data = pty_data_queue.get_nowait()
                        self._write_to_telnet(pty_data)
                    # EAGAIN was hit - flush any remaining partial line
                    self._flush_remaining()
                    pty_read_event.clear()
            except Exception as e:
                logger.debug("bridge loop error: %s", e)
                self._closing = True
                break

    def _write_to_pty(self, data: Union[str, bytes]) -> None:
        """
        Write data from telnet to PTY.

        Translates DEL (0x7F) to ``^H`` (0x08) so that both backspace encodings work with the PTY's
        VERASE setting (``^H``).
        """
        if self.master_fd is None:
            return
        if isinstance(data, str):
            charset = self.writer.get_extra_info("charset") or "utf-8"
            data = data.encode(charset, errors="replace")
        data = data.replace(b"\x7f", b"\x08")
        try:
            os.write(self.master_fd, data)
        except OSError:
            self._closing = True

    def _write_to_telnet(self, data: bytes) -> None:
        """Write data from PTY to telnet, respecting synchronized update boundaries."""
        if self._ga_timer is not None:
            self._ga_timer.cancel()
            self._ga_timer = None
        self._output_buffer += data

        # Process buffer, flushing on ESU or newline boundaries
        while True:
            if self._in_sync_update:
                # Look for End Synchronized Update
                esu_pos = self._output_buffer.find(_ESU)
                if esu_pos != -1:
                    # Flush up to and including ESU
                    end = esu_pos + len(_ESU)
                    self._flush_output(self._output_buffer[:end])
                    self._output_buffer = self._output_buffer[end:]
                    self._in_sync_update = False
                else:
                    # Still waiting for ESU, but flush if buffer too large
                    if len(self._output_buffer) > 262144:  # 256KB safety limit
                        self._flush_output(self._output_buffer)
                        self._output_buffer = b""
                    break
            else:
                # Look for Begin Synchronized Update
                bsu_pos = self._output_buffer.find(_BSU)
                if bsu_pos != -1:
                    # Flush everything before BSU (up to last newline if any)
                    if bsu_pos > 0:
                        self._flush_output(self._output_buffer[:bsu_pos])
                    self._output_buffer = self._output_buffer[bsu_pos:]
                    self._in_sync_update = True
                else:
                    # Flush up to and including last newline for line-oriented output
                    nl_pos = self._output_buffer.rfind(b"\n")
                    if nl_pos != -1:
                        end = nl_pos + 1
                        self._flush_output(self._output_buffer[:end])
                        self._output_buffer = self._output_buffer[end:]
                    # Keep any partial line in buffer (will flush on next newline,
                    # next sync boundary, or when more data arrives with EAGAIN)
                    break

    def _flush_output(self, data: bytes, final: bool = False) -> None:
        """Send data to telnet client using incremental decoder."""
        if not data:
            return
        charset = self.writer.get_extra_info("charset") or "utf-8"

        # Get or create incremental decoder, recreating if charset changed
        if self._decoder is None or self._decoder_charset != charset:
            self._decoder = codecs.getincrementaldecoder(charset)(errors="replace")
            self._decoder_charset = charset

        # Decode using incremental decoder - it buffers incomplete sequences
        text = self._decoder.decode(data, final)
        if text:
            cast(TelnetWriterUnicode, self.writer).write(text)

    def _flush_remaining(self) -> None:
        """Flush remaining buffer after EAGAIN (partial lines, prompts, etc.)."""
        if self._output_buffer and not self._in_sync_update:
            self._flush_output(self._output_buffer)
            self._output_buffer = b""
        self._schedule_ga()

    def _schedule_ga(self) -> None:
        """Schedule IAC GA after 500ms idle, for clients that refuse SGA."""
        if self._ga_timer is not None:
            self._ga_timer.cancel()
            self._ga_timer = None
        if self.raw_mode:
            return
        if getattr(self.writer.protocol, "never_send_ga", False):
            return
        loop = asyncio.get_event_loop()
        self._ga_timer = loop.call_later(_GA_IDLE, self._fire_ga)

    def _fire_ga(self) -> None:
        """Send IAC GA if writer is still open."""
        self._ga_timer = None
        if not self.writer.is_closing():
            self.writer.send_ga()

    def _isalive(self) -> bool:
        """Check if child process is still running."""
        if self.child_pid is None:
            return False
        try:
            pid, _status = os.waitpid(self.child_pid, os.WNOHANG)
            return pid == 0
        except ChildProcessError:
            return False

    def _terminate(self, force: bool = False) -> bool:
        """
        Terminate child with signal escalation (ptyprocess pattern).

        Tries SIGHUP, SIGCONT, SIGINT in sequence. If force=True, also tries SIGKILL.

        :param force: If True, use SIGKILL as last resort.
        :returns: True if child was terminated, False otherwise.
        """
        import signal

        if not self._isalive():
            return True

        assert self.child_pid is not None
        signals = [signal.SIGHUP, signal.SIGCONT, signal.SIGINT]
        if force:
            signals.append(signal.SIGKILL)

        for sig in signals:
            try:
                os.kill(self.child_pid, sig)
            except ProcessLookupError:
                return True
            time.sleep(_TERMINATE_DELAY)
            if not self._isalive():
                return True

        return not self._isalive()

    def cleanup(self) -> None:
        """Kill child process and close PTY fd."""
        # Cancel any pending timers
        if self._ga_timer is not None:
            self._ga_timer.cancel()
            self._ga_timer = None
        if self._naws_timer is not None:
            self._naws_timer.cancel()
            self._naws_timer = None
            self._naws_pending = None

        # Flush any remaining output buffer with final=True to emit buffered bytes
        if self._output_buffer:
            self._flush_output(self._output_buffer, final=True)
            self._output_buffer = b""

        if self.master_fd is not None:
            try:
                os.close(self.master_fd)
            except OSError:
                pass
            self.master_fd = None

        if self.child_pid is not None:
            self._terminate(force=True)
            try:
                os.waitpid(self.child_pid, os.WNOHANG)
            except ChildProcessError:
                pass
            self.child_pid = None


async def _wait_for_terminal_info(
    writer: Union[TelnetWriter, TelnetWriterUnicode], timeout: float = 2.0
) -> None:
    """
    Wait for TERM and window size to be negotiated.

    :param writer: TelnetWriter instance.
    :param timeout: Maximum time to wait in seconds.
    """
    loop = asyncio.get_event_loop()
    start = loop.time()

    while loop.time() - start < timeout:
        term = writer.get_extra_info("TERM")
        rows = writer.get_extra_info("rows")
        if term and rows:
            return
        await asyncio.sleep(_TERMINAL_INFO_POLL)


async def pty_shell(
    reader: Union[TelnetReader, TelnetReaderUnicode],
    writer: Union[TelnetWriter, TelnetWriterUnicode],
    program: str,
    args: Optional[List[str]] = None,
    preexec_fn: Optional[Callable[[], None]] = None,
    raw_mode: bool = False,
) -> None:
    """
    PTY shell callback for telnet server.

    :param reader: TelnetReader instance.
    :param writer: TelnetWriter instance.
    :param program: Path to program to execute.
    :param args: List of arguments for the program.
    :param preexec_fn: Optional callable to run in child before exec.
    :param raw_mode: If True, disable PTY echo and canonical mode. Use for programs that handle
        their own terminal I/O (e.g., blessed, curses, ucs-detect).
    """
    _platform_check()

    await _wait_for_terminal_info(writer, timeout=2.0)

    # Echo handling depends on raw_mode:
    # - Normal mode: Send WONT ECHO so client does local echo, PTY handles
    #   echo with proper ONLCR translation (\n -> \r\n) for input() display.
    # - Raw mode: Keep WILL ECHO so client doesn't local-echo, but PTY echo
    #   is disabled. This prevents terminal responses (CPR, etc.) from being
    #   echoed back. The program handles its own output.
    if not raw_mode and writer.will_echo:
        writer.iac(WONT, ECHO)
        await writer.drain()

    session = PTYSession(reader, writer, program, args, preexec_fn=preexec_fn, raw_mode=raw_mode)
    try:
        session.start()
        await session.run()
    finally:
        session.cleanup()
        if not writer.is_closing():
            writer.close()


def make_pty_shell(
    program: str,
    args: Optional[List[str]] = None,
    preexec_fn: Optional[Callable[[], None]] = None,
    raw_mode: bool = False,
) -> Callable[
    [Union[TelnetReader, TelnetReaderUnicode], Union[TelnetWriter, TelnetWriterUnicode]],
    Awaitable[None],
]:
    """
    Factory returning a shell callback for PTY execution.

    :param program: Path to program to execute.
    :param args: List of arguments for the program.
    :param preexec_fn: Optional callable to run in child before exec.
        Useful for test coverage tracking in the forked child process.
    :param raw_mode: If True, disable PTY echo and canonical mode. Use for programs
        that handle their own terminal I/O (e.g., blessed, curses, ucs-detect).
    :returns: Async shell callback suitable for use with create_server().

    Example usage::

        from telnetlib3 import create_server, make_pty_shell

        server = await create_server(
            host='localhost',
            port=6023,
            shell=make_pty_shell('/bin/bash', ['-l'])
        )
    """

    async def shell(
        reader: Union[TelnetReader, TelnetReaderUnicode],
        writer: Union[TelnetWriter, TelnetWriterUnicode],
    ) -> None:
        await pty_shell(reader, writer, program, args, preexec_fn=preexec_fn, raw_mode=raw_mode)

    return shell
