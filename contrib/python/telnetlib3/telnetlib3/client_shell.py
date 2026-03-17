"""Telnet client shell implementations for interactive terminal sessions."""

# std imports
import sys
import asyncio
import logging
import threading
import collections
from typing import Any, Dict, Tuple, Union, Callable, Optional
from dataclasses import dataclass

# local
from . import accessories
from ._session_context import TelnetSessionContext

log = logging.getLogger(__name__)

# local
from .accessories import TRACE  # noqa: E402
from .stream_reader import TelnetReader, TelnetReaderUnicode  # noqa: E402
from .stream_writer import TelnetWriter, TelnetWriterUnicode  # noqa: E402

__all__ = ("InputFilter", "telnet_client_shell")

# ATASCII graphics characters that map to byte 0x0D and 0x0A respectively.
# When --ascii-eol is active, these are replaced with \r and \n before
# terminal display so that BBSes using ASCII CR/LF render correctly.
_ATASCII_CR_CHAR = "\U0001fb82"  # UPPER ONE QUARTER BLOCK (from byte 0x0D)
_ATASCII_LF_CHAR = "\u25e3"  # BLACK LOWER LEFT TRIANGLE (from byte 0x0A)

# Input byte translation tables for retro encodings in raw mode.
# Maps terminal keyboard bytes to the raw bytes the BBS expects.
# Applied BEFORE decoding/encoding, bypassing the codec entirely for
# characters that can't round-trip through Unicode (e.g. ATASCII 0x7E
# shares its Unicode codepoint U+25C0 with 0xFE).
_INPUT_XLAT: Dict[str, Dict[int, int]] = {
    "atascii": {
        0x7F: 0x7E,  # DEL -> ATASCII backspace (byte 0x7E)
        0x08: 0x7E,  # BS  -> ATASCII backspace (byte 0x7E)
        0x0D: 0x9B,  # CR  -> ATASCII EOL (byte 0x9B)
        0x0A: 0x9B,  # LF  -> ATASCII EOL (byte 0x9B)
    },
    "petscii": {
        0x7F: 0x14,  # DEL -> PETSCII DEL (byte 0x14)
        0x08: 0x14,  # BS  -> PETSCII DEL (byte 0x14)
    },
}

# Multi-byte escape sequence translation tables for retro encodings.
# Maps common ANSI terminal escape sequences (arrow keys, delete, etc.)
# to the raw bytes the BBS expects.  Inspired by blessed's
# DEFAULT_SEQUENCE_MIXIN but kept minimal for the sequences that matter.
_INPUT_SEQ_XLAT: Dict[str, Dict[bytes, bytes]] = {
    "atascii": {
        b"\x1b[A": b"\x1c",  # cursor up (CSI)
        b"\x1b[B": b"\x1d",  # cursor down
        b"\x1b[C": b"\x1f",  # cursor right
        b"\x1b[D": b"\x1e",  # cursor left
        b"\x1bOA": b"\x1c",  # cursor up (SS3 / application mode)
        b"\x1bOB": b"\x1d",  # cursor down
        b"\x1bOC": b"\x1f",  # cursor right
        b"\x1bOD": b"\x1e",  # cursor left
        b"\x1b[3~": b"\x7e",  # delete -> ATASCII backspace
        b"\t": b"\x7f",  # tab -> ATASCII tab
    },
    "petscii": {
        b"\x1b[A": b"\x91",  # cursor up (CSI)
        b"\x1b[B": b"\x11",  # cursor down
        b"\x1b[C": b"\x1d",  # cursor right
        b"\x1b[D": b"\x9d",  # cursor left
        b"\x1bOA": b"\x91",  # cursor up (SS3 / application mode)
        b"\x1bOB": b"\x11",  # cursor down
        b"\x1bOC": b"\x1d",  # cursor right
        b"\x1bOD": b"\x9d",  # cursor left
        b"\x1b[3~": b"\x14",  # delete -> PETSCII DEL
        b"\x1b[H": b"\x13",  # home -> PETSCII HOME
        b"\x1b[2~": b"\x94",  # insert -> PETSCII INSERT
    },
}


class InputFilter:
    """
    Translate terminal escape sequences and single bytes to retro encoding bytes.

    Combines single-byte translation (backspace, delete) with multi-byte
    escape sequence matching (arrow keys, function keys).  Uses prefix-based
    buffering inspired by blessed's ``get_leading_prefixes`` to handle
    sequences split across reads.

    When a partial match is buffered (e.g. a bare ESC), :attr:`has_pending`
    becomes ``True``.  The caller should start an ``esc_delay`` timer and
    call :meth:`flush` if no further input arrives before the timer fires.

    :param seq_xlat: Multi-byte escape sequence -> replacement bytes.
    :param byte_xlat: Single input byte -> replacement byte.
    :param esc_delay: Seconds to wait before flushing a buffered prefix
        (default 0.35, matching blessed's ``DEFAULT_ESCDELAY``).
    """

    def __init__(
        self, seq_xlat: Dict[bytes, bytes], byte_xlat: Dict[int, int], esc_delay: float = 0.35
    ) -> None:
        """Initialize input filter with sequence and byte translation tables."""
        self._byte_xlat = byte_xlat
        self.esc_delay = esc_delay
        # Sort sequences longest-first so \x1b[3~ matches before \x1b[3
        self._seq_sorted: Tuple[Tuple[bytes, bytes], ...] = tuple(
            sorted(seq_xlat.items(), key=lambda kv: len(kv[0]), reverse=True)
        )
        # Prefix set for partial-match buffering (blessed's get_leading_prefixes)
        self._prefixes: frozenset[bytes] = frozenset(
            seq[:i] for seq in seq_xlat for i in range(1, len(seq))
        )
        self._buf = b""

    @property
    def has_pending(self) -> bool:
        """Return ``True`` when the internal buffer holds a partial sequence."""
        return bool(self._buf)

    def flush(self) -> bytes:
        """
        Flush buffered bytes, applying single-byte translation.

        Called when the ``esc_delay`` timer fires without new input,
        meaning the buffered prefix is not a real escape sequence.

        :returns: Translated bytes from the buffer (may be empty).
        """
        result = bytearray()
        while self._buf:
            b = self._buf[0]
            self._buf = self._buf[1:]
            result.append(self._byte_xlat.get(b, b))
        return bytes(result)

    def feed(self, data: bytes) -> bytes:
        """
        Process input bytes, returning raw bytes to send to the remote host.

        Escape sequences are matched against the configured table and replaced. Partial sequences
        are buffered until the next call.  Single bytes are translated via the byte translation
        table.

        :param data: Raw bytes from terminal stdin.
        :returns: Translated bytes ready to send to the remote BBS.
        """
        self._buf += data
        result = bytearray()
        while self._buf:
            # Try multi-byte sequence match at current position
            matched = False
            for seq, repl in self._seq_sorted:
                if self._buf[: len(seq)] == seq:
                    result.extend(repl)
                    self._buf = self._buf[len(seq) :]
                    matched = True
                    break
            if matched:
                continue
            # Check if buffer is a prefix of any known sequence -- wait for more
            if self._buf in self._prefixes:
                break
            # No sequence match, emit single byte with translation
            b = self._buf[0]
            self._buf = self._buf[1:]
            result.append(self._byte_xlat.get(b, b))
        return bytes(result)


@dataclass
class _RawLoopState:
    """Mutable state bundle for :func:`_raw_event_loop`."""

    switched_to_raw: bool
    last_will_echo: bool
    local_echo: bool
    linesep: str
    reactivate_repl: bool = False


if sys.platform == "win32":

    async def telnet_client_shell(
        telnet_reader: Union[TelnetReader, TelnetReaderUnicode],
        telnet_writer: Union[TelnetWriter, TelnetWriterUnicode],
    ) -> None:
        """Win32 telnet client shell (not implemented)."""
        raise NotImplementedError("win32 not yet supported as telnet client. Please contribute!")

else:
    import os
    import signal
    import termios

    class Terminal:
        """
        Context manager for terminal mode handling on POSIX systems.

        When sys.stdin is attached to a terminal, it is configured for the matching telnet modes
        negotiated for the given telnet_writer.
        """

        ModeDef = collections.namedtuple(
            "ModeDef", ["iflag", "oflag", "cflag", "lflag", "ispeed", "ospeed", "cc"]
        )

        def __init__(self, telnet_writer: Union[TelnetWriter, TelnetWriterUnicode]) -> None:
            self.telnet_writer = telnet_writer
            self._fileno = sys.stdin.fileno()
            self._istty = os.path.sameopenfile(0, 1)
            self._save_mode: Optional[Terminal.ModeDef] = None
            self.software_echo = False
            self._remove_winch = False
            self._resize_pending = threading.Event()
            self.on_resize: Optional[Callable[[int, int], None]] = None
            self._stdin_transport: Optional[asyncio.BaseTransport] = None

        def setup_winch(self) -> None:
            """Register SIGWINCH handler to set ``_resize_pending`` flag."""
            if not self._istty or not hasattr(signal, "SIGWINCH"):
                return
            try:
                loop = asyncio.get_event_loop()

                def _on_winch() -> None:
                    self._resize_pending.set()

                loop.add_signal_handler(signal.SIGWINCH, _on_winch)
                self._remove_winch = True
            except Exception:
                self._remove_winch = False

        def cleanup_winch(self) -> None:
            """Remove SIGWINCH handler."""
            if self._istty and self._remove_winch:
                try:
                    asyncio.get_event_loop().remove_signal_handler(signal.SIGWINCH)
                except Exception:
                    pass
                self._remove_winch = False

        def __enter__(self) -> "Terminal":
            self._save_mode = self.get_mode()
            if self._istty:
                assert self._save_mode is not None
                self.set_mode(self.determine_mode(self._save_mode))
            return self

        def __exit__(self, *_: Any) -> None:
            self.cleanup_winch()
            if self._istty:
                assert self._save_mode is not None
                termios.tcsetattr(self._fileno, termios.TCSADRAIN, list(self._save_mode))

        def get_mode(self) -> Optional["Terminal.ModeDef"]:
            """Return current terminal mode if attached to a tty, otherwise None."""
            if self._istty:
                return self.ModeDef(*termios.tcgetattr(self._fileno))
            return None

        def set_mode(self, mode: "Terminal.ModeDef") -> None:
            """Set terminal mode attributes."""
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, list(mode))

        @staticmethod
        def _suppress_echo(mode: "Terminal.ModeDef") -> "Terminal.ModeDef":
            """Return copy of *mode* with local ECHO disabled, keeping ICANON."""
            return Terminal.ModeDef(
                iflag=mode.iflag,
                oflag=mode.oflag,
                cflag=mode.cflag,
                lflag=mode.lflag & ~termios.ECHO,
                ispeed=mode.ispeed,
                ospeed=mode.ospeed,
                cc=mode.cc,
            )

        def _make_raw(
            self, mode: "Terminal.ModeDef", suppress_echo: bool = True
        ) -> "Terminal.ModeDef":
            """
            Return copy of *mode* with raw terminal attributes set.

            :param suppress_echo: When True, disable local ECHO (server echoes). When False, keep
                  local ECHO enabled (character-at-a-time with local echo, e.g. SGA without ECHO).
            """
            iflag = mode.iflag & ~(
                termios.BRKINT | termios.ICRNL | termios.INPCK | termios.ISTRIP | termios.IXON
            )
            cflag = mode.cflag & ~(termios.CSIZE | termios.PARENB)
            cflag = cflag | termios.CS8
            lflag_mask = termios.ICANON | termios.IEXTEN | termios.ISIG
            if suppress_echo:
                lflag_mask |= termios.ECHO
            lflag = mode.lflag & ~lflag_mask
            oflag = mode.oflag & ~(termios.OPOST | termios.ONLCR)
            cc = list(mode.cc)
            cc[termios.VMIN] = 1
            cc[termios.VTIME] = 0
            return self.ModeDef(
                iflag=iflag,
                oflag=oflag,
                cflag=cflag,
                lflag=lflag,
                ispeed=mode.ispeed,
                ospeed=mode.ospeed,
                cc=cc,
            )

        def _server_will_sga(self) -> bool:
            """Whether server has negotiated WILL SGA."""
            from .telopt import SGA

            return bool(self.telnet_writer.client and self.telnet_writer.remote_option.enabled(SGA))

        def check_auto_mode(
            self, switched_to_raw: bool, last_will_echo: bool
        ) -> "tuple[bool, bool, bool] | None":
            """
            Check if auto-mode switching is needed.

            :param switched_to_raw: Whether terminal has already switched to raw mode.
            :param last_will_echo: Previous value of server's WILL ECHO state.
            :returns: ``(switched_to_raw, last_will_echo, local_echo)`` tuple
                if mode changed, or ``None`` if no change needed.
            """
            if not self._istty:
                return None
            wecho = self.telnet_writer.will_echo
            wsga = self._server_will_sga()
            # WILL ECHO alone = line mode with server echo (suppress local echo)
            # WILL SGA (with or without ECHO) = raw/character-at-a-time
            should_go_raw = not switched_to_raw and wsga
            should_suppress_echo = not switched_to_raw and wecho and not wsga
            echo_changed = switched_to_raw and wecho != last_will_echo
            if not (should_go_raw or should_suppress_echo or echo_changed):
                return None
            assert self._save_mode is not None
            if should_suppress_echo:
                self.set_mode(self._suppress_echo(self._save_mode))
                self.telnet_writer.log.debug(
                    "auto: server echo without SGA, line mode (server WILL ECHO)"
                )
                return (False, wecho, False)
            self.set_mode(self._make_raw(self._save_mode, suppress_echo=True))
            self.telnet_writer.log.debug(
                "auto: %s (server %s ECHO)",
                (
                    "switching to raw mode"
                    if should_go_raw
                    else ("disabling" if wecho else "enabling") + " software echo"
                ),
                "WILL" if wecho else "WONT",
            )
            return (True if should_go_raw else switched_to_raw, wecho, not wecho)

        def determine_mode(self, mode: "Terminal.ModeDef") -> "Terminal.ModeDef":
            """
            Return copy of 'mode' with changes suggested for telnet connection.

            Auto mode (``_raw_mode is None``): follows the server's negotiation.

            =================  ========  ==========  ================================
            Server negotiates  ICANON    ECHO        Behavior
            =================  ========  ==========  ================================
            Nothing            on        on          Line mode, local echo
            WILL SGA only      **off**   on          Character-at-a-time, local echo
            WILL ECHO only     on        **off**     Line mode, server echoes
            WILL SGA + ECHO    **off**   **off**     Full kludge mode (most common)
            =================  ========  ==========  ================================
            """
            raw_mode = _get_raw_mode(self.telnet_writer)
            will_echo = self.telnet_writer.will_echo
            will_sga = self._server_will_sga()
            # Auto mode (None): follow server negotiation
            if raw_mode is None:
                if will_echo and will_sga:
                    self.telnet_writer.log.debug("auto: server echo + SGA, kludge mode")
                    return self._make_raw(mode)
                if will_echo:
                    self.telnet_writer.log.debug("auto: server echo without SGA, line mode")
                    return self._suppress_echo(mode)
                if will_sga:
                    self.telnet_writer.log.debug("auto: SGA without echo, character-at-a-time")
                    self.software_echo = True
                    return self._make_raw(mode, suppress_echo=True)
                self.telnet_writer.log.debug("auto: no server echo yet, line mode")
                return mode
            # Explicit line mode (False)
            if not raw_mode:
                self.telnet_writer.log.debug("local echo, linemode")
                return mode
            # Explicit raw mode (True)
            if not will_echo:
                self.telnet_writer.log.debug("raw mode forced, no server echo")
            else:
                self.telnet_writer.log.debug("server echo, kludge mode")
            return self._make_raw(mode)

        async def make_stdout(self) -> asyncio.StreamWriter:
            """
            Return an asyncio StreamWriter for local terminal output.

            This does **not** connect stdin -- call :meth:`connect_stdin`
            separately when an asyncio stdin reader is needed (the REPL
            manages its own stdin via blessed async_inkey).
            """
            write_fobj = sys.stdout
            if self._istty:
                write_fobj = sys.stdin
            loop = asyncio.get_event_loop()
            writer_transport, writer_protocol = await loop.connect_write_pipe(
                asyncio.streams.FlowControlMixin, write_fobj
            )
            return asyncio.StreamWriter(writer_transport, writer_protocol, None, loop)

        async def connect_stdin(self) -> asyncio.StreamReader:
            """
            Connect sys.stdin to an asyncio StreamReader.

            Must be called **after** any REPL session has finished, because the REPL and asyncio
            cannot both own the stdin file descriptor at the same time.
            """
            reader = asyncio.StreamReader()
            reader_protocol = asyncio.StreamReaderProtocol(reader)
            transport, _ = await asyncio.get_event_loop().connect_read_pipe(
                lambda: reader_protocol, sys.stdin
            )
            self._stdin_transport = transport
            return reader

        def disconnect_stdin(self, reader: asyncio.StreamReader) -> None:
            """Disconnect stdin pipe so the REPL can reclaim it."""
            transport = getattr(self, "_stdin_transport", None)
            if transport is not None:
                transport.close()
                self._stdin_transport = None
            reader.feed_eof()

        async def make_stdio(self) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
            """Return (reader, writer) pair for sys.stdin, sys.stdout."""
            stdout = await self.make_stdout()
            stdin = await self.connect_stdin()
            return stdin, stdout

    def _transform_output(
        out: str, writer: Union[TelnetWriter, TelnetWriterUnicode], in_raw_mode: bool
    ) -> str:
        r"""
        Apply color filter, ASCII EOL substitution, and CRLF normalization.

        :param out: Server output text to transform.
        :param writer: Telnet writer (``ctx`` provides color filter and ascii_eol).
        :param in_raw_mode: When ``True``, normalize line endings to ``\r\n``.
        :returns: Transformed output string.
        """
        ctx: TelnetSessionContext = writer.ctx
        cf = ctx.color_filter
        if cf is not None:
            out = cf.filter(out)
        if ctx.ascii_eol:
            out = out.replace(_ATASCII_CR_CHAR, "\r").replace(_ATASCII_LF_CHAR, "\n")
        if in_raw_mode:
            out = out.replace("\r\n", "\n").replace("\n", "\r\n")
        else:
            # Cooked mode: PTY ONLCR converts \n -> \r\n, so strip \r before \n
            # to avoid doubling (\r\n -> \r\r\n).
            out = out.replace("\r\n", "\n")
        return out

    def _send_stdin(
        inp: bytes,
        telnet_writer: Union[TelnetWriter, TelnetWriterUnicode],
        stdout: asyncio.StreamWriter,
        local_echo: bool,
    ) -> "tuple[Optional[asyncio.Task[None]], bool]":
        """
        Send stdin input to server and optionally echo locally.

        :param inp: Raw bytes from terminal stdin.
        :param telnet_writer: Telnet writer for sending to server.
        :param stdout: Local stdout writer for software echo.
        :param local_echo: When ``True``, echo input bytes to stdout.
        :returns: ``(esc_timer_task_or_None, has_pending)`` tuple.
        """
        ctx: TelnetSessionContext = telnet_writer.ctx
        inf = ctx.input_filter
        pending = False
        new_timer: Optional[asyncio.Task[None]] = None
        if inf is not None:
            translated = inf.feed(inp)
            if translated:
                telnet_writer._write(translated)
            if inf.has_pending:
                pending = True
                new_timer = asyncio.ensure_future(asyncio.sleep(inf.esc_delay))
        else:
            telnet_writer._write(inp)
        if local_echo:
            echo_buf = bytearray()
            for b in inp:
                if b in (0x7F, 0x08):
                    echo_buf.extend(b"\b \b")
                elif b == 0x0D:
                    echo_buf.extend(b"\r\n")
                elif b >= 0x20:
                    echo_buf.append(b)
            if echo_buf:
                stdout.write(bytes(echo_buf))
        return new_timer, pending

    def _get_raw_mode(writer: Union[TelnetWriter, TelnetWriterUnicode]) -> "bool | None":
        """Return the writer's ``ctx.raw_mode`` (``None``, ``True``, or ``False``)."""
        return writer.ctx.raw_mode

    def _flush_color_filter(
        writer: Union[TelnetWriter, TelnetWriterUnicode], stdout: asyncio.StreamWriter
    ) -> None:
        """Flush any pending color filter output to stdout."""
        cf = writer.ctx.color_filter
        if cf is not None:
            flush = cf.flush()
            if flush:
                stdout.write(flush.encode())

    def _ensure_autoreply_engine(
        telnet_writer: Union[TelnetWriter, TelnetWriterUnicode],
    ) -> "Optional[Any]":
        """Return the autoreply engine from the writer's context, if set."""
        return telnet_writer.ctx.autoreply_engine

    async def _raw_event_loop(
        telnet_reader: Union[TelnetReader, TelnetReaderUnicode],
        telnet_writer: Union[TelnetWriter, TelnetWriterUnicode],
        tty_shell: "Terminal",
        stdin: asyncio.StreamReader,
        stdout: asyncio.StreamWriter,
        keyboard_escape: str,
        state: _RawLoopState,
        handle_close: Callable[[str], None],
        want_repl: Callable[[], bool],
    ) -> None:
        """Standard byte-at-a-time event loop (mutates *state* in-place)."""
        stdin_task = accessories.make_reader_task(stdin)
        telnet_task = accessories.make_reader_task(telnet_reader, size=2**24)
        esc_timer_task: Optional[asyncio.Task[None]] = None
        wait_for: set[asyncio.Task[Any]] = {stdin_task, telnet_task}

        while wait_for:
            done, _ = await asyncio.wait(wait_for, return_when=asyncio.FIRST_COMPLETED)
            if stdin_task in done:
                task = stdin_task
                done.discard(task)
            else:
                task = done.pop()
            wait_for.discard(task)

            telnet_writer.log.log(TRACE, "task=%s, wait_for=%s", task, wait_for)

            # ESC_DELAY timer fired -- flush buffered partial sequence
            if task is esc_timer_task:
                esc_timer_task = None
                inf = telnet_writer.ctx.input_filter
                if inf is not None and inf.has_pending:
                    flushed = inf.flush()
                    if flushed:
                        telnet_writer._write(flushed)
                continue

            # client input
            if task == stdin_task:
                if esc_timer_task is not None and esc_timer_task in wait_for:
                    esc_timer_task.cancel()
                    wait_for.discard(esc_timer_task)
                    esc_timer_task = None
                inp = task.result()
                if not inp:
                    telnet_writer.log.debug("EOF from client stdin")
                    continue
                if keyboard_escape in inp.decode():
                    try:
                        telnet_writer.close()
                    except Exception:
                        pass
                    if telnet_task in wait_for:
                        telnet_task.cancel()
                        wait_for.remove(telnet_task)
                    handle_close("Connection closed.")
                    break
                new_timer, has_pending = _send_stdin(inp, telnet_writer, stdout, state.local_echo)
                if has_pending and esc_timer_task not in wait_for:
                    esc_timer_task = new_timer
                    if esc_timer_task is not None:
                        wait_for.add(esc_timer_task)
                stdin_task = accessories.make_reader_task(stdin)
                wait_for.add(stdin_task)

            # server output
            elif task == telnet_task:
                out = task.result()
                if not out and telnet_reader.at_eof():
                    if stdin_task in wait_for:
                        stdin_task.cancel()
                        wait_for.remove(stdin_task)
                    handle_close("Connection closed by foreign host.")
                    continue
                raw_mode = _get_raw_mode(telnet_writer)
                in_raw = raw_mode is True or (raw_mode is None and state.switched_to_raw)
                out = _transform_output(out, telnet_writer, in_raw)
                ar_engine = _ensure_autoreply_engine(telnet_writer)
                if ar_engine is not None:
                    ar_engine.feed(out)
                if raw_mode is None:
                    mode_result = tty_shell.check_auto_mode(
                        state.switched_to_raw, state.last_will_echo
                    )
                    if mode_result is not None:
                        if not state.switched_to_raw:
                            state.linesep = "\r\n"
                        state.switched_to_raw, state.last_will_echo, state.local_echo = mode_result
                        # When transitioning cooked -> raw, the data was
                        # processed for ONLCR (\r\n -> \n) but the terminal
                        # now has ONLCR disabled.  Re-normalize so bare \n
                        # becomes \r\n for correct display.
                        if state.switched_to_raw and not in_raw:
                            out = out.replace("\n", "\r\n")
                    if want_repl():
                        state.reactivate_repl = True
                stdout.write(out.encode())
                _ts_file = telnet_writer.ctx.typescript_file
                if _ts_file is not None:
                    _ts_file.write(out)
                    _ts_file.flush()
                if state.reactivate_repl:
                    telnet_writer.log.debug("mode returned to local, reactivating REPL")
                    if stdin_task in wait_for:
                        stdin_task.cancel()
                        wait_for.discard(stdin_task)
                    state.switched_to_raw = False
                    break
                telnet_task = accessories.make_reader_task(telnet_reader, size=2**24)
                wait_for.add(telnet_task)

    async def telnet_client_shell(
        telnet_reader: Union[TelnetReader, TelnetReaderUnicode],
        telnet_writer: Union[TelnetWriter, TelnetWriterUnicode],
    ) -> None:
        """
        Minimal telnet client shell for POSIX terminals.

        This shell performs minimal tty mode handling when a terminal is attached to standard in
        (keyboard), notably raw mode is often set and this shell may exit only by disconnect from
        server, or the escape character, ^].

        stdin or stdout may also be a pipe or file, behaving much like nc(1).
        """
        keyboard_escape = "\x1d"

        with Terminal(telnet_writer=telnet_writer) as tty_shell:
            linesep = "\n"
            switched_to_raw = False
            last_will_echo = False
            local_echo = tty_shell.software_echo
            if tty_shell._istty:
                raw_mode = _get_raw_mode(telnet_writer)
                if telnet_writer.will_echo or raw_mode is True:
                    linesep = "\r\n"
            stdout = await tty_shell.make_stdout()
            tty_shell.setup_winch()

            # EOR/GA-based command pacing for raw-mode autoreplies.
            prompt_ready_raw = asyncio.Event()
            prompt_ready_raw.set()
            ga_detected_raw = False

            _sh_ctx: TelnetSessionContext = telnet_writer.ctx

            def _on_prompt_signal_raw(_cmd: bytes) -> None:
                nonlocal ga_detected_raw
                ga_detected_raw = True
                prompt_ready_raw.set()
                ar = _sh_ctx.autoreply_engine
                if ar is not None:
                    ar.on_prompt()

            from .telopt import GA, CMD_EOR

            telnet_writer.set_iac_callback(GA, _on_prompt_signal_raw)
            telnet_writer.set_iac_callback(CMD_EOR, _on_prompt_signal_raw)

            async def _wait_for_prompt_raw() -> None:
                if not ga_detected_raw:
                    return
                try:
                    await asyncio.wait_for(prompt_ready_raw.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pass
                prompt_ready_raw.clear()

            _sh_ctx.autoreply_wait_fn = _wait_for_prompt_raw

            escape_name = accessories.name_unicode(keyboard_escape)
            banner_sep = "\r\n" if tty_shell._istty else linesep
            stdout.write(f"Escape character is '{escape_name}'.{banner_sep}".encode())

            def _handle_close(msg: str) -> None:
                _flush_color_filter(telnet_writer, stdout)
                stdout.write(f"\033[m{linesep}{msg}{linesep}".encode())
                tty_shell.cleanup_winch()

            def _want_repl() -> bool:
                return False

            # Standard event loop (byte-at-a-time).
            if not switched_to_raw and tty_shell._istty and tty_shell._save_mode is not None:
                tty_shell.set_mode(tty_shell._make_raw(tty_shell._save_mode, suppress_echo=True))
                switched_to_raw = True
                local_echo = not telnet_writer.will_echo
                linesep = "\r\n"
            stdin = await tty_shell.connect_stdin()
            state = _RawLoopState(
                switched_to_raw=switched_to_raw,
                last_will_echo=last_will_echo,
                local_echo=local_echo,
                linesep=linesep,
            )
            await _raw_event_loop(
                telnet_reader,
                telnet_writer,
                tty_shell,
                stdin,
                stdout,
                keyboard_escape,
                state,
                _handle_close,
                _want_repl,
            )
            tty_shell.disconnect_stdin(stdin)
