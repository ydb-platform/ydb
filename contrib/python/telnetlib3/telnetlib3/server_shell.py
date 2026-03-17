"""Telnet server shell implementations."""

from __future__ import annotations

# std imports
import types
import asyncio
from typing import Union, Optional, Generator, cast

# 3rd party
from wcwidth import wcswidth as _wcswidth
from wcwidth import iter_graphemes_reverse as _iter_graphemes_reverse
from wcwidth.escape_sequences import ZERO_WIDTH_PATTERN as _ZERO_WIDTH_PATTERN

# local
from . import slc, telopt, accessories
from .stream_reader import TelnetReader, TelnetReaderUnicode
from .stream_writer import TelnetWriter, TelnetWriterUnicode

CR, LF, NUL = ("\r", "\n", "\x00")
ESC = "\x1b"

# Characters after ESC that start multi-byte output sequences.
# Fe sequences (ESC + 0x40-0x5F) that overlap with these starters
# are not matched as 2-byte sequences, avoiding premature consumption
# of a CSI, OSC, DCS, APC, PM, or charset designation start.
_SEQ_STARTERS = frozenset("[])P_^(")

# SS3 (ESC O) introduces 3-byte input sequences: F1-F4 (ESC O P-S),
# application-mode arrows (ESC O A-D), and keypad keys (ESC O j-y, M, X).
# wcwidth's ZERO_WIDTH_PATTERN matches ESC O as a 2-byte Fe sequence
# (0x4F is in Fe range 0x40-0x5F), missing the third byte.  We handle
# SS3 explicitly since it is an input sequence, not an output sequence.
_SS3 = "O"


async def filter_ansi(reader: TelnetReaderUnicode, _writer: TelnetWriterUnicode) -> str:
    """
    Read and return the next non-ANSI-escape character from reader.

    When wcwidth is available, handles CSI, OSC, DCS, APC, PM, charset designation, Fe, Fp, and SS3
    sequences. Otherwise falls back to CSI and SS3 only.
    """
    while True:
        char = await reader.read(1)
        if not char:
            return ""
        if char != ESC:
            return char

        next_char = await reader.read(1)
        if not next_char:
            return ""

        # SS3: ESC O + one final byte (F1-F4, keypad, app-mode arrows).
        # Handled before wcwidth's ZERO_WIDTH_PATTERN which would match
        # ESC O as a 2-byte Fe sequence, missing the third byte.
        if next_char == _SS3:
            await reader.read(1)
            continue

        buf = ESC + next_char
        if next_char in _SEQ_STARTERS:
            # Multi-byte: CSI, OSC, DCS, APC, PM, or charset
            while len(buf) < 256:
                seq_char = await reader.read(1)
                if not seq_char:
                    break
                buf += seq_char
                match = _ZERO_WIDTH_PATTERN.match(buf)
                # Skip spurious 2-byte Fe matches on the
                # ESC+starter prefix -- the real sequence is
                # longer (CSI 3+, charset 3, OSC/DCS/APC/PM 4+)
                if match and match.end() > 2:
                    if match.end() < len(buf):
                        return buf[match.end()]
                    break
        else:
            # Check for 2-byte Fe/Fp sequence
            match = _ZERO_WIDTH_PATTERN.match(buf)
            if not match:
                return next_char


def _backspace_grapheme(command: str) -> tuple[str, str]:
    """Remove last grapheme cluster, return (new_command, echo_str)."""
    if not command:
        return command, ""
    last = next(_iter_graphemes_reverse(command))
    new_command = command[: len(command) - len(last)]
    w = int(_wcswidth(last))
    w = max(w, 1)
    return new_command, "\b \b" * w


def _visible_width(text: str) -> int:
    """Return visible display width of text."""
    result = int(_wcswidth(text))
    return max(0, result)


class _LineEditor:
    """Shared line-editing state machine for readline and readline_async."""

    def __init__(self, max_visible_width: int = 0) -> None:
        self.command: str = ""
        self.last_char: str = ""
        self.max_visible_width: int = max_visible_width

    def feed(self, char: str) -> tuple[str, Optional[str]]:
        """Feed one character, return (echo_str, command_or_none)."""
        # LF/NUL after CR: silently consume
        if char in (LF, NUL) and self.last_char == CR:
            self.last_char = char
            return "", None

        # Line terminator (CR or LF)
        if char in (CR, LF):
            self.last_char = char
            cmd = self.command
            self.command = ""
            return "", cmd

        # Backspace
        if char in ("\b", "\x7f"):
            self.last_char = char
            if self.command:
                self.command, echo = _backspace_grapheme(self.command)
                return echo, None
            return "", None

        # Regular character -- check max_visible_width
        self.last_char = char
        if self.max_visible_width and _visible_width(self.command + char) > self.max_visible_width:
            return "", None
        self.command += char
        return char, None


__all__ = ("telnet_server_shell", "readline_async", "readline")


async def telnet_server_shell(
    reader: Union[TelnetReader, TelnetReaderUnicode],
    writer: Union[TelnetWriter, TelnetWriterUnicode],
) -> None:
    """
    A default telnet shell, appropriate for use with telnetlib3.create_server.

    This shell provides a very simple REPL, allowing introspection and state toggling of the
    connected client session.
    """
    _reader = cast(TelnetReaderUnicode, reader)
    writer = cast(TelnetWriterUnicode, writer)

    ssl_obj = writer.get_extra_info("ssl_object")
    if ssl_obj is not None:
        version = ssl_obj.version() or "TLS"
        writer.write(f"Ready (secure: {version})." + CR + LF)
    else:
        writer.write("Ready." + CR + LF)

    command = None
    while not writer.is_closing():
        if command:
            writer.write(CR + LF)
        writer.write("tel:sh> ")
        if not getattr(writer.protocol, "never_send_ga", False):
            writer.send_ga()
        await writer.drain()

        command = await readline_async(_reader, writer)
        if command is None:
            return
        writer.write(CR + LF)

        if command == "quit":
            # server hangs up on client
            writer.write("Goodbye." + CR + LF)
            break
        if command == "help":
            writer.write("quit, writer, slc, toggle [option|all], reader, proto, dump")
        elif command == "writer":
            # show 'writer' status
            writer.write(repr(writer))
        elif command == "reader":
            # show 'reader' status
            writer.write(repr(reader))
        elif command == "proto":
            # show 'proto' details of writer
            writer.write(repr(writer.protocol))
        elif command == "version":
            writer.write(accessories.get_version())
        elif command == "slc":
            # show 'slc' support and data tables
            writer.write(get_slcdata(writer))
        elif command.startswith("toggle"):
            # toggle specified options
            option = command[len("toggle ") :] or None
            writer.write(do_toggle(writer, option))
        elif command.startswith("dump"):
            # dump [kb] [ms_delay] [drain|nodrain] [close|noclose]
            #
            # this allows you to experiment with the effects of
            # 'drain', and, some longer-running programs that check
            # for early break through writer.is_closing().
            try:
                kb_limit = int(command.split()[1])
            except (ValueError, IndexError):
                kb_limit = 1000
            try:
                delay = int(float(command.split()[2]) / 1000)
            except (ValueError, IndexError):
                delay = 0
            # experiment with large sizes and 'nodrain', the server
            # pretty much locks up and stops talking to new clients.
            try:
                drain = command.split()[3].lower() == "nodrain"
            except IndexError:
                drain = True
            try:
                do_close = command.split()[4].lower() == "close"
            except IndexError:
                do_close = False
            msg = f"kb_limit={kb_limit}, delay={delay}," f" drain={drain}, do_close={do_close}:\r\n"
            writer.write(msg)
            for lineout in character_dump(kb_limit):
                if writer.is_closing():
                    break
                writer.write(lineout)
                if drain:
                    await writer.drain()
                if delay:
                    await asyncio.sleep(delay)

            if not writer.is_closing():
                writer.write(f"\r\n{kb_limit} OK")
            if do_close:
                break
        elif command:
            writer.write("no such command.")
    writer.close()


def character_dump(kb_limit: int) -> Generator[str, None, None]:
    """Generate character dump output up to kb_limit kilobytes."""
    num_bytes = 0
    while (num_bytes) < (kb_limit * 1024):
        for char in ("/", "\\"):
            lineout = (char * 80) + "\033[1G"
            yield lineout
            num_bytes += len(lineout)
    yield "\033[1G" + "wrote " + str(num_bytes) + " bytes"


@types.coroutine
def readline(
    _reader: Union[TelnetReader, TelnetReaderUnicode],
    writer: Union[TelnetWriter, TelnetWriterUnicode],
    max_visible_width: int = 0,
) -> Generator[Optional[str], str, None]:
    """
    Blocking readline using generator yield/send protocol.

    Characters are fed in via ``send()`` and complete lines are yielded.
    Uses ``_LineEditor`` for grapheme-aware backspace and max_visible_width
    support.
    """
    _writer = cast(TelnetWriterUnicode, writer)
    editor = _LineEditor(max_visible_width=max_visible_width)
    inp = yield None
    while True:
        echo, cmd = editor.feed(inp)
        if echo:
            _writer.echo(echo)
        inp = yield cmd


async def readline_async(
    reader: Union[TelnetReader, TelnetReaderUnicode],
    writer: Union[TelnetWriter, TelnetWriterUnicode],
    max_visible_width: int = 0,
) -> Optional[str]:
    """
    Async readline that filters ANSI escape sequences.

    Uses ``filter_ansi()`` to strip escape sequences and
    ``_LineEditor`` for grapheme-aware backspace and max_visible_width support.
    """
    _reader = cast(TelnetReaderUnicode, reader)
    _writer = cast(TelnetWriterUnicode, writer)
    editor = _LineEditor(max_visible_width=max_visible_width)
    while True:
        next_char = await filter_ansi(_reader, _writer)
        if not next_char:
            return None
        # Skip leading LF/NUL on empty buffer -- accounts for
        # CR+LF pairs split across successive readline_async calls
        if next_char in (LF, NUL) and not editor.command:
            continue
        echo, cmd = editor.feed(next_char)
        if echo:
            _writer.echo(echo)
        if cmd is not None:
            return cmd


readline2 = readline_async


def get_slcdata(writer: Union[TelnetWriter, TelnetWriterUnicode]) -> str:
    """Display Special Line Editing (SLC) characters."""
    _slcs = sorted(
        [
            f"{slc.name_slc_command(slc_func):>15}: {slc_def}"
            for (slc_func, slc_def) in sorted(writer.slctab.items())
            if not (slc_def.nosupport or slc_def.val == slc.theNULL)
        ]
    )
    _unset = sorted(
        [
            slc.name_slc_command(slc_func)
            for (slc_func, slc_def) in sorted(writer.slctab.items())
            if slc_def.val == slc.theNULL
        ]
    )
    _nosupport = sorted(
        [
            slc.name_slc_command(slc_func)
            for (slc_func, slc_def) in sorted(writer.slctab.items())
            if slc_def.nosupport
        ]
    )

    return (
        "Special Line Characters:\r\n"
        + "\r\n".join(_slcs)
        + "\r\nUnset by client: "
        + ", ".join(_unset)
        + "\r\nNot supported by server: "
        + ", ".join(_nosupport)
    )


def do_toggle(writer: Union[TelnetWriter, TelnetWriterUnicode], option: Optional[str]) -> str:
    """Display or toggle telnet session parameters."""
    tbl_opt = {
        "echo": writer.local_option.enabled(telopt.ECHO),
        "goahead": not writer.local_option.enabled(telopt.SGA),
        "outbinary": writer.outbinary,
        "inbinary": writer.inbinary,
        "binary": writer.outbinary and writer.inbinary,
        "xon-any": writer.xon_any,
        "lflow": writer.lflow,
    }

    if not option:
        return "\r\n".join(
            f"{opt} {'ON' if enabled else 'off'}" for opt, enabled in sorted(tbl_opt.items())
        )

    msgs = []
    if option in ("echo", "all"):
        cmd = telopt.WONT if tbl_opt["echo"] else telopt.WILL
        writer.iac(cmd, telopt.ECHO)
        msgs.append(f"{telopt.name_command(cmd).lower()} echo.")

    if option in ("goahead", "all"):
        cmd = telopt.WILL if tbl_opt["goahead"] else telopt.WONT
        writer.iac(cmd, telopt.SGA)
        msgs.append(f"{telopt.name_command(cmd).lower()}" " suppress go-ahead.")

    if option in ("outbinary", "binary", "all"):
        cmd = telopt.WONT if tbl_opt["outbinary"] else telopt.WILL
        writer.iac(cmd, telopt.BINARY)
        msgs.append(f"{telopt.name_command(cmd).lower()} outbinary.")

    if option in ("inbinary", "binary", "all"):
        cmd = telopt.DONT if tbl_opt["inbinary"] else telopt.DO
        writer.iac(cmd, telopt.BINARY)
        msgs.append(f"{telopt.name_command(cmd).lower()} inbinary.")

    if option in ("xon-any", "all"):
        writer.xon_any = not tbl_opt["xon-any"]
        writer.send_lineflow_mode()
        msgs.append(f"xon-any {'en' if writer.xon_any else 'dis'}abled.")

    if option in ("lflow", "all"):
        writer.lflow = not tbl_opt["lflow"]
        writer.send_lineflow_mode()
        msgs.append(f"lineflow {'en' if writer.lflow else 'dis'}abled.")

    if option not in tbl_opt and option != "all":
        msgs.append("toggle: not an option.")

    return "\r\n".join(msgs)
