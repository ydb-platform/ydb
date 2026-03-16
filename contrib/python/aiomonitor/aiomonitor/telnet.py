from __future__ import annotations

import asyncio
import collections
import fcntl
import os
import struct
import sys
import termios
from typing import Dict, Optional, TextIO, Tuple

import telnetlib3

ModeDef = collections.namedtuple(
    "ModeDef", ["iflag", "oflag", "cflag", "lflag", "ispeed", "ospeed", "cc"]
)


class TelnetClient:
    """
    A minimal telnet client-side protocol implementation to support `prompt_toolkit`.
    Some details like terminal mode handling is taken from telnetlib3.
    (https://github.com/jquast/telnetlib3)
    """

    def __init__(
        self,
        host: str,
        port: int,
        stdin: Optional[TextIO] = None,
        stdout: Optional[TextIO] = None,
    ) -> None:
        self._host = host
        self._port = port
        self._term = os.environ.get("TERM", "unknown")
        self._stdin = stdin or sys.stdin
        self._stdout = stdout or sys.stdout
        try:
            self._isatty = os.path.sameopenfile(
                self._stdin.fileno(), self._stdout.fileno()
            )
        except (NotImplementedError, ValueError):
            self._isatty = False
        self._remote_options: Dict[bytes, bool] = collections.defaultdict(lambda: False)

    def get_mode(self) -> Optional[ModeDef]:
        if self._isatty:
            try:
                return ModeDef(*termios.tcgetattr(self._stdin.fileno()))
            except termios.error:
                return None
        return None

    def set_mode(self, mode: ModeDef) -> None:
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSAFLUSH, list(mode))

    def restore_mode(self) -> None:
        if self._isatty and self._saved_mode is not None:
            termios.tcsetattr(
                self._stdin.fileno(), termios.TCSAFLUSH, list(self._saved_mode)
            )

    def determine_mode(self, mode: ModeDef) -> ModeDef:
        # Reference: https://github.com/jquast/telnetlib3/blob/b90616f/telnetlib3/client_shell.py#L76
        if not self._remote_options[telnetlib3.ECHO]:
            return mode
        iflag = mode.iflag & ~(
            termios.BRKINT
            | termios.ICRNL  # Do not send INTR signal on break
            | termios.INPCK  # Do not map CR to NL on input
            | termios.ISTRIP  # Disable input parity checking
            | termios.IXON  # Do not strip input characters to 7 bits
        )
        cflag = mode.cflag & ~(termios.CSIZE | termios.PARENB)
        cflag = cflag | termios.CS8
        lflag = mode.lflag & ~(
            termios.ICANON | termios.IEXTEN | termios.ISIG | termios.ECHO
        )
        oflag = mode.oflag & ~(termios.OPOST | termios.ONLCR)
        cc = list(mode.cc)
        cc[termios.VMIN] = 1
        cc[termios.VTIME] = 0
        return ModeDef(
            iflag=iflag,
            oflag=oflag,
            cflag=cflag,
            lflag=lflag,
            ispeed=mode.ispeed,
            ospeed=mode.ospeed,
            cc=cc,
        )

    async def _create_stdio_streams(
        self,
    ) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        loop = asyncio.get_running_loop()
        stdin_reader = asyncio.StreamReader()
        stdin_protocol = asyncio.StreamReaderProtocol(stdin_reader)
        await loop.connect_read_pipe(lambda: stdin_protocol, self._stdin)
        write_target = self._stdin if self._isatty else self._stdout
        writer_transport, writer_protocol = await loop.connect_write_pipe(
            asyncio.streams.FlowControlMixin,
            write_target,
        )
        stdout_writer = asyncio.StreamWriter(
            writer_transport, writer_protocol, stdin_reader, loop
        )
        return stdin_reader, stdout_writer

    async def __aenter__(self) -> TelnetClient:
        self._conn_reader, self._conn_writer = await asyncio.open_connection(
            self._host, self._port
        )
        self._closed = asyncio.Event()
        self._stdin_reader, self._stdout_writer = await self._create_stdio_streams()
        self._recv_task = asyncio.create_task(self._handle_received())
        self._input_task = asyncio.create_task(self._handle_user_input())
        await asyncio.sleep(0.3)  # wait for negotiation to complete
        self._saved_mode = self.get_mode()
        if self._isatty:
            assert self._saved_mode is not None
            self.set_mode(self.determine_mode(self._saved_mode))
        return self

    async def __aexit__(self, *exc_info) -> Optional[bool]:
        try:
            self._input_task.cancel()
            await self._input_task
            self._conn_writer.close()
            try:
                await self._conn_writer.wait_closed()
            except NotImplementedError:
                pass
            self._conn_reader.feed_eof()
            await self._recv_task
            self._stdout_writer.close()
            try:
                await self._stdout_writer.wait_closed()
            except NotImplementedError:
                pass
        finally:
            self.restore_mode()
        return None

    async def interact(self) -> None:
        try:
            await self._closed.wait()
        finally:
            self._closed.set()
            self._conn_writer.write_eof()

    async def _handle_user_input(self) -> None:
        try:
            while True:
                buf = await self._stdin_reader.read(128)
                if not buf:
                    return
                self._conn_writer.write(buf)
                await self._conn_writer.drain()
        except asyncio.CancelledError:
            pass

    async def _handle_nego(self, command: bytes, option: bytes) -> None:
        if command == telnetlib3.DO and option == telnetlib3.TTYPE:
            self._conn_writer.write(telnetlib3.IAC + telnetlib3.WILL + telnetlib3.TTYPE)
            self._conn_writer.write(telnetlib3.IAC + telnetlib3.SB + telnetlib3.TTYPE)
            self._conn_writer.write(telnetlib3.BINARY + self._term.encode("ascii"))
            self._conn_writer.write(telnetlib3.IAC + telnetlib3.SE)
            await self._conn_writer.drain()
        elif command == telnetlib3.DO and option == telnetlib3.NAWS:
            self._conn_writer.write(telnetlib3.IAC + telnetlib3.WILL + telnetlib3.NAWS)
            self._conn_writer.write(telnetlib3.IAC + telnetlib3.SB + telnetlib3.NAWS)
            fmt = "HHHH"
            buf = b"\x00" * struct.calcsize(fmt)
            try:
                buf = fcntl.ioctl(self._stdin.fileno(), termios.TIOCGWINSZ, buf)
                rows, cols, _, _ = struct.unpack(fmt, buf)
                self._conn_writer.write(struct.pack(">HH", cols, rows))
                self._conn_writer.write(telnetlib3.IAC + telnetlib3.SE)
            except OSError:
                rows, cols = 22, 80
            await self._conn_writer.drain()
        elif command == telnetlib3.WILL:
            self._remote_options[option] = True
            if option in (telnetlib3.ECHO, telnetlib3.BINARY, telnetlib3.SGA):
                self._conn_writer.write(telnetlib3.IAC + telnetlib3.DO + option)
                await self._conn_writer.drain()
        elif command == telnetlib3.WONT:
            self._remote_options[option] = False

    async def _handle_sb(self, option: bytes, chunk: bytes) -> None:
        pass

    async def _handle_received(self):
        buf = b""
        try:
            while not self._conn_reader.at_eof():
                buf += await self._conn_reader.read(128)
                while buf:
                    cmd_begin = buf.find(telnetlib3.IAC)
                    if cmd_begin == -1:
                        self._stdout_writer.write(buf)
                        await self._stdout_writer.drain()
                        buf = b""
                    else:
                        if cmd_begin >= len(buf) - 2:
                            buf += await self._conn_reader.readexactly(
                                3 - (len(buf) - cmd_begin),
                            )
                        command = buf[cmd_begin + 1 : cmd_begin + 2]
                        option = buf[cmd_begin + 2 : cmd_begin + 3]
                        buf_before, buf_after = buf[:cmd_begin], buf[cmd_begin + 3 :]
                        self._stdout_writer.write(buf_before)
                        await self._stdout_writer.drain()
                        if command in (
                            telnetlib3.WILL,
                            telnetlib3.WONT,
                            telnetlib3.DO,
                            telnetlib3.DONT,
                        ):
                            await self._handle_nego(command, option)
                        elif command == telnetlib3.SB:
                            subnego_end = buf_after.find(telnetlib3.IAC + telnetlib3.SE)
                            if subnego_end == -1:
                                subnego_chunk = (
                                    buf_after
                                    + (
                                        await self._conn_reader.readuntil(
                                            telnetlib3.IAC + telnetlib3.SE
                                        )
                                    )[:-2]
                                )
                                buf_after = b""
                            else:
                                subnego_chunk = buf_after[:subnego_end]
                                buf_after = buf_after[subnego_end + 2 :]
                            await self._handle_sb(option, subnego_chunk)
                        buf = buf_after
        finally:
            self._closed.set()
