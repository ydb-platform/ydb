from __future__ import annotations
from pathlib import Path

import asyncio
import io
import json
import os
from typing import Awaitable, Callable, Literal
from asyncio.subprocess import Process
import logging

from importlib.metadata import version
import uuid

from textual_serve.download_manager import DownloadManager
from textual_serve._binary_encode import load as binary_load

log = logging.getLogger("textual-serve")


class AppService:
    """Creates and manages a single Textual app subprocess.

    When a user connects to the websocket in their browser, a new AppService
    instance is created to manage the corresponding Textual app process.
    """

    def __init__(
        self,
        command: str,
        *,
        write_bytes: Callable[[bytes], Awaitable[None]],
        write_str: Callable[[str], Awaitable[None]],
        close: Callable[[], Awaitable[None]],
        download_manager: DownloadManager,
        debug: bool = False,
    ) -> None:
        self.app_service_id: str = uuid.uuid4().hex
        """The unique ID of this running app service."""
        self.command = command
        """The command to launch the Textual app subprocess."""
        self.remote_write_bytes = write_bytes
        """Write bytes to the client browser websocket."""
        self.remote_write_str = write_str
        """Write string to the client browser websocket."""
        self.remote_close = close
        """Close the client browser websocket."""
        self.debug = debug
        """Enable/disable debug mode."""

        self._process: Process | None = None
        self._task: asyncio.Task[None] | None = None
        self._stdin: asyncio.StreamWriter | None = None
        self._exit_event = asyncio.Event()
        self._download_manager = download_manager

    @property
    def stdin(self) -> asyncio.StreamWriter:
        """The processes standard input stream."""
        assert self._stdin is not None
        return self._stdin

    def _build_environment(self, width: int = 80, height: int = 24) -> dict[str, str]:
        """Build an environment dict for the App subprocess.

        Args:
            width: Initial width.
            height: Initial height.

        Returns:
            A environment dict.
        """
        environment = dict(os.environ.copy())
        environment["TEXTUAL_DRIVER"] = "textual.drivers.web_driver:WebDriver"
        environment["TEXTUAL_FPS"] = "60"
        environment["TEXTUAL_COLOR_SYSTEM"] = "truecolor"
        environment["TERM_PROGRAM"] = "textual"
        environment["TERM_PROGRAM_VERSION"] = version("textual-serve")
        environment["COLUMNS"] = str(width)
        environment["ROWS"] = str(height)
        if self.debug:
            environment["TEXTUAL"] = "debug,devtools"
            environment["TEXTUAL_LOG"] = "textual.log"
        return environment

    async def _open_app_process(self, width: int = 80, height: int = 24) -> Process:
        """Open a process to run the app.

        Args:
            width: Width of the terminal.
            height: height of the terminal.
        """
        environment = self._build_environment(width=width, height=height)
        self._process = process = await asyncio.create_subprocess_shell(
            self.command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=environment,
        )
        assert process.stdin is not None
        self._stdin = process.stdin

        return process

    @classmethod
    def encode_packet(cls, packet_type: Literal[b"D", b"M"], payload: bytes) -> bytes:
        """Encode a packet.

        Args:
            packet_type: The packet type (b"D" for data or b"M" for meta)
            payload: The payload.

        Returns:
            Data as bytes.
        """
        return b"%s%s%s" % (packet_type, len(payload).to_bytes(4, "big"), payload)

    async def send_bytes(self, data: bytes) -> bool:
        """Send bytes to process.

        Args:
            data: Data to send.

        Returns:
            True if the data was sent, otherwise False.
        """
        stdin = self.stdin
        try:
            stdin.write(self.encode_packet(b"D", data))
        except RuntimeError:
            return False
        try:
            await stdin.drain()
        except Exception:
            return False
        return True

    async def send_meta(self, data: dict[str, str | None | int | bool]) -> bool:
        """Send meta information to process.

        Args:
            data: Meta dict to send.

        Returns:
            True if the data was sent, otherwise False.
        """
        stdin = self.stdin
        data_bytes = json.dumps(data).encode("utf-8")
        try:
            stdin.write(self.encode_packet(b"M", data_bytes))
        except RuntimeError:
            return False
        try:
            await stdin.drain()
        except Exception:
            return False
        return True

    async def set_terminal_size(self, width: int, height: int) -> None:
        """Tell the process about the new terminal size.

        Args:
            width: Width of terminal in cells.
            height: Height of terminal in cells.
        """
        await self.send_meta(
            {
                "type": "resize",
                "width": width,
                "height": height,
            }
        )

    async def blur(self) -> None:
        """Send an (app) blur to the process."""
        await self.send_meta({"type": "blur"})

    async def focus(self) -> None:
        """Send an (app) focus to the process."""
        await self.send_meta({"type": "focus"})

    async def start(self, width: int, height: int) -> None:
        await self._open_app_process(width, height)
        self._task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        """Stop the process and wait for it to complete."""
        if self._task is not None:
            await self._download_manager.cancel_app_downloads(
                app_service_id=self.app_service_id
            )

            await self.send_meta({"type": "quit"})
            await self._task
            self._task = None

    async def run(self) -> None:
        """Run the Textual app process.

        !!! note

            Do not call this manually, use `start`.

        """
        META = b"M"
        DATA = b"D"
        PACKED = b"P"

        assert self._process is not None
        process = self._process

        stdout = process.stdout
        stderr = process.stderr
        assert stdout is not None
        assert stderr is not None

        stderr_data = io.BytesIO()

        async def read_stderr() -> None:
            """Task to read stderr."""
            try:
                while True:
                    data = await stderr.read(1024 * 4)
                    if not data:
                        break
                    stderr_data.write(data)
            except asyncio.CancelledError:
                pass

        stderr_task = asyncio.create_task(read_stderr())

        try:
            ready = False
            # Wait for prelude text, so we know it is a Textual app
            for _ in range(10):
                if not (line := await stdout.readline()):
                    break
                if line == b"__GANGLION__\n":
                    ready = True
                    break

            if not ready:
                log.error("Application failed to start")
                if error_text := stderr_data.getvalue():
                    import sys

                    sys.stdout.write(error_text.decode("utf-8", "replace"))

            readexactly = stdout.readexactly
            int_from_bytes = int.from_bytes
            while True:
                type_bytes = await readexactly(1)
                size_bytes = await readexactly(4)
                size = int_from_bytes(size_bytes, "big")
                payload = await readexactly(size)
                if type_bytes == DATA:
                    await self.on_data(payload)
                elif type_bytes == META:
                    await self.on_meta(payload)
                elif type_bytes == PACKED:
                    await self.on_packed(payload)

        except asyncio.IncompleteReadError:
            pass
        except ConnectionResetError:
            pass
        except asyncio.CancelledError:
            pass

        finally:
            stderr_task.cancel()
            await stderr_task

        if error_text := stderr_data.getvalue():
            import sys

            sys.stdout.write(error_text.decode("utf-8", "replace"))

    async def on_data(self, payload: bytes) -> None:
        """Called when there is data.

        Args:
            payload: Data received from process.
        """
        await self.remote_write_bytes(payload)

    async def on_meta(self, data: bytes) -> None:
        """Called when there is a meta packet sent from the running app process.

        Args:
            data: Encoded meta data.
        """
        meta_data: dict[str, object] = json.loads(data)
        meta_type = meta_data["type"]

        if meta_type == "exit":
            await self.remote_close()
        elif meta_type == "open_url":
            payload = json.dumps(
                [
                    "open_url",
                    {
                        "url": meta_data["url"],
                        "new_tab": meta_data["new_tab"],
                    },
                ]
            )
            await self.remote_write_str(payload)
        elif meta_type == "deliver_file_start":
            log.debug("deliver_file_start, %s", meta_data)
            try:
                # Record this delivery key as available for download.
                delivery_key = str(meta_data["key"])
                await self._download_manager.create_download(
                    app_service=self,
                    delivery_key=delivery_key,
                    file_name=Path(meta_data["path"]).name,
                    open_method=meta_data["open_method"],
                    mime_type=meta_data["mime_type"],
                    encoding=meta_data["encoding"],
                    name=meta_data.get("name", None),
                )
            except KeyError:
                log.error("Missing key in `deliver_file_start` meta packet")
                return
            else:
                # Tell the browser front-end about the new delivery key,
                # so that it may hit the "/download/{key}" endpoint
                # to start the download.
                json_string = json.dumps(["deliver_file_start", delivery_key])
                await self.remote_write_str(json_string)
        else:
            log.warning(
                f"Unknown meta type: {meta_type!r}. You may need to update `textual-serve`."
            )

    async def on_packed(self, payload: bytes) -> None:
        """Called when there is a packed packet sent from the running app process.

        Args:
            payload: Encoded packed data.
        """
        unpacked = binary_load(payload)
        if unpacked[0] == "deliver_chunk":
            # If we receive a chunk, hand it to the download manager to
            # handle distribution to the browser.
            _, delivery_key, chunk = unpacked
            await self._download_manager.chunk_received(delivery_key, chunk)
