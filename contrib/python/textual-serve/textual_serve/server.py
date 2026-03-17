from __future__ import annotations

import asyncio
import importlib

import logging
import os
from pathlib import Path
import signal
import sys

from typing import Any

import aiohttp_jinja2
from aiohttp import web
from aiohttp import WSMsgType
from aiohttp.web_runner import GracefulExit
import jinja2

from importlib.metadata import version

from rich.console import Console
from rich.logging import RichHandler
from rich.highlighter import RegexHighlighter

from textual_serve.download_manager import DownloadManager

from .app_service import AppService

log = logging.getLogger("textual-serve")

LOGO = r"""[bold magenta]___ ____ _  _ ___ _  _ ____ _       ____ ____ ____ _  _ ____ 
 |  |___  \/   |  |  | |__| |    __ [__  |___ |__/ |  | |___ 
 |  |___ _/\_  |  |__| |  | |___    ___] |___ |  \  \/  |___ [not bold]VVVVV
""".replace("VVVVV", f"v{version('textual-serve')}")


WINDOWS = sys.platform == "WINDOWS"


class LogHighlighter(RegexHighlighter):
    base_style = "repr."
    highlights = [
        r"(?P<number>(?<!\w)\-?[0-9]+\.?[0-9]*(e[-+]?\d+?)?\b|0x[0-9a-fA-F]*)",
        r"(?P<path>\[.*?\])",
        r"(?<![\\\w])(?P<str>b?'''.*?(?<!\\)'''|b?'.*?(?<!\\)'|b?\"\"\".*?(?<!\\)\"\"\"|b?\".*?(?<!\\)\")",
    ]


def to_int(value: str, default: int) -> int:
    """Convert to an integer, or return a default if that's not possible.

    Args:
        number: A string possibly containing a decimal.
        default: Default value if value can't be decoded.

    Returns:
        Integer.
    """
    try:
        return int(value)
    except ValueError:
        return default


class Server:
    """Serve a Textual app."""

    def __init__(
        self,
        command: str,
        host: str = "localhost",
        port: int = 8000,
        title: str | None = None,
        public_url: str | None = None,
        statics_path: str | os.PathLike = "./static",
        templates_path: str | os.PathLike = "./templates",
    ):
        """

        Args:
            app_factory: A callable that returns a new App instance.
            host: Host of web application.
            port: Port for server.
            statics_path: Path to statics folder. May be absolute or relative to server.py.
            templates_path" Path to templates folder. May be absolute or relative to server.py.
        """
        self.command = command
        self.host = host
        self.port = port
        self.title = title or command
        self.debug = False

        if public_url is None:
            if self.port == 80:
                self.public_url = f"http://{self.host}"
            elif self.port == 443:
                self.public_url = f"https://{self.host}"
            else:
                self.public_url = f"http://{self.host}:{self.port}"
        else:
            self.public_url = public_url

        base_path = importlib.resources.files(__package__)
        self.statics_path = base_path / statics_path
        self.templates_path = templates_path
        self.console = Console()
        self.download_manager = DownloadManager()

    def initialize_logging(self) -> None:
        """Initialize logging.

        May be overridden in a subclass.
        """
        FORMAT = "%(message)s"
        logging.basicConfig(
            level="DEBUG" if self.debug else "INFO",
            format=FORMAT,
            datefmt="[%X]",
            handlers=[
                RichHandler(
                    show_path=False,
                    show_time=False,
                    rich_tracebacks=True,
                    tracebacks_show_locals=True,
                    highlighter=LogHighlighter(),
                    console=self.console,
                )
            ],
        )

    def request_exit(self) -> None:
        """Gracefully exit the app."""
        raise GracefulExit()

    async def _make_app(self) -> web.Application:
        """Make the aiohttp web.Application.

        Returns:
            New aiohttp web application.
        """
        app = web.Application()

        aiohttp_jinja2.setup(app, loader=jinja2.PackageLoader(__package__, self.templates_path))

        ROUTES = [
            web.get("/", self.handle_index, name="index"),
            web.get("/ws", self.handle_websocket, name="websocket"),
            web.get("/download/{key}", self.handle_download, name="download"),
            web.static("/static", self.statics_path, show_index=True, name="static"),
        ]
        app.add_routes(ROUTES)

        app.on_startup.append(self.on_startup)
        app.on_shutdown.append(self.on_shutdown)
        return app

    async def handle_download(self, request: web.Request) -> web.StreamResponse:
        """Handle a download request."""
        key = request.match_info["key"]

        try:
            download_meta = await self.download_manager.get_download_metadata(key)
        except KeyError:
            raise web.HTTPNotFound(text=f"Download with key {key!r} not found")

        response = web.StreamResponse()
        mime_type = download_meta.mime_type

        content_type = mime_type
        if download_meta.encoding:
            content_type += f"; charset={download_meta.encoding}"

        response.headers["Content-Type"] = content_type
        disposition = (
            "inline" if download_meta.open_method == "browser" else "attachment"
        )
        response.headers["Content-Disposition"] = (
            f"{disposition}; filename={download_meta.file_name}"
        )

        await response.prepare(request)

        async for chunk in self.download_manager.download(key):
            await response.write(chunk)

        await response.write_eof()
        return response

    async def on_shutdown(self, app: web.Application) -> None:
        """Called on shutdown.

        Args:
            app: App instance.
        """

    async def on_startup(self, app: web.Application) -> None:
        """Called on startup.

        Args:
            app: App instance.
        """

        self.console.print(LOGO, highlight=False)
        self.console.print(f"Serving {self.command!r} on {self.public_url}")
        self.console.print("\n[cyan]Press Ctrl+C to quit")

    def serve(self, debug: bool = False) -> None:
        """Serve the Textual application.

        This will run a local webserver until it is closed with Ctrl+C

        """
        self.debug = debug
        self.initialize_logging()

        try:
            loop = asyncio.get_event_loop()
        except Exception:
            loop = asyncio.new_event_loop()
        try:
            loop.add_signal_handler(signal.SIGINT, self.request_exit)
            loop.add_signal_handler(signal.SIGTERM, self.request_exit)
        except NotImplementedError:
            pass

        if self.debug:
            log.info("Running in debug mode. You may use textual dev tools.")

        web.run_app(
            self._make_app(),
            host=self.host,
            port=self.port,
            handle_signals=False,
            loop=loop,
            print=lambda *args: None,
        )

    @aiohttp_jinja2.template("app_index.html")
    async def handle_index(self, request: web.Request) -> dict[str, Any]:
        """Serves the HTML for an app.

        Args:
            request: Request object.

        Returns:
            Template data.
        """
        router = request.app.router
        font_size = to_int(request.query.get("fontsize", "16"), 16)

        def get_url(route: str, **args) -> str:
            """Get a URL from the aiohttp router."""
            path = router[route].url_for(**args)
            return f"{self.public_url}{path}"

        def get_websocket_url(route: str, **args) -> str:
            """Get a URL with a websocket prefix."""
            url = get_url(route, **args)

            if self.public_url.startswith("https"):
                return "wss:" + url.split(":", 1)[1]
            else:
                return "ws:" + url.split(":", 1)[1]

        context = {
            "font_size": font_size,
            "app_websocket_url": get_websocket_url("websocket"),
        }
        context["config"] = {
            "static": {
                "url": get_url("static", filename="/").rstrip("/") + "/",
            },
        }
        context["application"] = {
            "name": self.title,
        }
        return context

    async def _process_messages(
        self, websocket: web.WebSocketResponse, app_service: AppService
    ) -> None:
        """Process messages from the client browser websocket.

        Args:
            websocket: Websocket instance.
            app_service: App service.
        """
        TEXT = WSMsgType.TEXT

        async for message in websocket:
            if message.type != TEXT:
                continue
            envelope = message.json()
            assert isinstance(envelope, list)
            type_ = envelope[0]
            if type_ == "stdin":
                data = envelope[1]
                await app_service.send_bytes(data.encode("utf-8"))
            elif type_ == "resize":
                data = envelope[1]
                await app_service.set_terminal_size(data["width"], data["height"])
            elif type_ == "ping":
                data = envelope[1]
                await websocket.send_json(["pong", data])
            elif type_ == "blur":
                await app_service.blur()
            elif type_ == "focus":
                await app_service.focus()

    async def handle_websocket(self, request: web.Request) -> web.WebSocketResponse:
        """Handle the websocket that drives the remote process.

        This is called when the browser connects to the websocket.

        Args:
            request: Request object.

        Returns:
            Websocket response.
        """
        websocket = web.WebSocketResponse(heartbeat=15)

        width = to_int(request.query.get("width", "80"), 80)
        height = to_int(request.query.get("height", "24"), 24)

        app_service: AppService | None = None
        try:
            await websocket.prepare(request)
            app_service = AppService(
                self.command,
                write_bytes=websocket.send_bytes,
                write_str=websocket.send_str,
                close=websocket.close,
                download_manager=self.download_manager,
                debug=self.debug,
            )
            await app_service.start(width, height)
            try:
                await self._process_messages(websocket, app_service)
            finally:
                await app_service.stop()

        except asyncio.CancelledError:
            await websocket.close()

        except Exception as error:
            log.exception(error)

        finally:
            if app_service is not None:
                await app_service.stop()

        return websocket
