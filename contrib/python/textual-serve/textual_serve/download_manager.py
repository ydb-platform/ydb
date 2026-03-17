from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import logging
from typing import AsyncGenerator, TYPE_CHECKING

if TYPE_CHECKING:
    from textual_serve.app_service import AppService

log = logging.getLogger("textual-serve")

DOWNLOAD_TIMEOUT = 4
DOWNLOAD_CHUNK_SIZE = 1024 * 64  # 64 KB


@dataclass
class Download:
    app_service: "AppService"
    """The app service that the download is associated with."""

    delivery_key: str
    """Key which identifies the download."""

    file_name: str
    """The name of the file to download. This will be used to set
    the Content-Disposition filename."""

    open_method: str
    """The method to open the file with. "browser" or "download"."""

    mime_type: str
    """The mime type of the content."""

    encoding: str | None = None
    """The encoding of the content.
    Will be None if the content is binary.
    """

    name: str | None = None
    """Optional name set bt the client."""

    incoming_chunks: asyncio.Queue[bytes | None] = field(default_factory=asyncio.Queue)
    """A queue of incoming chunks for the download.
    Chunks are sent from the app service to the download handler
    via this queue."""


class DownloadManager:
    """Class which manages downloads for the server.

    Serves as the link between the web server and app processes during downloads.

    A single server has a single download manager, which manages all downloads for all
    running app processes.
    """

    def __init__(self) -> None:
        self._active_downloads: dict[str, Download] = {}
        """A dictionary of active downloads.

        When a delivery key is received in a meta packet, it is added to this set.
        When the user hits the "/download/{key}" endpoint, we ensure the key is in
        this set and start the download by requesting chunks from the app process.

        When the download is complete, the app process sends a "deliver_file_end"
        meta packet, and we remove the key from this set.
        """

    async def create_download(
        self,
        *,
        app_service: "AppService",
        delivery_key: str,
        file_name: str,
        open_method: str,
        mime_type: str,
        encoding: str | None = None,
        name: str | None = None,
    ) -> None:
        """Prepare for a new download.

        Args:
            app_service: The app service to start the download for.
            delivery_key: The delivery key to start the download for.
            file_name: The name of the file to download.
            open_method: The method to open the file with.
            mime_type: The mime type of the content.
            encoding: The encoding of the content or None if the content is binary.
        """
        self._active_downloads[delivery_key] = Download(
            app_service,
            delivery_key,
            file_name,
            open_method,
            mime_type,
            encoding,
            name=name,
        )

    async def download(self, delivery_key: str) -> AsyncGenerator[bytes, None]:
        """Download a file from the given app service.

        Args:
            delivery_key: The delivery key to download.
        """

        app_service = await self._get_app_service(delivery_key)
        download = self._active_downloads[delivery_key]
        incoming_chunks = download.incoming_chunks

        while True:
            # Request a chunk from the app service.
            send_result = await app_service.send_meta(
                {
                    "type": "deliver_chunk_request",
                    "key": delivery_key,
                    "size": DOWNLOAD_CHUNK_SIZE,
                    "name": download.name,
                }
            )

            if not send_result:
                log.warning(
                    "Download {delivery_key!r} failed to request chunk from app service"
                )
                del self._active_downloads[delivery_key]
                break

            try:
                chunk = await asyncio.wait_for(incoming_chunks.get(), DOWNLOAD_TIMEOUT)
            except asyncio.TimeoutError:
                log.warning(
                    "Download %r failed to receive chunk from app service within %r seconds",
                    delivery_key,
                    DOWNLOAD_TIMEOUT,
                )
                chunk = None

            if not chunk:
                # Empty chunk - the app process has finished sending the file
                # or the download has been cancelled.
                incoming_chunks.task_done()
                del self._active_downloads[delivery_key]
                break
            else:
                incoming_chunks.task_done()
                yield chunk

    async def chunk_received(self, delivery_key: str, chunk: bytes | str) -> None:
        """Handle a chunk received from the app service for a download.

        Args:
            delivery_key: The delivery key that the chunk was received for.
            chunk: The chunk that was received.
        """

        download = self._active_downloads.get(delivery_key)
        if not download:
            # The download may have been cancelled - e.g. the websocket
            # was closed before the download could complete.
            log.debug("Chunk received for cancelled download %r", delivery_key)
            return

        if isinstance(chunk, str):
            chunk = chunk.encode(download.encoding or "utf-8")
        await download.incoming_chunks.put(chunk)

    async def _get_app_service(self, delivery_key: str) -> "AppService":
        """Get the app service that the given delivery key is linked to.

        Args:
            delivery_key: The delivery key to get the app service for.
        """
        for key in self._active_downloads.keys():
            if key == delivery_key:
                return self._active_downloads[key].app_service
        else:
            raise ValueError(f"No active download for delivery key {delivery_key!r}")

    async def get_download_metadata(self, delivery_key: str) -> Download:
        """Get the metadata for a download.

        Args:
            delivery_key: The delivery key to get the metadata for.
        """
        return self._active_downloads[delivery_key]

    async def cancel_app_downloads(self, app_service_id: str) -> None:
        """Cancel all downloads for the given app service.

        Args:
            app_service_id: The app service ID to cancel downloads for.
        """
        for download in self._active_downloads.values():
            if download.app_service.app_service_id == app_service_id:
                await download.incoming_chunks.put(None)
