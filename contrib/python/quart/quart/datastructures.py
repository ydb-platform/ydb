from __future__ import annotations

from os import PathLike
from pathlib import Path
from typing import IO, Optional

from aiofiles import open as async_open
from werkzeug.datastructures import FileStorage as WerkzeugFileStorage, Headers


class FileStorage(WerkzeugFileStorage):
    """A thin wrapper over incoming files."""

    def __init__(
        self,
        stream: Optional[IO[bytes]] = None,
        filename: Optional[str] = None,
        name: Optional[str] = None,
        content_type: Optional[str] = None,
        content_length: Optional[int] = None,
        headers: Optional[Headers] = None,
    ) -> None:
        super().__init__(stream, filename, name, content_type, content_length, headers)

    async def save(self, destination: PathLike, buffer_size: int = 16384) -> None:  # type: ignore
        """Save the file to the destination.

        Arguments:
            destination: A filename (str) or file object to write to.
            buffer_size: Buffer size to keep in memory.
        """
        async with async_open(destination, "wb") as file_:
            data = self.stream.read(buffer_size)
            while data != b"":
                await file_.write(data)
                data = self.stream.read(buffer_size)

    async def load(self, source: PathLike, buffer_size: int = 16384) -> None:
        path = Path(source)
        self.filename = path.name
        async with async_open(path, "rb") as file_:
            data = await file_.read(buffer_size)
            while data != b"":
                self.stream.write(data)
                data = await file_.read(buffer_size)
