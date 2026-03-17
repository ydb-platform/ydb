"""In-memory filesystem to store ephemeral dependencies.

Warning: Not for external use. May change at any moment. Likely to be made internal.
"""

from __future__ import annotations

import io
import typing

from fsspec import AbstractFileSystem
from fsspec.implementations.memory import MemoryFile, MemoryFileSystem

from .bytes_io_wrapper import BytesIOWrapper


class ModifiedMemoryFileSystem(MemoryFileSystem):
    """In-memory filesystem implementation that uses its own protocol."""

    protocol = ("DUCKDB_INTERNAL_OBJECTSTORE",)
    # defer to the original implementation that doesn't hardcode the protocol
    _strip_protocol: typing.Callable[[str], str] = classmethod(AbstractFileSystem._strip_protocol.__func__)  # type: ignore[assignment]

    def add_file(self, obj: io.IOBase | BytesIOWrapper | object, path: str) -> None:
        """Add a file to the filesystem."""
        if not (hasattr(obj, "read") and hasattr(obj, "seek")):
            msg = "Can not read from a non file-like object"
            raise TypeError(msg)
        if isinstance(obj, io.TextIOBase):
            # Wrap this so that we can return a bytes object from 'read'
            obj = BytesIOWrapper(obj)
        path = self._strip_protocol(path)
        self.store[path] = MemoryFile(self, path, obj.read())
