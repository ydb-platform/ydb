from __future__ import annotations

import asyncio
import contextlib
import io
import os
import shutil
import sys
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, Literal, Self

from zarr.abc.store import (
    ByteRequest,
    OffsetByteRequest,
    RangeByteRequest,
    Store,
    SuffixByteRequest,
)
from zarr.core.buffer import Buffer
from zarr.core.buffer.core import default_buffer_prototype
from zarr.core.common import AccessModeLiteral, concurrent_map

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Iterator

    from zarr.core.buffer import BufferPrototype


def _get(path: Path, prototype: BufferPrototype, byte_range: ByteRequest | None) -> Buffer:
    if byte_range is None:
        return prototype.buffer.from_bytes(path.read_bytes())
    with path.open("rb") as f:
        size = f.seek(0, io.SEEK_END)
        if isinstance(byte_range, RangeByteRequest):
            f.seek(byte_range.start)
            return prototype.buffer.from_bytes(f.read(byte_range.end - f.tell()))
        elif isinstance(byte_range, OffsetByteRequest):
            f.seek(byte_range.offset)
        elif isinstance(byte_range, SuffixByteRequest):
            f.seek(max(0, size - byte_range.suffix))
        else:
            raise TypeError(f"Unexpected byte_range, got {byte_range}.")
        return prototype.buffer.from_bytes(f.read())


if sys.platform == "win32":
    # Per the os.rename docs:
    # On Windows, if dst exists a FileExistsError is always raised.
    _safe_move = os.rename
else:
    # On Unix, os.rename silently replace files, so instead we use os.link like
    # atomicwrites:
    # https://github.com/untitaker/python-atomicwrites/blob/1.4.1/atomicwrites/__init__.py#L59-L60
    # This also raises FileExistsError if dst exists.
    def _safe_move(src: Path, dst: Path) -> None:
        os.link(src, dst)
        os.unlink(src)


@contextlib.contextmanager
def _atomic_write(
    path: Path,
    mode: Literal["r+b", "wb"],
    exclusive: bool = False,
) -> Iterator[BinaryIO]:
    tmp_path = path.with_suffix(f".{uuid.uuid4().hex}.partial")
    try:
        with tmp_path.open(mode) as f:
            yield f
        if exclusive:
            _safe_move(tmp_path, path)
        else:
            tmp_path.replace(path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def _put(path: Path, value: Buffer, exclusive: bool = False) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    # write takes any object supporting the buffer protocol
    view = value.as_buffer_like()
    with _atomic_write(path, "wb", exclusive=exclusive) as f:
        return f.write(view)


class LocalStore(Store):
    """
    Store for the local file system.

    Parameters
    ----------
    root : str or Path
        Directory to use as root of store.
    read_only : bool
        Whether the store is read-only

    Attributes
    ----------
    supports_writes
    supports_deletes
    supports_listing
    root
    """

    supports_writes: bool = True
    supports_deletes: bool = True
    supports_listing: bool = True

    root: Path

    def __init__(self, root: Path | str, *, read_only: bool = False) -> None:
        super().__init__(read_only=read_only)
        if isinstance(root, str):
            root = Path(root)
        if not isinstance(root, Path):
            raise TypeError(
                f"'root' must be a string or Path instance. Got an instance of {type(root)} instead."
            )
        self.root = root

    def with_read_only(self, read_only: bool = False) -> Self:
        # docstring inherited
        return type(self)(
            root=self.root,
            read_only=read_only,
        )

    @classmethod
    async def open(
        cls, root: Path | str, *, read_only: bool = False, mode: AccessModeLiteral | None = None
    ) -> Self:
        """
        Create and open the store.

        Parameters
        ----------
        root : str or Path
            Directory to use as root of store.
        read_only : bool
            Whether the store is read-only
        mode :
            Mode in which to create the store. This only affects opening the store,
            and the final read-only state of the store is controlled through the
            read_only parameter.

        Returns
        -------
        Store
            The opened store instance.
        """
        # If mode = 'r+', want to open in read only mode (fail if exists),
        # but return a writeable store
        if mode is not None:
            read_only_creation = mode in ["r", "r+"]
        else:
            read_only_creation = read_only
        store = cls(root, read_only=read_only_creation)
        await store._open()

        # Set read_only state
        store = store.with_read_only(read_only)
        await store._open()
        return store

    async def _open(self, *, mode: AccessModeLiteral | None = None) -> None:
        if not self.read_only:
            self.root.mkdir(parents=True, exist_ok=True)

        if not self.root.exists():
            raise FileNotFoundError(f"{self.root} does not exist")
        return await super()._open()

    async def clear(self) -> None:
        # docstring inherited
        self._check_writable()
        shutil.rmtree(self.root)
        self.root.mkdir()

    def __str__(self) -> str:
        return f"file://{self.root.as_posix()}"

    def __repr__(self) -> str:
        return f"LocalStore('{self}')"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, type(self)) and self.root == other.root

    async def get(
        self,
        key: str,
        prototype: BufferPrototype | None = None,
        byte_range: ByteRequest | None = None,
    ) -> Buffer | None:
        # docstring inherited
        if prototype is None:
            prototype = default_buffer_prototype()
        if not self._is_open:
            await self._open()
        assert isinstance(key, str)
        path = self.root / key

        try:
            return await asyncio.to_thread(_get, path, prototype, byte_range)
        except (FileNotFoundError, IsADirectoryError, NotADirectoryError):
            return None

    async def get_partial_values(
        self,
        prototype: BufferPrototype,
        key_ranges: Iterable[tuple[str, ByteRequest | None]],
    ) -> list[Buffer | None]:
        # docstring inherited
        args = []
        for key, byte_range in key_ranges:
            assert isinstance(key, str)
            path = self.root / key
            args.append((_get, path, prototype, byte_range))
        return await concurrent_map(args, asyncio.to_thread, limit=None)  # TODO: fix limit

    async def set(self, key: str, value: Buffer) -> None:
        # docstring inherited
        return await self._set(key, value)

    async def set_if_not_exists(self, key: str, value: Buffer) -> None:
        # docstring inherited
        try:
            return await self._set(key, value, exclusive=True)
        except FileExistsError:
            pass

    async def _set(self, key: str, value: Buffer, exclusive: bool = False) -> None:
        if not self._is_open:
            await self._open()
        self._check_writable()
        assert isinstance(key, str)
        if not isinstance(value, Buffer):
            raise TypeError(
                f"LocalStore.set(): `value` must be a Buffer instance. Got an instance of {type(value)} instead."
            )
        path = self.root / key
        await asyncio.to_thread(_put, path, value, exclusive=exclusive)

    async def delete(self, key: str) -> None:
        """
        Remove a key from the store.

        Parameters
        ----------
        key : str

        Notes
        -----
        If ``key`` is a directory within this store, the entire directory
        at ``store.root / key`` is deleted.
        """
        # docstring inherited
        self._check_writable()
        path = self.root / key
        if path.is_dir():  # TODO: support deleting directories? shutil.rmtree?
            shutil.rmtree(path)
        else:
            await asyncio.to_thread(path.unlink, True)  # Q: we may want to raise if path is missing

    async def delete_dir(self, prefix: str) -> None:
        # docstring inherited
        self._check_writable()
        path = self.root / prefix
        if path.is_dir():
            shutil.rmtree(path)
        elif path.is_file():
            raise ValueError(f"delete_dir was passed a {prefix=!r} that is a file")
        else:
            # Non-existent directory
            # This path is tested by test_group:test_create_creates_parents for one
            pass

    async def exists(self, key: str) -> bool:
        # docstring inherited
        path = self.root / key
        return await asyncio.to_thread(path.is_file)

    async def list(self) -> AsyncIterator[str]:
        # docstring inherited
        to_strip = self.root.as_posix() + "/"
        for p in list(self.root.rglob("*")):
            if p.is_file():
                yield p.as_posix().replace(to_strip, "")

    async def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        # docstring inherited
        to_strip = self.root.as_posix() + "/"
        prefix = prefix.rstrip("/")
        for p in (self.root / prefix).rglob("*"):
            if p.is_file():
                yield p.as_posix().replace(to_strip, "")

    async def list_dir(self, prefix: str) -> AsyncIterator[str]:
        # docstring inherited
        base = self.root / prefix
        try:
            key_iter = base.iterdir()
            for key in key_iter:
                yield key.relative_to(base).as_posix()
        except (FileNotFoundError, NotADirectoryError):
            pass

    async def move(self, dest_root: Path | str) -> None:
        """
        Move the store to another path. The old root directory is deleted.
        """
        if isinstance(dest_root, str):
            dest_root = Path(dest_root)
        os.makedirs(dest_root.parent, exist_ok=True)
        if os.path.exists(dest_root):
            raise FileExistsError(f"Destination root {dest_root} already exists.")
        shutil.move(self.root, dest_root)
        self.root = dest_root

    async def getsize(self, key: str) -> int:
        return os.path.getsize(self.root / key)
