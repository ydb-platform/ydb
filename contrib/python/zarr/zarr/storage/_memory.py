from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING, Self

from zarr.abc.store import ByteRequest, Store
from zarr.core.buffer import Buffer, gpu
from zarr.core.buffer.core import default_buffer_prototype
from zarr.core.common import concurrent_map
from zarr.storage._utils import _normalize_byte_range_index

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, MutableMapping

    from zarr.core.buffer import BufferPrototype


logger = getLogger(__name__)


class MemoryStore(Store):
    """
    Store for local memory.

    Parameters
    ----------
    store_dict : dict
        Initial data
    read_only : bool
        Whether the store is read-only

    Attributes
    ----------
    supports_writes
    supports_deletes
    supports_listing
    """

    supports_writes: bool = True
    supports_deletes: bool = True
    supports_listing: bool = True

    _store_dict: MutableMapping[str, Buffer]

    def __init__(
        self,
        store_dict: MutableMapping[str, Buffer] | None = None,
        *,
        read_only: bool = False,
    ) -> None:
        super().__init__(read_only=read_only)
        if store_dict is None:
            store_dict = {}
        self._store_dict = store_dict

    def with_read_only(self, read_only: bool = False) -> MemoryStore:
        # docstring inherited
        return type(self)(
            store_dict=self._store_dict,
            read_only=read_only,
        )

    async def clear(self) -> None:
        # docstring inherited
        self._store_dict.clear()

    def __str__(self) -> str:
        return f"memory://{id(self._store_dict)}"

    def __repr__(self) -> str:
        return f"MemoryStore('{self}')"

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, type(self))
            and self._store_dict == other._store_dict
            and self.read_only == other.read_only
        )

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
        try:
            value = self._store_dict[key]
            start, stop = _normalize_byte_range_index(value, byte_range)
            return prototype.buffer.from_buffer(value[start:stop])
        except KeyError:
            return None

    async def get_partial_values(
        self,
        prototype: BufferPrototype,
        key_ranges: Iterable[tuple[str, ByteRequest | None]],
    ) -> list[Buffer | None]:
        # docstring inherited

        # All the key-ranges arguments goes with the same prototype
        async def _get(key: str, byte_range: ByteRequest | None) -> Buffer | None:
            return await self.get(key, prototype=prototype, byte_range=byte_range)

        return await concurrent_map(key_ranges, _get, limit=None)

    async def exists(self, key: str) -> bool:
        # docstring inherited
        return key in self._store_dict

    async def set(self, key: str, value: Buffer, byte_range: tuple[int, int] | None = None) -> None:
        # docstring inherited
        self._check_writable()
        await self._ensure_open()
        assert isinstance(key, str)
        if not isinstance(value, Buffer):
            raise TypeError(
                f"MemoryStore.set(): `value` must be a Buffer instance. Got an instance of {type(value)} instead."
            )

        if byte_range is not None:
            buf = self._store_dict[key]
            buf[byte_range[0] : byte_range[1]] = value
            self._store_dict[key] = buf
        else:
            self._store_dict[key] = value

    async def set_if_not_exists(self, key: str, value: Buffer) -> None:
        # docstring inherited
        self._check_writable()
        await self._ensure_open()
        self._store_dict.setdefault(key, value)

    async def delete(self, key: str) -> None:
        # docstring inherited
        self._check_writable()
        try:
            del self._store_dict[key]
        except KeyError:
            logger.debug("Key %s does not exist.", key)

    async def list(self) -> AsyncIterator[str]:
        # docstring inherited
        for key in self._store_dict:
            yield key

    async def list_prefix(self, prefix: str) -> AsyncIterator[str]:
        # docstring inherited
        # note: we materialize all dict keys into a list here so we can mutate the dict in-place (e.g. in delete_prefix)
        for key in list(self._store_dict):
            if key.startswith(prefix):
                yield key

    async def list_dir(self, prefix: str) -> AsyncIterator[str]:
        # docstring inherited
        prefix = prefix.rstrip("/")

        if prefix == "":
            keys_unique = {k.split("/")[0] for k in self._store_dict}
        else:
            # Our dictionary doesn't contain directory markers, but we want to include
            # a pseudo directory when there's a nested item and we're listing an
            # intermediate level.
            keys_unique = {
                key.removeprefix(prefix + "/").split("/")[0]
                for key in self._store_dict
                if key.startswith(prefix + "/") and key != prefix
            }

        for key in keys_unique:
            yield key


class GpuMemoryStore(MemoryStore):
    """
    Store for GPU memory.

    Stores every chunk in GPU memory irrespective of the original location.

    The dictionary of buffers to initialize this memory store with *must* be
    GPU Buffers.

    Writing data to this store through ``.set`` will move the buffer to the GPU
    if necessary.

    Parameters
    ----------
    store_dict : MutableMapping, optional
        A mutable mapping with string keys and [zarr.core.buffer.gpu.Buffer][]
        values.
    read_only : bool
        Whether to open the store in read-only mode.
    """

    _store_dict: MutableMapping[str, gpu.Buffer]  # type: ignore[assignment]

    def __init__(
        self,
        store_dict: MutableMapping[str, gpu.Buffer] | None = None,
        *,
        read_only: bool = False,
    ) -> None:
        super().__init__(store_dict=store_dict, read_only=read_only)  # type: ignore[arg-type]

    def __str__(self) -> str:
        return f"gpumemory://{id(self._store_dict)}"

    def __repr__(self) -> str:
        return f"GpuMemoryStore('{self}')"

    @classmethod
    def from_dict(cls, store_dict: MutableMapping[str, Buffer]) -> Self:
        """
        Create a GpuMemoryStore from a dictionary of buffers at any location.

        The dictionary backing the newly created ``GpuMemoryStore`` will not be
        the same as ``store_dict``.

        Parameters
        ----------
        store_dict : mapping
            A mapping of strings keys to arbitrary Buffers. The buffer data
            will be moved into a [`gpu.Buffer`][zarr.core.buffer.gpu.Buffer].

        Returns
        -------
        GpuMemoryStore
        """
        gpu_store_dict = {k: gpu.Buffer.from_buffer(v) for k, v in store_dict.items()}
        return cls(gpu_store_dict)

    async def set(self, key: str, value: Buffer, byte_range: tuple[int, int] | None = None) -> None:
        # docstring inherited
        self._check_writable()
        assert isinstance(key, str)
        if not isinstance(value, Buffer):
            raise TypeError(
                f"GpuMemoryStore.set(): `value` must be a Buffer instance. Got an instance of {type(value)} instead."
            )
        # Convert to gpu.Buffer
        gpu_value = value if isinstance(value, gpu.Buffer) else gpu.Buffer.from_buffer(value)
        await super().set(key, gpu_value, byte_range=byte_range)
