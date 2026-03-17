from __future__ import annotations

import asyncio
import contextlib
import pickle
from collections import defaultdict
from typing import TYPE_CHECKING, Generic, Self, TypedDict, TypeVar

from zarr.abc.store import (
    ByteRequest,
    OffsetByteRequest,
    RangeByteRequest,
    Store,
    SuffixByteRequest,
)
from zarr.core.common import concurrent_map
from zarr.core.config import config

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Coroutine, Iterable, Sequence
    from typing import Any

    from obstore import ListResult, ListStream, ObjectMeta, OffsetRange, SuffixRange
    from obstore.store import ObjectStore as _UpstreamObjectStore

    from zarr.core.buffer import Buffer, BufferPrototype

__all__ = ["ObjectStore"]

_ALLOWED_EXCEPTIONS: tuple[type[Exception], ...] = (
    FileNotFoundError,
    IsADirectoryError,
    NotADirectoryError,
)


T_Store = TypeVar("T_Store", bound="_UpstreamObjectStore")


class ObjectStore(Store, Generic[T_Store]):
    """
    Store that uses obstore for fast read/write from AWS, GCP, Azure.

    Parameters
    ----------
    store : obstore.store.ObjectStore
        An obstore store instance that is set up with the proper credentials.
    read_only : bool
        Whether to open the store in read-only mode.

    Warnings
    --------
    ObjectStore is experimental and subject to API changes without notice. Please
    raise an issue with any comments/concerns about the store.
    """

    store: T_Store
    """The underlying obstore instance."""

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, ObjectStore):
            return False

        if not self.read_only == value.read_only:
            return False

        return self.store == value.store  # type: ignore[no-any-return]

    def __init__(self, store: T_Store, *, read_only: bool = False) -> None:
        if not store.__class__.__module__.startswith("obstore"):
            raise TypeError(f"expected ObjectStore class, got {store!r}")
        super().__init__(read_only=read_only)
        self.store = store

    def with_read_only(self, read_only: bool = False) -> Self:
        # docstring inherited
        return type(self)(
            store=self.store,
            read_only=read_only,
        )

    def __str__(self) -> str:
        return f"object_store://{self.store}"

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self})"

    def __getstate__(self) -> dict[Any, Any]:
        state = self.__dict__.copy()
        state["store"] = pickle.dumps(self.store)
        return state

    def __setstate__(self, state: dict[Any, Any]) -> None:
        state["store"] = pickle.loads(state["store"])
        self.__dict__.update(state)

    async def get(
        self, key: str, prototype: BufferPrototype, byte_range: ByteRequest | None = None
    ) -> Buffer | None:
        # docstring inherited
        import obstore as obs

        try:
            if byte_range is None:
                resp = await obs.get_async(self.store, key)
                return prototype.buffer.from_bytes(await resp.bytes_async())  # type: ignore[arg-type]
            elif isinstance(byte_range, RangeByteRequest):
                bytes = await obs.get_range_async(
                    self.store, key, start=byte_range.start, end=byte_range.end
                )
                return prototype.buffer.from_bytes(bytes)  # type: ignore[arg-type]
            elif isinstance(byte_range, OffsetByteRequest):
                resp = await obs.get_async(
                    self.store, key, options={"range": {"offset": byte_range.offset}}
                )
                return prototype.buffer.from_bytes(await resp.bytes_async())  # type: ignore[arg-type]
            elif isinstance(byte_range, SuffixByteRequest):
                # some object stores (Azure) don't support suffix requests. In this
                # case, our workaround is to first get the length of the object and then
                # manually request the byte range at the end.
                try:
                    resp = await obs.get_async(
                        self.store, key, options={"range": {"suffix": byte_range.suffix}}
                    )
                    return prototype.buffer.from_bytes(await resp.bytes_async())  # type: ignore[arg-type]
                except obs.exceptions.NotSupportedError:
                    head_resp = await obs.head_async(self.store, key)
                    file_size = head_resp["size"]
                    suffix_len = byte_range.suffix
                    buffer = await obs.get_range_async(
                        self.store,
                        key,
                        start=file_size - suffix_len,
                        length=suffix_len,
                    )
                    return prototype.buffer.from_bytes(buffer)  # type: ignore[arg-type]
            else:
                raise ValueError(f"Unexpected byte_range, got {byte_range}")
        except _ALLOWED_EXCEPTIONS:
            return None

    async def get_partial_values(
        self,
        prototype: BufferPrototype,
        key_ranges: Iterable[tuple[str, ByteRequest | None]],
    ) -> list[Buffer | None]:
        # docstring inherited
        return await _get_partial_values(self.store, prototype=prototype, key_ranges=key_ranges)

    async def exists(self, key: str) -> bool:
        # docstring inherited
        import obstore as obs

        try:
            await obs.head_async(self.store, key)
        except FileNotFoundError:
            return False
        else:
            return True

    @property
    def supports_writes(self) -> bool:
        # docstring inherited
        return True

    async def set(self, key: str, value: Buffer) -> None:
        # docstring inherited
        import obstore as obs

        self._check_writable()

        buf = value.as_buffer_like()
        await obs.put_async(self.store, key, buf)

    async def set_if_not_exists(self, key: str, value: Buffer) -> None:
        # docstring inherited
        import obstore as obs

        self._check_writable()
        buf = value.as_buffer_like()
        with contextlib.suppress(obs.exceptions.AlreadyExistsError):
            await obs.put_async(self.store, key, buf, mode="create")

    @property
    def supports_deletes(self) -> bool:
        # docstring inherited
        return True

    async def delete(self, key: str) -> None:
        # docstring inherited
        import obstore as obs

        self._check_writable()

        # Some obstore stores such as local filesystems, GCP and Azure raise an error
        # when deleting a non-existent key, while others such as S3 and in-memory do
        # not. We suppress the error to make the behavior consistent across all obstore
        # stores. This is also in line with the behavior of the other Zarr store adapters.
        with contextlib.suppress(FileNotFoundError):
            await obs.delete_async(self.store, key)

    async def delete_dir(self, prefix: str) -> None:
        # docstring inherited
        import obstore as obs

        self._check_writable()
        if prefix != "" and not prefix.endswith("/"):
            prefix += "/"

        metas = await obs.list(self.store, prefix).collect_async()
        keys = [(m["path"],) for m in metas]
        await concurrent_map(keys, self.delete, limit=config.get("async.concurrency"))

    @property
    def supports_listing(self) -> bool:
        # docstring inherited
        return True

    async def _list(self, prefix: str | None = None) -> AsyncGenerator[ObjectMeta, None]:
        import obstore as obs

        objects: ListStream[Sequence[ObjectMeta]] = obs.list(self.store, prefix=prefix)
        async for batch in objects:
            for item in batch:
                yield item

    def list(self) -> AsyncGenerator[str, None]:
        # docstring inherited
        return (obj["path"] async for obj in self._list())

    def list_prefix(self, prefix: str) -> AsyncGenerator[str, None]:
        # docstring inherited
        return (obj["path"] async for obj in self._list(prefix))

    def list_dir(self, prefix: str) -> AsyncGenerator[str, None]:
        # docstring inherited
        import obstore as obs

        coroutine = obs.list_with_delimiter_async(self.store, prefix=prefix)
        return _transform_list_dir(coroutine, prefix)

    async def getsize(self, key: str) -> int:
        # docstring inherited
        import obstore as obs

        resp = await obs.head_async(self.store, key)
        return resp["size"]

    async def getsize_prefix(self, prefix: str) -> int:
        # docstring inherited
        sizes = [obj["size"] async for obj in self._list(prefix=prefix)]
        return sum(sizes)


async def _transform_list_dir(
    list_result_coroutine: Coroutine[Any, Any, ListResult[Sequence[ObjectMeta]]], prefix: str
) -> AsyncGenerator[str, None]:
    """
    Transform the result of list_with_delimiter into an async generator of paths.
    """
    list_result = await list_result_coroutine

    # We assume that the underlying object-store implementation correctly handles the
    # prefix, so we don't double-check that the returned results actually start with the
    # given prefix.
    prefixes = [obj.lstrip(prefix).lstrip("/") for obj in list_result["common_prefixes"]]
    objects = [obj["path"].removeprefix(prefix).lstrip("/") for obj in list_result["objects"]]
    for item in prefixes + objects:
        yield item


class _BoundedRequest(TypedDict):
    """Range request with a known start and end byte.

    These requests can be multiplexed natively on the Rust side with
    `obstore.get_ranges_async`.
    """

    original_request_index: int
    """The positional index in the original key_ranges input"""

    start: int
    """Start byte offset."""

    end: int
    """End byte offset."""


class _OtherRequest(TypedDict):
    """Offset or suffix range requests.

    These requests cannot be concurrent on the Rust side, and each need their own call
    to `obstore.get_async`, passing in the `range` parameter.
    """

    original_request_index: int
    """The positional index in the original key_ranges input"""

    path: str
    """The path to request from."""

    range: OffsetRange | None
    # Note: suffix requests are handled separately because some object stores (Azure)
    # don't support them
    """The range request type."""


class _SuffixRequest(TypedDict):
    """Offset or suffix range requests.

    These requests cannot be concurrent on the Rust side, and each need their own call
    to `obstore.get_async`, passing in the `range` parameter.
    """

    original_request_index: int
    """The positional index in the original key_ranges input"""

    path: str
    """The path to request from."""

    range: SuffixRange
    """The suffix range."""


class _Response(TypedDict):
    """A response buffer associated with the original index that it should be restored to."""

    original_request_index: int
    """The positional index in the original key_ranges input"""

    buffer: Buffer
    """The buffer returned from obstore's range request."""


async def _make_bounded_requests(
    store: _UpstreamObjectStore,
    path: str,
    requests: list[_BoundedRequest],
    prototype: BufferPrototype,
    semaphore: asyncio.Semaphore,
) -> list[_Response]:
    """Make all bounded requests for a specific file.

    `obstore.get_ranges_async` allows for making concurrent requests for multiple ranges
    within a single file, and will e.g. merge concurrent requests. This only uses one
    single Python coroutine.
    """
    import obstore as obs

    starts = [r["start"] for r in requests]
    ends = [r["end"] for r in requests]
    async with semaphore:
        responses = await obs.get_ranges_async(store, path=path, starts=starts, ends=ends)

    buffer_responses: list[_Response] = []
    for request, response in zip(requests, responses, strict=True):
        buffer_responses.append(
            {
                "original_request_index": request["original_request_index"],
                "buffer": prototype.buffer.from_bytes(response),  # type: ignore[arg-type]
            }
        )

    return buffer_responses


async def _make_other_request(
    store: _UpstreamObjectStore,
    request: _OtherRequest,
    prototype: BufferPrototype,
    semaphore: asyncio.Semaphore,
) -> list[_Response]:
    """Make offset or full-file requests.

    We return a `list[_Response]` for symmetry with `_make_bounded_requests` so that all
    futures can be gathered together.
    """
    import obstore as obs

    async with semaphore:
        if request["range"] is None:
            resp = await obs.get_async(store, request["path"])
        else:
            resp = await obs.get_async(store, request["path"], options={"range": request["range"]})
        buffer = await resp.bytes_async()

    return [
        {
            "original_request_index": request["original_request_index"],
            "buffer": prototype.buffer.from_bytes(buffer),  # type: ignore[arg-type]
        }
    ]


async def _make_suffix_request(
    store: _UpstreamObjectStore,
    request: _SuffixRequest,
    prototype: BufferPrototype,
    semaphore: asyncio.Semaphore,
) -> list[_Response]:
    """Make suffix requests.

    This is separated out from `_make_other_request` because some object stores (Azure)
    don't support suffix requests. In this case, our workaround is to first get the
    length of the object and then manually request the byte range at the end.

    We return a `list[_Response]` for symmetry with `_make_bounded_requests` so that all
    futures can be gathered together.
    """
    import obstore as obs

    async with semaphore:
        try:
            resp = await obs.get_async(store, request["path"], options={"range": request["range"]})
            buffer = await resp.bytes_async()
        except obs.exceptions.NotSupportedError:
            head_resp = await obs.head_async(store, request["path"])
            file_size = head_resp["size"]
            suffix_len = request["range"]["suffix"]
            buffer = await obs.get_range_async(
                store,
                request["path"],
                start=file_size - suffix_len,
                length=suffix_len,
            )

    return [
        {
            "original_request_index": request["original_request_index"],
            "buffer": prototype.buffer.from_bytes(buffer),  # type: ignore[arg-type]
        }
    ]


async def _get_partial_values(
    store: _UpstreamObjectStore,
    prototype: BufferPrototype,
    key_ranges: Iterable[tuple[str, ByteRequest | None]],
) -> list[Buffer | None]:
    """Make multiple range requests.

    ObjectStore has a `get_ranges` method that will additionally merge nearby ranges,
    but it's _per_ file. So we need to split these key_ranges into **per-file** key
    ranges, and then reassemble the results in the original order.

    We separate into different requests:

    - One call to `obstore.get_ranges_async` **per target file**
    - One call to `obstore.get_async` for each other request.
    """
    key_ranges = list(key_ranges)
    per_file_bounded_requests: dict[str, list[_BoundedRequest]] = defaultdict(list)
    other_requests: list[_OtherRequest] = []
    suffix_requests: list[_SuffixRequest] = []

    for idx, (path, byte_range) in enumerate(key_ranges):
        if byte_range is None:
            other_requests.append(
                {
                    "original_request_index": idx,
                    "path": path,
                    "range": None,
                }
            )
        elif isinstance(byte_range, RangeByteRequest):
            per_file_bounded_requests[path].append(
                {"original_request_index": idx, "start": byte_range.start, "end": byte_range.end}
            )
        elif isinstance(byte_range, OffsetByteRequest):
            other_requests.append(
                {
                    "original_request_index": idx,
                    "path": path,
                    "range": {"offset": byte_range.offset},
                }
            )
        elif isinstance(byte_range, SuffixByteRequest):
            suffix_requests.append(
                {
                    "original_request_index": idx,
                    "path": path,
                    "range": {"suffix": byte_range.suffix},
                }
            )
        else:
            raise ValueError(f"Unsupported range input: {byte_range}")

    semaphore = asyncio.Semaphore(config.get("async.concurrency"))

    futs: list[Coroutine[Any, Any, list[_Response]]] = []
    for path, bounded_ranges in per_file_bounded_requests.items():
        futs.append(
            _make_bounded_requests(store, path, bounded_ranges, prototype, semaphore=semaphore)
        )

    for request in other_requests:
        futs.append(_make_other_request(store, request, prototype, semaphore=semaphore))  # noqa: PERF401

    for suffix_request in suffix_requests:
        futs.append(_make_suffix_request(store, suffix_request, prototype, semaphore=semaphore))  # noqa: PERF401

    buffers: list[Buffer | None] = [None] * len(key_ranges)

    for responses in await asyncio.gather(*futs):
        for resp in responses:
            buffers[resp["original_request_index"]] = resp["buffer"]

    return buffers
