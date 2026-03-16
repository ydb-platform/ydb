from __future__ import annotations

import dataclasses
import textwrap
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from zarr.abc.codec import ArrayArrayCodec, ArrayBytesCodec, BytesBytesCodec
    from zarr.abc.numcodec import Numcodec
    from zarr.core.common import ZarrFormat
    from zarr.core.dtype.wrapper import TBaseDType, TBaseScalar, ZDType


@dataclasses.dataclass(kw_only=True)
class GroupInfo:
    """
    Visual summary for a Group.

    Note that this method and its properties is not part of
    Zarr's public API.
    """

    _name: str
    _type: Literal["Group"] = "Group"
    _zarr_format: ZarrFormat
    _read_only: bool
    _store_type: str
    _count_members: int | None = None
    _count_arrays: int | None = None
    _count_groups: int | None = None

    def __repr__(self) -> str:
        template = textwrap.dedent("""\
        Name        : {_name}
        Type        : {_type}
        Zarr format : {_zarr_format}
        Read-only   : {_read_only}
        Store type  : {_store_type}""")

        if self._count_members is not None:
            template += "\nNo. members : {_count_members}"
        if self._count_arrays is not None:
            template += "\nNo. arrays  : {_count_arrays}"
        if self._count_groups is not None:
            template += "\nNo. groups  : {_count_groups}"
        return template.format(**dataclasses.asdict(self))


def human_readable_size(size: int) -> str:
    if size < 2**10:
        return f"{size}"
    elif size < 2**20:
        return f"{size / float(2**10):.1f}K"
    elif size < 2**30:
        return f"{size / float(2**20):.1f}M"
    elif size < 2**40:
        return f"{size / float(2**30):.1f}G"
    elif size < 2**50:
        return f"{size / float(2**40):.1f}T"
    else:
        return f"{size / float(2**50):.1f}P"


def byte_info(size: int) -> str:
    if size < 2**10:
        return str(size)
    else:
        return f"{size} ({human_readable_size(size)})"


@dataclasses.dataclass(kw_only=True, frozen=True, slots=True)
class ArrayInfo:
    """
    Visual summary for an Array.

    Note that this method and its properties is not part of
    Zarr's public API.
    """

    _type: Literal["Array"] = "Array"
    _zarr_format: ZarrFormat
    _data_type: ZDType[TBaseDType, TBaseScalar]
    _fill_value: object
    _shape: tuple[int, ...]
    _shard_shape: tuple[int, ...] | None = None
    _chunk_shape: tuple[int, ...] | None = None
    _order: Literal["C", "F"]
    _read_only: bool
    _store_type: str
    _filters: tuple[Numcodec, ...] | tuple[ArrayArrayCodec, ...] = ()
    _serializer: ArrayBytesCodec | None = None
    _compressors: tuple[Numcodec, ...] | tuple[BytesBytesCodec, ...] = ()
    _count_bytes: int | None = None
    _count_bytes_stored: int | None = None
    _count_chunks_initialized: int | None = None

    def __repr__(self) -> str:
        template = textwrap.dedent("""\
        Type               : {_type}
        Zarr format        : {_zarr_format}
        Data type          : {_data_type}
        Fill value         : {_fill_value}
        Shape              : {_shape}""")

        if self._shard_shape is not None:
            template += textwrap.dedent("""
        Shard shape        : {_shard_shape}""")

        template += textwrap.dedent("""
        Chunk shape        : {_chunk_shape}
        Order              : {_order}
        Read-only          : {_read_only}
        Store type         : {_store_type}""")

        # We can't use dataclasses.asdict, because we only want a shallow dict
        kwargs = {field.name: getattr(self, field.name) for field in dataclasses.fields(self)}

        if self._chunk_shape is None:
            # for non-regular chunk grids
            kwargs["chunk_shape"] = "<variable>"

        template += "\nFilters            : {_filters}"

        if self._serializer is not None:
            template += "\nSerializer         : {_serializer}"

        template += "\nCompressors        : {_compressors}"

        if self._count_bytes is not None:
            template += "\nNo. bytes          : {_count_bytes}"
            kwargs["_count_bytes"] = byte_info(self._count_bytes)

        if self._count_bytes_stored is not None:
            template += "\nNo. bytes stored   : {_count_bytes_stored}"
            kwargs["_count_bytes_stored"] = byte_info(self._count_bytes_stored)

        if (
            self._count_bytes is not None
            and self._count_bytes_stored is not None
            and self._count_bytes_stored > 0
        ):
            template += "\nStorage ratio      : {_storage_ratio}"
            kwargs["_storage_ratio"] = f"{self._count_bytes / self._count_bytes_stored:.1f}"

        if self._count_chunks_initialized is not None:
            if self._shard_shape is not None:
                template += "\nShards Initialized : {_count_chunks_initialized}"
            else:
                template += "\nChunks Initialized : {_count_chunks_initialized}"
        return template.format(**kwargs)
