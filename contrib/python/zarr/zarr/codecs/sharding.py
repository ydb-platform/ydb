from __future__ import annotations

from collections.abc import Iterable, Mapping, MutableMapping
from dataclasses import dataclass, replace
from enum import Enum
from functools import lru_cache
from operator import itemgetter
from typing import TYPE_CHECKING, Any, NamedTuple, cast

import numpy as np
import numpy.typing as npt

from zarr.abc.codec import (
    ArrayBytesCodec,
    ArrayBytesCodecPartialDecodeMixin,
    ArrayBytesCodecPartialEncodeMixin,
    Codec,
    CodecPipeline,
)
from zarr.abc.store import (
    ByteGetter,
    ByteRequest,
    ByteSetter,
    RangeByteRequest,
    SuffixByteRequest,
)
from zarr.codecs.bytes import BytesCodec
from zarr.codecs.crc32c_ import Crc32cCodec
from zarr.core.array_spec import ArrayConfig, ArraySpec
from zarr.core.buffer import (
    Buffer,
    BufferPrototype,
    NDBuffer,
    default_buffer_prototype,
    numpy_buffer_prototype,
)
from zarr.core.chunk_grids import ChunkGrid, RegularChunkGrid
from zarr.core.common import (
    ShapeLike,
    parse_enum,
    parse_named_configuration,
    parse_shapelike,
    product,
)
from zarr.core.dtype.npy.int import UInt64
from zarr.core.indexing import (
    BasicIndexer,
    SelectorTuple,
    c_order_iter,
    get_indexer,
    morton_order_iter,
)
from zarr.core.metadata.v3 import parse_codecs
from zarr.registry import get_ndbuffer_class, get_pipeline_class

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import Self

    from zarr.core.common import JSON
    from zarr.core.dtype.wrapper import TBaseDType, TBaseScalar, ZDType

MAX_UINT_64 = 2**64 - 1
ShardMapping = Mapping[tuple[int, ...], Buffer | None]
ShardMutableMapping = MutableMapping[tuple[int, ...], Buffer | None]


class ShardingCodecIndexLocation(Enum):
    """
    Enum for index location used by the sharding codec.
    """

    start = "start"
    end = "end"


def parse_index_location(data: object) -> ShardingCodecIndexLocation:
    return parse_enum(data, ShardingCodecIndexLocation)


@dataclass(frozen=True)
class _ShardingByteGetter(ByteGetter):
    shard_dict: ShardMapping
    chunk_coords: tuple[int, ...]

    async def get(
        self, prototype: BufferPrototype, byte_range: ByteRequest | None = None
    ) -> Buffer | None:
        assert byte_range is None, "byte_range is not supported within shards"
        assert prototype == default_buffer_prototype(), (
            f"prototype is not supported within shards currently. diff: {prototype} != {default_buffer_prototype()}"
        )
        return self.shard_dict.get(self.chunk_coords)


@dataclass(frozen=True)
class _ShardingByteSetter(_ShardingByteGetter, ByteSetter):
    shard_dict: ShardMutableMapping

    async def set(self, value: Buffer, byte_range: ByteRequest | None = None) -> None:
        assert byte_range is None, "byte_range is not supported within shards"
        self.shard_dict[self.chunk_coords] = value

    async def delete(self) -> None:
        del self.shard_dict[self.chunk_coords]

    async def set_if_not_exists(self, default: Buffer) -> None:
        self.shard_dict.setdefault(self.chunk_coords, default)


class _ShardIndex(NamedTuple):
    # dtype uint64, shape (chunks_per_shard_0, chunks_per_shard_1, ..., 2)
    offsets_and_lengths: npt.NDArray[np.uint64]

    @property
    def chunks_per_shard(self) -> tuple[int, ...]:
        result = tuple(self.offsets_and_lengths.shape[0:-1])
        # The cast is required until https://github.com/numpy/numpy/pull/27211 is merged
        return cast("tuple[int, ...]", result)

    def _localize_chunk(self, chunk_coords: tuple[int, ...]) -> tuple[int, ...]:
        return tuple(
            chunk_i % shard_i
            for chunk_i, shard_i in zip(chunk_coords, self.offsets_and_lengths.shape, strict=False)
        )

    def is_all_empty(self) -> bool:
        return bool(np.array_equiv(self.offsets_and_lengths, MAX_UINT_64))

    def get_full_chunk_map(self) -> npt.NDArray[np.bool_]:
        return np.not_equal(self.offsets_and_lengths[..., 0], MAX_UINT_64)

    def get_chunk_slice(self, chunk_coords: tuple[int, ...]) -> tuple[int, int] | None:
        localized_chunk = self._localize_chunk(chunk_coords)
        chunk_start, chunk_len = self.offsets_and_lengths[localized_chunk]
        if (chunk_start, chunk_len) == (MAX_UINT_64, MAX_UINT_64):
            return None
        else:
            return (int(chunk_start), int(chunk_start + chunk_len))

    def set_chunk_slice(self, chunk_coords: tuple[int, ...], chunk_slice: slice | None) -> None:
        localized_chunk = self._localize_chunk(chunk_coords)
        if chunk_slice is None:
            self.offsets_and_lengths[localized_chunk] = (MAX_UINT_64, MAX_UINT_64)
        else:
            self.offsets_and_lengths[localized_chunk] = (
                chunk_slice.start,
                chunk_slice.stop - chunk_slice.start,
            )

    def is_dense(self, chunk_byte_length: int) -> bool:
        sorted_offsets_and_lengths = sorted(
            [
                (offset, length)
                for offset, length in self.offsets_and_lengths
                if offset != MAX_UINT_64
            ],
            key=itemgetter(0),
        )

        # Are all non-empty offsets unique?
        if len(
            {offset for offset, _ in sorted_offsets_and_lengths if offset != MAX_UINT_64}
        ) != len(sorted_offsets_and_lengths):
            return False

        return all(
            offset % chunk_byte_length == 0 and length == chunk_byte_length
            for offset, length in sorted_offsets_and_lengths
        )

    @classmethod
    def create_empty(cls, chunks_per_shard: tuple[int, ...]) -> _ShardIndex:
        offsets_and_lengths = np.zeros(chunks_per_shard + (2,), dtype="<u8", order="C")
        offsets_and_lengths.fill(MAX_UINT_64)
        return cls(offsets_and_lengths)


class _ShardReader(ShardMapping):
    buf: Buffer
    index: _ShardIndex

    @classmethod
    async def from_bytes(
        cls, buf: Buffer, codec: ShardingCodec, chunks_per_shard: tuple[int, ...]
    ) -> _ShardReader:
        shard_index_size = codec._shard_index_size(chunks_per_shard)
        obj = cls()
        obj.buf = buf
        if codec.index_location == ShardingCodecIndexLocation.start:
            shard_index_bytes = obj.buf[:shard_index_size]
        else:
            shard_index_bytes = obj.buf[-shard_index_size:]

        obj.index = await codec._decode_shard_index(shard_index_bytes, chunks_per_shard)
        return obj

    @classmethod
    def create_empty(
        cls, chunks_per_shard: tuple[int, ...], buffer_prototype: BufferPrototype | None = None
    ) -> _ShardReader:
        if buffer_prototype is None:
            buffer_prototype = default_buffer_prototype()
        index = _ShardIndex.create_empty(chunks_per_shard)
        obj = cls()
        obj.buf = buffer_prototype.buffer.create_zero_length()
        obj.index = index
        return obj

    def __getitem__(self, chunk_coords: tuple[int, ...]) -> Buffer:
        chunk_byte_slice = self.index.get_chunk_slice(chunk_coords)
        if chunk_byte_slice:
            return self.buf[chunk_byte_slice[0] : chunk_byte_slice[1]]
        raise KeyError

    def __len__(self) -> int:
        return int(self.index.offsets_and_lengths.size / 2)

    def __iter__(self) -> Iterator[tuple[int, ...]]:
        return c_order_iter(self.index.offsets_and_lengths.shape[:-1])


@dataclass(frozen=True)
class ShardingCodec(
    ArrayBytesCodec, ArrayBytesCodecPartialDecodeMixin, ArrayBytesCodecPartialEncodeMixin
):
    """Sharding codec"""

    chunk_shape: tuple[int, ...]
    codecs: tuple[Codec, ...]
    index_codecs: tuple[Codec, ...]
    index_location: ShardingCodecIndexLocation = ShardingCodecIndexLocation.end

    def __init__(
        self,
        *,
        chunk_shape: ShapeLike,
        codecs: Iterable[Codec | dict[str, JSON]] = (BytesCodec(),),
        index_codecs: Iterable[Codec | dict[str, JSON]] = (BytesCodec(), Crc32cCodec()),
        index_location: ShardingCodecIndexLocation | str = ShardingCodecIndexLocation.end,
    ) -> None:
        chunk_shape_parsed = parse_shapelike(chunk_shape)
        codecs_parsed = parse_codecs(codecs)
        index_codecs_parsed = parse_codecs(index_codecs)
        index_location_parsed = parse_index_location(index_location)

        object.__setattr__(self, "chunk_shape", chunk_shape_parsed)
        object.__setattr__(self, "codecs", codecs_parsed)
        object.__setattr__(self, "index_codecs", index_codecs_parsed)
        object.__setattr__(self, "index_location", index_location_parsed)

        # Use instance-local lru_cache to avoid memory leaks

        # numpy void scalars are not hashable, which means an array spec with a fill value that is
        # a numpy void scalar will break the lru_cache. This is commented for now but should be
        # fixed. See https://github.com/zarr-developers/zarr-python/issues/3054
        # object.__setattr__(self, "_get_chunk_spec", lru_cache()(self._get_chunk_spec))
        object.__setattr__(self, "_get_index_chunk_spec", lru_cache()(self._get_index_chunk_spec))
        object.__setattr__(self, "_get_chunks_per_shard", lru_cache()(self._get_chunks_per_shard))

    # todo: typedict return type
    def __getstate__(self) -> dict[str, Any]:
        return self.to_dict()

    def __setstate__(self, state: dict[str, Any]) -> None:
        config = state["configuration"]
        object.__setattr__(self, "chunk_shape", parse_shapelike(config["chunk_shape"]))
        object.__setattr__(self, "codecs", parse_codecs(config["codecs"]))
        object.__setattr__(self, "index_codecs", parse_codecs(config["index_codecs"]))
        object.__setattr__(self, "index_location", parse_index_location(config["index_location"]))

        # Use instance-local lru_cache to avoid memory leaks
        # object.__setattr__(self, "_get_chunk_spec", lru_cache()(self._get_chunk_spec))
        object.__setattr__(self, "_get_index_chunk_spec", lru_cache()(self._get_index_chunk_spec))
        object.__setattr__(self, "_get_chunks_per_shard", lru_cache()(self._get_chunks_per_shard))

    @classmethod
    def from_dict(cls, data: dict[str, JSON]) -> Self:
        _, configuration_parsed = parse_named_configuration(data, "sharding_indexed")
        return cls(**configuration_parsed)  # type: ignore[arg-type]

    @property
    def codec_pipeline(self) -> CodecPipeline:
        return get_pipeline_class().from_codecs(self.codecs)

    def to_dict(self) -> dict[str, JSON]:
        return {
            "name": "sharding_indexed",
            "configuration": {
                "chunk_shape": self.chunk_shape,
                "codecs": tuple(s.to_dict() for s in self.codecs),
                "index_codecs": tuple(s.to_dict() for s in self.index_codecs),
                "index_location": self.index_location.value,
            },
        }

    def evolve_from_array_spec(self, array_spec: ArraySpec) -> Self:
        shard_spec = self._get_chunk_spec(array_spec)
        evolved_codecs = tuple(c.evolve_from_array_spec(array_spec=shard_spec) for c in self.codecs)
        if evolved_codecs != self.codecs:
            return replace(self, codecs=evolved_codecs)
        return self

    def validate(
        self,
        *,
        shape: tuple[int, ...],
        dtype: ZDType[TBaseDType, TBaseScalar],
        chunk_grid: ChunkGrid,
    ) -> None:
        if len(self.chunk_shape) != len(shape):
            raise ValueError(
                "The shard's `chunk_shape` and array's `shape` need to have the same number of dimensions."
            )
        if not isinstance(chunk_grid, RegularChunkGrid):
            raise TypeError("Sharding is only compatible with regular chunk grids.")
        if not all(
            s % c == 0
            for s, c in zip(
                chunk_grid.chunk_shape,
                self.chunk_shape,
                strict=False,
            )
        ):
            raise ValueError(
                f"The array's `chunk_shape` (got {chunk_grid.chunk_shape}) "
                f"needs to be divisible by the shard's inner `chunk_shape` (got {self.chunk_shape})."
            )

    async def _decode_single(
        self,
        shard_bytes: Buffer,
        shard_spec: ArraySpec,
    ) -> NDBuffer:
        shard_shape = shard_spec.shape
        chunk_shape = self.chunk_shape
        chunks_per_shard = self._get_chunks_per_shard(shard_spec)
        chunk_spec = self._get_chunk_spec(shard_spec)

        indexer = BasicIndexer(
            tuple(slice(0, s) for s in shard_shape),
            shape=shard_shape,
            chunk_grid=RegularChunkGrid(chunk_shape=chunk_shape),
        )

        # setup output array
        out = chunk_spec.prototype.nd_buffer.empty(
            shape=shard_shape,
            dtype=shard_spec.dtype.to_native_dtype(),
            order=shard_spec.order,
        )
        shard_dict = await _ShardReader.from_bytes(shard_bytes, self, chunks_per_shard)

        if shard_dict.index.is_all_empty():
            out.fill(shard_spec.fill_value)
            return out

        # decoding chunks and writing them into the output buffer
        await self.codec_pipeline.read(
            [
                (
                    _ShardingByteGetter(shard_dict, chunk_coords),
                    chunk_spec,
                    chunk_selection,
                    out_selection,
                    is_complete_shard,
                )
                for chunk_coords, chunk_selection, out_selection, is_complete_shard in indexer
            ],
            out,
        )

        return out

    async def _decode_partial_single(
        self,
        byte_getter: ByteGetter,
        selection: SelectorTuple,
        shard_spec: ArraySpec,
    ) -> NDBuffer | None:
        shard_shape = shard_spec.shape
        chunk_shape = self.chunk_shape
        chunks_per_shard = self._get_chunks_per_shard(shard_spec)
        chunk_spec = self._get_chunk_spec(shard_spec)

        indexer = get_indexer(
            selection,
            shape=shard_shape,
            chunk_grid=RegularChunkGrid(chunk_shape=chunk_shape),
        )

        # setup output array
        out = shard_spec.prototype.nd_buffer.empty(
            shape=indexer.shape,
            dtype=shard_spec.dtype.to_native_dtype(),
            order=shard_spec.order,
        )

        indexed_chunks = list(indexer)
        all_chunk_coords = {chunk_coords for chunk_coords, *_ in indexed_chunks}

        # reading bytes of all requested chunks
        shard_dict: ShardMapping = {}
        if self._is_total_shard(all_chunk_coords, chunks_per_shard):
            # read entire shard
            shard_dict_maybe = await self._load_full_shard_maybe(
                byte_getter=byte_getter,
                prototype=chunk_spec.prototype,
                chunks_per_shard=chunks_per_shard,
            )
            if shard_dict_maybe is None:
                return None
            shard_dict = shard_dict_maybe
        else:
            # read some chunks within the shard
            shard_index = await self._load_shard_index_maybe(byte_getter, chunks_per_shard)
            if shard_index is None:
                return None
            shard_dict = {}
            for chunk_coords in all_chunk_coords:
                chunk_byte_slice = shard_index.get_chunk_slice(chunk_coords)
                if chunk_byte_slice:
                    chunk_bytes = await byte_getter.get(
                        prototype=chunk_spec.prototype,
                        byte_range=RangeByteRequest(chunk_byte_slice[0], chunk_byte_slice[1]),
                    )
                    if chunk_bytes:
                        shard_dict[chunk_coords] = chunk_bytes

        # decoding chunks and writing them into the output buffer
        await self.codec_pipeline.read(
            [
                (
                    _ShardingByteGetter(shard_dict, chunk_coords),
                    chunk_spec,
                    chunk_selection,
                    out_selection,
                    is_complete_shard,
                )
                for chunk_coords, chunk_selection, out_selection, is_complete_shard in indexer
            ],
            out,
        )

        if hasattr(indexer, "sel_shape"):
            return out.reshape(indexer.sel_shape)
        else:
            return out

    async def _encode_single(
        self,
        shard_array: NDBuffer,
        shard_spec: ArraySpec,
    ) -> Buffer | None:
        shard_shape = shard_spec.shape
        chunk_shape = self.chunk_shape
        chunks_per_shard = self._get_chunks_per_shard(shard_spec)
        chunk_spec = self._get_chunk_spec(shard_spec)

        indexer = list(
            BasicIndexer(
                tuple(slice(0, s) for s in shard_shape),
                shape=shard_shape,
                chunk_grid=RegularChunkGrid(chunk_shape=chunk_shape),
            )
        )

        shard_builder = dict.fromkeys(morton_order_iter(chunks_per_shard))

        await self.codec_pipeline.write(
            [
                (
                    _ShardingByteSetter(shard_builder, chunk_coords),
                    chunk_spec,
                    chunk_selection,
                    out_selection,
                    is_complete_shard,
                )
                for chunk_coords, chunk_selection, out_selection, is_complete_shard in indexer
            ],
            shard_array,
        )

        return await self._encode_shard_dict(
            shard_builder,
            chunks_per_shard=chunks_per_shard,
            buffer_prototype=default_buffer_prototype(),
        )

    async def _encode_partial_single(
        self,
        byte_setter: ByteSetter,
        shard_array: NDBuffer,
        selection: SelectorTuple,
        shard_spec: ArraySpec,
    ) -> None:
        shard_shape = shard_spec.shape
        chunk_shape = self.chunk_shape
        chunks_per_shard = self._get_chunks_per_shard(shard_spec)
        chunk_spec = self._get_chunk_spec(shard_spec)

        shard_reader = await self._load_full_shard_maybe(
            byte_getter=byte_setter,
            prototype=chunk_spec.prototype,
            chunks_per_shard=chunks_per_shard,
        )
        shard_reader = shard_reader or _ShardReader.create_empty(chunks_per_shard)
        shard_dict = {k: shard_reader.get(k) for k in morton_order_iter(chunks_per_shard)}

        indexer = list(
            get_indexer(
                selection, shape=shard_shape, chunk_grid=RegularChunkGrid(chunk_shape=chunk_shape)
            )
        )

        await self.codec_pipeline.write(
            [
                (
                    _ShardingByteSetter(shard_dict, chunk_coords),
                    chunk_spec,
                    chunk_selection,
                    out_selection,
                    is_complete_shard,
                )
                for chunk_coords, chunk_selection, out_selection, is_complete_shard in indexer
            ],
            shard_array,
        )
        buf = await self._encode_shard_dict(
            shard_dict,
            chunks_per_shard=chunks_per_shard,
            buffer_prototype=default_buffer_prototype(),
        )

        if buf is None:
            await byte_setter.delete()
        else:
            await byte_setter.set(buf)

    async def _encode_shard_dict(
        self,
        map: ShardMapping,
        chunks_per_shard: tuple[int, ...],
        buffer_prototype: BufferPrototype,
    ) -> Buffer | None:
        index = _ShardIndex.create_empty(chunks_per_shard)

        buffers = []

        template = buffer_prototype.buffer.create_zero_length()
        chunk_start = 0
        for chunk_coords in morton_order_iter(chunks_per_shard):
            value = map.get(chunk_coords)
            if value is None:
                continue

            if len(value) == 0:
                continue

            chunk_length = len(value)
            buffers.append(value)
            index.set_chunk_slice(chunk_coords, slice(chunk_start, chunk_start + chunk_length))
            chunk_start += chunk_length

        if len(buffers) == 0:
            return None

        index_bytes = await self._encode_shard_index(index)
        if self.index_location == ShardingCodecIndexLocation.start:
            empty_chunks_mask = index.offsets_and_lengths[..., 0] == MAX_UINT_64
            index.offsets_and_lengths[~empty_chunks_mask, 0] += len(index_bytes)
            index_bytes = await self._encode_shard_index(
                index
            )  # encode again with corrected offsets
            buffers.insert(0, index_bytes)
        else:
            buffers.append(index_bytes)

        return template.combine(buffers)

    def _is_total_shard(
        self, all_chunk_coords: set[tuple[int, ...]], chunks_per_shard: tuple[int, ...]
    ) -> bool:
        return len(all_chunk_coords) == product(chunks_per_shard) and all(
            chunk_coords in all_chunk_coords for chunk_coords in c_order_iter(chunks_per_shard)
        )

    async def _decode_shard_index(
        self, index_bytes: Buffer, chunks_per_shard: tuple[int, ...]
    ) -> _ShardIndex:
        index_array = next(
            iter(
                await get_pipeline_class()
                .from_codecs(self.index_codecs)
                .decode(
                    [(index_bytes, self._get_index_chunk_spec(chunks_per_shard))],
                )
            )
        )
        assert index_array is not None
        return _ShardIndex(index_array.as_numpy_array())

    async def _encode_shard_index(self, index: _ShardIndex) -> Buffer:
        index_bytes = next(
            iter(
                await get_pipeline_class()
                .from_codecs(self.index_codecs)
                .encode(
                    [
                        (
                            get_ndbuffer_class().from_numpy_array(index.offsets_and_lengths),
                            self._get_index_chunk_spec(index.chunks_per_shard),
                        )
                    ],
                )
            )
        )
        assert index_bytes is not None
        assert isinstance(index_bytes, Buffer)
        return index_bytes

    def _shard_index_size(self, chunks_per_shard: tuple[int, ...]) -> int:
        return (
            get_pipeline_class()
            .from_codecs(self.index_codecs)
            .compute_encoded_size(
                16 * product(chunks_per_shard), self._get_index_chunk_spec(chunks_per_shard)
            )
        )

    def _get_index_chunk_spec(self, chunks_per_shard: tuple[int, ...]) -> ArraySpec:
        return ArraySpec(
            shape=chunks_per_shard + (2,),
            dtype=UInt64(endianness="little"),
            fill_value=MAX_UINT_64,
            config=ArrayConfig(
                order="C", write_empty_chunks=False
            ),  # Note: this is hard-coded for simplicity -- it is not surfaced into user code,
            prototype=default_buffer_prototype(),
        )

    def _get_chunk_spec(self, shard_spec: ArraySpec) -> ArraySpec:
        return ArraySpec(
            shape=self.chunk_shape,
            dtype=shard_spec.dtype,
            fill_value=shard_spec.fill_value,
            config=shard_spec.config,
            prototype=shard_spec.prototype,
        )

    def _get_chunks_per_shard(self, shard_spec: ArraySpec) -> tuple[int, ...]:
        return tuple(
            s // c
            for s, c in zip(
                shard_spec.shape,
                self.chunk_shape,
                strict=False,
            )
        )

    async def _load_shard_index_maybe(
        self, byte_getter: ByteGetter, chunks_per_shard: tuple[int, ...]
    ) -> _ShardIndex | None:
        shard_index_size = self._shard_index_size(chunks_per_shard)
        if self.index_location == ShardingCodecIndexLocation.start:
            index_bytes = await byte_getter.get(
                prototype=numpy_buffer_prototype(),
                byte_range=RangeByteRequest(0, shard_index_size),
            )
        else:
            index_bytes = await byte_getter.get(
                prototype=numpy_buffer_prototype(), byte_range=SuffixByteRequest(shard_index_size)
            )
        if index_bytes is not None:
            return await self._decode_shard_index(index_bytes, chunks_per_shard)
        return None

    async def _load_shard_index(
        self, byte_getter: ByteGetter, chunks_per_shard: tuple[int, ...]
    ) -> _ShardIndex:
        return (
            await self._load_shard_index_maybe(byte_getter, chunks_per_shard)
        ) or _ShardIndex.create_empty(chunks_per_shard)

    async def _load_full_shard_maybe(
        self, byte_getter: ByteGetter, prototype: BufferPrototype, chunks_per_shard: tuple[int, ...]
    ) -> _ShardReader | None:
        shard_bytes = await byte_getter.get(prototype=prototype)

        return (
            await _ShardReader.from_bytes(shard_bytes, self, chunks_per_shard)
            if shard_bytes
            else None
        )

    def compute_encoded_size(self, input_byte_length: int, shard_spec: ArraySpec) -> int:
        chunks_per_shard = self._get_chunks_per_shard(shard_spec)
        return input_byte_length + self._shard_index_size(chunks_per_shard)
