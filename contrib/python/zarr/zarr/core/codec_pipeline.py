from __future__ import annotations

from dataclasses import dataclass
from itertools import islice, pairwise
from typing import TYPE_CHECKING, Any, TypeVar
from warnings import warn

from zarr.abc.codec import (
    ArrayArrayCodec,
    ArrayBytesCodec,
    ArrayBytesCodecPartialDecodeMixin,
    ArrayBytesCodecPartialEncodeMixin,
    BytesBytesCodec,
    Codec,
    CodecPipeline,
)
from zarr.core.common import concurrent_map
from zarr.core.config import config
from zarr.core.indexing import SelectorTuple, is_scalar
from zarr.errors import ZarrUserWarning
from zarr.registry import register_pipeline

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator
    from typing import Self

    from zarr.abc.store import ByteGetter, ByteSetter
    from zarr.core.array_spec import ArraySpec
    from zarr.core.buffer import Buffer, BufferPrototype, NDBuffer
    from zarr.core.chunk_grids import ChunkGrid
    from zarr.core.dtype.wrapper import TBaseDType, TBaseScalar, ZDType

T = TypeVar("T")
U = TypeVar("U")


def _unzip2(iterable: Iterable[tuple[T, U]]) -> tuple[list[T], list[U]]:
    out0: list[T] = []
    out1: list[U] = []
    for item0, item1 in iterable:
        out0.append(item0)
        out1.append(item1)
    return (out0, out1)


def batched(iterable: Iterable[T], n: int) -> Iterable[tuple[T, ...]]:
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


def resolve_batched(codec: Codec, chunk_specs: Iterable[ArraySpec]) -> Iterable[ArraySpec]:
    return [codec.resolve_metadata(chunk_spec) for chunk_spec in chunk_specs]


def fill_value_or_default(chunk_spec: ArraySpec) -> Any:
    fill_value = chunk_spec.fill_value
    if fill_value is None:
        # Zarr V2 allowed `fill_value` to be null in the metadata.
        # Zarr V3 requires it to be set. This has already been
        # validated when decoding the metadata, but we support reading
        # Zarr V2 data and need to support the case where fill_value
        # is None.
        return chunk_spec.dtype.default_scalar()
    else:
        return fill_value


@dataclass(frozen=True)
class BatchedCodecPipeline(CodecPipeline):
    """Default codec pipeline.

    This batched codec pipeline divides the chunk batches into batches of a configurable
    batch size ("mini-batch"). Fetching, decoding, encoding and storing are performed in
    lock step for each mini-batch. Multiple mini-batches are processing concurrently.
    """

    array_array_codecs: tuple[ArrayArrayCodec, ...]
    array_bytes_codec: ArrayBytesCodec
    bytes_bytes_codecs: tuple[BytesBytesCodec, ...]
    batch_size: int

    def evolve_from_array_spec(self, array_spec: ArraySpec) -> Self:
        return type(self).from_codecs(c.evolve_from_array_spec(array_spec=array_spec) for c in self)

    @classmethod
    def from_codecs(cls, codecs: Iterable[Codec], *, batch_size: int | None = None) -> Self:
        array_array_codecs, array_bytes_codec, bytes_bytes_codecs = codecs_from_list(codecs)

        return cls(
            array_array_codecs=array_array_codecs,
            array_bytes_codec=array_bytes_codec,
            bytes_bytes_codecs=bytes_bytes_codecs,
            batch_size=batch_size or config.get("codec_pipeline.batch_size"),
        )

    @property
    def supports_partial_decode(self) -> bool:
        """Determines whether the codec pipeline supports partial decoding.

        Currently, only codec pipelines with a single ArrayBytesCodec that supports
        partial decoding can support partial decoding. This limitation is due to the fact
        that ArrayArrayCodecs can change the slice selection leading to non-contiguous
        slices and BytesBytesCodecs can change the chunk bytes in a way that slice
        selections cannot be attributed to byte ranges anymore which renders partial
        decoding infeasible.

        This limitation may softened in the future."""
        return (len(self.array_array_codecs) + len(self.bytes_bytes_codecs)) == 0 and isinstance(
            self.array_bytes_codec, ArrayBytesCodecPartialDecodeMixin
        )

    @property
    def supports_partial_encode(self) -> bool:
        """Determines whether the codec pipeline supports partial encoding.

        Currently, only codec pipelines with a single ArrayBytesCodec that supports
        partial encoding can support partial encoding. This limitation is due to the fact
        that ArrayArrayCodecs can change the slice selection leading to non-contiguous
        slices and BytesBytesCodecs can change the chunk bytes in a way that slice
        selections cannot be attributed to byte ranges anymore which renders partial
        encoding infeasible.

        This limitation may softened in the future."""
        return (len(self.array_array_codecs) + len(self.bytes_bytes_codecs)) == 0 and isinstance(
            self.array_bytes_codec, ArrayBytesCodecPartialEncodeMixin
        )

    def __iter__(self) -> Iterator[Codec]:
        yield from self.array_array_codecs
        yield self.array_bytes_codec
        yield from self.bytes_bytes_codecs

    def validate(
        self,
        *,
        shape: tuple[int, ...],
        dtype: ZDType[TBaseDType, TBaseScalar],
        chunk_grid: ChunkGrid,
    ) -> None:
        for codec in self:
            codec.validate(shape=shape, dtype=dtype, chunk_grid=chunk_grid)

    def compute_encoded_size(self, byte_length: int, array_spec: ArraySpec) -> int:
        for codec in self:
            byte_length = codec.compute_encoded_size(byte_length, array_spec)
            array_spec = codec.resolve_metadata(array_spec)
        return byte_length

    def _codecs_with_resolved_metadata_batched(
        self, chunk_specs: Iterable[ArraySpec]
    ) -> tuple[
        list[tuple[ArrayArrayCodec, list[ArraySpec]]],
        tuple[ArrayBytesCodec, list[ArraySpec]],
        list[tuple[BytesBytesCodec, list[ArraySpec]]],
    ]:
        aa_codecs_with_spec: list[tuple[ArrayArrayCodec, list[ArraySpec]]] = []
        chunk_specs = list(chunk_specs)
        for aa_codec in self.array_array_codecs:
            aa_codecs_with_spec.append((aa_codec, chunk_specs))
            chunk_specs = [aa_codec.resolve_metadata(chunk_spec) for chunk_spec in chunk_specs]

        ab_codec_with_spec = (self.array_bytes_codec, chunk_specs)
        chunk_specs = [
            self.array_bytes_codec.resolve_metadata(chunk_spec) for chunk_spec in chunk_specs
        ]

        bb_codecs_with_spec: list[tuple[BytesBytesCodec, list[ArraySpec]]] = []
        for bb_codec in self.bytes_bytes_codecs:
            bb_codecs_with_spec.append((bb_codec, chunk_specs))
            chunk_specs = [bb_codec.resolve_metadata(chunk_spec) for chunk_spec in chunk_specs]

        return (aa_codecs_with_spec, ab_codec_with_spec, bb_codecs_with_spec)

    async def decode_batch(
        self,
        chunk_bytes_and_specs: Iterable[tuple[Buffer | None, ArraySpec]],
    ) -> Iterable[NDBuffer | None]:
        chunk_bytes_batch: Iterable[Buffer | None]
        chunk_bytes_batch, chunk_specs = _unzip2(chunk_bytes_and_specs)
        (
            aa_codecs_with_spec,
            ab_codec_with_spec,
            bb_codecs_with_spec,
        ) = self._codecs_with_resolved_metadata_batched(chunk_specs)

        for bb_codec, chunk_spec_batch in bb_codecs_with_spec[::-1]:
            chunk_bytes_batch = await bb_codec.decode(
                zip(chunk_bytes_batch, chunk_spec_batch, strict=False)
            )

        ab_codec, chunk_spec_batch = ab_codec_with_spec
        chunk_array_batch = await ab_codec.decode(
            zip(chunk_bytes_batch, chunk_spec_batch, strict=False)
        )

        for aa_codec, chunk_spec_batch in aa_codecs_with_spec[::-1]:
            chunk_array_batch = await aa_codec.decode(
                zip(chunk_array_batch, chunk_spec_batch, strict=False)
            )

        return chunk_array_batch

    async def decode_partial_batch(
        self,
        batch_info: Iterable[tuple[ByteGetter, SelectorTuple, ArraySpec]],
    ) -> Iterable[NDBuffer | None]:
        assert self.supports_partial_decode
        assert isinstance(self.array_bytes_codec, ArrayBytesCodecPartialDecodeMixin)
        return await self.array_bytes_codec.decode_partial(batch_info)

    async def encode_batch(
        self,
        chunk_arrays_and_specs: Iterable[tuple[NDBuffer | None, ArraySpec]],
    ) -> Iterable[Buffer | None]:
        chunk_array_batch: Iterable[NDBuffer | None]
        chunk_specs: Iterable[ArraySpec]
        chunk_array_batch, chunk_specs = _unzip2(chunk_arrays_and_specs)

        for aa_codec in self.array_array_codecs:
            chunk_array_batch = await aa_codec.encode(
                zip(chunk_array_batch, chunk_specs, strict=False)
            )
            chunk_specs = resolve_batched(aa_codec, chunk_specs)

        chunk_bytes_batch = await self.array_bytes_codec.encode(
            zip(chunk_array_batch, chunk_specs, strict=False)
        )
        chunk_specs = resolve_batched(self.array_bytes_codec, chunk_specs)

        for bb_codec in self.bytes_bytes_codecs:
            chunk_bytes_batch = await bb_codec.encode(
                zip(chunk_bytes_batch, chunk_specs, strict=False)
            )
            chunk_specs = resolve_batched(bb_codec, chunk_specs)

        return chunk_bytes_batch

    async def encode_partial_batch(
        self,
        batch_info: Iterable[tuple[ByteSetter, NDBuffer, SelectorTuple, ArraySpec]],
    ) -> None:
        assert self.supports_partial_encode
        assert isinstance(self.array_bytes_codec, ArrayBytesCodecPartialEncodeMixin)
        await self.array_bytes_codec.encode_partial(batch_info)

    async def read_batch(
        self,
        batch_info: Iterable[tuple[ByteGetter, ArraySpec, SelectorTuple, SelectorTuple, bool]],
        out: NDBuffer,
        drop_axes: tuple[int, ...] = (),
    ) -> None:
        if self.supports_partial_decode:
            chunk_array_batch = await self.decode_partial_batch(
                [
                    (byte_getter, chunk_selection, chunk_spec)
                    for byte_getter, chunk_spec, chunk_selection, *_ in batch_info
                ]
            )
            for chunk_array, (_, chunk_spec, _, out_selection, _) in zip(
                chunk_array_batch, batch_info, strict=False
            ):
                if chunk_array is not None:
                    out[out_selection] = chunk_array
                else:
                    out[out_selection] = fill_value_or_default(chunk_spec)
        else:
            chunk_bytes_batch = await concurrent_map(
                [(byte_getter, array_spec.prototype) for byte_getter, array_spec, *_ in batch_info],
                lambda byte_getter, prototype: byte_getter.get(prototype),
                config.get("async.concurrency"),
            )
            chunk_array_batch = await self.decode_batch(
                [
                    (chunk_bytes, chunk_spec)
                    for chunk_bytes, (_, chunk_spec, *_) in zip(
                        chunk_bytes_batch, batch_info, strict=False
                    )
                ],
            )
            for chunk_array, (_, chunk_spec, chunk_selection, out_selection, _) in zip(
                chunk_array_batch, batch_info, strict=False
            ):
                if chunk_array is not None:
                    tmp = chunk_array[chunk_selection]
                    if drop_axes != ():
                        tmp = tmp.squeeze(axis=drop_axes)
                    out[out_selection] = tmp
                else:
                    out[out_selection] = fill_value_or_default(chunk_spec)

    def _merge_chunk_array(
        self,
        existing_chunk_array: NDBuffer | None,
        value: NDBuffer,
        out_selection: SelectorTuple,
        chunk_spec: ArraySpec,
        chunk_selection: SelectorTuple,
        is_complete_chunk: bool,
        drop_axes: tuple[int, ...],
    ) -> NDBuffer:
        if (
            is_complete_chunk
            and value.shape == chunk_spec.shape
            # Guard that this is not a partial chunk at the end with is_complete_chunk=True
            and value[out_selection].shape == chunk_spec.shape
        ):
            return value
        if existing_chunk_array is None:
            chunk_array = chunk_spec.prototype.nd_buffer.create(
                shape=chunk_spec.shape,
                dtype=chunk_spec.dtype.to_native_dtype(),
                order=chunk_spec.order,
                fill_value=fill_value_or_default(chunk_spec),
            )
        else:
            chunk_array = existing_chunk_array.copy()  # make a writable copy
        if chunk_selection == () or is_scalar(
            value.as_ndarray_like(), chunk_spec.dtype.to_native_dtype()
        ):
            chunk_value = value
        else:
            chunk_value = value[out_selection]
            # handle missing singleton dimensions
            if drop_axes != ():
                item = tuple(
                    None  # equivalent to np.newaxis
                    if idx in drop_axes
                    else slice(None)
                    for idx in range(chunk_spec.ndim)
                )
                chunk_value = chunk_value[item]
        chunk_array[chunk_selection] = chunk_value
        return chunk_array

    async def write_batch(
        self,
        batch_info: Iterable[tuple[ByteSetter, ArraySpec, SelectorTuple, SelectorTuple, bool]],
        value: NDBuffer,
        drop_axes: tuple[int, ...] = (),
    ) -> None:
        if self.supports_partial_encode:
            # Pass scalar values as is
            if len(value.shape) == 0:
                await self.encode_partial_batch(
                    [
                        (byte_setter, value, chunk_selection, chunk_spec)
                        for byte_setter, chunk_spec, chunk_selection, out_selection, _ in batch_info
                    ],
                )
            else:
                await self.encode_partial_batch(
                    [
                        (byte_setter, value[out_selection], chunk_selection, chunk_spec)
                        for byte_setter, chunk_spec, chunk_selection, out_selection, _ in batch_info
                    ],
                )

        else:
            # Read existing bytes if not total slice
            async def _read_key(
                byte_setter: ByteSetter | None, prototype: BufferPrototype
            ) -> Buffer | None:
                if byte_setter is None:
                    return None
                return await byte_setter.get(prototype=prototype)

            chunk_bytes_batch: Iterable[Buffer | None]
            chunk_bytes_batch = await concurrent_map(
                [
                    (
                        None if is_complete_chunk else byte_setter,
                        chunk_spec.prototype,
                    )
                    for byte_setter, chunk_spec, chunk_selection, _, is_complete_chunk in batch_info
                ],
                _read_key,
                config.get("async.concurrency"),
            )
            chunk_array_decoded = await self.decode_batch(
                [
                    (chunk_bytes, chunk_spec)
                    for chunk_bytes, (_, chunk_spec, *_) in zip(
                        chunk_bytes_batch, batch_info, strict=False
                    )
                ],
            )

            chunk_array_merged = [
                self._merge_chunk_array(
                    chunk_array,
                    value,
                    out_selection,
                    chunk_spec,
                    chunk_selection,
                    is_complete_chunk,
                    drop_axes,
                )
                for chunk_array, (
                    _,
                    chunk_spec,
                    chunk_selection,
                    out_selection,
                    is_complete_chunk,
                ) in zip(chunk_array_decoded, batch_info, strict=False)
            ]
            chunk_array_batch: list[NDBuffer | None] = []
            for chunk_array, (_, chunk_spec, *_) in zip(
                chunk_array_merged, batch_info, strict=False
            ):
                if chunk_array is None:
                    chunk_array_batch.append(None)  # type: ignore[unreachable]
                else:
                    if not chunk_spec.config.write_empty_chunks and chunk_array.all_equal(
                        fill_value_or_default(chunk_spec)
                    ):
                        chunk_array_batch.append(None)
                    else:
                        chunk_array_batch.append(chunk_array)

            chunk_bytes_batch = await self.encode_batch(
                [
                    (chunk_array, chunk_spec)
                    for chunk_array, (_, chunk_spec, *_) in zip(
                        chunk_array_batch, batch_info, strict=False
                    )
                ],
            )

            async def _write_key(byte_setter: ByteSetter, chunk_bytes: Buffer | None) -> None:
                if chunk_bytes is None:
                    await byte_setter.delete()
                else:
                    await byte_setter.set(chunk_bytes)

            await concurrent_map(
                [
                    (byte_setter, chunk_bytes)
                    for chunk_bytes, (byte_setter, *_) in zip(
                        chunk_bytes_batch, batch_info, strict=False
                    )
                ],
                _write_key,
                config.get("async.concurrency"),
            )

    async def decode(
        self,
        chunk_bytes_and_specs: Iterable[tuple[Buffer | None, ArraySpec]],
    ) -> Iterable[NDBuffer | None]:
        output: list[NDBuffer | None] = []
        for batch_info in batched(chunk_bytes_and_specs, self.batch_size):
            output.extend(await self.decode_batch(batch_info))
        return output

    async def encode(
        self,
        chunk_arrays_and_specs: Iterable[tuple[NDBuffer | None, ArraySpec]],
    ) -> Iterable[Buffer | None]:
        output: list[Buffer | None] = []
        for single_batch_info in batched(chunk_arrays_and_specs, self.batch_size):
            output.extend(await self.encode_batch(single_batch_info))
        return output

    async def read(
        self,
        batch_info: Iterable[tuple[ByteGetter, ArraySpec, SelectorTuple, SelectorTuple, bool]],
        out: NDBuffer,
        drop_axes: tuple[int, ...] = (),
    ) -> None:
        await concurrent_map(
            [
                (single_batch_info, out, drop_axes)
                for single_batch_info in batched(batch_info, self.batch_size)
            ],
            self.read_batch,
            config.get("async.concurrency"),
        )

    async def write(
        self,
        batch_info: Iterable[tuple[ByteSetter, ArraySpec, SelectorTuple, SelectorTuple, bool]],
        value: NDBuffer,
        drop_axes: tuple[int, ...] = (),
    ) -> None:
        await concurrent_map(
            [
                (single_batch_info, value, drop_axes)
                for single_batch_info in batched(batch_info, self.batch_size)
            ],
            self.write_batch,
            config.get("async.concurrency"),
        )


def codecs_from_list(
    codecs: Iterable[Codec],
) -> tuple[tuple[ArrayArrayCodec, ...], ArrayBytesCodec, tuple[BytesBytesCodec, ...]]:
    from zarr.codecs.sharding import ShardingCodec

    array_array: tuple[ArrayArrayCodec, ...] = ()
    array_bytes_maybe: ArrayBytesCodec | None = None
    bytes_bytes: tuple[BytesBytesCodec, ...] = ()

    if any(isinstance(codec, ShardingCodec) for codec in codecs) and len(tuple(codecs)) > 1:
        warn(
            "Combining a `sharding_indexed` codec disables partial reads and "
            "writes, which may lead to inefficient performance.",
            category=ZarrUserWarning,
            stacklevel=3,
        )

    for prev_codec, cur_codec in pairwise((None, *codecs)):
        if isinstance(cur_codec, ArrayArrayCodec):
            if isinstance(prev_codec, ArrayBytesCodec | BytesBytesCodec):
                msg = (
                    f"Invalid codec order. ArrayArrayCodec {cur_codec}"
                    "must be preceded by another ArrayArrayCodec. "
                    f"Got {type(prev_codec)} instead."
                )
                raise TypeError(msg)
            array_array += (cur_codec,)

        elif isinstance(cur_codec, ArrayBytesCodec):
            if isinstance(prev_codec, BytesBytesCodec):
                msg = (
                    f"Invalid codec order. ArrayBytes codec {cur_codec}"
                    f" must be preceded by an ArrayArrayCodec. Got {type(prev_codec)} instead."
                )
                raise TypeError(msg)

            if array_bytes_maybe is not None:
                msg = (
                    f"Got two instances of ArrayBytesCodec: {array_bytes_maybe} and {cur_codec}. "
                    "Only one array-to-bytes codec is allowed."
                )
                raise ValueError(msg)

            array_bytes_maybe = cur_codec

        elif isinstance(cur_codec, BytesBytesCodec):
            if isinstance(prev_codec, ArrayArrayCodec):
                msg = (
                    f"Invalid codec order. BytesBytesCodec {cur_codec}"
                    "must be preceded by either another BytesBytesCodec, or an ArrayBytesCodec. "
                    f"Got {type(prev_codec)} instead."
                )
            bytes_bytes += (cur_codec,)
        else:
            raise TypeError

    if array_bytes_maybe is None:
        raise ValueError("Required ArrayBytesCodec was not found.")
    else:
        return array_array, array_bytes_maybe, bytes_bytes


register_pipeline(BatchedCodecPipeline)
