from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np
from numcodecs.compat import ensure_bytes, ensure_ndarray_like

from zarr.abc.codec import ArrayBytesCodec
from zarr.registry import get_ndbuffer_class

if TYPE_CHECKING:
    from zarr.abc.numcodec import Numcodec
    from zarr.core.array_spec import ArraySpec
    from zarr.core.buffer import Buffer, NDBuffer


@dataclass(frozen=True)
class V2Codec(ArrayBytesCodec):
    filters: tuple[Numcodec, ...] | None
    compressor: Numcodec | None

    is_fixed_size = False

    async def _decode_single(
        self,
        chunk_bytes: Buffer,
        chunk_spec: ArraySpec,
    ) -> NDBuffer:
        cdata = chunk_bytes.as_array_like()
        # decompress
        if self.compressor:
            chunk = await asyncio.to_thread(self.compressor.decode, cdata)
        else:
            chunk = cdata

        # apply filters
        if self.filters:
            for f in reversed(self.filters):
                chunk = await asyncio.to_thread(f.decode, chunk)

        # view as numpy array with correct dtype
        chunk = ensure_ndarray_like(chunk)
        # special case object dtype, because incorrect handling can lead to
        # segfaults and other bad things happening
        if chunk_spec.dtype.dtype_cls is not np.dtypes.ObjectDType:
            try:
                chunk = chunk.view(chunk_spec.dtype.to_native_dtype())
            except TypeError:
                # this will happen if the dtype of the chunk
                # does not match the dtype of the array spec i.g. if
                # the dtype of the chunk_spec is a string dtype, but the chunk
                # is an object array. In this case, we need to convert the object
                # array to the correct dtype.

                chunk = np.array(chunk).astype(chunk_spec.dtype.to_native_dtype())

        elif chunk.dtype != object:
            # If we end up here, someone must have hacked around with the filters.
            # We cannot deal with object arrays unless there is an object
            # codec in the filter chain, i.e., a filter that converts from object
            # array to something else during encoding, and converts back to object
            # array during decoding.
            raise RuntimeError("cannot read object array without object codec")

        # ensure correct chunk shape
        chunk = chunk.reshape(-1, order="A")
        chunk = chunk.reshape(chunk_spec.shape, order=chunk_spec.order)

        return get_ndbuffer_class().from_ndarray_like(chunk)

    async def _encode_single(
        self,
        chunk_array: NDBuffer,
        chunk_spec: ArraySpec,
    ) -> Buffer | None:
        chunk = chunk_array.as_ndarray_like()

        # ensure contiguous and correct order
        chunk = chunk.astype(chunk_spec.dtype.to_native_dtype(), order=chunk_spec.order, copy=False)

        # apply filters
        if self.filters:
            for f in self.filters:
                chunk = await asyncio.to_thread(f.encode, chunk)
        # check object encoding
        if ensure_ndarray_like(chunk).dtype == object:
            raise RuntimeError("cannot write object array without object codec")

        # compress
        if self.compressor:
            cdata = await asyncio.to_thread(self.compressor.encode, chunk)
        else:
            cdata = chunk
        cdata = ensure_bytes(cdata)
        return chunk_spec.prototype.buffer.from_bytes(cdata)

    def compute_encoded_size(self, _input_byte_length: int, _chunk_spec: ArraySpec) -> int:
        raise NotImplementedError
