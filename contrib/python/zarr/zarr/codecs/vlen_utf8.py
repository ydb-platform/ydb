from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np
from numcodecs.vlen import VLenBytes, VLenUTF8

from zarr.abc.codec import ArrayBytesCodec
from zarr.core.buffer import Buffer, NDBuffer
from zarr.core.common import JSON, parse_named_configuration

if TYPE_CHECKING:
    from typing import Self

    from zarr.core.array_spec import ArraySpec


# can use a global because there are no parameters
_vlen_utf8_codec = VLenUTF8()
_vlen_bytes_codec = VLenBytes()


@dataclass(frozen=True)
class VLenUTF8Codec(ArrayBytesCodec):
    """Variable-length UTF8 codec"""

    @classmethod
    def from_dict(cls, data: dict[str, JSON]) -> Self:
        _, configuration_parsed = parse_named_configuration(
            data, "vlen-utf8", require_configuration=False
        )
        configuration_parsed = configuration_parsed or {}
        return cls(**configuration_parsed)

    def to_dict(self) -> dict[str, JSON]:
        return {"name": "vlen-utf8", "configuration": {}}

    def evolve_from_array_spec(self, array_spec: ArraySpec) -> Self:
        return self

    # TODO: expand the tests for this function
    async def _decode_single(
        self,
        chunk_bytes: Buffer,
        chunk_spec: ArraySpec,
    ) -> NDBuffer:
        assert isinstance(chunk_bytes, Buffer)

        raw_bytes = chunk_bytes.as_array_like()
        decoded = _vlen_utf8_codec.decode(raw_bytes)
        assert decoded.dtype == np.object_
        decoded.shape = chunk_spec.shape
        as_string_dtype = decoded.astype(chunk_spec.dtype.to_native_dtype(), copy=False)
        return chunk_spec.prototype.nd_buffer.from_numpy_array(as_string_dtype)

    async def _encode_single(
        self,
        chunk_array: NDBuffer,
        chunk_spec: ArraySpec,
    ) -> Buffer | None:
        assert isinstance(chunk_array, NDBuffer)
        return chunk_spec.prototype.buffer.from_bytes(
            _vlen_utf8_codec.encode(chunk_array.as_numpy_array())
        )

    def compute_encoded_size(self, input_byte_length: int, _chunk_spec: ArraySpec) -> int:
        # what is input_byte_length for an object dtype?
        raise NotImplementedError("compute_encoded_size is not implemented for VLen codecs")


@dataclass(frozen=True)
class VLenBytesCodec(ArrayBytesCodec):
    @classmethod
    def from_dict(cls, data: dict[str, JSON]) -> Self:
        _, configuration_parsed = parse_named_configuration(
            data, "vlen-bytes", require_configuration=False
        )
        configuration_parsed = configuration_parsed or {}
        return cls(**configuration_parsed)

    def to_dict(self) -> dict[str, JSON]:
        return {"name": "vlen-bytes", "configuration": {}}

    def evolve_from_array_spec(self, array_spec: ArraySpec) -> Self:
        return self

    async def _decode_single(
        self,
        chunk_bytes: Buffer,
        chunk_spec: ArraySpec,
    ) -> NDBuffer:
        assert isinstance(chunk_bytes, Buffer)

        raw_bytes = chunk_bytes.as_array_like()
        decoded = _vlen_bytes_codec.decode(raw_bytes)
        assert decoded.dtype == np.object_
        decoded.shape = chunk_spec.shape
        return chunk_spec.prototype.nd_buffer.from_numpy_array(decoded)

    async def _encode_single(
        self,
        chunk_array: NDBuffer,
        chunk_spec: ArraySpec,
    ) -> Buffer | None:
        assert isinstance(chunk_array, NDBuffer)
        return chunk_spec.prototype.buffer.from_bytes(
            _vlen_bytes_codec.encode(chunk_array.as_numpy_array())
        )

    def compute_encoded_size(self, input_byte_length: int, _chunk_spec: ArraySpec) -> int:
        # what is input_byte_length for an object dtype?
        raise NotImplementedError("compute_encoded_size is not implemented for VLen codecs")
