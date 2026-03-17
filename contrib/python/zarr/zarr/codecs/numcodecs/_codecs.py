"""
This module provides compatibility for [numcodecs][] in Zarr version 3.

These codecs were previously defined in [numcodecs][], and have now been moved to `zarr`.

```python
import numpy as np
import zarr
import zarr.codecs.numcodecs as numcodecs

array = zarr.create_array(
  store="data_numcodecs.zarr",
  shape=(1024, 1024),
  chunks=(64, 64),
  dtype="uint32",
  filters=[numcodecs.Delta(dtype="uint32")],
  compressors=[numcodecs.BZ2(level=5)],
  overwrite=True)
array[:] = np.arange(np.prod(array.shape), dtype=array.dtype).reshape(*array.shape)
```

!!! note
    Please note that the codecs in [zarr.codecs.numcodecs][] are not part of the Zarr version
    3 specification. Using these codecs might cause interoperability issues with other Zarr
    implementations.
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass, replace
from functools import cached_property
from typing import TYPE_CHECKING, Any, Self
from warnings import warn

import numpy as np

from zarr.abc.codec import ArrayArrayCodec, ArrayBytesCodec, BytesBytesCodec
from zarr.abc.metadata import Metadata
from zarr.core.buffer.cpu import as_numpy_array_wrapper
from zarr.core.common import JSON, parse_named_configuration, product
from zarr.dtype import UInt8, ZDType, parse_dtype
from zarr.errors import ZarrUserWarning
from zarr.registry import get_numcodec

if TYPE_CHECKING:
    from zarr.abc.numcodec import Numcodec
    from zarr.core.array_spec import ArraySpec
    from zarr.core.buffer import Buffer, BufferPrototype, NDBuffer

CODEC_PREFIX = "numcodecs."


def _expect_name_prefix(codec_name: str) -> str:
    if not codec_name.startswith(CODEC_PREFIX):
        raise ValueError(
            f"Expected name to start with '{CODEC_PREFIX}'. Got {codec_name} instead."
        )  # pragma: no cover
    return codec_name.removeprefix(CODEC_PREFIX)


def _parse_codec_configuration(data: dict[str, JSON]) -> dict[str, JSON]:
    parsed_name, parsed_configuration = parse_named_configuration(data)
    if not parsed_name.startswith(CODEC_PREFIX):
        raise ValueError(
            f"Expected name to start with '{CODEC_PREFIX}'. Got {parsed_name} instead."
        )  # pragma: no cover
    id = _expect_name_prefix(parsed_name)
    return {"id": id, **parsed_configuration}


@dataclass(frozen=True)
class _NumcodecsCodec(Metadata):
    codec_name: str
    codec_config: dict[str, JSON]

    def __init_subclass__(cls, *, codec_name: str | None = None, **kwargs: Any) -> None:
        """To be used only when creating the actual public-facing codec class."""
        super().__init_subclass__(**kwargs)
        if codec_name is not None:
            namespace = codec_name

            cls_name = f"{CODEC_PREFIX}{namespace}.{cls.__name__}"
            cls.codec_name = f"{CODEC_PREFIX}{namespace}"
            cls.__doc__ = f"""
            See [{cls_name}][] for more details and parameters.
            """

    def __init__(self, **codec_config: JSON) -> None:
        if not self.codec_name:
            raise ValueError(
                "The codec name needs to be supplied through the `codec_name` attribute."
            )  # pragma: no cover
        unprefixed_codec_name = _expect_name_prefix(self.codec_name)

        if "id" not in codec_config:
            codec_config = {"id": unprefixed_codec_name, **codec_config}
        elif codec_config["id"] != unprefixed_codec_name:
            raise ValueError(
                f"Codec id does not match {unprefixed_codec_name}. Got: {codec_config['id']}."
            )  # pragma: no cover

        object.__setattr__(self, "codec_config", codec_config)
        warn(
            "Numcodecs codecs are not in the Zarr version 3 specification and "
            "may not be supported by other zarr implementations.",
            category=ZarrUserWarning,
            stacklevel=2,
        )

    @cached_property
    def _codec(self) -> Numcodec:
        return get_numcodec(self.codec_config)  # type: ignore[arg-type]

    @classmethod
    def from_dict(cls, data: dict[str, JSON]) -> Self:
        codec_config = _parse_codec_configuration(data)
        return cls(**codec_config)

    def to_dict(self) -> dict[str, JSON]:
        codec_config = self.codec_config.copy()
        codec_config.pop("id", None)
        return {
            "name": self.codec_name,
            "configuration": codec_config,
        }

    def compute_encoded_size(self, input_byte_length: int, chunk_spec: ArraySpec) -> int:
        raise NotImplementedError  # pragma: no cover

    # Override __repr__ because dynamically constructed classes don't seem to work otherwise
    def __repr__(self) -> str:
        codec_config = self.codec_config.copy()
        codec_config.pop("id", None)
        return f"{self.__class__.__name__}(codec_name={self.codec_name!r}, codec_config={codec_config!r})"


class _NumcodecsBytesBytesCodec(_NumcodecsCodec, BytesBytesCodec):
    def __init__(self, **codec_config: JSON) -> None:
        super().__init__(**codec_config)

    async def _decode_single(self, chunk_data: Buffer, chunk_spec: ArraySpec) -> Buffer:
        return await asyncio.to_thread(
            as_numpy_array_wrapper,
            self._codec.decode,
            chunk_data,
            chunk_spec.prototype,
        )

    def _encode(self, chunk_data: Buffer, prototype: BufferPrototype) -> Buffer:
        encoded = self._codec.encode(chunk_data.as_array_like())
        if isinstance(encoded, np.ndarray):  # Required for checksum codecs
            return prototype.buffer.from_bytes(encoded.tobytes())
        return prototype.buffer.from_bytes(encoded)

    async def _encode_single(self, chunk_data: Buffer, chunk_spec: ArraySpec) -> Buffer:
        return await asyncio.to_thread(self._encode, chunk_data, chunk_spec.prototype)


class _NumcodecsArrayArrayCodec(_NumcodecsCodec, ArrayArrayCodec):
    def __init__(self, **codec_config: JSON) -> None:
        super().__init__(**codec_config)

    async def _decode_single(self, chunk_data: NDBuffer, chunk_spec: ArraySpec) -> NDBuffer:
        chunk_ndarray = chunk_data.as_ndarray_like()
        out = await asyncio.to_thread(self._codec.decode, chunk_ndarray)
        return chunk_spec.prototype.nd_buffer.from_ndarray_like(out.reshape(chunk_spec.shape))

    async def _encode_single(self, chunk_data: NDBuffer, chunk_spec: ArraySpec) -> NDBuffer:
        chunk_ndarray = chunk_data.as_ndarray_like()
        out = await asyncio.to_thread(self._codec.encode, chunk_ndarray)
        return chunk_spec.prototype.nd_buffer.from_ndarray_like(out)


class _NumcodecsArrayBytesCodec(_NumcodecsCodec, ArrayBytesCodec):
    def __init__(self, **codec_config: JSON) -> None:
        super().__init__(**codec_config)

    async def _decode_single(self, chunk_data: Buffer, chunk_spec: ArraySpec) -> NDBuffer:
        chunk_bytes = chunk_data.to_bytes()
        out = await asyncio.to_thread(self._codec.decode, chunk_bytes)
        return chunk_spec.prototype.nd_buffer.from_ndarray_like(out.reshape(chunk_spec.shape))

    async def _encode_single(self, chunk_data: NDBuffer, chunk_spec: ArraySpec) -> Buffer:
        chunk_ndarray = chunk_data.as_ndarray_like()
        out = await asyncio.to_thread(self._codec.encode, chunk_ndarray)
        return chunk_spec.prototype.buffer.from_bytes(out)


# bytes-to-bytes codecs
class Blosc(_NumcodecsBytesBytesCodec, codec_name="blosc"):
    pass


class LZ4(_NumcodecsBytesBytesCodec, codec_name="lz4"):
    pass


class Zstd(_NumcodecsBytesBytesCodec, codec_name="zstd"):
    pass


class Zlib(_NumcodecsBytesBytesCodec, codec_name="zlib"):
    pass


class GZip(_NumcodecsBytesBytesCodec, codec_name="gzip"):
    pass


class BZ2(_NumcodecsBytesBytesCodec, codec_name="bz2"):
    pass


class LZMA(_NumcodecsBytesBytesCodec, codec_name="lzma"):
    pass


class Shuffle(_NumcodecsBytesBytesCodec, codec_name="shuffle"):
    def evolve_from_array_spec(self, array_spec: ArraySpec) -> Shuffle:
        if self.codec_config.get("elementsize") is None:
            dtype = array_spec.dtype.to_native_dtype()
            return Shuffle(**{**self.codec_config, "elementsize": dtype.itemsize})
        return self  # pragma: no cover


# array-to-array codecs ("filters")
class Delta(_NumcodecsArrayArrayCodec, codec_name="delta"):
    def resolve_metadata(self, chunk_spec: ArraySpec) -> ArraySpec:
        if astype := self.codec_config.get("astype"):
            dtype = parse_dtype(np.dtype(astype), zarr_format=3)  # type: ignore[call-overload]
            return replace(chunk_spec, dtype=dtype)
        return chunk_spec


class BitRound(_NumcodecsArrayArrayCodec, codec_name="bitround"):
    pass


class FixedScaleOffset(_NumcodecsArrayArrayCodec, codec_name="fixedscaleoffset"):
    def resolve_metadata(self, chunk_spec: ArraySpec) -> ArraySpec:
        if astype := self.codec_config.get("astype"):
            dtype = parse_dtype(np.dtype(astype), zarr_format=3)  # type: ignore[call-overload]
            return replace(chunk_spec, dtype=dtype)
        return chunk_spec

    def evolve_from_array_spec(self, array_spec: ArraySpec) -> FixedScaleOffset:
        if self.codec_config.get("dtype") is None:
            dtype = array_spec.dtype.to_native_dtype()
            return FixedScaleOffset(**{**self.codec_config, "dtype": str(dtype)})
        return self


class Quantize(_NumcodecsArrayArrayCodec, codec_name="quantize"):
    def __init__(self, **codec_config: JSON) -> None:
        super().__init__(**codec_config)

    def evolve_from_array_spec(self, array_spec: ArraySpec) -> Quantize:
        if self.codec_config.get("dtype") is None:
            dtype = array_spec.dtype.to_native_dtype()
            return Quantize(**{**self.codec_config, "dtype": str(dtype)})
        return self


class PackBits(_NumcodecsArrayArrayCodec, codec_name="packbits"):
    def resolve_metadata(self, chunk_spec: ArraySpec) -> ArraySpec:
        return replace(
            chunk_spec,
            shape=(1 + math.ceil(product(chunk_spec.shape) / 8),),
            dtype=UInt8(),
        )

    # todo: remove this type: ignore when this class can be defined w.r.t.
    # a single zarr dtype API
    def validate(self, *, dtype: ZDType[Any, Any], **_kwargs: Any) -> None:
        # this is bugged and will fail
        _dtype = dtype.to_native_dtype()
        if _dtype != np.dtype("bool"):
            raise ValueError(f"Packbits filter requires bool dtype. Got {dtype}.")


class AsType(_NumcodecsArrayArrayCodec, codec_name="astype"):
    def resolve_metadata(self, chunk_spec: ArraySpec) -> ArraySpec:
        dtype = parse_dtype(np.dtype(self.codec_config["encode_dtype"]), zarr_format=3)  # type: ignore[arg-type]
        return replace(chunk_spec, dtype=dtype)

    def evolve_from_array_spec(self, array_spec: ArraySpec) -> AsType:
        if self.codec_config.get("decode_dtype") is None:
            # TODO: remove these coverage exemptions the correct way, i.e. with tests
            dtype = array_spec.dtype.to_native_dtype()  # pragma: no cover
            return AsType(**{**self.codec_config, "decode_dtype": str(dtype)})  # pragma: no cover
        return self


# bytes-to-bytes checksum codecs
class _NumcodecsChecksumCodec(_NumcodecsBytesBytesCodec):
    def compute_encoded_size(self, input_byte_length: int, chunk_spec: ArraySpec) -> int:
        return input_byte_length + 4  # pragma: no cover


class CRC32(_NumcodecsChecksumCodec, codec_name="crc32"):
    pass


class CRC32C(_NumcodecsChecksumCodec, codec_name="crc32c"):
    pass


class Adler32(_NumcodecsChecksumCodec, codec_name="adler32"):
    pass


class Fletcher32(_NumcodecsChecksumCodec, codec_name="fletcher32"):
    pass


class JenkinsLookup3(_NumcodecsChecksumCodec, codec_name="jenkins_lookup3"):
    pass


# array-to-bytes codecs
class PCodec(_NumcodecsArrayBytesCodec, codec_name="pcodec"):
    pass


class ZFPY(_NumcodecsArrayBytesCodec, codec_name="zfpy"):
    pass
