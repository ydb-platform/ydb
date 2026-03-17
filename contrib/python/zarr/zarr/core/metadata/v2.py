from __future__ import annotations

import warnings
from collections.abc import Iterable, Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any, TypeAlias, TypedDict, cast

from zarr.abc.metadata import Metadata
from zarr.abc.numcodec import Numcodec, _is_numcodec
from zarr.core.chunk_grids import RegularChunkGrid
from zarr.core.dtype import get_data_type_from_json
from zarr.core.dtype.common import OBJECT_CODEC_IDS, DTypeSpec_V2
from zarr.errors import ZarrUserWarning
from zarr.registry import get_numcodec

if TYPE_CHECKING:
    from typing import Literal, Self

    import numpy.typing as npt

    from zarr.core.buffer import Buffer, BufferPrototype
    from zarr.core.dtype.wrapper import (
        TBaseDType,
        TBaseScalar,
        TDType_co,
        TScalar_co,
        ZDType,
    )

import json
from dataclasses import dataclass, field, fields, replace

import numpy as np

from zarr.core.array_spec import ArrayConfig, ArraySpec
from zarr.core.chunk_key_encodings import parse_separator
from zarr.core.common import (
    JSON,
    ZARRAY_JSON,
    ZATTRS_JSON,
    MemoryOrder,
    parse_shapelike,
)
from zarr.core.config import config, parse_indexing_order
from zarr.core.metadata.common import parse_attributes


class ArrayV2MetadataDict(TypedDict):
    """
    A typed dictionary model for Zarr format 2 metadata.
    """

    zarr_format: Literal[2]
    attributes: dict[str, JSON]


# Union of acceptable types for v2 compressors
CompressorLikev2: TypeAlias = dict[str, JSON] | Numcodec | None


@dataclass(frozen=True, kw_only=True)
class ArrayV2Metadata(Metadata):
    shape: tuple[int, ...]
    chunks: tuple[int, ...]
    dtype: ZDType[TBaseDType, TBaseScalar]
    fill_value: int | float | str | bytes | None = None
    order: MemoryOrder = "C"
    filters: tuple[Numcodec, ...] | None = None
    dimension_separator: Literal[".", "/"] = "."
    compressor: Numcodec | None
    attributes: dict[str, JSON] = field(default_factory=dict)
    zarr_format: Literal[2] = field(init=False, default=2)

    def __init__(
        self,
        *,
        shape: tuple[int, ...],
        dtype: ZDType[TDType_co, TScalar_co],
        chunks: tuple[int, ...],
        fill_value: Any,
        order: MemoryOrder,
        dimension_separator: Literal[".", "/"] = ".",
        compressor: CompressorLikev2 = None,
        filters: Iterable[Numcodec | dict[str, JSON]] | None = None,
        attributes: dict[str, JSON] | None = None,
    ) -> None:
        """
        Metadata for a Zarr format 2 array.
        """
        shape_parsed = parse_shapelike(shape)
        chunks_parsed = parse_shapelike(chunks)
        compressor_parsed = parse_compressor(compressor)
        order_parsed = parse_indexing_order(order)
        dimension_separator_parsed = parse_separator(dimension_separator)
        filters_parsed = parse_filters(filters)
        fill_value_parsed: TBaseScalar | None
        if fill_value is not None:
            fill_value_parsed = dtype.cast_scalar(fill_value)
        else:
            fill_value_parsed = fill_value
        attributes_parsed = parse_attributes(attributes)

        object.__setattr__(self, "shape", shape_parsed)
        object.__setattr__(self, "dtype", dtype)
        object.__setattr__(self, "chunks", chunks_parsed)
        object.__setattr__(self, "compressor", compressor_parsed)
        object.__setattr__(self, "order", order_parsed)
        object.__setattr__(self, "dimension_separator", dimension_separator_parsed)
        object.__setattr__(self, "filters", filters_parsed)
        object.__setattr__(self, "fill_value", fill_value_parsed)
        object.__setattr__(self, "attributes", attributes_parsed)

        # ensure that the metadata document is consistent
        _ = parse_metadata(self)

    @property
    def ndim(self) -> int:
        return len(self.shape)

    @cached_property
    def chunk_grid(self) -> RegularChunkGrid:
        return RegularChunkGrid(chunk_shape=self.chunks)

    @property
    def shards(self) -> tuple[int, ...] | None:
        return None

    def to_buffer_dict(self, prototype: BufferPrototype) -> dict[str, Buffer]:
        zarray_dict = self.to_dict()
        zattrs_dict = zarray_dict.pop("attributes", {})
        json_indent = config.get("json_indent")
        return {
            ZARRAY_JSON: prototype.buffer.from_bytes(
                json.dumps(zarray_dict, indent=json_indent, allow_nan=True).encode()
            ),
            ZATTRS_JSON: prototype.buffer.from_bytes(
                json.dumps(zattrs_dict, indent=json_indent, allow_nan=True).encode()
            ),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ArrayV2Metadata:
        # Make a copy to protect the original from modification.
        _data = data.copy()
        # Check that the zarr_format attribute is correct.
        _ = parse_zarr_format(_data.pop("zarr_format"))

        # To resolve a numpy object dtype array, we need to search for an object codec,
        # which could be in filters or as a compressor.
        # we will reference a hard-coded collection of object codec ids for this search.

        _filters, _compressor = (data.get("filters"), data.get("compressor"))
        if _filters is not None:
            _filters = cast("tuple[dict[str, JSON], ...]", _filters)
            object_codec_id = get_object_codec_id(tuple(_filters) + (_compressor,))
        else:
            object_codec_id = get_object_codec_id((_compressor,))
        # we add a layer of indirection here around the dtype attribute of the array metadata
        # because we also need to know the object codec id, if any, to resolve the data type
        dtype_spec: DTypeSpec_V2 = {
            "name": data["dtype"],
            "object_codec_id": object_codec_id,
        }
        dtype = get_data_type_from_json(dtype_spec, zarr_format=2)

        _data["dtype"] = dtype
        fill_value_encoded = _data.get("fill_value")
        if fill_value_encoded is not None:
            fill_value = dtype.from_json_scalar(fill_value_encoded, zarr_format=2)
            _data["fill_value"] = fill_value

        # zarr v2 allowed arbitrary keys here.
        # We don't want the ArrayV2Metadata constructor to fail just because someone put an
        # extra key in the metadata.
        expected = {x.name for x in fields(cls)}
        expected |= {"dtype", "chunks"}

        # check if `filters` is an empty sequence; if so use None instead and raise a warning
        filters = _data.get("filters")
        if (
            isinstance(filters, Sequence)
            and not isinstance(filters, (str, bytes))
            and len(filters) == 0
        ):
            msg = (
                "Found an empty list of filters in the array metadata document. "
                "This is contrary to the Zarr V2 specification, and will cause an error in the future. "
                "Use None (or Null in a JSON document) instead of an empty list of filters."
            )
            warnings.warn(msg, ZarrUserWarning, stacklevel=1)
            _data["filters"] = None

        _data = {k: v for k, v in _data.items() if k in expected}

        return cls(**_data)

    def to_dict(self) -> dict[str, JSON]:
        zarray_dict = super().to_dict()
        if _is_numcodec(zarray_dict["compressor"]):
            codec_config = zarray_dict["compressor"].get_config()
            # Hotfix for https://github.com/zarr-developers/zarr-python/issues/2647
            if codec_config["id"] == "zstd" and not codec_config.get("checksum", False):
                codec_config.pop("checksum")
            zarray_dict["compressor"] = codec_config

        if zarray_dict["filters"] is not None:
            raw_filters = zarray_dict["filters"]
            # TODO: remove this when we can stratically type the output JSON data structure
            # entirely
            if not isinstance(raw_filters, list | tuple):
                raise TypeError("Invalid type for filters. Expected a list or tuple.")
            new_filters = []
            for f in raw_filters:
                if _is_numcodec(f):
                    new_filters.append(f.get_config())
                else:
                    new_filters.append(f)
            zarray_dict["filters"] = new_filters

        # serialize the fill value after dtype-specific JSON encoding
        if self.fill_value is not None:
            fill_value = self.dtype.to_json_scalar(self.fill_value, zarr_format=2)
            zarray_dict["fill_value"] = fill_value

        # pull the "name" attribute out of the dtype spec returned by self.dtype.to_json
        zarray_dict["dtype"] = self.dtype.to_json(zarr_format=2)["name"]

        return zarray_dict

    def get_chunk_spec(
        self, _chunk_coords: tuple[int, ...], array_config: ArrayConfig, prototype: BufferPrototype
    ) -> ArraySpec:
        return ArraySpec(
            shape=self.chunks,
            dtype=self.dtype,
            fill_value=self.fill_value,
            config=array_config,
            prototype=prototype,
        )

    def encode_chunk_key(self, chunk_coords: tuple[int, ...]) -> str:
        chunk_identifier = self.dimension_separator.join(map(str, chunk_coords))
        return "0" if chunk_identifier == "" else chunk_identifier

    def update_shape(self, shape: tuple[int, ...]) -> Self:
        return replace(self, shape=shape)

    def update_attributes(self, attributes: dict[str, JSON]) -> Self:
        return replace(self, attributes=attributes)


def parse_dtype(data: npt.DTypeLike) -> np.dtype[Any]:
    if isinstance(data, list):  # this is a valid _VoidDTypeLike check
        data = [tuple(d) for d in data]
    return np.dtype(data)


def parse_zarr_format(data: object) -> Literal[2]:
    if data == 2:
        return 2
    raise ValueError(f"Invalid value. Expected 2. Got {data}.")


def parse_filters(data: object) -> tuple[Numcodec, ...] | None:
    """
    Parse a potential tuple of filters
    """
    out: list[Numcodec] = []

    if data is None:
        return data
    if isinstance(data, Iterable):
        for idx, val in enumerate(data):
            if _is_numcodec(val):
                out.append(val)
            elif isinstance(val, dict):
                out.append(get_numcodec(val))  # type: ignore[arg-type]
            else:
                msg = f"Invalid filter at index {idx}. Expected a numcodecs.abc.Codec or a dict representation of numcodecs.abc.Codec. Got {type(val)} instead."
                raise TypeError(msg)
        if len(out) == 0:
            # Per the v2 spec, an empty tuple is not allowed -- use None to express "no filters"
            return None
        else:
            return tuple(out)
    # take a single codec instance and wrap it in a tuple
    if _is_numcodec(data):
        return (data,)
    msg = f"Invalid filters. Expected None, an iterable of numcodecs.abc.Codec or dict representations of numcodecs.abc.Codec. Got {type(data)} instead."
    raise TypeError(msg)


def parse_compressor(data: object) -> Numcodec | None:
    """
    Parse a potential compressor.
    """
    if data is None or _is_numcodec(data):
        return data
    if isinstance(data, dict):
        return get_numcodec(data)  # type: ignore[arg-type]
    msg = f"Invalid compressor. Expected None, a numcodecs.abc.Codec, or a dict representation of a numcodecs.abc.Codec. Got {type(data)} instead."
    raise ValueError(msg)


def parse_metadata(data: ArrayV2Metadata) -> ArrayV2Metadata:
    if (l_chunks := len(data.chunks)) != (l_shape := len(data.shape)):
        msg = (
            f"The `shape` and `chunks` attributes must have the same length. "
            f"`chunks` has length {l_chunks}, but `shape` has length {l_shape}."
        )
        raise ValueError(msg)
    return data


def get_object_codec_id(maybe_object_codecs: Sequence[JSON]) -> str | None:
    """
    Inspect a sequence of codecs / filters for an "object codec", i.e. a codec
    that can serialize object arrays to contiguous bytes. Zarr python
    maintains a hard-coded set of object codec ids. If any element from the input
    has an id that matches one of the hard-coded object codec ids, that id
    is returned immediately.
    """
    object_codec_id = None
    for maybe_object_codec in maybe_object_codecs:
        if (
            isinstance(maybe_object_codec, dict)
            and maybe_object_codec.get("id") in OBJECT_CODEC_IDS
        ):
            return cast("str", maybe_object_codec["id"])
    return object_codec_id
