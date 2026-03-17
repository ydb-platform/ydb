from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, NotRequired, TypedDict, TypeGuard, cast

from zarr.abc.metadata import Metadata
from zarr.core.buffer.core import default_buffer_prototype
from zarr.core.dtype import VariableLengthUTF8, ZDType, get_data_type_from_json
from zarr.core.dtype.common import check_dtype_spec_v3

if TYPE_CHECKING:
    from typing import Self

    from zarr.core.buffer import Buffer, BufferPrototype
    from zarr.core.chunk_grids import ChunkGrid
    from zarr.core.common import JSON
    from zarr.core.dtype.wrapper import TBaseDType, TBaseScalar


import json
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from typing import Any, Literal

from zarr.abc.codec import ArrayArrayCodec, ArrayBytesCodec, BytesBytesCodec, Codec
from zarr.core.array_spec import ArrayConfig, ArraySpec
from zarr.core.chunk_grids import ChunkGrid, RegularChunkGrid
from zarr.core.chunk_key_encodings import (
    ChunkKeyEncoding,
    ChunkKeyEncodingLike,
    parse_chunk_key_encoding,
)
from zarr.core.common import (
    JSON,
    ZARR_JSON,
    DimensionNames,
    NamedConfig,
    parse_named_configuration,
    parse_shapelike,
)
from zarr.core.config import config
from zarr.core.metadata.common import parse_attributes
from zarr.errors import MetadataValidationError, NodeTypeValidationError, UnknownCodecError
from zarr.registry import get_codec_class


def parse_zarr_format(data: object) -> Literal[3]:
    if data == 3:
        return 3
    msg = f"Invalid value for 'zarr_format'. Expected '3'. Got '{data}'."
    raise MetadataValidationError(msg)


def parse_node_type_array(data: object) -> Literal["array"]:
    if data == "array":
        return "array"
    msg = f"Invalid value for 'node_type'. Expected 'array'. Got '{data}'."
    raise NodeTypeValidationError(msg)


def parse_codecs(data: object) -> tuple[Codec, ...]:
    out: tuple[Codec, ...] = ()

    if not isinstance(data, Iterable):
        raise TypeError(f"Expected iterable, got {type(data)}")

    for c in data:
        if isinstance(
            c, ArrayArrayCodec | ArrayBytesCodec | BytesBytesCodec
        ):  # Can't use Codec here because of mypy limitation
            out += (c,)
        else:
            name_parsed, _ = parse_named_configuration(c, require_configuration=False)

            try:
                out += (get_codec_class(name_parsed).from_dict(c),)
            except KeyError as e:
                raise UnknownCodecError(f"Unknown codec: {e.args[0]!r}") from e

    return out


def validate_array_bytes_codec(codecs: tuple[Codec, ...]) -> ArrayBytesCodec:
    # ensure that we have at least one ArrayBytesCodec
    abcs: list[ArrayBytesCodec] = [codec for codec in codecs if isinstance(codec, ArrayBytesCodec)]
    if len(abcs) == 0:
        raise ValueError("At least one ArrayBytesCodec is required.")
    elif len(abcs) > 1:
        raise ValueError("Only one ArrayBytesCodec is allowed.")

    return abcs[0]


def validate_codecs(codecs: tuple[Codec, ...], dtype: ZDType[TBaseDType, TBaseScalar]) -> None:
    """Check that the codecs are valid for the given dtype"""
    from zarr.codecs.sharding import ShardingCodec

    abc = validate_array_bytes_codec(codecs)

    # Recursively resolve array-bytes codecs within sharding codecs
    while isinstance(abc, ShardingCodec):
        abc = validate_array_bytes_codec(abc.codecs)

    # we need to have special codecs if we are decoding vlen strings or bytestrings
    # TODO: use codec ID instead of class name
    codec_class_name = abc.__class__.__name__
    # TODO: Fix typing here
    if isinstance(dtype, VariableLengthUTF8) and not codec_class_name == "VLenUTF8Codec":  # type: ignore[unreachable]
        raise ValueError(
            f"For string dtype, ArrayBytesCodec must be `VLenUTF8Codec`, got `{codec_class_name}`."
        )


def parse_dimension_names(data: object) -> tuple[str | None, ...] | None:
    if data is None:
        return data
    elif isinstance(data, Iterable) and all(isinstance(x, type(None) | str) for x in data):
        return tuple(data)
    else:
        msg = f"Expected either None or a iterable of str, got {type(data)}"
        raise TypeError(msg)


def parse_storage_transformers(data: object) -> tuple[dict[str, JSON], ...]:
    """
    Parse storage_transformers. Zarr python cannot use storage transformers
    at this time, so this function doesn't attempt to validate them.
    """
    if data is None:
        return ()
    if isinstance(data, Iterable):
        if len(tuple(data)) >= 1:
            return data  # type: ignore[return-value]
        else:
            return ()
    raise TypeError(
        f"Invalid storage_transformers. Expected an iterable of dicts. Got {type(data)} instead."
    )


class AllowedExtraField(TypedDict):
    """
    This class models allowed extra fields in array metadata.
    They are ignored by Zarr Python.
    """

    must_understand: Literal[False]


def check_allowed_extra_field(data: object) -> TypeGuard[AllowedExtraField]:
    """
    Check if the extra field is allowed according to the Zarr v3 spec. The object
    must be a mapping with a "must_understand" key set to `False`.
    """
    return isinstance(data, Mapping) and data.get("must_understand") is False


def parse_extra_fields(
    data: Mapping[str, AllowedExtraField] | None,
) -> dict[str, AllowedExtraField]:
    if data is None:
        return {}
    else:
        conflict_keys = ARRAY_METADATA_KEYS & set(data.keys())
        if len(conflict_keys) > 0:
            msg = (
                "Invalid extra fields. "
                "The following keys: "
                f"{sorted(conflict_keys)} "
                "are invalid because they collide with keys reserved for use by the "
                "array metadata document."
            )
            raise ValueError(msg)
        return dict(data)


class ArrayMetadataJSON_V3(TypedDict):
    """
    A typed dictionary model for zarr v3 metadata.
    """

    zarr_format: Literal[3]
    node_type: Literal["array"]
    data_type: str | NamedConfig[str, Mapping[str, object]]
    shape: tuple[int, ...]
    chunk_grid: NamedConfig[str, Mapping[str, object]]
    chunk_key_encoding: NamedConfig[str, Mapping[str, object]]
    fill_value: object
    codecs: tuple[str | NamedConfig[str, Mapping[str, object]], ...]
    attributes: NotRequired[Mapping[str, JSON]]
    storage_transformers: NotRequired[tuple[NamedConfig[str, Mapping[str, object]], ...]]
    dimension_names: NotRequired[tuple[str | None]]


ARRAY_METADATA_KEYS = set(ArrayMetadataJSON_V3.__annotations__.keys())


@dataclass(frozen=True, kw_only=True)
class ArrayV3Metadata(Metadata):
    shape: tuple[int, ...]
    data_type: ZDType[TBaseDType, TBaseScalar]
    chunk_grid: ChunkGrid
    chunk_key_encoding: ChunkKeyEncoding
    fill_value: Any
    codecs: tuple[Codec, ...]
    attributes: dict[str, Any] = field(default_factory=dict)
    dimension_names: tuple[str | None, ...] | None = None
    zarr_format: Literal[3] = field(default=3, init=False)
    node_type: Literal["array"] = field(default="array", init=False)
    storage_transformers: tuple[dict[str, JSON], ...]
    extra_fields: dict[str, AllowedExtraField]

    def __init__(
        self,
        *,
        shape: Iterable[int],
        data_type: ZDType[TBaseDType, TBaseScalar],
        chunk_grid: dict[str, JSON] | ChunkGrid | NamedConfig[str, Any],
        chunk_key_encoding: ChunkKeyEncodingLike,
        fill_value: object,
        codecs: Iterable[Codec | dict[str, JSON] | NamedConfig[str, Any] | str],
        attributes: dict[str, JSON] | None,
        dimension_names: DimensionNames,
        storage_transformers: Iterable[dict[str, JSON]] | None = None,
        extra_fields: Mapping[str, AllowedExtraField] | None = None,
    ) -> None:
        """
        Because the class is a frozen dataclass, we set attributes using object.__setattr__
        """

        shape_parsed = parse_shapelike(shape)
        chunk_grid_parsed = ChunkGrid.from_dict(chunk_grid)
        chunk_key_encoding_parsed = parse_chunk_key_encoding(chunk_key_encoding)
        dimension_names_parsed = parse_dimension_names(dimension_names)
        # Note: relying on a type method is numpy-specific
        fill_value_parsed = data_type.cast_scalar(fill_value)
        attributes_parsed = parse_attributes(attributes)
        codecs_parsed_partial = parse_codecs(codecs)
        storage_transformers_parsed = parse_storage_transformers(storage_transformers)
        extra_fields_parsed = parse_extra_fields(extra_fields)
        array_spec = ArraySpec(
            shape=shape_parsed,
            dtype=data_type,
            fill_value=fill_value_parsed,
            config=ArrayConfig.from_dict({}),  # TODO: config is not needed here.
            prototype=default_buffer_prototype(),  # TODO: prototype is not needed here.
        )
        codecs_parsed = tuple(c.evolve_from_array_spec(array_spec) for c in codecs_parsed_partial)
        validate_codecs(codecs_parsed_partial, data_type)

        object.__setattr__(self, "shape", shape_parsed)
        object.__setattr__(self, "data_type", data_type)
        object.__setattr__(self, "chunk_grid", chunk_grid_parsed)
        object.__setattr__(self, "chunk_key_encoding", chunk_key_encoding_parsed)
        object.__setattr__(self, "codecs", codecs_parsed)
        object.__setattr__(self, "dimension_names", dimension_names_parsed)
        object.__setattr__(self, "fill_value", fill_value_parsed)
        object.__setattr__(self, "attributes", attributes_parsed)
        object.__setattr__(self, "storage_transformers", storage_transformers_parsed)
        object.__setattr__(self, "extra_fields", extra_fields_parsed)

        self._validate_metadata()

    def _validate_metadata(self) -> None:
        if isinstance(self.chunk_grid, RegularChunkGrid) and len(self.shape) != len(
            self.chunk_grid.chunk_shape
        ):
            raise ValueError(
                "`chunk_shape` and `shape` need to have the same number of dimensions."
            )
        if self.dimension_names is not None and len(self.shape) != len(self.dimension_names):
            raise ValueError(
                "`dimension_names` and `shape` need to have the same number of dimensions."
            )
        if self.fill_value is None:
            raise ValueError("`fill_value` is required.")
        for codec in self.codecs:
            codec.validate(shape=self.shape, dtype=self.data_type, chunk_grid=self.chunk_grid)

    @property
    def ndim(self) -> int:
        return len(self.shape)

    @property
    def dtype(self) -> ZDType[TBaseDType, TBaseScalar]:
        return self.data_type

    @property
    def chunks(self) -> tuple[int, ...]:
        if isinstance(self.chunk_grid, RegularChunkGrid):
            from zarr.codecs.sharding import ShardingCodec

            if len(self.codecs) == 1 and isinstance(self.codecs[0], ShardingCodec):
                sharding_codec = self.codecs[0]
                assert isinstance(sharding_codec, ShardingCodec)  # for mypy
                return sharding_codec.chunk_shape
            else:
                return self.chunk_grid.chunk_shape

        msg = (
            f"The `chunks` attribute is only defined for arrays using `RegularChunkGrid`."
            f"This array has a {self.chunk_grid} instead."
        )
        raise NotImplementedError(msg)

    @property
    def shards(self) -> tuple[int, ...] | None:
        if isinstance(self.chunk_grid, RegularChunkGrid):
            from zarr.codecs.sharding import ShardingCodec

            if len(self.codecs) == 1 and isinstance(self.codecs[0], ShardingCodec):
                return self.chunk_grid.chunk_shape
            else:
                return None

        msg = (
            f"The `shards` attribute is only defined for arrays using `RegularChunkGrid`."
            f"This array has a {self.chunk_grid} instead."
        )
        raise NotImplementedError(msg)

    @property
    def inner_codecs(self) -> tuple[Codec, ...]:
        if isinstance(self.chunk_grid, RegularChunkGrid):
            from zarr.codecs.sharding import ShardingCodec

            if len(self.codecs) == 1 and isinstance(self.codecs[0], ShardingCodec):
                return self.codecs[0].codecs
        return self.codecs

    def get_chunk_spec(
        self, _chunk_coords: tuple[int, ...], array_config: ArrayConfig, prototype: BufferPrototype
    ) -> ArraySpec:
        assert isinstance(self.chunk_grid, RegularChunkGrid), (
            "Currently, only regular chunk grid is supported"
        )
        return ArraySpec(
            shape=self.chunk_grid.chunk_shape,
            dtype=self.dtype,
            fill_value=self.fill_value,
            config=array_config,
            prototype=prototype,
        )

    def encode_chunk_key(self, chunk_coords: tuple[int, ...]) -> str:
        return self.chunk_key_encoding.encode_chunk_key(chunk_coords)

    def to_buffer_dict(self, prototype: BufferPrototype) -> dict[str, Buffer]:
        json_indent = config.get("json_indent")
        d = self.to_dict()
        return {
            ZARR_JSON: prototype.buffer.from_bytes(
                json.dumps(d, allow_nan=True, indent=json_indent).encode()
            )
        }

    @classmethod
    def from_dict(cls, data: dict[str, JSON]) -> Self:
        # make a copy because we are modifying the dict
        _data = data.copy()

        # check that the zarr_format attribute is correct
        _ = parse_zarr_format(_data.pop("zarr_format"))
        # check that the node_type attribute is correct
        _ = parse_node_type_array(_data.pop("node_type"))

        data_type_json = _data.pop("data_type")
        if not check_dtype_spec_v3(data_type_json):
            raise ValueError(f"Invalid data_type: {data_type_json!r}")
        data_type = get_data_type_from_json(data_type_json, zarr_format=3)

        # check that the fill value is consistent with the data type
        try:
            fill = _data.pop("fill_value")
            fill_value_parsed = data_type.from_json_scalar(fill, zarr_format=3)
        except ValueError as e:
            raise TypeError(f"Invalid fill_value: {fill!r}") from e

        # check if there are extra keys
        extra_keys = set(_data.keys()) - ARRAY_METADATA_KEYS
        allowed_extra_fields: dict[str, AllowedExtraField] = {}
        invalid_extra_fields = {}
        for key in extra_keys:
            val = _data[key]
            if check_allowed_extra_field(val):
                allowed_extra_fields[key] = val
            else:
                invalid_extra_fields[key] = val
        if len(invalid_extra_fields) > 0:
            msg = (
                "Got a Zarr V3 metadata document with the following disallowed extra fields:"
                f"{sorted(invalid_extra_fields.keys())}."
                'Extra fields are not allowed unless they are a dict with a "must_understand" key'
                "which is assigned the value `False`."
            )
            raise MetadataValidationError(msg)
        # TODO: replace this with a real type check!
        _data_typed = cast(ArrayMetadataJSON_V3, _data)

        return cls(
            shape=_data_typed["shape"],
            chunk_grid=_data_typed["chunk_grid"],
            chunk_key_encoding=_data_typed["chunk_key_encoding"],
            codecs=_data_typed["codecs"],
            attributes=_data_typed.get("attributes", {}),  # type: ignore[arg-type]
            dimension_names=_data_typed.get("dimension_names", None),
            fill_value=fill_value_parsed,
            data_type=data_type,
            extra_fields=allowed_extra_fields,
            storage_transformers=_data_typed.get("storage_transformers", ()),  # type: ignore[arg-type]
        )

    def to_dict(self) -> dict[str, JSON]:
        out_dict = super().to_dict()
        extra_fields = out_dict.pop("extra_fields")
        out_dict = out_dict | extra_fields  # type: ignore[operator]

        out_dict["fill_value"] = self.data_type.to_json_scalar(
            self.fill_value, zarr_format=self.zarr_format
        )
        if not isinstance(out_dict, dict):
            raise TypeError(f"Expected dict. Got {type(out_dict)}.")

        # if `dimension_names` is `None`, we do not include it in
        # the metadata document
        if out_dict["dimension_names"] is None:
            out_dict.pop("dimension_names")

        # TODO: replace the `to_dict` / `from_dict` on the `Metadata`` class with
        # to_json, from_json, and have ZDType inherit from `Metadata`
        # until then, we have this hack here, which relies on the fact that to_dict will pass through
        # any non-`Metadata` fields as-is.
        dtype_meta = out_dict["data_type"]
        if isinstance(dtype_meta, ZDType):
            out_dict["data_type"] = dtype_meta.to_json(zarr_format=3)  # type: ignore[unreachable]
        return out_dict

    def update_shape(self, shape: tuple[int, ...]) -> Self:
        return replace(self, shape=shape)

    def update_attributes(self, attributes: dict[str, JSON]) -> Self:
        return replace(self, attributes=attributes)
