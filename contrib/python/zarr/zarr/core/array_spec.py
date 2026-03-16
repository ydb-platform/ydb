from __future__ import annotations

from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, Any, Literal, Self, TypedDict, cast

from zarr.core.common import (
    MemoryOrder,
    parse_bool,
    parse_fill_value,
    parse_order,
    parse_shapelike,
)
from zarr.core.config import config as zarr_config

if TYPE_CHECKING:
    from typing import NotRequired

    from zarr.core.buffer import BufferPrototype
    from zarr.core.dtype.wrapper import TBaseDType, TBaseScalar, ZDType


class ArrayConfigParams(TypedDict):
    """
    A TypedDict model of the attributes of an ArrayConfig class, but with no required fields.
    This allows for partial construction of an ArrayConfig, with the assumption that the unset
    keys will be taken from a global configuration.
    """

    order: NotRequired[MemoryOrder]
    write_empty_chunks: NotRequired[bool]


@dataclass(frozen=True)
class ArrayConfig:
    """
    A model of the runtime configuration of an array.

    Parameters
    ----------
    order : MemoryOrder
        The memory layout of the arrays returned when reading data from the store.
    write_empty_chunks : bool
        If True, empty chunks will be written to the store.
    """

    order: MemoryOrder
    write_empty_chunks: bool

    def __init__(self, order: MemoryOrder, write_empty_chunks: bool) -> None:
        order_parsed = parse_order(order)
        write_empty_chunks_parsed = parse_bool(write_empty_chunks)

        object.__setattr__(self, "order", order_parsed)
        object.__setattr__(self, "write_empty_chunks", write_empty_chunks_parsed)

    @classmethod
    def from_dict(cls, data: ArrayConfigParams) -> Self:
        """
        Create an ArrayConfig from a dict. The keys of that dict are a subset of the
        attributes of the ArrayConfig class. Any keys missing from that dict will be set to the
        the values in the ``array`` namespace of ``zarr.config``.
        """
        kwargs_out: ArrayConfigParams = {}
        for f in fields(ArrayConfig):
            field_name = cast("Literal['order', 'write_empty_chunks']", f.name)
            if field_name not in data:
                kwargs_out[field_name] = zarr_config.get(f"array.{field_name}")
            else:
                kwargs_out[field_name] = data[field_name]
        return cls(**kwargs_out)


ArrayConfigLike = ArrayConfig | ArrayConfigParams


def parse_array_config(data: ArrayConfigLike | None) -> ArrayConfig:
    """
    Convert various types of data to an ArrayConfig.
    """
    if data is None:
        return ArrayConfig.from_dict({})
    elif isinstance(data, ArrayConfig):
        return data
    else:
        return ArrayConfig.from_dict(data)


@dataclass(frozen=True)
class ArraySpec:
    shape: tuple[int, ...]
    dtype: ZDType[TBaseDType, TBaseScalar]
    fill_value: Any
    config: ArrayConfig
    prototype: BufferPrototype

    def __init__(
        self,
        shape: tuple[int, ...],
        dtype: ZDType[TBaseDType, TBaseScalar],
        fill_value: Any,
        config: ArrayConfig,
        prototype: BufferPrototype,
    ) -> None:
        shape_parsed = parse_shapelike(shape)
        fill_value_parsed = parse_fill_value(fill_value)

        object.__setattr__(self, "shape", shape_parsed)
        object.__setattr__(self, "dtype", dtype)
        object.__setattr__(self, "fill_value", fill_value_parsed)
        object.__setattr__(self, "config", config)
        object.__setattr__(self, "prototype", prototype)

    @property
    def ndim(self) -> int:
        return len(self.shape)

    @property
    def order(self) -> MemoryOrder:
        return self.config.order
