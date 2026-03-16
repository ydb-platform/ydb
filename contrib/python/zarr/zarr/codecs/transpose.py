from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, cast

import numpy as np

from zarr.abc.codec import ArrayArrayCodec
from zarr.core.array_spec import ArraySpec
from zarr.core.common import JSON, parse_named_configuration

if TYPE_CHECKING:
    from typing import Self

    from zarr.core.buffer import NDBuffer
    from zarr.core.chunk_grids import ChunkGrid
    from zarr.core.dtype.wrapper import TBaseDType, TBaseScalar, ZDType


def parse_transpose_order(data: JSON | Iterable[int]) -> tuple[int, ...]:
    if not isinstance(data, Iterable):
        raise TypeError(f"Expected an iterable. Got {data} instead.")
    if not all(isinstance(a, int) for a in data):
        raise TypeError(f"Expected an iterable of integers. Got {data} instead.")
    return tuple(cast("Iterable[int]", data))


@dataclass(frozen=True)
class TransposeCodec(ArrayArrayCodec):
    """Transpose codec"""

    is_fixed_size = True

    order: tuple[int, ...]

    def __init__(self, *, order: Iterable[int]) -> None:
        order_parsed = parse_transpose_order(order)

        object.__setattr__(self, "order", order_parsed)

    @classmethod
    def from_dict(cls, data: dict[str, JSON]) -> Self:
        _, configuration_parsed = parse_named_configuration(data, "transpose")
        return cls(**configuration_parsed)  # type: ignore[arg-type]

    def to_dict(self) -> dict[str, JSON]:
        return {"name": "transpose", "configuration": {"order": tuple(self.order)}}

    def validate(
        self,
        shape: tuple[int, ...],
        dtype: ZDType[TBaseDType, TBaseScalar],
        chunk_grid: ChunkGrid,
    ) -> None:
        if len(self.order) != len(shape):
            raise ValueError(
                f"The `order` tuple must have as many entries as there are dimensions in the array. Got {self.order}."
            )
        if len(self.order) != len(set(self.order)):
            raise ValueError(
                f"There must not be duplicates in the `order` tuple. Got {self.order}."
            )
        if not all(0 <= x < len(shape) for x in self.order):
            raise ValueError(
                f"All entries in the `order` tuple must be between 0 and the number of dimensions in the array. Got {self.order}."
            )

    def evolve_from_array_spec(self, array_spec: ArraySpec) -> Self:
        ndim = array_spec.ndim
        if len(self.order) != ndim:
            raise ValueError(
                f"The `order` tuple must have as many entries as there are dimensions in the array. Got {self.order}."
            )
        if len(self.order) != len(set(self.order)):
            raise ValueError(
                f"There must not be duplicates in the `order` tuple. Got {self.order}."
            )
        if not all(0 <= x < ndim for x in self.order):
            raise ValueError(
                f"All entries in the `order` tuple must be between 0 and the number of dimensions in the array. Got {self.order}."
            )
        order = tuple(self.order)

        if order != self.order:
            return replace(self, order=order)
        return self

    def resolve_metadata(self, chunk_spec: ArraySpec) -> ArraySpec:
        return ArraySpec(
            shape=tuple(chunk_spec.shape[self.order[i]] for i in range(chunk_spec.ndim)),
            dtype=chunk_spec.dtype,
            fill_value=chunk_spec.fill_value,
            config=chunk_spec.config,
            prototype=chunk_spec.prototype,
        )

    async def _decode_single(
        self,
        chunk_array: NDBuffer,
        chunk_spec: ArraySpec,
    ) -> NDBuffer:
        inverse_order = np.argsort(self.order)
        return chunk_array.transpose(inverse_order)

    async def _encode_single(
        self,
        chunk_array: NDBuffer,
        _chunk_spec: ArraySpec,
    ) -> NDBuffer | None:
        return chunk_array.transpose(self.order)

    def compute_encoded_size(self, input_byte_length: int, _chunk_spec: ArraySpec) -> int:
        return input_byte_length
