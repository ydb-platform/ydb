from __future__ import annotations

import asyncio
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING

import numcodecs
from numcodecs.zstd import Zstd
from packaging.version import Version

from zarr.abc.codec import BytesBytesCodec
from zarr.core.buffer.cpu import as_numpy_array_wrapper
from zarr.core.common import JSON, parse_named_configuration

if TYPE_CHECKING:
    from typing import Self

    from zarr.core.array_spec import ArraySpec
    from zarr.core.buffer import Buffer


def parse_zstd_level(data: JSON) -> int:
    if isinstance(data, int):
        if data >= 23:
            raise ValueError(f"Value must be less than or equal to 22. Got {data} instead.")
        return data
    raise TypeError(f"Got value with type {type(data)}, but expected an int.")


def parse_checksum(data: JSON) -> bool:
    if isinstance(data, bool):
        return data
    raise TypeError(f"Expected bool. Got {type(data)}.")


@dataclass(frozen=True)
class ZstdCodec(BytesBytesCodec):
    """zstd codec"""

    is_fixed_size = True

    level: int = 0
    checksum: bool = False

    def __init__(self, *, level: int = 0, checksum: bool = False) -> None:
        # numcodecs 0.13.0 introduces the checksum attribute for the zstd codec
        _numcodecs_version = Version(numcodecs.__version__)
        if _numcodecs_version < Version("0.13.0"):
            raise RuntimeError(
                "numcodecs version >= 0.13.0 is required to use the zstd codec. "
                f"Version {_numcodecs_version} is currently installed."
            )

        level_parsed = parse_zstd_level(level)
        checksum_parsed = parse_checksum(checksum)

        object.__setattr__(self, "level", level_parsed)
        object.__setattr__(self, "checksum", checksum_parsed)

    @classmethod
    def from_dict(cls, data: dict[str, JSON]) -> Self:
        _, configuration_parsed = parse_named_configuration(data, "zstd")
        return cls(**configuration_parsed)  # type: ignore[arg-type]

    def to_dict(self) -> dict[str, JSON]:
        return {"name": "zstd", "configuration": {"level": self.level, "checksum": self.checksum}}

    @cached_property
    def _zstd_codec(self) -> Zstd:
        config_dict = {"level": self.level, "checksum": self.checksum}
        return Zstd.from_config(config_dict)

    async def _decode_single(
        self,
        chunk_bytes: Buffer,
        chunk_spec: ArraySpec,
    ) -> Buffer:
        return await asyncio.to_thread(
            as_numpy_array_wrapper, self._zstd_codec.decode, chunk_bytes, chunk_spec.prototype
        )

    async def _encode_single(
        self,
        chunk_bytes: Buffer,
        chunk_spec: ArraySpec,
    ) -> Buffer | None:
        return await asyncio.to_thread(
            as_numpy_array_wrapper, self._zstd_codec.encode, chunk_bytes, chunk_spec.prototype
        )

    def compute_encoded_size(self, _input_byte_length: int, _chunk_spec: ArraySpec) -> int:
        raise NotImplementedError
