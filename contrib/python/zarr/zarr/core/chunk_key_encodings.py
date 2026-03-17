from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypeAlias, TypedDict, cast

if TYPE_CHECKING:
    from typing import NotRequired, Self

from zarr.abc.metadata import Metadata
from zarr.core.common import (
    JSON,
    NamedConfig,
    parse_named_configuration,
)
from zarr.registry import get_chunk_key_encoding_class, register_chunk_key_encoding

SeparatorLiteral = Literal[".", "/"]


def parse_separator(data: JSON) -> SeparatorLiteral:
    if data not in (".", "/"):
        raise ValueError(f"Expected an '.' or '/' separator. Got {data} instead.")
    return cast("SeparatorLiteral", data)


class ChunkKeyEncodingParams(TypedDict):
    name: Literal["v2", "default"]
    separator: NotRequired[SeparatorLiteral]


@dataclass(frozen=True)
class ChunkKeyEncoding(ABC, Metadata):
    """
    Defines how chunk coordinates are mapped to store keys.

    Subclasses must define a class variable `name` and implement `encode_chunk_key`.
    """

    name: ClassVar[str]

    @classmethod
    def from_dict(cls, data: dict[str, JSON]) -> Self:
        _, config_parsed = parse_named_configuration(data, require_configuration=False)
        return cls(**config_parsed if config_parsed else {})

    def to_dict(self) -> dict[str, JSON]:
        return {"name": self.name, "configuration": super().to_dict()}

    def decode_chunk_key(self, chunk_key: str) -> tuple[int, ...]:
        """
        Optional: decode a chunk key string into chunk coordinates.
        Not required for normal operation; override if needed for testing or debugging.
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not implement decode_chunk_key.")

    @abstractmethod
    def encode_chunk_key(self, chunk_coords: tuple[int, ...]) -> str:
        """
        Encode chunk coordinates into a chunk key string.
        Must be implemented by subclasses.
        """


ChunkKeyEncodingLike: TypeAlias = (
    dict[str, JSON] | ChunkKeyEncodingParams | ChunkKeyEncoding | NamedConfig[str, Any]
)


@dataclass(frozen=True)
class DefaultChunkKeyEncoding(ChunkKeyEncoding):
    name: ClassVar[Literal["default"]] = "default"
    separator: SeparatorLiteral = "/"

    def __post_init__(self) -> None:
        separator_parsed = parse_separator(self.separator)
        object.__setattr__(self, "separator", separator_parsed)

    def decode_chunk_key(self, chunk_key: str) -> tuple[int, ...]:
        if chunk_key == "c":
            return ()
        return tuple(map(int, chunk_key[1:].split(self.separator)))

    def encode_chunk_key(self, chunk_coords: tuple[int, ...]) -> str:
        return self.separator.join(map(str, ("c",) + chunk_coords))


@dataclass(frozen=True)
class V2ChunkKeyEncoding(ChunkKeyEncoding):
    name: ClassVar[Literal["v2"]] = "v2"
    separator: SeparatorLiteral = "."

    def __post_init__(self) -> None:
        separator_parsed = parse_separator(self.separator)
        object.__setattr__(self, "separator", separator_parsed)

    def decode_chunk_key(self, chunk_key: str) -> tuple[int, ...]:
        return tuple(map(int, chunk_key.split(self.separator)))

    def encode_chunk_key(self, chunk_coords: tuple[int, ...]) -> str:
        chunk_identifier = self.separator.join(map(str, chunk_coords))
        return "0" if chunk_identifier == "" else chunk_identifier


def parse_chunk_key_encoding(data: ChunkKeyEncodingLike) -> ChunkKeyEncoding:
    """
    Take an implicit specification of a chunk key encoding and parse it into a ChunkKeyEncoding object.
    """
    if isinstance(data, ChunkKeyEncoding):
        return data

    # handle ChunkKeyEncodingParams
    if "name" in data and "separator" in data:
        data = {"name": data["name"], "configuration": {"separator": data["separator"]}}  # type: ignore[typeddict-item]

    # Now must be a named config
    data = cast("dict[str, JSON]", data)

    name_parsed, _ = parse_named_configuration(data, require_configuration=False)
    try:
        chunk_key_encoding = get_chunk_key_encoding_class(name_parsed).from_dict(data)
    except KeyError as e:
        raise ValueError(f"Unknown chunk key encoding: {e.args[0]!r}") from e

    return chunk_key_encoding


register_chunk_key_encoding("default", DefaultChunkKeyEncoding)
register_chunk_key_encoding("v2", V2ChunkKeyEncoding)
