from __future__ import annotations

import enum
from collections.abc import (
    Mapping,
    MutableMapping,
    MutableSequence,
    Sequence,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Protocol,
    TypeAlias,
    TypedDict,
    TypeVar,
    Union,
)


if TYPE_CHECKING:
    from .containers import DockerContainer


_T_co = TypeVar("_T_co", covariant=True)


class SupportsRead(Protocol[_T_co]):
    def read(self, length: int = ..., /) -> _T_co: ...


# NOTE: Currently these types are used to annotate arguments only.
# When returning values, we need extra type-narrowing for individual fields,
# so it is better to define per-API typed DTOs.
JSONValue: TypeAlias = Union[
    str,
    int,
    float,
    bool,
    None,
    Mapping[str, "JSONValue"],
    Sequence["JSONValue"],
]
JSONObject: TypeAlias = Mapping[str, JSONValue]
JSONList: TypeAlias = Sequence[JSONValue]


MutableJSONValue: TypeAlias = Union[
    str,
    int,
    float,
    bool,
    None,
    MutableMapping[str, "JSONValue"],
    MutableSequence["JSONValue"],
]
MutableJSONObject: TypeAlias = MutableMapping[str, MutableJSONValue]
MutableJSONList: TypeAlias = MutableSequence[MutableJSONValue]


class AsyncContainerFactory(Protocol):
    async def __call__(self, config: dict[str, Any], name: str) -> DockerContainer: ...


class PortInfo(TypedDict):
    HostIp: str
    HostPort: str


class Sentinel(enum.Enum):
    """
    A special single-value enum constant to represent "unspecified" value in the contexts
    where ``None`` has another meaning.

    For example, ``None`` in timeouts means infinity.
    If you want to represent an unspecified/default value for timeouts, use the ``SENTINEL`` or ``Sentinel.TOKEN``.
    """

    TOKEN = enum.auto()


SENTINEL = Sentinel.TOKEN
