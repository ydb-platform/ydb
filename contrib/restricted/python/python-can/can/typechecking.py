"""Types for mypy type-checking"""

import io
import sys
from collections.abc import Iterable, Sequence
from typing import IO, TYPE_CHECKING, Any, NewType, Union

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

if sys.version_info >= (3, 12):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict


if TYPE_CHECKING:
    import os
    import struct


class _CanFilterBase(TypedDict):
    can_id: int
    can_mask: int


class CanFilter(_CanFilterBase, total=False):
    extended: bool


CanFilters = Sequence[CanFilter]

# TODO: Once buffer protocol support lands in typing, we should switch to that,
# since can.message.Message attempts to call bytearray() on the given data, so
# this should have the same typing info.
#
# See: https://github.com/python/typing/issues/593
CanData = Union[bytes, bytearray, int, Iterable[int]]

# Used for the Abstract Base Class
ChannelStr = str
ChannelInt = int
Channel = Union[ChannelInt, ChannelStr, Sequence[ChannelInt]]

# Used by the IO module
FileLike = Union[IO[Any], io.TextIOWrapper, io.BufferedIOBase]
StringPathLike = Union[str, "os.PathLike[str]"]

BusConfig = NewType("BusConfig", dict[str, Any])

# Used by CLI scripts
TAdditionalCliArgs: TypeAlias = dict[str, Union[str, int, float, bool]]
TDataStructs: TypeAlias = dict[
    Union[int, tuple[int, ...]],
    "Union[struct.Struct, tuple[struct.Struct, *tuple[float, ...]]]",
]


class AutoDetectedConfig(TypedDict):
    interface: str
    channel: Channel


ReadableBytesLike = Union[bytes, bytearray, memoryview]


class BitTimingDict(TypedDict):
    f_clock: int
    brp: int
    tseg1: int
    tseg2: int
    sjw: int
    nof_samples: int


class BitTimingFdDict(TypedDict):
    f_clock: int
    nom_brp: int
    nom_tseg1: int
    nom_tseg2: int
    nom_sjw: int
    data_brp: int
    data_tseg1: int
    data_tseg2: int
    data_sjw: int
