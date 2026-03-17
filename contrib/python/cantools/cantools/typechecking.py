import os
from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    NamedTuple,
    TypeAlias,
    TypedDict,
    Union,
)

from bitstruct import CompiledFormatDict  # type: ignore

if TYPE_CHECKING:
    from .database import Message, Signal
    from .database.namedsignalvalue import NamedSignalValue


class Formats(NamedTuple):
    big_endian: CompiledFormatDict
    little_endian: CompiledFormatDict
    padding_mask: int


StringPathLike = str | os.PathLike[str]
Comments = dict[str | None, str]
class Codec(TypedDict):
    signals: list["Signal"]
    formats: Formats
    multiplexers: Mapping[str, Mapping[int, Any]]

ByteOrder = Literal["little_endian", "big_endian"]
Choices = OrderedDict[int, Union[str, "NamedSignalValue"]]

# Type aliases. Introduced to reduce type annotation complexity while
# allowing for more complex encode/decode schemes like the one used
# for AUTOSAR container messages.
SignalValueType = Union[int, float, str, "NamedSignalValue"]
SignalDictType = dict[str, SignalValueType]
SignalMappingType = Mapping[str, SignalValueType]
ContainerHeaderSpecType = Union["Message", str, int]
ContainerUnpackResultType = Sequence[tuple["Message", bytes] | tuple[int, bytes]]
ContainerUnpackListType = list[tuple["Message", bytes] | tuple[int, bytes]]
ContainerDecodeResultType = Sequence[
    tuple["Message", SignalMappingType] | tuple["Message", bytes] | tuple[int, bytes]
]
ContainerDecodeResultListType = list[
    tuple["Message", SignalDictType] | tuple["Message", bytes] | tuple[int, bytes]
]
ContainerEncodeInputType = Sequence[
    tuple[ContainerHeaderSpecType, bytes | SignalMappingType]
]
DecodeResultType = SignalDictType | ContainerDecodeResultType
EncodeInputType = SignalMappingType | ContainerEncodeInputType

SecOCAuthenticatorFn = Callable[["Message", bytes, int], bytes]

TAdditionalCliArgs: TypeAlias = dict[str, str | int | float | bool]
