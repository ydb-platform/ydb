from __future__ import annotations

import warnings
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import (
    ClassVar,
    Final,
    Generic,
    Literal,
    TypedDict,
    TypeGuard,
    TypeVar,
)

from typing_extensions import ReadOnly

from zarr.core.common import NamedConfig
from zarr.errors import UnstableSpecificationWarning

EndiannessStr = Literal["little", "big"]
ENDIANNESS_STR: Final = "little", "big"

SpecialFloatStrings = Literal["NaN", "Infinity", "-Infinity"]
SPECIAL_FLOAT_STRINGS: Final = ("NaN", "Infinity", "-Infinity")

JSONFloatV2 = float | SpecialFloatStrings
JSONFloatV3 = float | SpecialFloatStrings | str

ObjectCodecID = Literal["vlen-utf8", "vlen-bytes", "vlen-array", "pickle", "json2", "msgpack2"]
# These are the ids of the known object codecs for zarr v2.
OBJECT_CODEC_IDS: Final = ("vlen-utf8", "vlen-bytes", "vlen-array", "pickle", "json2", "msgpack2")

# This is a wider type than our standard JSON type because we need
# to work with typeddict objects which are assignable to Mapping[str, object]
DTypeJSON = str | int | float | Sequence["DTypeJSON"] | None | Mapping[str, object]

# The DTypeJSON_V2 type exists because ZDType.from_json takes a single argument, which must contain
# all the information necessary to decode the data type. Zarr v2 supports multiple distinct
# data types that all used the "|O" data type identifier. These data types can only be
# discriminated on the basis of their "object codec", i.e. a special data type specific
# compressor or filter. So to figure out what data type a zarr v2 array has, we need the
# data type identifier from metadata, as well as an object codec id if the data type identifier
# is "|O".
# So we will pack the name of the dtype alongside the name of the object codec id, if applicable,
# in a single dict, and pass that to the data type inference logic.
# These type variables have a very wide bound because the individual zdtype
# classes can perform a very specific type check.

# This is the JSON representation of a structured dtype in zarr v2
StructuredName_V2 = Sequence["str | StructuredName_V2"]

# This models the type of the name a dtype might have in zarr v2 array metadata
DTypeName_V2 = StructuredName_V2 | str

TDTypeNameV2_co = TypeVar("TDTypeNameV2_co", bound=DTypeName_V2, covariant=True)
TObjectCodecID_co = TypeVar("TObjectCodecID_co", bound=None | str, covariant=True)


class DTypeConfig_V2(TypedDict, Generic[TDTypeNameV2_co, TObjectCodecID_co]):
    name: ReadOnly[TDTypeNameV2_co]
    object_codec_id: ReadOnly[TObjectCodecID_co]


DTypeSpec_V2 = DTypeConfig_V2[DTypeName_V2, None | str]


def check_structured_dtype_v2_inner(data: object) -> TypeGuard[StructuredName_V2]:
    """
    A type guard for the inner elements of a structured dtype. This is a recursive check because
    the type is itself recursive.

    This check ensures that all the elements are 2-element sequences beginning with a string
    and ending with either another string or another 2-element sequence beginning with a string and
    ending with another instance of that type.
    """
    if isinstance(data, (str, Mapping)):
        return False
    if not isinstance(data, Sequence):
        return False
    if len(data) != 2:
        return False
    if not (isinstance(data[0], str)):
        return False
    if isinstance(data[-1], str):
        return True
    elif isinstance(data[-1], Sequence):
        return check_structured_dtype_v2_inner(data[-1])
    return False


def check_structured_dtype_name_v2(data: Sequence[object]) -> TypeGuard[StructuredName_V2]:
    """
    Check that all the elements of a sequence are valid zarr v2 structured dtype identifiers
    """
    return all(check_structured_dtype_v2_inner(d) for d in data)


def check_dtype_name_v2(data: object) -> TypeGuard[DTypeName_V2]:
    """
    Type guard for narrowing the type of a python object to a valid zarr v2 dtype name.
    """
    if isinstance(data, str):
        return True
    elif isinstance(data, Sequence):
        return check_structured_dtype_name_v2(data)
    return False


def check_dtype_spec_v2(data: object) -> TypeGuard[DTypeSpec_V2]:
    """
    Type guard for narrowing a python object to an instance of DTypeSpec_V2
    """
    if not isinstance(data, Mapping):
        return False
    if set(data.keys()) != {"name", "object_codec_id"}:
        return False
    if not check_dtype_name_v2(data["name"]):
        return False
    return isinstance(data["object_codec_id"], str | None)


# By comparison, The JSON representation of a dtype in zarr v3 is much simpler.
# It's either a string, or a structured dict
DTypeSpec_V3 = str | NamedConfig[str, Mapping[str, object]]


def check_dtype_spec_v3(data: object) -> TypeGuard[DTypeSpec_V3]:
    """
    Type guard for narrowing the type of a python object to an instance of
    DTypeSpec_V3, i.e either a string or a dict with a "name" field that's a string and a
    "configuration" field that's a mapping with string keys.
    """
    if isinstance(data, str) or (  # noqa: SIM103
        isinstance(data, Mapping)
        and set(data.keys()) == {"name", "configuration"}
        and isinstance(data["configuration"], Mapping)
        and all(isinstance(k, str) for k in data["configuration"])
    ):
        return True
    return False


def unpack_dtype_json(data: DTypeSpec_V2 | DTypeSpec_V3) -> DTypeJSON:
    """
    Return the array metadata form of the dtype JSON representation. For the Zarr V3 form of dtype
    metadata, this is a no-op. For the Zarr V2 form of dtype metadata, this unpacks the dtype name.
    """
    if isinstance(data, Mapping) and set(data.keys()) == {"name", "object_codec_id"}:
        return data["name"]
    return data


class DataTypeValidationError(ValueError): ...


class ScalarTypeValidationError(ValueError): ...


@dataclass(frozen=True, kw_only=True)
class HasLength:
    """
    A mix-in class for data types with a length attribute, such as fixed-size collections
    of unicode strings, or bytes.

    Attributes
    ----------
    length : int
        The length of the scalars belonging to this data type. Note that this class does not assign
        a unit to the length. Child classes may assign units.
    """

    length: int


@dataclass(frozen=True, kw_only=True)
class HasEndianness:
    """
    A mix-in class for data types with an endianness attribute
    """

    endianness: EndiannessStr = "little"


@dataclass(frozen=True, kw_only=True)
class HasItemSize:
    """
    A mix-in class for data types with an item size attribute.
    This mix-in bears a property ``item_size``, which denotes the size of each element of the data
    type, in bytes.
    """

    @property
    def item_size(self) -> int:
        raise NotImplementedError


@dataclass(frozen=True, kw_only=True)
class HasObjectCodec:
    """
    A mix-in class for data types that require an object codec id.
    This class bears the property ``object_codec_id``, which is the string name of an object
    codec that is required to encode and decode the data type.

    In zarr-python 2.x certain data types like variable-length strings or variable-length arrays
    used the catch-all numpy "object" data type for their in-memory representation. But these data
    types cannot be stored as numpy object data types, because the object data type does not define
    a fixed memory layout. So these data types required a special codec, called an "object codec",
    that effectively defined a compact representation for the data type, which was used to encode
    and decode the data type.

    Zarr-python 2.x would not allow the creation of arrays with the "object" data type if an object
    codec was not specified, and thus the name of the object codec is effectively part of the data
    type model.
    """

    object_codec_id: ClassVar[str]


def v3_unstable_dtype_warning(dtype: object) -> None:
    """
    Emit this warning when a data type does not have a stable zarr v3 spec
    """
    msg = (
        f"The data type ({dtype}) does not have a Zarr V3 specification. "
        "That means that the representation of arrays saved with this data type may change without "
        "warning in a future version of Zarr Python. "
        "Arrays stored with this data type may be unreadable by other Zarr libraries. "
        "Use this data type at your own risk! "
        "Check https://github.com/zarr-developers/zarr-extensions/tree/main/data-types for the "
        "status of data type specifications for Zarr V3."
    )
    warnings.warn(msg, category=UnstableSpecificationWarning, stacklevel=2)
