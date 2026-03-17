from _typeshed import Incomplete
from typing import Any, Literal, TypeVar, TypedDict
from typing_extensions import NotRequired

T = TypeVar('T')
DataType: Incomplete
DateFormat: Incomplete
IPFormat: Incomplete
Format: Incomplete
_EnumBase = TypedDict('_EnumBase', {'x-python-datatype': Literal['Enum']})

class _EnumAny(_EnumBase):
    type: Literal['object']
    enum: list[Any]

class _EnumString(_EnumBase):
    type: Literal['string']
    enum: list[str]

class _EnumInt(_EnumBase):
    type: Literal['integer']
    enum: list[int]

class _EnumFloat(_EnumBase):
    type: Literal['number']
    enum: list[float]

class _EnumBool(_EnumBase):
    type: Literal['boolean']
    enum: list[bool]

EnumSchema: Incomplete

class _Items(TypedDict):
    items: JSONSchema

class _PrefixItems(TypedDict):
    prefixItems: list[JSONSchema]

_ArrayBase = TypedDict('_ArrayBase', {'type': Literal['array'], 'x-python-datatype': Literal['tuple', 'deque', 'set', 'frozenset', 'ndarray'], 'x-columns': NotRequired[list[str]], 'x-indices': NotRequired[list[Any]], 'x-shape': NotRequired[list[int]], 'x-dtype': NotRequired[str]})

class _ArrayItems(_ArrayBase, _Items): ...
class _ArrayPrefixItems(_ArrayBase, _PrefixItems): ...

ArraySchema: Incomplete
_PropertyDataType = TypedDict('_PropertyDataType', {'x-python-datatype': DataType}, total=False)
Type: Incomplete

class _Property(_PropertyDataType, total=False):
    type: Type
    title: str
    format: Format
    properties: dict[str, JSONSchema]

JSONSchema: Incomplete
