from __future__ import annotations

from typing import Any, Literal, TypedDict, TypeVar, Union

from typing_extensions import NotRequired

T = TypeVar('T')

DataType = Literal[
    # scalar types
    'Decimal',
    'UUID',
    'Enum',
    # string
    'str',
    # number
    'int',
    'float',
    # bytes
    'bytes',
    # temporal types
    'date',
    'datetime',
    'time',
    'timedelta',
    # ipaddress types
    'IPv4Address',
    'IPv4Interface',
    'IPv4Network',
    'IPv6Address',
    'IPv6Interface',
    'IPv6Network',
    'PosixPath',
    'Pattern',
    # iterable types
    'set',
    'frozenset',
    'tuple',
    'deque',
    'generator',
    'Mapping',
    'Sequence',
    'dataclass',
    # exceptions
    'Exception',
    # pydantic types
    'PydanticModel',
    'AnyUrl',
    'Url',
    'NameEmail',
    'SecretBytes',
    'SecretStr',
    # pandas types
    'DataFrame',
    # numpy types
    'ndarray',
    # any other type
    'attrs',
    'sqlalchemy',
    'unknown',
]


DateFormat = Literal['date', 'date-time', 'time', 'timedelta']
IPFormat = Literal['ipv4', 'ipv4interface', 'ipv4network', 'ipv6', 'ipv6interface', 'ipv6network']
Format = Union[Literal['decimal', 'path', 'regex', 'uuid'], DateFormat, IPFormat]


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


EnumSchema = Union[_EnumString, _EnumInt, _EnumFloat, _EnumBool, _EnumAny]


class _Items(TypedDict):
    items: JSONSchema


class _PrefixItems(TypedDict):
    prefixItems: list[JSONSchema]


_ArrayBase = TypedDict(
    '_ArrayBase',
    {
        'type': Literal['array'],
        'x-python-datatype': Literal['tuple', 'deque', 'set', 'frozenset', 'ndarray'],
        'x-columns': NotRequired[list[str]],
        'x-indices': NotRequired[list[Any]],
        'x-shape': NotRequired[list[int]],
        'x-dtype': NotRequired[str],
    },
)


class _ArrayItems(_ArrayBase, _Items):
    pass


class _ArrayPrefixItems(_ArrayBase, _PrefixItems):
    pass


ArraySchema = Union[_ArrayItems, _ArrayPrefixItems, _ArrayBase]

_PropertyDataType = TypedDict('_PropertyDataType', {'x-python-datatype': DataType}, total=False)


Type = Literal['array', 'boolean', 'integer', 'number', 'object', 'string']


class _Property(_PropertyDataType, total=False):
    type: Type
    title: str
    format: Format
    properties: dict[str, JSONSchema]


JSONSchema = Union[EnumSchema, ArraySchema, _Property]
