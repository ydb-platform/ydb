import datetime
import decimal
import enum
import typing
import uuid
from base64 import b64encode
from collections import namedtuple
from dataclasses import dataclass
from typing import (
    Annotated,
    Any,
    Dict,
    FrozenSet,
    Generic,
    List,
    Literal,
    NamedTuple,
    NewType,
    Set,
    Tuple,
    TypedDict,
    TypeVar,
    Union,
)

import pytest

import msgspec
from msgspec import Meta

from .utils import temp_module

T = TypeVar("T")


def test_any():
    assert msgspec.json.schema(Any) == {}


def test_raw():
    assert msgspec.json.schema(msgspec.Raw) == {}


def test_msgpack_ext():
    with pytest.raises(TypeError):
        assert msgspec.json.schema(msgspec.msgpack.Ext)


def test_custom():
    with pytest.raises(TypeError, match="Generating JSON schema for custom types"):
        assert msgspec.json.schema(complex)

    schema = {"type": "string", "format": "complex"}

    assert (
        msgspec.json.schema(Annotated[complex, Meta(extra_json_schema=schema)])
        == schema
    )


def test_custom_schema_hook():
    schema = {"type": "string", "format": "complex"}

    def schema_hook(cls):
        if cls is complex:
            return schema
        raise NotImplementedError

    assert msgspec.json.schema(complex, schema_hook=schema_hook) == schema
    assert msgspec.json.schema(
        Annotated[complex, Meta(extra_json_schema={"title": "A complex field"})],
        schema_hook=schema_hook,
    ) == {**schema, "title": "A complex field"}

    with pytest.raises(TypeError, match="Generating JSON schema for custom types"):
        msgspec.json.schema(slice, schema_hook=schema_hook)


def test_none():
    assert msgspec.json.schema(None) == {"type": "null"}


def test_bool():
    assert msgspec.json.schema(bool) == {"type": "boolean"}


def test_int():
    assert msgspec.json.schema(int) == {"type": "integer"}


def test_float():
    assert msgspec.json.schema(float) == {"type": "number"}


def test_string():
    assert msgspec.json.schema(str) == {"type": "string"}


@pytest.mark.parametrize("typ", [bytes, bytearray, memoryview])
def test_binary(typ):
    assert msgspec.json.schema(typ) == {
        "type": "string",
        "contentEncoding": "base64",
    }


@pytest.mark.parametrize(
    "annot, extra",
    [
        (None, {}),
        (Meta(tz=None), {}),
        (Meta(tz=True), {"format": "date-time"}),
        (Meta(tz=False), {}),
    ],
)
def test_datetime(annot, extra):
    typ = datetime.datetime
    if annot is not None:
        typ = Annotated[typ, annot]
    assert msgspec.json.schema(typ) == {"type": "string", **extra}


@pytest.mark.parametrize(
    "annot, extra",
    [
        (None, {}),
        (Meta(tz=None), {}),
        (Meta(tz=True), {"format": "time"}),
        (Meta(tz=False), {"format": "partial-time"}),
    ],
)
def test_time(annot, extra):
    typ = datetime.time
    if annot is not None:
        typ = Annotated[typ, annot]
    assert msgspec.json.schema(typ) == {"type": "string", **extra}


def test_date():
    assert msgspec.json.schema(datetime.date) == {
        "type": "string",
        "format": "date",
    }


def test_timedelta():
    assert msgspec.json.schema(datetime.timedelta) == {
        "type": "string",
        "format": "duration",
    }


def test_uuid():
    assert msgspec.json.schema(uuid.UUID) == {
        "type": "string",
        "format": "uuid",
    }


def test_decimal():
    assert msgspec.json.schema(decimal.Decimal) == {
        "type": "string",
        "format": "decimal",
    }


def test_newtype():
    UserId = NewType("UserId", str)
    assert msgspec.json.schema(UserId) == {"type": "string"}
    assert msgspec.json.schema(Annotated[UserId, Meta(max_length=10)]) == {
        "type": "string",
        "maxLength": 10,
    }


@pytest.mark.parametrize(
    "typ", [list, tuple, set, frozenset, List, Tuple, Set, FrozenSet]
)
def test_sequence_any(typ):
    assert msgspec.json.schema(typ) == {"type": "array"}


@pytest.mark.parametrize(
    "cls", [list, tuple, set, frozenset, List, Tuple, Set, FrozenSet]
)
def test_sequence_typed(cls):
    args = (int, ...) if cls in (tuple, Tuple) else int
    typ = cls[args]
    assert msgspec.json.schema(typ) == {"type": "array", "items": {"type": "integer"}}


@pytest.mark.parametrize("cls", [tuple, Tuple])
def test_tuple(cls):
    typ = cls[int, float, str]
    assert msgspec.json.schema(typ) == {
        "type": "array",
        "minItems": 3,
        "maxItems": 3,
        "items": False,
        "prefixItems": [
            {"type": "integer"},
            {"type": "number"},
            {"type": "string"},
        ],
    }


@pytest.mark.parametrize("cls", [tuple, Tuple])
def test_empty_tuple(cls):
    typ = cls[()]
    assert msgspec.json.schema(typ) == {
        "type": "array",
        "minItems": 0,
        "maxItems": 0,
    }


@pytest.mark.parametrize("typ", [dict, Dict])
def test_dict_any(typ):
    assert msgspec.json.schema(typ) == {"type": "object"}


@pytest.mark.parametrize("cls", [dict, Dict])
def test_dict_typed(cls):
    typ = cls[str, int]
    assert msgspec.json.schema(typ) == {
        "type": "object",
        "additionalProperties": {"type": "integer"},
    }


def test_abstract_sequence():
    # Only testing one here, the main tests are in `test_inspect`
    typ = typing.Sequence[int]
    assert msgspec.json.schema(typ) == {"type": "array", "items": {"type": "integer"}}


def test_abstract_mapping():
    # Only testing one here, the main tests are in `test_inspect`
    typ = typing.MutableMapping[str, int]
    assert msgspec.json.schema(typ) == {
        "type": "object",
        "additionalProperties": {"type": "integer"},
    }


def test_int_enum():
    class Example(enum.IntEnum):
        C = 1
        B = 3
        A = 2

    assert msgspec.json.schema(Example) == {
        "$ref": "#/$defs/Example",
        "$defs": {"Example": {"title": "Example", "enum": [1, 2, 3]}},
    }


def test_enum():
    class Example(enum.Enum):
        """A docstring"""

        C = "x"
        B = "z"
        A = "y"

    assert msgspec.json.schema(Example) == {
        "$ref": "#/$defs/Example",
        "$defs": {
            "Example": {
                "title": "Example",
                "description": "A docstring",
                "enum": ["x", "y", "z"],
            }
        },
    }


def test_int_literal():
    assert msgspec.json.schema(Literal[3, 1, 2]) == {"enum": [1, 2, 3]}


def test_str_literal():
    assert msgspec.json.schema(Literal["c", "a", "b"]) == {"enum": ["a", "b", "c"]}


def test_struct_object():
    class Point(msgspec.Struct, forbid_unknown_fields=True):
        x: int
        y: int

    class Polygon(msgspec.Struct):
        """An example docstring"""

        vertices: List[Point]
        name: Union[str, None] = None
        metadata: Dict[str, str] = {}

    assert msgspec.json.schema(Polygon) == {
        "$ref": "#/$defs/Polygon",
        "$defs": {
            "Polygon": {
                "title": "Polygon",
                "description": "An example docstring",
                "type": "object",
                "properties": {
                    "vertices": {
                        "type": "array",
                        "items": {"$ref": "#/$defs/Point"},
                    },
                    "name": {
                        "anyOf": [{"type": "string"}, {"type": "null"}],
                        "default": None,
                    },
                    "metadata": {
                        "type": "object",
                        "additionalProperties": {"type": "string"},
                        "default": {},
                    },
                },
                "required": ["vertices"],
            },
            "Point": {
                "title": "Point",
                "type": "object",
                "properties": {
                    "x": {"type": "integer"},
                    "y": {"type": "integer"},
                },
                "required": ["x", "y"],
                "additionalProperties": False,
            },
        },
    }


@pytest.mark.parametrize("forbid_unknown_fields", [False, True])
def test_struct_array_like(forbid_unknown_fields):
    class Example(
        msgspec.Struct, array_like=True, forbid_unknown_fields=forbid_unknown_fields
    ):
        """An example docstring"""

        a: int
        b: str
        c: List[int] = []
        d: Dict[str, int] = {}

    sol = {
        "$ref": "#/$defs/Example",
        "$defs": {
            "Example": {
                "title": "Example",
                "description": "An example docstring",
                "type": "array",
                "prefixItems": [
                    {"type": "integer"},
                    {"type": "string"},
                    {"type": "array", "items": {"type": "integer"}, "default": []},
                    {
                        "type": "object",
                        "additionalProperties": {"type": "integer"},
                        "default": {},
                    },
                ],
                "minItems": 2,
            }
        },
    }
    if forbid_unknown_fields:
        sol["$defs"]["Example"]["maxItems"] = 4
    assert msgspec.json.schema(Example) == sol


def test_struct_no_fields():
    class Example(msgspec.Struct):
        pass

    assert msgspec.json.schema(Example) == {
        "$ref": "#/$defs/Example",
        "$defs": {
            "Example": {
                "title": "Example",
                "type": "object",
                "properties": {},
                "required": [],
            }
        },
    }


def test_struct_object_tagged():
    class Point(msgspec.Struct, tag=True):
        x: int
        y: int

    assert msgspec.json.schema(Point) == {
        "$ref": "#/$defs/Point",
        "$defs": {
            "Point": {
                "title": "Point",
                "type": "object",
                "properties": {
                    "type": {"enum": ["Point"]},
                    "x": {"type": "integer"},
                    "y": {"type": "integer"},
                },
                "required": ["type", "x", "y"],
            }
        },
    }


def test_struct_array_tagged():
    class Point(msgspec.Struct, tag=True, array_like=True):
        x: int
        y: int

    assert msgspec.json.schema(Point) == {
        "$ref": "#/$defs/Point",
        "$defs": {
            "Point": {
                "title": "Point",
                "type": "array",
                "prefixItems": [
                    {"enum": ["Point"]},
                    {"type": "integer"},
                    {"type": "integer"},
                ],
                "minItems": 3,
            }
        },
    }


def test_struct_keyword_only():
    class Base(msgspec.Struct, kw_only=True):
        x: int = 1
        y: int
        z: int = 2

    class Test(Base):
        a: int
        b: int = 0

    assert msgspec.json.schema(Test) == {
        "$ref": "#/$defs/Test",
        "$defs": {
            "Test": {
                "title": "Test",
                "type": "object",
                "properties": {
                    "a": {"type": "integer"},
                    "b": {"type": "integer", "default": 0},
                    "x": {"type": "integer", "default": 1},
                    "y": {"type": "integer"},
                    "z": {"type": "integer", "default": 2},
                },
                "required": ["a", "y"],
            }
        },
    }


def test_struct_array_keyword_only():
    class Base(msgspec.Struct, kw_only=True, array_like=True):
        x: int = 1
        y: int
        z: int = 2

    class Test(Base):
        a: int
        b: int = 0

    assert msgspec.json.schema(Test) == {
        "$ref": "#/$defs/Test",
        "$defs": {
            "Test": {
                "title": "Test",
                "type": "array",
                "prefixItems": [
                    {"type": "integer"},
                    {"type": "integer", "default": 0},
                    {"type": "integer", "default": 1},
                    {"type": "integer"},
                    {"type": "integer", "default": 2},
                ],
                "minItems": 4,
            }
        },
    }


def test_typing_namedtuple():
    class Example(NamedTuple):
        """An example docstring"""

        a: str
        b: bool
        c: int = 0

    assert msgspec.json.schema(Example) == {
        "$ref": "#/$defs/Example",
        "$defs": {
            "Example": {
                "title": "Example",
                "description": "An example docstring",
                "type": "array",
                "prefixItems": [
                    {"type": "string"},
                    {"type": "boolean"},
                    {"type": "integer", "default": 0},
                ],
                "minItems": 2,
                "maxItems": 3,
            }
        },
    }


def test_collections_namedtuple():
    Example = namedtuple("Example", ["a", "b", "c"], defaults=(0,))

    assert msgspec.json.schema(Example) == {
        "$ref": "#/$defs/Example",
        "$defs": {
            "Example": {
                "title": "Example",
                "type": "array",
                "prefixItems": [{}, {}, {"default": 0}],
                "minItems": 2,
                "maxItems": 3,
            }
        },
    }


def test_generic_namedtuple():
    NamedTuple = pytest.importorskip("typing_extensions").NamedTuple

    class Ex(NamedTuple, Generic[T]):
        """An example docstring"""

        x: T
        y: List[T]

    assert msgspec.json.schema(Ex) == {
        "$ref": "#/$defs/Ex",
        "$defs": {
            "Ex": {
                "title": "Ex",
                "description": "An example docstring",
                "type": "array",
                "prefixItems": [{}, {"type": "array"}],
                "minItems": 2,
                "maxItems": 2,
            },
        },
    }

    assert msgspec.json.schema(Ex[int]) == {
        "$ref": "#/$defs/Ex_int_",
        "$defs": {
            "Ex_int_": {
                "title": "Ex[int]",
                "description": "An example docstring",
                "type": "array",
                "prefixItems": [
                    {"type": "integer"},
                    {"type": "array", "items": {"type": "integer"}},
                ],
                "minItems": 2,
                "maxItems": 2,
            },
        },
    }


@pytest.mark.parametrize("use_typing_extensions", [False, True])
def test_typeddict(use_typing_extensions):
    if use_typing_extensions:
        tex = pytest.importorskip("typing_extensions")
        cls = tex.TypedDict
    else:
        cls = TypedDict

    class Example(cls):
        """An example docstring"""

        a: str
        b: bool
        c: int

    assert msgspec.json.schema(Example) == {
        "$ref": "#/$defs/Example",
        "$defs": {
            "Example": {
                "title": "Example",
                "description": "An example docstring",
                "type": "object",
                "properties": {
                    "a": {"type": "string"},
                    "b": {"type": "boolean"},
                    "c": {"type": "integer"},
                },
                "required": ["a", "b", "c"],
            }
        },
    }


@pytest.mark.parametrize("use_typing_extensions", [False, True])
def test_typeddict_optional(use_typing_extensions):
    if use_typing_extensions:
        tex = pytest.importorskip("typing_extensions")
        cls = tex.TypedDict
    else:
        cls = TypedDict

    class Base(cls):
        a: str
        b: bool

    class Example(Base, total=False):
        """An example docstring"""

        c: int

    assert msgspec.json.schema(Example) == {
        "$ref": "#/$defs/Example",
        "$defs": {
            "Example": {
                "title": "Example",
                "description": "An example docstring",
                "type": "object",
                "properties": {
                    "a": {"type": "string"},
                    "b": {"type": "boolean"},
                    "c": {"type": "integer"},
                },
                "required": ["a", "b"],
            }
        },
    }


def test_generic_typeddict():
    TypedDict = pytest.importorskip("typing_extensions").TypedDict

    class Ex(TypedDict, Generic[T]):
        """An example docstring"""

        x: T
        y: List[T]

    assert msgspec.json.schema(Ex) == {
        "$ref": "#/$defs/Ex",
        "$defs": {
            "Ex": {
                "title": "Ex",
                "description": "An example docstring",
                "type": "object",
                "properties": {
                    "x": {},
                    "y": {"type": "array"},
                },
                "required": ["x", "y"],
            },
        },
    }

    assert msgspec.json.schema(Ex[int]) == {
        "$ref": "#/$defs/Ex_int_",
        "$defs": {
            "Ex_int_": {
                "title": "Ex[int]",
                "description": "An example docstring",
                "type": "object",
                "properties": {
                    "x": {"type": "integer"},
                    "y": {"type": "array", "items": {"type": "integer"}},
                },
                "required": ["x", "y"],
            },
        },
    }


@pytest.mark.parametrize("module", ["dataclasses", "attrs"])
def test_dataclass_or_attrs(module):
    m = pytest.importorskip(module)
    if module == "attrs":
        decorator = m.define
        factory_default = m.field(factory=dict)
    else:
        decorator = m.dataclass
        factory_default = m.field(default_factory=dict)

    @decorator
    class Point:
        x: int
        y: int

    @decorator
    class Polygon:
        """An example docstring"""

        vertices: List[Point]
        name: Union[str, None] = None
        metadata: Dict[str, str] = factory_default

    assert msgspec.json.schema(Polygon) == {
        "$ref": "#/$defs/Polygon",
        "$defs": {
            "Polygon": {
                "title": "Polygon",
                "description": "An example docstring",
                "type": "object",
                "properties": {
                    "vertices": {
                        "type": "array",
                        "items": {"$ref": "#/$defs/Point"},
                    },
                    "name": {
                        "anyOf": [{"type": "string"}, {"type": "null"}],
                        "default": None,
                    },
                    "metadata": {
                        "type": "object",
                        "additionalProperties": {"type": "string"},
                    },
                },
                "required": ["vertices"],
            },
            "Point": {
                "title": "Point",
                "type": "object",
                "properties": {
                    "x": {"type": "integer"},
                    "y": {"type": "integer"},
                },
                "required": ["x", "y"],
            },
        },
    }


@pytest.mark.parametrize("module", ["dataclasses", "attrs"])
def test_generic_dataclass_or_attrs(module):
    m = pytest.importorskip(module)
    decorator = m.define if module == "attrs" else m.dataclass

    @decorator
    class Ex(Generic[T]):
        """An example docstring"""

        x: T
        y: List[T]

    assert msgspec.json.schema(Ex) == {
        "$ref": "#/$defs/Ex",
        "$defs": {
            "Ex": {
                "title": "Ex",
                "description": "An example docstring",
                "type": "object",
                "properties": {
                    "x": {},
                    "y": {"type": "array"},
                },
                "required": ["x", "y"],
            },
        },
    }

    assert msgspec.json.schema(Ex[int]) == {
        "$ref": "#/$defs/Ex_int_",
        "$defs": {
            "Ex_int_": {
                "title": "Ex[int]",
                "description": "An example docstring",
                "type": "object",
                "properties": {
                    "x": {"type": "integer"},
                    "y": {"type": "array", "items": {"type": "integer"}},
                },
                "required": ["x", "y"],
            },
        },
    }


@pytest.mark.parametrize("use_union_operator", [False, True])
def test_union(use_union_operator):
    class Example(msgspec.Struct):
        x: int
        y: int

    if use_union_operator:
        try:
            typ = int | str | Example
        except TypeError:
            pytest.skip("Union operator not supported")
    else:
        typ = Union[int, str, Example]

    assert msgspec.json.schema(typ) == {
        "anyOf": [
            {"type": "integer"},
            {"type": "string"},
            {"$ref": "#/$defs/Example"},
        ],
        "$defs": {
            "Example": {
                "title": "Example",
                "type": "object",
                "properties": {"x": {"type": "integer"}, "y": {"type": "integer"}},
                "required": ["x", "y"],
            }
        },
    }


def test_struct_tagged_union():
    class Point(msgspec.Struct, tag=True):
        x: int
        y: int

    class Point3D(Point):
        z: int

    assert msgspec.json.schema(Union[Point, Point3D]) == {
        "anyOf": [{"$ref": "#/$defs/Point"}, {"$ref": "#/$defs/Point3D"}],
        "discriminator": {
            "mapping": {"Point": "#/$defs/Point", "Point3D": "#/$defs/Point3D"},
            "propertyName": "type",
        },
        "$defs": {
            "Point": {
                "properties": {
                    "type": {"enum": ["Point"]},
                    "x": {"type": "integer"},
                    "y": {"type": "integer"},
                },
                "required": ["type", "x", "y"],
                "title": "Point",
                "type": "object",
            },
            "Point3D": {
                "properties": {
                    "type": {"enum": ["Point3D"]},
                    "x": {"type": "integer"},
                    "y": {"type": "integer"},
                    "z": {"type": "integer"},
                },
                "required": ["type", "x", "y", "z"],
                "title": "Point3D",
                "type": "object",
            },
        },
    }


def test_struct_tagged_union_mixed_types():
    class Point(msgspec.Struct, tag=True):
        x: int
        y: int

    class Point3D(Point):
        z: int

    assert msgspec.json.schema(Union[Point, Point3D, int, float]) == {
        "anyOf": [
            {"type": "integer"},
            {"type": "number"},
            {
                "anyOf": [{"$ref": "#/$defs/Point"}, {"$ref": "#/$defs/Point3D"}],
                "discriminator": {
                    "mapping": {"Point": "#/$defs/Point", "Point3D": "#/$defs/Point3D"},
                    "propertyName": "type",
                },
            },
        ],
        "$defs": {
            "Point": {
                "properties": {
                    "type": {"enum": ["Point"]},
                    "x": {"type": "integer"},
                    "y": {"type": "integer"},
                },
                "required": ["type", "x", "y"],
                "title": "Point",
                "type": "object",
            },
            "Point3D": {
                "properties": {
                    "type": {"enum": ["Point3D"]},
                    "x": {"type": "integer"},
                    "y": {"type": "integer"},
                    "z": {"type": "integer"},
                },
                "required": ["type", "x", "y", "z"],
                "title": "Point3D",
                "type": "object",
            },
        },
    }


def test_struct_array_union():
    class Point(msgspec.Struct, array_like=True, tag=True):
        x: int
        y: int

    class Point3D(Point):
        z: int

    assert msgspec.json.schema(Union[Point, Point3D]) == {
        "anyOf": [{"$ref": "#/$defs/Point"}, {"$ref": "#/$defs/Point3D"}],
        "$defs": {
            "Point": {
                "minItems": 3,
                "prefixItems": [
                    {"enum": ["Point"]},
                    {"type": "integer"},
                    {"type": "integer"},
                ],
                "title": "Point",
                "type": "array",
            },
            "Point3D": {
                "minItems": 4,
                "prefixItems": [
                    {"enum": ["Point3D"]},
                    {"type": "integer"},
                    {"type": "integer"},
                    {"type": "integer"},
                ],
                "title": "Point3D",
                "type": "array",
            },
        },
    }


def test_struct_unset_fields():
    class Ex(msgspec.Struct):
        x: Union[int, msgspec.UnsetType] = msgspec.UNSET

    assert msgspec.json.schema(Ex) == {
        "$ref": "#/$defs/Ex",
        "$defs": {
            "Ex": {
                "properties": {"x": {"type": "integer"}},
                "required": [],
                "title": "Ex",
                "type": "object",
            }
        },
    }


def test_generic_struct():
    class Ex(msgspec.Struct, Generic[T]):
        """An example docstring"""

        x: T
        y: List[T]

    assert msgspec.json.schema(Ex) == {
        "$ref": "#/$defs/Ex",
        "$defs": {
            "Ex": {
                "title": "Ex",
                "description": "An example docstring",
                "type": "object",
                "properties": {
                    "x": {},
                    "y": {"type": "array"},
                },
                "required": ["x", "y"],
            },
        },
    }

    assert msgspec.json.schema(Ex[int]) == {
        "$ref": "#/$defs/Ex_int_",
        "$defs": {
            "Ex_int_": {
                "title": "Ex[int]",
                "description": "An example docstring",
                "type": "object",
                "properties": {
                    "x": {"type": "integer"},
                    "y": {"type": "array", "items": {"type": "integer"}},
                },
                "required": ["x", "y"],
            },
        },
    }


def test_generic_struct_tagged_union():
    class Point(msgspec.Struct, Generic[T], tag=True):
        x: T
        y: T

    class Point3D(Point[T]):
        z: T

    sol = {
        "anyOf": [{"$ref": "#/$defs/Point_int_"}, {"$ref": "#/$defs/Point3D_int_"}],
        "discriminator": {
            "mapping": {
                "Point": "#/$defs/Point_int_",
                "Point3D": "#/$defs/Point3D_int_",
            },
            "propertyName": "type",
        },
        "$defs": {
            "Point_int_": {
                "properties": {
                    "type": {"enum": ["Point"]},
                    "x": {"type": "integer"},
                    "y": {"type": "integer"},
                },
                "required": ["type", "x", "y"],
                "title": "Point[int]",
                "type": "object",
            },
            "Point3D_int_": {
                "properties": {
                    "type": {"enum": ["Point3D"]},
                    "x": {"type": "integer"},
                    "y": {"type": "integer"},
                    "z": {"type": "integer"},
                },
                "required": ["type", "x", "y", "z"],
                "title": "Point3D[int]",
                "type": "object",
            },
        },
    }
    res = msgspec.json.schema(Union[Point[int], Point3D[int]])
    assert res == sol


@pytest.mark.parametrize(
    "field, constraint",
    [
        ("ge", "minimum"),
        ("gt", "exclusiveMinimum"),
        ("le", "maximum"),
        ("lt", "exclusiveMaximum"),
        ("multiple_of", "multipleOf"),
    ],
)
def test_numeric_metadata(field, constraint):
    typ = Annotated[int, Meta(**{field: 2})]
    assert msgspec.json.schema(typ) == {"type": "integer", constraint: 2}


@pytest.mark.parametrize(
    "field, val, constraint",
    [
        ("pattern", "[a-z]*", "pattern"),
        ("min_length", 0, "minLength"),
        ("max_length", 3, "maxLength"),
    ],
)
def test_string_metadata(field, val, constraint):
    typ = Annotated[str, Meta(**{field: val})]
    assert msgspec.json.schema(typ) == {"type": "string", constraint: val}


@pytest.mark.parametrize(
    "field, val, constraint",
    [
        ("pattern", "[a-z]*", "pattern"),
        ("min_length", 0, "minLength"),
        ("max_length", 3, "maxLength"),
    ],
)
def test_dict_key_metadata(field, val, constraint):
    typ = Annotated[str, Meta(**{field: val})]
    assert msgspec.json.schema(Dict[typ, int]) == {
        "type": "object",
        "additionalProperties": {"type": "integer"},
        "propertyNames": {constraint: val},
    }


@pytest.mark.parametrize("typ", [bytes, bytearray, memoryview])
@pytest.mark.parametrize(
    "field, n, constraint",
    [("min_length", 2, "minLength"), ("max_length", 7, "maxLength")],
)
def test_binary_metadata(typ, field, n, constraint):
    n2 = len(b64encode(b"x" * n))
    typ = Annotated[typ, Meta(**{field: n})]
    assert msgspec.json.schema(typ) == {
        "type": "string",
        constraint: n2,
        "contentEncoding": "base64",
    }


@pytest.mark.parametrize("typ", [list, tuple, set, frozenset])
@pytest.mark.parametrize(
    "field, constraint",
    [("min_length", "minItems"), ("max_length", "maxItems")],
)
def test_array_metadata(typ, field, constraint):
    typ = Annotated[typ, Meta(**{field: 2})]
    assert msgspec.json.schema(typ) == {"type": "array", constraint: 2}


@pytest.mark.parametrize(
    "field, constraint",
    [("min_length", "minProperties"), ("max_length", "maxProperties")],
)
def test_object_metadata(field, constraint):
    typ = Annotated[dict, Meta(**{field: 2})]
    assert msgspec.json.schema(typ) == {"type": "object", constraint: 2}


def test_generic_metadata():
    typ = Annotated[
        int,
        Meta(
            title="the title",
            description="the description",
            examples=[1, 2, 3],
            extra_json_schema={"title": "an override", "default": 1},
        ),
    ]
    assert msgspec.json.schema(typ) == {
        "type": "integer",
        "title": "an override",
        "description": "the description",
        "examples": [1, 2, 3],
        "default": 1,
    }


def test_component_names_collide():
    s1 = """
    import msgspec
    Ex = msgspec.defstruct("Ex", [("x", int), ("y", int)])
    """

    s2 = """
    import msgspec
    Ex = msgspec.defstruct("Ex", [("a", str), ("b", str)])
    """

    with temp_module(s1) as m1, temp_module(s2) as m2:
        (r1, r2), components = msgspec.json.schema_components([m1.Ex, m2.Ex])

    assert r1 == {"$ref": f"#/$defs/{m1.__name__}.Ex"}
    assert r2 == {"$ref": f"#/$defs/{m2.__name__}.Ex"}
    assert components == {
        f"{m1.__name__}.Ex": {
            "properties": {"x": {"type": "integer"}, "y": {"type": "integer"}},
            "required": ["x", "y"],
            "title": "Ex",
            "type": "object",
        },
        f"{m2.__name__}.Ex": {
            "properties": {"a": {"type": "string"}, "b": {"type": "string"}},
            "required": ["a", "b"],
            "title": "Ex",
            "type": "object",
        },
    }


def test_schema_components_collects_subtypes():
    class ExEnum(enum.Enum):
        A = 1

    class ExStruct(msgspec.Struct):
        b: Union[Set[FrozenSet[ExEnum]], int]

    class ExDict(TypedDict):
        c: Tuple[ExStruct, ...]

    class ExTuple(NamedTuple):
        d: List[ExDict]

    @dataclass
    class ExDataclass:
        e: List[ExTuple]

    (s,), components = msgspec.json.schema_components([Dict[str, ExDataclass]])

    r1 = {"$ref": "#/$defs/ExEnum"}
    r2 = {"$ref": "#/$defs/ExStruct"}
    r3 = {"$ref": "#/$defs/ExDict"}
    r4 = {"$ref": "#/$defs/ExTuple"}
    r5 = {"$ref": "#/$defs/ExDataclass"}

    assert s == {"type": "object", "additionalProperties": r5}
    assert components == {
        "ExEnum": {"enum": [1], "title": "ExEnum"},
        "ExStruct": {
            "type": "object",
            "title": "ExStruct",
            "properties": {
                "b": {
                    "anyOf": [
                        {"items": {"items": r1, "type": "array"}, "type": "array"},
                        {"type": "integer"},
                    ]
                }
            },
            "required": ["b"],
        },
        "ExDict": {
            "title": "ExDict",
            "type": "object",
            "properties": {"c": {"items": r2, "type": "array"}},
            "required": ["c"],
        },
        "ExTuple": {
            "title": "ExTuple",
            "type": "array",
            "prefixItems": [{"items": r3, "type": "array"}],
            "maxItems": 1,
            "minItems": 1,
        },
        "ExDataclass": {
            "title": "ExDataclass",
            "type": "object",
            "properties": {"e": {"items": r4, "type": "array"}},
            "required": ["e"],
        },
    }


def test_ref_template():
    class Ex1(msgspec.Struct):
        a: int

    class Ex2(msgspec.Struct):
        b: Ex1

    (s1, s2), components = msgspec.json.schema_components(
        [Ex1, Ex2], ref_template="#/definitions/{name}"
    )

    assert s1 == {"$ref": "#/definitions/Ex1"}
    assert s2 == {"$ref": "#/definitions/Ex2"}

    assert components == {
        "Ex1": {
            "title": "Ex1",
            "type": "object",
            "properties": {"a": {"type": "integer"}},
            "required": ["a"],
        },
        "Ex2": {
            "title": "Ex2",
            "type": "object",
            "properties": {"b": s1},
            "required": ["b"],
        },
    }


def test_multiline_docstring():
    class Example(msgspec.Struct):
        """
            indented first line

        last line.
        """

        pass

    assert msgspec.json.schema(Example) == {
        "$ref": "#/$defs/Example",
        "$defs": {
            "Example": {
                "description": "    indented first line\n\nlast line.",
                "title": "Example",
                "type": "object",
                "properties": {},
                "required": [],
            }
        },
    }
