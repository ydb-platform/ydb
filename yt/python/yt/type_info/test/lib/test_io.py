# -*- coding: utf-8 -*-

from . import helpers
import yt.type_info.typing as typing
from yt.type_info import is_valid_type, serialize_yson, deserialize_yson

from yt import yson

import pytest
import six


def to_snake_case(s):
    res = []
    first = True
    for c in s:
        if c.isupper():
            if first:
                res.append(c.lower())
            else:
                res += ["_", c.lower()]
        else:
            res.append(c)
        first = False
    return "".join(res)


@pytest.mark.parametrize("human_readable", [True, False])
def test_primitive_types(human_readable):
    for type_name in helpers.NO_ARGUMENT_TYPES:
        assert hasattr(typing, type_name)
        type_ = getattr(typing, type_name)
        serialized = serialize_yson(type_, human_readable=human_readable)
        assert deserialize_yson(serialized) == type_
        obj = yson.loads(serialized)
        assert obj == to_snake_case(type_name)


@pytest.mark.parametrize("human_readable", [True, False])
def test_compound_types(human_readable):
    def check(type_, expected_obj):
        serialized = serialize_yson(type_, human_readable=human_readable)
        deserialized = deserialize_yson(serialized)
        assert is_valid_type(deserialized)
        obj = yson.loads(serialized)
        assert obj == expected_obj
        assert deserialized == type_

    check(typing.Optional[typing.Int32], {
        "type_name": "optional",
        "item": "int32",
    })

    check(typing.Dict[typing.Double, typing.Bool], {
        "type_name": "dict",
        "key": "double",
        "value": "bool",
    })

    check(typing.Tuple[typing.Double, typing.Uuid, typing.Json], {
        "type_name": "tuple",
        "elements": [
            {"type": "double"},
            {"type": "uuid"},
            {"type": "json"},
        ],
    })

    check(typing.EmptyTuple, {"type_name": "tuple", "elements": []})

    if six.PY2:
        russian_name = u"ой".encode("utf-8")
    else:
        russian_name = u"ой"

    check(
        typing.Struct[
            "a": typing.Uint8,
            "b": typing.Yson,
            u"ой": typing.Uint64,
        ],
        {
            "type_name": "struct",
            "members": [
                {"name": "a", "type": "uint8"},
                {"name": "b", "type": "yson"},
                {"name": russian_name, "type": "uint64"},
            ],
        },
    )

    check(typing.EmptyStruct, {"type_name": "struct", "members": []})

    variant_struct = typing.Variant[
        "to_be": typing.Null,
        "not_to_be": typing.Void,
    ]
    check(variant_struct, {
        "type_name": "variant",
        "members": [
            {"name": "to_be", "type": "null"},
            {"name": "not_to_be", "type": "void"},
        ],
    })

    check(typing.Variant[typing.Uint8, typing.Int8], {
        "type_name": "variant",
        "elements": [
            {"type": "uint8"},
            {"type": "int8"},
        ],
    })

    check(typing.Tagged[variant_struct, "my_tag"], {
        "type_name": "tagged",
        "tag": "my_tag",
        "item": {
            "type_name": "variant",
            "members": [
                {"name": "to_be", "type": "null"},
                {"name": "not_to_be", "type": "void"},
            ],
        },
    })

    if six.PY2:
        russian_tag = u"мой_тэг".encode("utf-8")
    else:
        russian_tag = u"мой_тэг"
    check(typing.Tagged[typing.String, u"мой_тэг"], {
        "type_name": "tagged",
        "tag": russian_tag,
        "item": "string",
    })

    check(typing.Decimal[5, 3], {
        "type_name": "decimal",
        "precision": 5,
        "scale": 3,
    })

    check(typing.Decimal(5, 3), {
        "type_name": "decimal",
        "precision": 5,
        "scale": 3,
    })


def test_errors():
    with pytest.raises(TypeError):
        serialize_yson("I'm not a type")

    # Malformed YSON.
    with pytest.raises(ValueError):
        deserialize_yson("}}O_o{{")

    # Nonexistent type.
    with pytest.raises(ValueError):
        deserialize_yson("{type_name=\"I actually dont exist\"}")

    # More subtle problem with types.
    with pytest.raises(ValueError):
        deserialize_yson("""{
            type_name=decimal;
            precision=10;
            scale=10.0;
        }""")
