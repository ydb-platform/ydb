from . import helpers
import yt.type_info.typing as typing
from yt.type_info import is_valid_type

import pytest
import six


def test_primitive_types():
    for type_name in helpers.NO_ARGUMENT_TYPES:
        assert hasattr(typing, type_name)
        type_ = getattr(typing, type_name)
        assert type_.name == type_name
        assert str(type_) == type_name
        assert is_valid_type(type_)


def test_primitive_type_equality():
    for i, type_name in enumerate(helpers.NO_ARGUMENT_TYPES):
        type_ = getattr(typing, type_name)
        assert type_ == type_
        assert not(type_ != type_)
        type_name_next = helpers.NO_ARGUMENT_TYPES[(i + 1) % len(helpers.NO_ARGUMENT_TYPES)]
        type_next = getattr(typing, type_name_next)
        assert type_ != type_next
        assert not(type_ == type_next)


def test_compound_types():
    optional = typing.Optional[typing.Int32]
    assert is_valid_type(optional)
    assert str(optional) == "Optional<Int32>"
    assert optional.name == "Optional"
    assert optional.item == typing.Int32

    list_ = typing.List[typing.String]
    assert is_valid_type(list_)
    assert str(list_) == "List<String>"
    assert list_.name == "List"
    assert list_.item == typing.String

    dict_ = typing.Dict[typing.Double, typing.Bool]
    assert is_valid_type(dict_)
    assert str(dict_) == "Dict<Double, Bool>"
    assert dict_.name == "Dict"
    assert dict_.key == typing.Double
    assert dict_.value == typing.Bool

    tuple_ = typing.Tuple[typing.Double, typing.Uuid, typing.Json]
    assert is_valid_type(tuple_)
    assert str(tuple_) == "Tuple<Double, Uuid, Json>"
    assert tuple_.name == "Tuple"
    assert tuple_.items == (typing.Double, typing.Uuid, typing.Json)

    assert is_valid_type(typing.EmptyTuple)
    assert str(typing.EmptyTuple) == "Tuple<>"
    assert typing.EmptyTuple.name == "Tuple"
    assert typing.EmptyTuple.items == tuple()

    struct = typing.Struct[
        "a": typing.Uint8,
        "b": typing.Yson,
        u"ой": typing.Uint64,
    ]
    assert is_valid_type(struct)
    assert six.ensure_text(str(struct)) == u"Struct<'a': Uint8, 'b': Yson, 'ой': Uint64>"
    assert struct.name == "Struct"
    assert struct.items == (("a", typing.Uint8), ("b", typing.Yson), (u"ой", typing.Uint64))

    assert is_valid_type(typing.EmptyStruct)
    assert str(typing.EmptyStruct) == "Struct<>"
    assert typing.EmptyStruct.name == "Struct"
    assert typing.EmptyStruct.items == tuple()

    variant_struct = typing.Variant[
        "to_be": typing.Null,
        "not_to_be": typing.Void,
    ]
    assert is_valid_type(variant_struct)
    assert str(variant_struct) == "Variant<'to_be': Null, 'not_to_be': Void>"
    assert variant_struct.name == "Variant"
    assert variant_struct.items == (("to_be", typing.Null), ("not_to_be", typing.Void))

    variant_tuple = typing.Variant[typing.Uint8, typing.Int8]
    assert is_valid_type(variant_tuple)
    assert str(variant_tuple) == "Variant<Uint8, Int8>"
    assert variant_tuple.name == "Variant"
    assert variant_tuple.items == (typing.Uint8, typing.Int8)

    tagged = typing.Tagged[typing.String, "my_tag"]
    assert is_valid_type(tagged)
    assert str(tagged) == "Tagged<String, 'my_tag'>"
    assert tagged.name == "Tagged"
    assert tagged.tag == "my_tag"
    assert tagged.item == typing.String

    tagged_ru = typing.Tagged[typing.String, u"мой_тэг"]
    assert is_valid_type(tagged_ru)
    assert six.ensure_text(str(tagged_ru)) == u"Tagged<String, 'мой_тэг'>"
    assert tagged_ru.name == "Tagged"
    assert tagged_ru.tag == u"мой_тэг"
    assert tagged_ru.item == typing.String

    decimal_getitem = typing.Decimal[5, 3]
    assert is_valid_type(decimal_getitem)
    assert str(decimal_getitem) == "Decimal(5, 3)"
    assert decimal_getitem.name == "Decimal"
    assert decimal_getitem.precision == 5
    assert decimal_getitem.scale == 3

    decimal_call = typing.Decimal(5, 3)
    assert is_valid_type(decimal_call)
    assert str(decimal_call) == "Decimal(5, 3)"
    assert decimal_call.name == "Decimal"
    assert decimal_call.precision == 5
    assert decimal_call.scale == 3


def test_compound_type_equality():
    optional1 = typing.Optional[typing.Int32]
    optional2 = typing.Optional[typing.Int32]
    optional3 = typing.Optional[typing.Int64]
    assert optional1 == optional1
    assert optional1 == optional2
    assert optional1 != optional3

    list1 = typing.List[typing.String]
    list2 = typing.List[typing.String]
    list3 = typing.List[typing.Optional[typing.String]]
    assert list1 == list1
    assert list1 == list2
    assert list1 != list3

    dict1 = typing.Dict[typing.Double, typing.Bool]
    dict2 = typing.Dict[typing.Double, typing.Bool]
    dict3 = typing.Dict[typing.Optional[typing.Double], typing.Bool]
    assert dict1 == dict1
    assert dict1 == dict2
    assert dict1 != dict3

    tuple1 = typing.Tuple[typing.Double, typing.Uuid, typing.Json]
    tuple2 = typing.Tuple[typing.Double, typing.Uuid, typing.Json]
    tuple3 = typing.Tuple[typing.Double, typing.Json, typing.Uuid]
    assert tuple1 == tuple1
    assert tuple1 == tuple2
    assert tuple1 != tuple3

    assert typing.EmptyTuple == typing.EmptyTuple

    struct1 = typing.Struct["a": typing.Uint8, "b": typing.Yson, u"ой": typing.Uint64]
    struct2 = typing.Struct["a": typing.Uint8, "b": typing.Yson, u"ой": typing.Uint64]
    struct3 = typing.Struct["a": typing.Uint8, "B": typing.Yson, u"ой": typing.Uint64]
    assert struct1 == struct1
    assert struct1 == struct2
    assert struct1 != struct3

    assert typing.EmptyStruct == typing.EmptyStruct
    assert typing.EmptyStruct != typing.EmptyTuple

    variant_struct1 = typing.Variant["to_be": typing.Null, "not_to_be": typing.Void]
    variant_struct2 = typing.Variant["to_be": typing.Null, "not_to_be": typing.Void]
    variant_struct3 = typing.Variant["to_be": typing.Void, "not_to_be": typing.Void]
    assert variant_struct1 == variant_struct1
    assert variant_struct1 == variant_struct2
    assert variant_struct1 != variant_struct3

    variant_tuple1 = typing.Variant[typing.Uint8, typing.Int8]
    variant_tuple2 = typing.Variant[typing.Uint8, typing.Int8]
    variant_tuple3 = typing.Variant[typing.Uint8, typing.Int8, typing.Int16]
    assert variant_tuple1 == variant_tuple1
    assert variant_tuple1 == variant_tuple2
    assert variant_tuple1 != variant_tuple3

    tagged1 = typing.Tagged[typing.String, "my_tag"]
    tagged2 = typing.Tagged[typing.String, "my_tag"]
    tagged3 = typing.Tagged[typing.String, "my_tag1"]
    assert tagged1 == tagged1
    assert tagged1 == tagged2
    assert tagged1 != tagged3

    tagged_ru1 = typing.Tagged[typing.String, u"мой_тэг"]
    tagged_ru2 = typing.Tagged[typing.String, u"мой_тэг"]
    tagged_ru3 = typing.Tagged[typing.String, u"мой_тэг1"]
    assert tagged_ru1 == tagged_ru1
    assert tagged_ru1 == tagged_ru2
    assert tagged_ru1 != tagged_ru3

    decimal_getitem1 = typing.Decimal[5, 3]
    decimal_getitem2 = typing.Decimal[5, 3]
    decimal_getitem3 = typing.Decimal[5, 4]
    assert decimal_getitem1 == decimal_getitem1
    assert decimal_getitem1 == decimal_getitem2
    assert decimal_getitem1 != decimal_getitem3

    decimal_call1 = typing.Decimal(5, 3)
    decimal_call2 = typing.Decimal(5, 3)
    decimal_call3 = typing.Decimal(5, 4)
    assert decimal_call1 == decimal_call1
    assert decimal_call1 == decimal_call2
    assert decimal_call1 != decimal_call3

    assert decimal_call1 == decimal_getitem1
    assert decimal_call2 == decimal_getitem2
    assert decimal_call3 == decimal_getitem3


def test_errors():
    for T in [typing.Struct, typing.Variant]:
        with pytest.raises(ValueError):
            T[b"\xff": typing.Int8]

        with pytest.raises(ValueError):
            T[10: typing.Int8]

        with pytest.raises(ValueError):
            T["a": typing.Int8, "a": typing.Int8]

        with pytest.raises(ValueError):
            T["": typing.Int8]

    with pytest.raises(ValueError):
        typing.Tagged[typing.Int8, 10]

    with pytest.raises(ValueError):
        typing.Tagged[typing.Int8, b"\xff"]

    with pytest.raises(ValueError):
        typing.Decimal(10, 10.0)
