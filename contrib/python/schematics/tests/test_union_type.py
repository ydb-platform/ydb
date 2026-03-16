# -*- coding: utf-8 -*-

from __future__ import print_function, division

import pytest
import uuid

from schematics.common import *
from schematics.exceptions import *
from schematics.models import Model
from schematics.types import *
from schematics.types.compound import *
from schematics.types.serializable import Serializable


def test_id_or_uuid():

    id_field = UnionType((IntType, UUIDType))

    result = id_field.convert('ee5a16cb-0ee1-46bc-ac40-fb3018b1d29b')
    assert result == uuid.UUID('ee5a16cb-0ee1-46bc-ac40-fb3018b1d29b')
    assert type(id_field.to_primitive(result)) is str

    result = id_field.convert('99999')
    assert result == 99999
    assert type(result) is int
    assert type(id_field.to_primitive(result)) is int


def test_list_of_numbers():

    numbers = ListType(UnionType((IntType, FloatType)))
    result = numbers.convert(["2", "0.5", "123", "2.999"], None)
    assert result == [2, 0.5, 123, 2.999]
    assert [type(item) for item in result] == [int, float, int, float]
    assert [type(item) for item in numbers.to_primitive(result)] == [int, float, int, float]


def test_dict_or_list_of_ints():

    ints = UnionType((DictType, ListType), field=IntType)

    result = ints.convert(["1", "2", "3", "4"])
    assert result == [1, 2, 3, 4]
    assert ints.to_primitive(result) == [1, 2, 3, 4]

    result = ints.convert(dict(a=1, b=2, c=3, d=4))
    assert result == dict(a=1, b=2, c=3, d=4)
    assert [type(item) for item in result.values()] == [int, int, int, int]
    assert ints.to_primitive(result) == dict(a=1, b=2, c=3, d=4)


def test_custom_type():

    class ShoutType(StringType):
        def to_primitive(self, value, context):
            return StringType.convert(self, value) + '!!!'

    class QuestionType(StringType):
        def to_primitive(self, value, context):
            return StringType.convert(self, value) + '???'

    class FancyStringType(UnionType):
        types = (ShoutType, QuestionType)
        def resolve(self, value, context):
            if value.endswith('?'):
                return QuestionType
            elif value.endswith('!'):
                return ShoutType

    assert FancyStringType().to_primitive("Hello, world!") == "Hello, world!!!!"
    assert FancyStringType().to_primitive("Who's a good boy?") == "Who's a good boy????"

    def resolve(value, context):
        if value.endswith('?'):
            return QuestionType
        elif value.endswith('!'):
            return ShoutType

    assert UnionType((ShoutType, QuestionType), resolver=resolve).to_primitive("Hello, world!") \
        == "Hello, world!!!!"

    with pytest.raises(ValidationError):
        FancyStringType(max_length=20).validate("What is the meaning of life?")


def test_option_collation():

    class DerivedStringType(StringType):
        pass

    field = UnionType((IntType, DerivedStringType), required=True, max_value=100, max_length=50)
    assert field.required is True
    assert field._types[IntType].required is True
    assert field._types[DerivedStringType].required is True
    assert field._types[IntType].max_value == 100
    assert field._types[DerivedStringType].max_length == 50


def test_parameterize_validate():

    UnionType((IntType(min_value=1, strict=True), FloatType(min_value=0))).validate(0.00)

    UnionType((IntType(min_value=1), FloatType(min_value=0.5))).validate(0.75)
    with pytest.raises(ValidationError):
        UnionType((IntType(min_value=1), FloatType(min_value=0.5))).validate(0.00)


def test_invalid_args():

    with pytest.raises(TypeError):
        UnionType((IntType, dict))

