from enum import Enum as PyEnum

import pytest
from sqlalchemy.types import Enum as SQLAlchemyEnumType

from graphene import Enum

from graphene_sqlalchemy.enums import _convert_sa_to_graphene_enum, enum_for_field
from graphene_sqlalchemy.types import SQLAlchemyObjectType
from .models import HairKind, Pet


def test_convert_sa_to_graphene_enum_bad_type():
    re_err = "Expected sqlalchemy.types.Enum, but got: 'foo'"
    with pytest.raises(TypeError, match=re_err):
        _convert_sa_to_graphene_enum("foo")


def test_convert_sa_to_graphene_enum_based_on_py_enum():
    class Color(PyEnum):
        RED = 1
        GREEN = 2
        BLUE = 3

    sa_enum = SQLAlchemyEnumType(Color)
    graphene_enum = _convert_sa_to_graphene_enum(sa_enum, "FallbackName")
    assert isinstance(graphene_enum, type(Enum))
    assert graphene_enum._meta.name == "Color"
    assert graphene_enum._meta.enum is Color


def test_convert_sa_to_graphene_enum_based_on_py_enum_with_bad_names():
    class Color(PyEnum):
        red = 1
        green = 2
        blue = 3

    sa_enum = SQLAlchemyEnumType(Color)
    graphene_enum = _convert_sa_to_graphene_enum(sa_enum, "FallbackName")
    assert isinstance(graphene_enum, type(Enum))
    assert graphene_enum._meta.name == "Color"
    assert graphene_enum._meta.enum is not Color
    assert [
        (key, value.value)
        for key, value in graphene_enum._meta.enum.__members__.items()
    ] == [("RED", 1), ("GREEN", 2), ("BLUE", 3)]


def test_convert_sa_enum_to_graphene_enum_based_on_list_named():
    sa_enum = SQLAlchemyEnumType("red", "green", "blue", name="color_values")
    graphene_enum = _convert_sa_to_graphene_enum(sa_enum, "FallbackName")
    assert isinstance(graphene_enum, type(Enum))
    assert graphene_enum._meta.name == "ColorValues"
    assert [
        (key, value.value)
        for key, value in graphene_enum._meta.enum.__members__.items()
    ] == [("RED", 'red'), ("GREEN", 'green'), ("BLUE", 'blue')]


def test_convert_sa_enum_to_graphene_enum_based_on_list_unnamed():
    sa_enum = SQLAlchemyEnumType("red", "green", "blue")
    graphene_enum = _convert_sa_to_graphene_enum(sa_enum, "FallbackName")
    assert isinstance(graphene_enum, type(Enum))
    assert graphene_enum._meta.name == "FallbackName"
    assert [
        (key, value.value)
        for key, value in graphene_enum._meta.enum.__members__.items()
    ] == [("RED", 'red'), ("GREEN", 'green'), ("BLUE", 'blue')]


def test_convert_sa_enum_to_graphene_enum_based_on_list_without_name():
    sa_enum = SQLAlchemyEnumType("red", "green", "blue")
    re_err = r"No type name specified for Enum\('red', 'green', 'blue'\)"
    with pytest.raises(TypeError, match=re_err):
        _convert_sa_to_graphene_enum(sa_enum)


def test_enum_for_field():
    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet

    enum = enum_for_field(PetType, 'pet_kind')
    assert isinstance(enum, type(Enum))
    assert enum._meta.name == "PetKind"
    assert [
        (key, value.value)
        for key, value in enum._meta.enum.__members__.items()
    ] == [("CAT", 'cat'), ("DOG", 'dog')]
    enum2 = enum_for_field(PetType, 'pet_kind')
    assert enum2 is enum
    enum2 = PetType.enum_for_field('pet_kind')
    assert enum2 is enum

    enum = enum_for_field(PetType, 'hair_kind')
    assert isinstance(enum, type(Enum))
    assert enum._meta.name == "HairKind"
    assert enum._meta.enum is HairKind
    enum2 = PetType.enum_for_field('hair_kind')
    assert enum2 is enum

    re_err = r"Cannot get PetType\.other_kind"
    with pytest.raises(TypeError, match=re_err):
        enum_for_field(PetType, 'other_kind')
    with pytest.raises(TypeError, match=re_err):
        PetType.enum_for_field('other_kind')

    re_err = r"PetType\.name does not map to enum column"
    with pytest.raises(TypeError, match=re_err):
        enum_for_field(PetType, 'name')
    with pytest.raises(TypeError, match=re_err):
        PetType.enum_for_field('name')

    re_err = r"Expected a field name, but got: None"
    with pytest.raises(TypeError, match=re_err):
        enum_for_field(PetType, None)
    with pytest.raises(TypeError, match=re_err):
        PetType.enum_for_field(None)

    re_err = "Expected SQLAlchemyObjectType, but got: None"
    with pytest.raises(TypeError, match=re_err):
        enum_for_field(None, 'other_kind')
