import pytest
from sqlalchemy.types import Enum as SQLAlchemyEnum

import graphene
from graphene import Enum as GrapheneEnum

from graphene_sqlalchemy.registry import Registry
from graphene_sqlalchemy.types import SQLAlchemyObjectType
from graphene_sqlalchemy.utils import EnumValue
from .models import Pet, Reporter


def test_register_object_type():
    reg = Registry()

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            registry = reg

    reg.register(PetType)
    assert reg.get_type_for_model(Pet) is PetType


def test_register_incorrect_object_type():
    reg = Registry()

    class Spam:
        pass

    re_err = "Expected SQLAlchemyObjectType, but got: .*Spam"
    with pytest.raises(TypeError, match=re_err):
        reg.register(Spam)


def test_register_orm_field():
    reg = Registry()

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            registry = reg

    reg.register_orm_field(PetType, "name", Pet.name)
    assert reg.get_orm_field_for_graphene_field(PetType, "name") is Pet.name


def test_register_orm_field_incorrect_types():
    reg = Registry()

    class Spam:
        pass

    re_err = "Expected SQLAlchemyObjectType, but got: .*Spam"
    with pytest.raises(TypeError, match=re_err):
        reg.register_orm_field(Spam, "name", Pet.name)

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            registry = reg

    re_err = "Expected a field name, but got: .*Spam"
    with pytest.raises(TypeError, match=re_err):
        reg.register_orm_field(PetType, Spam, Pet.name)


def test_register_enum():
    reg = Registry()

    sa_enum = SQLAlchemyEnum("cat", "dog")
    graphene_enum = GrapheneEnum("PetKind", [("CAT", 1), ("DOG", 2)])

    reg.register_enum(sa_enum, graphene_enum)
    assert reg.get_graphene_enum_for_sa_enum(sa_enum) is graphene_enum


def test_register_enum_incorrect_types():
    reg = Registry()

    sa_enum = SQLAlchemyEnum("cat", "dog")
    graphene_enum = GrapheneEnum("PetKind", [("CAT", 1), ("DOG", 2)])

    re_err = r"Expected Graphene Enum, but got: Enum\('cat', 'dog'\)"
    with pytest.raises(TypeError, match=re_err):
        reg.register_enum(sa_enum, sa_enum)

    re_err = r"Expected SQLAlchemyEnumType, but got: .*PetKind.*"
    with pytest.raises(TypeError, match=re_err):
        reg.register_enum(graphene_enum, graphene_enum)


def test_register_sort_enum():
    reg = Registry()

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            registry = reg

    sort_enum = GrapheneEnum(
        "PetSort",
        [("ID", EnumValue("id", Pet.id)), ("NAME", EnumValue("name", Pet.name))],
    )

    reg.register_sort_enum(PetType, sort_enum)
    assert reg.get_sort_enum_for_object_type(PetType) is sort_enum


def test_register_sort_enum_incorrect_types():
    reg = Registry()

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            registry = reg

    sort_enum = GrapheneEnum(
        "PetSort",
        [("ID", EnumValue("id", Pet.id)), ("NAME", EnumValue("name", Pet.name))],
    )

    re_err = r"Expected SQLAlchemyObjectType, but got: .*PetSort.*"
    with pytest.raises(TypeError, match=re_err):
        reg.register_sort_enum(sort_enum, sort_enum)

    re_err = r"Expected Graphene Enum, but got: .*PetType.*"
    with pytest.raises(TypeError, match=re_err):
        reg.register_sort_enum(PetType, PetType)


def test_register_union():
    reg = Registry()

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            registry = reg

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

    union_types = [PetType, ReporterType]
    union = graphene.Union('ReporterPet', tuple(union_types))

    reg.register_union_type(union, union_types)

    assert reg.get_union_for_object_types(union_types) == union
    # Order should not matter
    assert reg.get_union_for_object_types([ReporterType, PetType]) == union


def test_register_union_scalar():
    reg = Registry()

    union_types = [graphene.String, graphene.Int]
    union = graphene.Union('StringInt', tuple(union_types))

    re_err = r"Expected Graphene ObjectType, but got: .*String.*"
    with pytest.raises(TypeError, match=re_err):
        reg.register_union_type(union, union_types)


def test_register_union_incorrect_types():
    reg = Registry()

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            registry = reg

    class ReporterType(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

    union_types = [PetType, ReporterType]
    union = PetType

    re_err = r"Expected graphene.Union, but got: .*PetType.*"
    with pytest.raises(TypeError, match=re_err):
        reg.register_union_type(union, union_types)
