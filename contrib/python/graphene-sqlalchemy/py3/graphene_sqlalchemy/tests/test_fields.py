import pytest
from promise import Promise

from graphene import NonNull, ObjectType
from graphene.relay import Connection, Node

from graphene_sqlalchemy.fields import (SQLAlchemyConnectionField,
                      UnsortedSQLAlchemyConnectionField)
from graphene_sqlalchemy.types import SQLAlchemyObjectType
from .models import Editor as EditorModel
from .models import Pet as PetModel


class Pet(SQLAlchemyObjectType):
    class Meta:
        model = PetModel
        interfaces = (Node,)


class Editor(SQLAlchemyObjectType):
    class Meta:
        model = EditorModel

##
# SQLAlchemyConnectionField
##


def test_nonnull_sqlalachemy_connection():
    field = SQLAlchemyConnectionField(NonNull(Pet.connection))
    assert isinstance(field.type, NonNull)
    assert issubclass(field.type.of_type, Connection)
    assert field.type.of_type._meta.node is Pet


def test_required_sqlalachemy_connection():
    field = SQLAlchemyConnectionField(Pet.connection, required=True)
    assert isinstance(field.type, NonNull)
    assert issubclass(field.type.of_type, Connection)
    assert field.type.of_type._meta.node is Pet


def test_promise_connection_resolver():
    def resolver(_obj, _info):
        return Promise.resolve([])

    result = UnsortedSQLAlchemyConnectionField.connection_resolver(
        resolver, Pet.connection, Pet, None, None
    )
    assert isinstance(result, Promise)


def test_type_assert_sqlalchemy_object_type():
    with pytest.raises(AssertionError, match="only accepts SQLAlchemyObjectType"):
        SQLAlchemyConnectionField(ObjectType).type


def test_type_assert_object_has_connection():
    with pytest.raises(AssertionError, match="doesn't have a connection"):
        SQLAlchemyConnectionField(Editor).type

##
# UnsortedSQLAlchemyConnectionField
##


def test_sort_added_by_default():
    field = SQLAlchemyConnectionField(Pet.connection)
    assert "sort" in field.args
    assert field.args["sort"] == Pet.sort_argument()


def test_sort_can_be_removed():
    field = SQLAlchemyConnectionField(Pet.connection, sort=None)
    assert "sort" not in field.args


def test_custom_sort():
    field = SQLAlchemyConnectionField(Pet.connection, sort=Editor.sort_argument())
    assert field.args["sort"] == Editor.sort_argument()


def test_sort_init_raises():
    with pytest.raises(TypeError, match="Cannot create sort"):
        SQLAlchemyConnectionField(Connection)
