import pytest
import sqlalchemy as sa

from graphene import Enum, List, ObjectType, Schema, String

from graphene_sqlalchemy.utils import (DummyImport, get_session, sort_argument_for_model,
                     sort_enum_for_model, to_enum_value_name, to_type_name)
from .models import Base, Editor, Pet


def test_get_session():
    session = "My SQLAlchemy session"

    class Query(ObjectType):
        x = String()

        def resolve_x(self, info):
            return get_session(info.context)

    query = """
        query ReporterQuery {
            x
        }
    """

    schema = Schema(query=Query)
    result = schema.execute(query, context_value={"session": session})
    assert not result.errors
    assert result.data["x"] == session


def test_to_type_name():
    assert to_type_name("make_camel_case") == "MakeCamelCase"
    assert to_type_name("AlreadyCamelCase") == "AlreadyCamelCase"
    assert to_type_name("A_Snake_and_a_Camel") == "ASnakeAndACamel"


def test_to_enum_value_name():
    assert to_enum_value_name("make_enum_value_name") == "MAKE_ENUM_VALUE_NAME"
    assert to_enum_value_name("makeEnumValueName") == "MAKE_ENUM_VALUE_NAME"
    assert to_enum_value_name("HTTPStatus400Message") == "HTTP_STATUS400_MESSAGE"
    assert to_enum_value_name("ALREADY_ENUM_VALUE_NAME") == "ALREADY_ENUM_VALUE_NAME"


# test deprecated sort enum utility functions


def test_sort_enum_for_model():
    with pytest.warns(DeprecationWarning):
        enum = sort_enum_for_model(Pet)
    assert isinstance(enum, type(Enum))
    assert str(enum) == "PetSortEnum"
    for col in sa.inspect(Pet).columns:
        assert hasattr(enum, col.name + "_asc")
        assert hasattr(enum, col.name + "_desc")


def test_sort_enum_for_model_custom_naming():
    with pytest.warns(DeprecationWarning):
        enum = sort_enum_for_model(
            Pet, "Foo", lambda n, d: n.upper() + ("A" if d else "D")
        )
    assert str(enum) == "Foo"
    for col in sa.inspect(Pet).columns:
        assert hasattr(enum, col.name.upper() + "A")
        assert hasattr(enum, col.name.upper() + "D")


def test_enum_cache():
    with pytest.warns(DeprecationWarning):
        assert sort_enum_for_model(Editor) is sort_enum_for_model(Editor)


def test_sort_argument_for_model():
    with pytest.warns(DeprecationWarning):
        arg = sort_argument_for_model(Pet)

    assert isinstance(arg.type, List)
    assert arg.default_value == [Pet.id.name + "_asc"]
    with pytest.warns(DeprecationWarning):
        assert arg.type.of_type is sort_enum_for_model(Pet)


def test_sort_argument_for_model_no_default():
    with pytest.warns(DeprecationWarning):
        arg = sort_argument_for_model(Pet, False)

    assert arg.default_value is None


def test_sort_argument_for_model_multiple_pk():
    class MultiplePK(Base):
        foo = sa.Column(sa.Integer, primary_key=True)
        bar = sa.Column(sa.Integer, primary_key=True)
        __tablename__ = "MultiplePK"

    with pytest.warns(DeprecationWarning):
        arg = sort_argument_for_model(MultiplePK)
    assert set(arg.default_value) == set(
        (MultiplePK.foo.name + "_asc", MultiplePK.bar.name + "_asc")
    )

def test_dummy_import():
    dummy_module = DummyImport()
    assert dummy_module.foo == object
