import enum

from py.test import raises
from sqlalchemy import Column, Table, case, func, select, types
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import column_property, composite
from sqlalchemy.sql.elements import Label
from sqlalchemy_utils import ChoiceType, JSONType, ScalarListType

import graphene
from graphene.relay import Node
from graphene.types.datetime import DateTime
from graphene.types.json import JSONString

from graphene_sqlalchemy.converter import (convert_sqlalchemy_column,
                         convert_sqlalchemy_composite,
                         convert_sqlalchemy_relationship)
from graphene_sqlalchemy.fields import (UnsortedSQLAlchemyConnectionField,
                      default_connection_field_factory)
from graphene_sqlalchemy.registry import Registry
from graphene_sqlalchemy.types import SQLAlchemyObjectType
from .models import Article, Pet, Reporter


def assert_column_conversion(sqlalchemy_type, graphene_field, **kwargs):
    column = Column(sqlalchemy_type, doc="Custom Help Text", **kwargs)
    graphene_type = convert_sqlalchemy_column(column)
    assert isinstance(graphene_type, graphene_field)
    field = (
        graphene_type
        if isinstance(graphene_type, graphene.Field)
        else graphene_type.Field()
    )
    assert field.description == "Custom Help Text"
    return field


def assert_composite_conversion(
    composite_class, composite_columns, graphene_field, registry, **kwargs
):
    composite_column = composite(
        composite_class, *composite_columns, doc="Custom Help Text", **kwargs
    )
    graphene_type = convert_sqlalchemy_composite(composite_column, registry)
    assert isinstance(graphene_type, graphene_field)
    field = graphene_type.Field()
    # SQLAlchemy currently does not persist the doc onto the column, even though
    # the documentation says it does....
    # assert field.description == 'Custom Help Text'
    return field


def test_should_unknown_sqlalchemy_field_raise_exception():
    with raises(Exception) as excinfo:
        convert_sqlalchemy_column(None)
    assert "Don't know how to convert the SQLAlchemy field" in str(excinfo.value)


def test_should_date_convert_string():
    assert_column_conversion(types.Date(), graphene.String)


def test_should_datetime_convert_string():
    assert_column_conversion(types.DateTime(), DateTime)


def test_should_time_convert_string():
    assert_column_conversion(types.Time(), graphene.String)


def test_should_string_convert_string():
    assert_column_conversion(types.String(), graphene.String)


def test_should_text_convert_string():
    assert_column_conversion(types.Text(), graphene.String)


def test_should_unicode_convert_string():
    assert_column_conversion(types.Unicode(), graphene.String)


def test_should_unicodetext_convert_string():
    assert_column_conversion(types.UnicodeText(), graphene.String)


def test_should_enum_convert_enum():
    field = assert_column_conversion(
        types.Enum(enum.Enum("one", "two")), graphene.Field
    )
    field_type = field.type()
    assert isinstance(field_type, graphene.Enum)
    assert hasattr(field_type, "two")
    field = assert_column_conversion(
        types.Enum("one", "two", name="two_numbers"), graphene.Field
    )
    field_type = field.type()
    assert field_type.__class__.__name__ == "two_numbers"
    assert isinstance(field_type, graphene.Enum)
    assert hasattr(field_type, "two")


def test_should_small_integer_convert_int():
    assert_column_conversion(types.SmallInteger(), graphene.Int)


def test_should_big_integer_convert_int():
    assert_column_conversion(types.BigInteger(), graphene.Float)


def test_should_integer_convert_int():
    assert_column_conversion(types.Integer(), graphene.Int)


def test_should_integer_convert_id():
    assert_column_conversion(types.Integer(), graphene.ID, primary_key=True)


def test_should_boolean_convert_boolean():
    assert_column_conversion(types.Boolean(), graphene.Boolean)


def test_should_float_convert_float():
    assert_column_conversion(types.Float(), graphene.Float)


def test_should_numeric_convert_float():
    assert_column_conversion(types.Numeric(), graphene.Float)


def test_should_label_convert_string():
    label = Label("label_test", case([], else_="foo"), type_=types.Unicode())
    graphene_type = convert_sqlalchemy_column(label)
    assert isinstance(graphene_type, graphene.String)


def test_should_label_convert_int():
    label = Label("int_label_test", case([], else_="foo"), type_=types.Integer())
    graphene_type = convert_sqlalchemy_column(label)
    assert isinstance(graphene_type, graphene.Int)


def test_should_choice_convert_enum():
    TYPES = [(u"es", u"Spanish"), (u"en", u"English")]
    column = Column(ChoiceType(TYPES), doc="Language", name="language")
    Base = declarative_base()

    Table("translatedmodel", Base.metadata, column)
    graphene_type = convert_sqlalchemy_column(column)
    assert issubclass(graphene_type, graphene.Enum)
    assert graphene_type._meta.name == "TRANSLATEDMODEL_LANGUAGE"
    assert graphene_type._meta.description == "Language"
    assert graphene_type._meta.enum.__members__["es"].value == "Spanish"
    assert graphene_type._meta.enum.__members__["en"].value == "English"


def test_should_columproperty_convert():

    Base = declarative_base()

    class Test(Base):
        __tablename__ = "test"
        id = Column(types.Integer, primary_key=True)
        column = column_property(
            select([func.sum(func.cast(id, types.Integer))]).where(id == 1)
        )

    graphene_type = convert_sqlalchemy_column(Test.column)
    assert not graphene_type.kwargs["required"]


def test_should_scalar_list_convert_list():
    assert_column_conversion(ScalarListType(), graphene.List)


def test_should_jsontype_convert_jsonstring():
    assert_column_conversion(JSONType(), JSONString)


def test_should_manytomany_convert_connectionorlist():
    registry = Registry()
    dynamic_field = convert_sqlalchemy_relationship(
        Reporter.pets.property, registry, default_connection_field_factory
    )
    assert isinstance(dynamic_field, graphene.Dynamic)
    assert not dynamic_field.get_type()


def test_should_manytomany_convert_connectionorlist_list():
    class A(SQLAlchemyObjectType):
        class Meta:
            model = Pet

    dynamic_field = convert_sqlalchemy_relationship(
        Reporter.pets.property, A._meta.registry, default_connection_field_factory
    )
    assert isinstance(dynamic_field, graphene.Dynamic)
    graphene_type = dynamic_field.get_type()
    assert isinstance(graphene_type, graphene.Field)
    assert isinstance(graphene_type.type, graphene.List)
    assert graphene_type.type.of_type == A


def test_should_manytomany_convert_connectionorlist_connection():
    class A(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            interfaces = (Node,)

    dynamic_field = convert_sqlalchemy_relationship(
        Reporter.pets.property, A._meta.registry, default_connection_field_factory
    )
    assert isinstance(dynamic_field, graphene.Dynamic)
    assert isinstance(dynamic_field.get_type(), UnsortedSQLAlchemyConnectionField)


def test_should_manytoone_convert_connectionorlist():
    registry = Registry()
    dynamic_field = convert_sqlalchemy_relationship(
        Article.reporter.property, registry, default_connection_field_factory
    )
    assert isinstance(dynamic_field, graphene.Dynamic)
    assert not dynamic_field.get_type()


def test_should_manytoone_convert_connectionorlist_list():
    class A(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

    dynamic_field = convert_sqlalchemy_relationship(
        Article.reporter.property, A._meta.registry, default_connection_field_factory
    )
    assert isinstance(dynamic_field, graphene.Dynamic)
    graphene_type = dynamic_field.get_type()
    assert isinstance(graphene_type, graphene.Field)
    assert graphene_type.type == A


def test_should_manytoone_convert_connectionorlist_connection():
    class A(SQLAlchemyObjectType):
        class Meta:
            model = Reporter
            interfaces = (Node,)

    dynamic_field = convert_sqlalchemy_relationship(
        Article.reporter.property, A._meta.registry, default_connection_field_factory
    )
    assert isinstance(dynamic_field, graphene.Dynamic)
    graphene_type = dynamic_field.get_type()
    assert isinstance(graphene_type, graphene.Field)
    assert graphene_type.type == A


def test_should_onetoone_convert_field():
    class A(SQLAlchemyObjectType):
        class Meta:
            model = Article
            interfaces = (Node,)

    dynamic_field = convert_sqlalchemy_relationship(
        Reporter.favorite_article.property, A._meta.registry, default_connection_field_factory
    )
    assert isinstance(dynamic_field, graphene.Dynamic)
    graphene_type = dynamic_field.get_type()
    assert isinstance(graphene_type, graphene.Field)
    assert graphene_type.type == A


def test_should_postgresql_uuid_convert():
    assert_column_conversion(postgresql.UUID(), graphene.String)


def test_should_postgresql_enum_convert():
    field = assert_column_conversion(
        postgresql.ENUM("one", "two", name="two_numbers"), graphene.Field
    )
    field_type = field.type()
    assert field_type.__class__.__name__ == "two_numbers"
    assert isinstance(field_type, graphene.Enum)
    assert hasattr(field_type, "two")


def test_should_postgresql_py_enum_convert():
    field = assert_column_conversion(
        postgresql.ENUM(enum.Enum("TwoNumbers", "one two"), name="two_numbers"), graphene.Field
    )
    field_type = field.type()
    assert field_type.__class__.__name__ == "TwoNumbers"
    assert isinstance(field_type, graphene.Enum)
    assert hasattr(field_type, "two")


def test_should_postgresql_array_convert():
    assert_column_conversion(postgresql.ARRAY(types.Integer), graphene.List)


def test_should_postgresql_json_convert():
    assert_column_conversion(postgresql.JSON(), JSONString)


def test_should_postgresql_jsonb_convert():
    assert_column_conversion(postgresql.JSONB(), JSONString)


def test_should_postgresql_hstore_convert():
    assert_column_conversion(postgresql.HSTORE(), JSONString)


def test_should_composite_convert():
    class CompositeClass(object):
        def __init__(self, col1, col2):
            self.col1 = col1
            self.col2 = col2

    registry = Registry()

    @convert_sqlalchemy_composite.register(CompositeClass, registry)
    def convert_composite_class(composite, registry):
        return graphene.String(description=composite.doc)

    assert_composite_conversion(
        CompositeClass,
        (Column(types.Unicode(50)), Column(types.Unicode(50))),
        graphene.String,
        registry,
    )


def test_should_unknown_sqlalchemy_composite_raise_exception():
    registry = Registry()

    with raises(Exception) as excinfo:

        class CompositeClass(object):
            def __init__(self, col1, col2):
                self.col1 = col1
                self.col2 = col2

        assert_composite_conversion(
            CompositeClass,
            (Column(types.Unicode(50)), Column(types.Unicode(50))),
            graphene.String,
            registry,
        )

    assert "Don't know how to convert the composite field" in str(excinfo.value)
