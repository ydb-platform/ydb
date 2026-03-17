import enum
import sys
from typing import Dict, Union

import pytest
import sqlalchemy_utils as sqa_utils
from sqlalchemy import Column, func, select, types
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import column_property, composite

import graphene
from graphene.relay import Node
from graphene.types.structures import Structure

from graphene_sqlalchemy.converter import (convert_sqlalchemy_column,
                         convert_sqlalchemy_composite,
                         convert_sqlalchemy_hybrid_method,
                         convert_sqlalchemy_relationship)
from graphene_sqlalchemy.fields import (UnsortedSQLAlchemyConnectionField,
                      default_connection_field_factory)
from graphene_sqlalchemy.registry import Registry, get_global_registry
from graphene_sqlalchemy.types import ORMField, SQLAlchemyObjectType
from .models import (Article, CompositeFullName, Pet, Reporter, ShoppingCart,
                     ShoppingCartItem)


def mock_resolver():
    pass


def get_field(sqlalchemy_type, **column_kwargs):
    class Model(declarative_base()):
        __tablename__ = 'model'
        id_ = Column(types.Integer, primary_key=True)
        column = Column(sqlalchemy_type, doc="Custom Help Text", **column_kwargs)

    column_prop = inspect(Model).column_attrs['column']
    return convert_sqlalchemy_column(column_prop, get_global_registry(), mock_resolver)


def get_field_from_column(column_):
    class Model(declarative_base()):
        __tablename__ = 'model'
        id_ = Column(types.Integer, primary_key=True)
        column = column_

    column_prop = inspect(Model).column_attrs['column']
    return convert_sqlalchemy_column(column_prop, get_global_registry(), mock_resolver)


def get_hybrid_property_type(prop_method):
    class Model(declarative_base()):
        __tablename__ = 'model'
        id_ = Column(types.Integer, primary_key=True)
        prop = prop_method

    column_prop = inspect(Model).all_orm_descriptors['prop']
    return convert_sqlalchemy_hybrid_method(column_prop, mock_resolver(), **ORMField().kwargs)


def test_hybrid_prop_int():
    @hybrid_property
    def prop_method() -> int:
        return 42

    assert get_hybrid_property_type(prop_method).type == graphene.Int


@pytest.mark.skipif(sys.version_info < (3, 10), reason="|-Style Unions are unsupported in python < 3.10")
def test_hybrid_prop_scalar_union_310():
    @hybrid_property
    def prop_method() -> int | str:
        return "not allowed in gql schema"

    with pytest.raises(ValueError,
                       match=r"Cannot convert hybrid_property Union to "
                             r"graphene.Union: the Union contains scalars. \.*"):
        get_hybrid_property_type(prop_method)


@pytest.mark.skipif(sys.version_info < (3, 10), reason="|-Style Unions are unsupported in python < 3.10")
def test_hybrid_prop_scalar_union_and_optional_310():
    """Checks if the use of Optionals does not interfere with non-conform scalar return types"""

    @hybrid_property
    def prop_method() -> int | None:
        return 42

    assert get_hybrid_property_type(prop_method).type == graphene.Int


@pytest.mark.skipif(sys.version_info < (3, 10), reason="|-Style Unions are unsupported in python < 3.10")
def test_should_union_work_310():
    reg = Registry()

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            registry = reg

    class ShoppingCartType(SQLAlchemyObjectType):
        class Meta:
            model = ShoppingCartItem
            registry = reg

    @hybrid_property
    def prop_method() -> Union[PetType, ShoppingCartType]:
        return None

    @hybrid_property
    def prop_method_2() -> Union[ShoppingCartType, PetType]:
        return None

    field_type_1 = get_hybrid_property_type(prop_method).type
    field_type_2 = get_hybrid_property_type(prop_method_2).type

    assert isinstance(field_type_1, graphene.Union)
    assert field_type_1 is field_type_2

    # TODO verify types of the union


@pytest.mark.skipif(sys.version_info < (3, 10), reason="|-Style Unions are unsupported in python < 3.10")
def test_should_union_work_310():
    reg = Registry()

    class PetType(SQLAlchemyObjectType):
        class Meta:
            model = Pet
            registry = reg

    class ShoppingCartType(SQLAlchemyObjectType):
        class Meta:
            model = ShoppingCartItem
            registry = reg

    @hybrid_property
    def prop_method() -> PetType | ShoppingCartType:
        return None

    @hybrid_property
    def prop_method_2() -> ShoppingCartType | PetType:
        return None

    field_type_1 = get_hybrid_property_type(prop_method).type
    field_type_2 = get_hybrid_property_type(prop_method_2).type

    assert isinstance(field_type_1, graphene.Union)
    assert field_type_1 is field_type_2


def test_should_datetime_convert_datetime():
    assert get_field(types.DateTime()).type == graphene.DateTime


def test_should_time_convert_time():
    assert get_field(types.Time()).type == graphene.Time


def test_should_date_convert_date():
    assert get_field(types.Date()).type == graphene.Date


def test_should_string_convert_string():
    assert get_field(types.String()).type == graphene.String


def test_should_text_convert_string():
    assert get_field(types.Text()).type == graphene.String


def test_should_unicode_convert_string():
    assert get_field(types.Unicode()).type == graphene.String


def test_should_unicodetext_convert_string():
    assert get_field(types.UnicodeText()).type == graphene.String


def test_should_tsvector_convert_string():
    assert get_field(sqa_utils.TSVectorType()).type == graphene.String


def test_should_email_convert_string():
    assert get_field(sqa_utils.EmailType()).type == graphene.String


def test_should_URL_convert_string():
    assert get_field(sqa_utils.URLType()).type == graphene.String


def test_should_IPaddress_convert_string():
    assert get_field(sqa_utils.IPAddressType()).type == graphene.String


def test_should_inet_convert_string():
    assert get_field(postgresql.INET()).type == graphene.String


def test_should_cidr_convert_string():
    assert get_field(postgresql.CIDR()).type == graphene.String


def test_should_enum_convert_enum():
    field = get_field(types.Enum(enum.Enum("TwoNumbers", ("one", "two"))))
    field_type = field.type()
    assert isinstance(field_type, graphene.Enum)
    assert field_type._meta.name == "TwoNumbers"
    assert hasattr(field_type, "ONE")
    assert not hasattr(field_type, "one")
    assert hasattr(field_type, "TWO")
    assert not hasattr(field_type, "two")

    field = get_field(types.Enum("one", "two", name="two_numbers"))
    field_type = field.type()
    assert isinstance(field_type, graphene.Enum)
    assert field_type._meta.name == "TwoNumbers"
    assert hasattr(field_type, "ONE")
    assert not hasattr(field_type, "one")
    assert hasattr(field_type, "TWO")
    assert not hasattr(field_type, "two")


def test_should_not_enum_convert_enum_without_name():
    field = get_field(types.Enum("one", "two"))
    re_err = r"No type name specified for Enum\('one', 'two'\)"
    with pytest.raises(TypeError, match=re_err):
        field.type()


def test_should_small_integer_convert_int():
    assert get_field(types.SmallInteger()).type == graphene.Int


def test_should_big_integer_convert_int():
    assert get_field(types.BigInteger()).type == graphene.Float


def test_should_integer_convert_int():
    assert get_field(types.Integer()).type == graphene.Int


def test_should_primary_integer_convert_id():
    assert get_field(types.Integer(), primary_key=True).type == graphene.NonNull(graphene.ID)


def test_should_boolean_convert_boolean():
    assert get_field(types.Boolean()).type == graphene.Boolean


def test_should_float_convert_float():
    assert get_field(types.Float()).type == graphene.Float


def test_should_numeric_convert_float():
    assert get_field(types.Numeric()).type == graphene.Float


def test_should_choice_convert_enum():
    field = get_field(sqa_utils.ChoiceType([(u"es", u"Spanish"), (u"en", u"English")]))
    graphene_type = field.type
    assert issubclass(graphene_type, graphene.Enum)
    assert graphene_type._meta.name == "MODEL_COLUMN"
    assert graphene_type._meta.enum.__members__["es"].value == "Spanish"
    assert graphene_type._meta.enum.__members__["en"].value == "English"


def test_should_enum_choice_convert_enum():
    class TestEnum(enum.Enum):
        es = u"Spanish"
        en = u"English"

    field = get_field(sqa_utils.ChoiceType(TestEnum, impl=types.String()))
    graphene_type = field.type
    assert issubclass(graphene_type, graphene.Enum)
    assert graphene_type._meta.name == "MODEL_COLUMN"
    assert graphene_type._meta.enum.__members__["es"].value == "Spanish"
    assert graphene_type._meta.enum.__members__["en"].value == "English"


def test_choice_enum_column_key_name_issue_301():
    """
    Verifies that the sort enum name is generated from the column key instead of the name,
    in case the column has an invalid enum name. See #330
    """

    class TestEnum(enum.Enum):
        es = u"Spanish"
        en = u"English"

    testChoice = Column("% descuento1", sqa_utils.ChoiceType(TestEnum, impl=types.String()), key="descuento1")
    field = get_field_from_column(testChoice)

    graphene_type = field.type
    assert issubclass(graphene_type, graphene.Enum)
    assert graphene_type._meta.name == "MODEL_DESCUENTO1"
    assert graphene_type._meta.enum.__members__["es"].value == "Spanish"
    assert graphene_type._meta.enum.__members__["en"].value == "English"


def test_should_intenum_choice_convert_enum():
    class TestEnum(enum.IntEnum):
        one = 1
        two = 2

    field = get_field(sqa_utils.ChoiceType(TestEnum, impl=types.String()))
    graphene_type = field.type
    assert issubclass(graphene_type, graphene.Enum)
    assert graphene_type._meta.name == "MODEL_COLUMN"
    assert graphene_type._meta.enum.__members__["one"].value == 1
    assert graphene_type._meta.enum.__members__["two"].value == 2


def test_should_columproperty_convert():
    field = get_field_from_column(column_property(
        select([func.sum(func.cast(id, types.Integer))]).where(id == 1)
    ))

    assert field.type == graphene.Int


def test_should_scalar_list_convert_list():
    field = get_field(sqa_utils.ScalarListType())
    assert isinstance(field.type, graphene.List)
    assert field.type.of_type == graphene.String


def test_should_jsontype_convert_jsonstring():
    assert get_field(sqa_utils.JSONType()).type == graphene.JSONString
    assert get_field(types.JSON).type == graphene.JSONString


def test_should_variant_int_convert_int():
    assert get_field(types.Variant(types.Integer(), {})).type == graphene.Int


def test_should_variant_string_convert_string():
    assert get_field(types.Variant(types.String(), {})).type == graphene.String


def test_should_manytomany_convert_connectionorlist():
    class A(SQLAlchemyObjectType):
        class Meta:
            model = Article

    dynamic_field = convert_sqlalchemy_relationship(
        Reporter.pets.property, A, default_connection_field_factory, True, 'orm_field_name',
    )
    assert isinstance(dynamic_field, graphene.Dynamic)
    assert not dynamic_field.get_type()


def test_should_manytomany_convert_connectionorlist_list():
    class A(SQLAlchemyObjectType):
        class Meta:
            model = Pet

    dynamic_field = convert_sqlalchemy_relationship(
        Reporter.pets.property, A, default_connection_field_factory, True, 'orm_field_name',
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
        Reporter.pets.property, A, default_connection_field_factory, True, 'orm_field_name',
    )
    assert isinstance(dynamic_field, graphene.Dynamic)
    assert isinstance(dynamic_field.get_type(), UnsortedSQLAlchemyConnectionField)


def test_should_manytoone_convert_connectionorlist():
    class A(SQLAlchemyObjectType):
        class Meta:
            model = Article

    dynamic_field = convert_sqlalchemy_relationship(
        Reporter.pets.property, A, default_connection_field_factory, True, 'orm_field_name',
    )
    assert isinstance(dynamic_field, graphene.Dynamic)
    assert not dynamic_field.get_type()


def test_should_manytoone_convert_connectionorlist_list():
    class A(SQLAlchemyObjectType):
        class Meta:
            model = Reporter

    dynamic_field = convert_sqlalchemy_relationship(
        Article.reporter.property, A, default_connection_field_factory, True, 'orm_field_name',
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
        Article.reporter.property, A, default_connection_field_factory, True, 'orm_field_name',
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
        Reporter.favorite_article.property, A, default_connection_field_factory, True, 'orm_field_name',
    )
    assert isinstance(dynamic_field, graphene.Dynamic)
    graphene_type = dynamic_field.get_type()
    assert isinstance(graphene_type, graphene.Field)
    assert graphene_type.type == A


def test_should_postgresql_uuid_convert():
    assert get_field(postgresql.UUID()).type == graphene.UUID


def test_should_sqlalchemy_utils_uuid_convert():
    assert get_field(sqa_utils.UUIDType()).type == graphene.UUID


def test_should_postgresql_enum_convert():
    field = get_field(postgresql.ENUM("one", "two", name="two_numbers"))
    field_type = field.type()
    assert isinstance(field_type, graphene.Enum)
    assert field_type._meta.name == "TwoNumbers"
    assert hasattr(field_type, "ONE")
    assert not hasattr(field_type, "one")
    assert hasattr(field_type, "TWO")
    assert not hasattr(field_type, "two")


def test_should_postgresql_py_enum_convert():
    field = get_field(postgresql.ENUM(enum.Enum("TwoNumbers", "one two"), name="two_numbers"))
    field_type = field.type()
    assert field_type._meta.name == "TwoNumbers"
    assert isinstance(field_type, graphene.Enum)
    assert hasattr(field_type, "ONE")
    assert not hasattr(field_type, "one")
    assert hasattr(field_type, "TWO")
    assert not hasattr(field_type, "two")


def test_should_postgresql_array_convert():
    field = get_field(postgresql.ARRAY(types.Integer))
    assert isinstance(field.type, graphene.List)
    assert field.type.of_type == graphene.Int


def test_should_array_convert():
    field = get_field(types.ARRAY(types.Integer))
    assert isinstance(field.type, graphene.List)
    assert field.type.of_type == graphene.Int


def test_should_2d_array_convert():
    field = get_field(types.ARRAY(types.Integer, dimensions=2))
    assert isinstance(field.type, graphene.List)
    assert isinstance(field.type.of_type, graphene.List)
    assert field.type.of_type.of_type == graphene.Int


def test_should_3d_array_convert():
    field = get_field(types.ARRAY(types.Integer, dimensions=3))
    assert isinstance(field.type, graphene.List)
    assert isinstance(field.type.of_type, graphene.List)
    assert isinstance(field.type.of_type.of_type, graphene.List)
    assert field.type.of_type.of_type.of_type == graphene.Int


def test_should_postgresql_json_convert():
    assert get_field(postgresql.JSON()).type == graphene.JSONString


def test_should_postgresql_jsonb_convert():
    assert get_field(postgresql.JSONB()).type == graphene.JSONString


def test_should_postgresql_hstore_convert():
    assert get_field(postgresql.HSTORE()).type == graphene.JSONString


def test_should_composite_convert():
    registry = Registry()

    class CompositeClass:
        def __init__(self, col1, col2):
            self.col1 = col1
            self.col2 = col2

    @convert_sqlalchemy_composite.register(CompositeClass, registry)
    def convert_composite_class(composite, registry):
        return graphene.String(description=composite.doc)

    field = convert_sqlalchemy_composite(
        composite(CompositeClass, (Column(types.Unicode(50)), Column(types.Unicode(50))), doc="Custom Help Text"),
        registry,
        mock_resolver,
    )
    assert isinstance(field, graphene.String)


def test_should_unknown_sqlalchemy_composite_raise_exception():
    class CompositeClass:
        def __init__(self, col1, col2):
            self.col1 = col1
            self.col2 = col2

    re_err = "Don't know how to convert the composite field"
    with pytest.raises(Exception, match=re_err):
        convert_sqlalchemy_composite(
            composite(CompositeFullName, (Column(types.Unicode(50)), Column(types.Unicode(50)))),
            Registry(),
            mock_resolver,
        )


def test_sqlalchemy_hybrid_property_type_inference():
    class ShoppingCartItemType(SQLAlchemyObjectType):
        class Meta:
            model = ShoppingCartItem
            interfaces = (Node,)

    class ShoppingCartType(SQLAlchemyObjectType):
        class Meta:
            model = ShoppingCart
            interfaces = (Node,)

    #######################################################
    # Check ShoppingCartItem's Properties and Return Types
    #######################################################

    shopping_cart_item_expected_types: Dict[str, Union[graphene.Scalar, Structure]] = {
        'hybrid_prop_shopping_cart': graphene.List(ShoppingCartType)
    }

    assert sorted(list(ShoppingCartItemType._meta.fields.keys())) == sorted([
        # Columns
        "id",
        # Append Hybrid Properties from Above
        *shopping_cart_item_expected_types.keys()
    ])

    for hybrid_prop_name, hybrid_prop_expected_return_type in shopping_cart_item_expected_types.items():
        hybrid_prop_field = ShoppingCartItemType._meta.fields[hybrid_prop_name]

        # this is a simple way of showing the failed property name
        # instead of having to unroll the loop.
        assert (hybrid_prop_name, str(hybrid_prop_field.type)) == (
            hybrid_prop_name,
            str(hybrid_prop_expected_return_type),
        )
        assert hybrid_prop_field.description is None  # "doc" is ignored by hybrid property

    ###################################################
    # Check ShoppingCart's Properties and Return Types
    ###################################################

    shopping_cart_expected_types: Dict[str, Union[graphene.Scalar, Structure]] = {
        # Basic types
        "hybrid_prop_str": graphene.String,
        "hybrid_prop_int": graphene.Int,
        "hybrid_prop_float": graphene.Float,
        "hybrid_prop_bool": graphene.Boolean,
        "hybrid_prop_decimal": graphene.String,  # Decimals should be serialized Strings
        "hybrid_prop_date": graphene.Date,
        "hybrid_prop_time": graphene.Time,
        "hybrid_prop_datetime": graphene.DateTime,
        # Lists and Nested Lists
        "hybrid_prop_list_int": graphene.List(graphene.Int),
        "hybrid_prop_list_date": graphene.List(graphene.Date),
        "hybrid_prop_nested_list_int": graphene.List(graphene.List(graphene.Int)),
        "hybrid_prop_deeply_nested_list_int": graphene.List(graphene.List(graphene.List(graphene.Int))),
        "hybrid_prop_first_shopping_cart_item": ShoppingCartItemType,
        "hybrid_prop_shopping_cart_item_list": graphene.List(ShoppingCartItemType),
        "hybrid_prop_unsupported_type_tuple": graphene.String,
        # Self Referential List
        "hybrid_prop_self_referential": ShoppingCartType,
        "hybrid_prop_self_referential_list": graphene.List(ShoppingCartType),
        # Optionals
        "hybrid_prop_optional_self_referential": ShoppingCartType,
    }

    assert sorted(list(ShoppingCartType._meta.fields.keys())) == sorted([
        # Columns
        "id",
        # Append Hybrid Properties from Above
        *shopping_cart_expected_types.keys()
    ])

    for hybrid_prop_name, hybrid_prop_expected_return_type in shopping_cart_expected_types.items():
        hybrid_prop_field = ShoppingCartType._meta.fields[hybrid_prop_name]

        # this is a simple way of showing the failed property name
        # instead of having to unroll the loop.
        assert (hybrid_prop_name, str(hybrid_prop_field.type)) == (
            hybrid_prop_name,
            str(hybrid_prop_expected_return_type),
        )
        assert hybrid_prop_field.description is None  # "doc" is ignored by hybrid property
