from singledispatch import singledispatch
from sqlalchemy import types
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import interfaces

from graphene import (ID, Boolean, Dynamic, Enum, Field, Float, Int, List,
                      String)
from graphene.types.json import JSONString

try:
    from sqlalchemy_utils import ChoiceType, JSONType, ScalarListType, TSVectorType
except ImportError:
    ChoiceType = JSONType = ScalarListType = TSVectorType = object


def get_column_doc(column):
    return getattr(column, "doc", None)


def is_column_nullable(column):
    return bool(getattr(column, "nullable", True))


def convert_sqlalchemy_relationship(relationship, registry, connection_field_factory):
    direction = relationship.direction
    model = relationship.mapper.entity

    def dynamic_type():
        _type = registry.get_type_for_model(model)
        if not _type:
            return None
        if direction == interfaces.MANYTOONE or not relationship.uselist:
            return Field(_type)
        elif direction in (interfaces.ONETOMANY, interfaces.MANYTOMANY):
            if _type._meta.connection:
                return connection_field_factory(relationship, registry)
            return Field(List(_type))

    return Dynamic(dynamic_type)


def convert_sqlalchemy_hybrid_method(hybrid_item):
    return String(description=getattr(hybrid_item, "__doc__", None), required=False)


def convert_sqlalchemy_composite(composite, registry):
    converter = registry.get_converter_for_composite(composite.composite_class)
    if not converter:
        try:
            raise Exception(
                "Don't know how to convert the composite field %s (%s)"
                % (composite, composite.composite_class)
            )
        except AttributeError:
            # handle fields that are not attached to a class yet (don't have a parent)
            raise Exception(
                "Don't know how to convert the composite field %r (%s)"
                % (composite, composite.composite_class)
            )
    return converter(composite, registry)


def _register_composite_class(cls, registry=None):
    if registry is None:
        from .registry import get_global_registry

        registry = get_global_registry()

    def inner(fn):
        registry.register_composite_converter(cls, fn)

    return inner


convert_sqlalchemy_composite.register = _register_composite_class


def convert_sqlalchemy_column(column, registry=None):
    return convert_sqlalchemy_type(getattr(column, "type", None), column, registry)


@singledispatch
def convert_sqlalchemy_type(type, column, registry=None):
    raise Exception(
        "Don't know how to convert the SQLAlchemy field %s (%s)"
        % (column, column.__class__)
    )


@convert_sqlalchemy_type.register(types.Date)
@convert_sqlalchemy_type.register(types.Time)
@convert_sqlalchemy_type.register(types.String)
@convert_sqlalchemy_type.register(types.Text)
@convert_sqlalchemy_type.register(types.Unicode)
@convert_sqlalchemy_type.register(types.UnicodeText)
@convert_sqlalchemy_type.register(postgresql.UUID)
@convert_sqlalchemy_type.register(postgresql.INET)
@convert_sqlalchemy_type.register(postgresql.CIDR)
@convert_sqlalchemy_type.register(TSVectorType)
def convert_column_to_string(type, column, registry=None):
    return String(
        description=get_column_doc(column), required=not (is_column_nullable(column))
    )


@convert_sqlalchemy_type.register(types.DateTime)
def convert_column_to_datetime(type, column, registry=None):
    from graphene.types.datetime import DateTime

    return DateTime(
        description=get_column_doc(column), required=not (is_column_nullable(column))
    )


@convert_sqlalchemy_type.register(types.SmallInteger)
@convert_sqlalchemy_type.register(types.Integer)
def convert_column_to_int_or_id(type, column, registry=None):
    if column.primary_key:
        return ID(
            description=get_column_doc(column),
            required=not (is_column_nullable(column)),
        )
    else:
        return Int(
            description=get_column_doc(column),
            required=not (is_column_nullable(column)),
        )


@convert_sqlalchemy_type.register(types.Boolean)
def convert_column_to_boolean(type, column, registry=None):
    return Boolean(
        description=get_column_doc(column), required=not (is_column_nullable(column))
    )


@convert_sqlalchemy_type.register(types.Float)
@convert_sqlalchemy_type.register(types.Numeric)
@convert_sqlalchemy_type.register(types.BigInteger)
def convert_column_to_float(type, column, registry=None):
    return Float(
        description=get_column_doc(column), required=not (is_column_nullable(column))
    )


@convert_sqlalchemy_type.register(types.Enum)
def convert_enum_to_enum(type, column, registry=None):
    enum_class = getattr(type, 'enum_class', None)
    if enum_class:  # Check if an enum.Enum type is used
        graphene_type = Enum.from_enum(enum_class)
    else:  # Nope, just a list of string options
        items = zip(type.enums, type.enums)
        graphene_type = Enum(type.name, items)
    return Field(
        graphene_type,
        description=get_column_doc(column),
        required=not (is_column_nullable(column)),
    )


@convert_sqlalchemy_type.register(ChoiceType)
def convert_column_to_enum(type, column, registry=None):
    name = "{}_{}".format(column.table.name, column.name).upper()
    return Enum(name, type.choices, description=get_column_doc(column))


@convert_sqlalchemy_type.register(ScalarListType)
def convert_scalar_list_to_list(type, column, registry=None):
    return List(String, description=get_column_doc(column))


@convert_sqlalchemy_type.register(postgresql.ARRAY)
def convert_postgres_array_to_list(_type, column, registry=None):
    graphene_type = convert_sqlalchemy_type(column.type.item_type, column)
    inner_type = type(graphene_type)
    return List(
        inner_type,
        description=get_column_doc(column),
        required=not (is_column_nullable(column)),
    )


@convert_sqlalchemy_type.register(postgresql.HSTORE)
@convert_sqlalchemy_type.register(postgresql.JSON)
@convert_sqlalchemy_type.register(postgresql.JSONB)
def convert_json_to_string(type, column, registry=None):
    return JSONString(
        description=get_column_doc(column), required=not (is_column_nullable(column))
    )


@convert_sqlalchemy_type.register(JSONType)
def convert_json_type_to_string(type, column, registry=None):
    return JSONString(
        description=get_column_doc(column), required=not (is_column_nullable(column))
    )
