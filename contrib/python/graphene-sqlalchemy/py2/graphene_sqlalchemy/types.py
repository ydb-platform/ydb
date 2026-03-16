import six
from collections import OrderedDict

import sqlalchemy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.inspection import inspect as sqlalchemyinspect
from sqlalchemy.orm.exc import NoResultFound

from graphene import Field, List, String  # , annotate, ResolveInfo
from graphene.relay import Connection, Node
from graphene.types.objecttype import ObjectType, ObjectTypeOptions
from graphene.types.utils import yank_fields_from_attrs

from .converter import (convert_sqlalchemy_column,
                        convert_sqlalchemy_composite,
                        convert_sqlalchemy_hybrid_method,
                        convert_sqlalchemy_relationship)
from .fields import default_connection_field_factory
from .registry import Registry, get_global_registry
from .utils import get_operator_function, get_query, get_snake_or_camel_attr, is_mapped_class, is_mapped_instance


def construct_fields(model, registry, only_fields, exclude_fields, connection_field_factory):
    inspected_model = sqlalchemyinspect(model)

    fields = OrderedDict()

    for name, column in inspected_model.columns.items():
        is_not_in_only = only_fields and name not in only_fields
        # is_already_created = name in options.fields
        is_excluded = name in exclude_fields  # or is_already_created
        if is_not_in_only or is_excluded:
            # We skip this field if we specify only_fields and is not
            # in there. Or when we exclude this field in exclude_fields
            continue
        converted_column = convert_sqlalchemy_column(column, registry)
        fields[name] = converted_column

    for name, composite in inspected_model.composites.items():
        is_not_in_only = only_fields and name not in only_fields
        # is_already_created = name in options.fields
        is_excluded = name in exclude_fields  # or is_already_created
        if is_not_in_only or is_excluded:
            # We skip this field if we specify only_fields and is not
            # in there. Or when we exclude this field in exclude_fields
            continue
        converted_composite = convert_sqlalchemy_composite(composite, registry)
        fields[name] = converted_composite

    for hybrid_item in inspected_model.all_orm_descriptors:

        if type(hybrid_item) == hybrid_property:
            name = hybrid_item.__name__

            is_not_in_only = only_fields and name not in only_fields
            # is_already_created = name in options.fields
            is_excluded = name in exclude_fields  # or is_already_created

            if is_not_in_only or is_excluded:
                # We skip this field if we specify only_fields and is not
                # in there. Or when we exclude this field in exclude_fields
                continue

            converted_hybrid_property = convert_sqlalchemy_hybrid_method(hybrid_item)
            fields[name] = converted_hybrid_property

    # Get all the columns for the relationships on the model
    for relationship in inspected_model.relationships:
        is_not_in_only = only_fields and relationship.key not in only_fields
        # is_already_created = relationship.key in options.fields
        is_excluded = relationship.key in exclude_fields  # or is_already_created
        if is_not_in_only or is_excluded:
            # We skip this field if we specify only_fields and is not
            # in there. Or when we exclude this field in exclude_fields
            continue
        converted_relationship = convert_sqlalchemy_relationship(relationship, registry, connection_field_factory)
        name = relationship.key
        fields[name] = converted_relationship

    return fields


class SQLAlchemyObjectTypeOptions(ObjectTypeOptions):
    model = None  # type: sqlalchemy.Model
    registry = None  # type: sqlalchemy.Registry
    connection = None  # type: sqlalchemy.Type[sqlalchemy.Connection]
    id = None  # type: str


class SQLAlchemyObjectType(ObjectType):
    @classmethod
    def __init_subclass_with_meta__(
        cls,
        model=None,
        registry=None,
        skip_registry=False,
        only_fields=(),
        exclude_fields=(),
        connection=None,
        connection_class=None,
        use_connection=None,
        interfaces=(),
        id=None,
        connection_field_factory=default_connection_field_factory,
        _meta=None,
        **options
    ):
        assert is_mapped_class(model), (
            "You need to pass a valid SQLAlchemy Model in " '{}.Meta, received "{}".'
        ).format(cls.__name__, model)

        if not registry:
            registry = get_global_registry()

        assert isinstance(registry, Registry), (
            "The attribute registry in {} needs to be an instance of "
            'Registry, received "{}".'
        ).format(cls.__name__, registry)

        sqla_fields = yank_fields_from_attrs(
            construct_fields(
                model=model,
                registry=registry,
                only_fields=only_fields,
                exclude_fields=exclude_fields,
                connection_field_factory=connection_field_factory
            ),
            _as=Field
        )

        if use_connection is None and interfaces:
            use_connection = any(
                (issubclass(interface, Node) for interface in interfaces)
            )

        if use_connection and not connection:
            # We create the connection automatically
            if not connection_class:
                connection_class = Connection

            connection = connection_class.create_type(
                "{}Connection".format(cls.__name__), node=cls
            )

        if connection is not None:
            assert issubclass(connection, Connection), (
                "The connection must be a Connection. Received {}"
            ).format(connection.__name__)

        if not _meta:
            _meta = SQLAlchemyObjectTypeOptions(cls)

        _meta.model = model
        _meta.registry = registry

        if _meta.fields:
            _meta.fields.update(sqla_fields)
        else:
            _meta.fields = sqla_fields

        _meta.connection = connection
        _meta.id = id or "id"

        super(SQLAlchemyObjectType, cls).__init_subclass_with_meta__(
            _meta=_meta, interfaces=interfaces, **options
        )

        if not skip_registry:
            registry.register(cls)

    @classmethod
    def is_type_of(cls, root, info):
        if isinstance(root, cls):
            return True
        if not is_mapped_instance(root):
            raise Exception(('Received incompatible instance "{}".').format(root))
        return isinstance(root, cls._meta.model)

    @classmethod
    def get_query(cls, info):
        model = cls._meta.model
        return get_query(model, info.context)

    @classmethod
    def get_node(cls, info, id):
        try:
            return cls.get_query(info).get(id)
        except NoResultFound:
            return None

    def resolve_id(self, info):
        # graphene_type = info.parent_type.graphene_type
        keys = self.__mapper__.primary_key_from_instance(self)
        return tuple(keys) if len(keys) > 1 else keys[0]


class SQLAlchemyList(List):
    def __init__(self, of_type, exclude_fields=(), include_fields=(), operator=None, order_by=(), *args, **kwargs):
        columns_dict = self.build_columns_dict(of_type)

        if include_fields:
            columns_dict = {k: columns_dict[k] for k in include_fields}
        for exclude_field in exclude_fields:
            if exclude_field in columns_dict.keys():
                del columns_dict[exclude_field]

        kwargs.update(**columns_dict)
        kwargs["operator"] = String(description="Operator to use for filtering")
        kwargs["order_by"] = List(String, description="Fields to use for results ordering")

        default_operator = get_operator_function(operator)
        if isinstance(order_by, six.string_types):
            order_by = (order_by,)
        default_order_by = order_by

        def filters_resolver(self, info, **kwargs):
            operator = default_operator
            if "operator" in kwargs:
                operator = get_operator_function(kwargs["operator"])

            query = of_type.get_query(info)

            for (k, v) in kwargs.items():
                if hasattr(of_type._meta.model, k):
                    query = query.filter(operator(get_snake_or_camel_attr(of_type._meta.model, k), v))

            order_by = default_order_by
            if "order_by" in kwargs:
                order_by = kwargs["order_by"]

            for order_by_item in order_by:
                if order_by_item.lower().endswith(" asc"):
                    order_by_item = order_by_item[:-len(" asc")]
                    query = query.order_by(get_snake_or_camel_attr(of_type._meta.model, order_by_item).asc())
                elif order_by_item.lower().endswith(" desc"):
                    order_by_item = order_by_item[:-len(" desc")]
                    query = query.order_by(get_snake_or_camel_attr(of_type._meta.model, order_by_item).desc())
                else:
                    query = query.order_by(get_snake_or_camel_attr(of_type._meta.model, order_by_item))

            query_transformer = getattr(info.parent_type.graphene_type, "query_" + info.field_name, False)
            if callable(query_transformer):
                transformed_query = query_transformer(info.parent_type.graphene_type(), info, query, **kwargs)
                if transformed_query:
                    query = transformed_query

            return query.all()

        kwargs["resolver"] = filters_resolver
        super(SQLAlchemyList, self).__init__(of_type, *args, **kwargs)

    @staticmethod
    def build_columns_dict(of_type):
        columns_dict = {}
        inspected_model = sqlalchemyinspect(of_type._meta.model)
        for column in inspected_model.columns:
            column.nullable = True  # Set nullable to false to build an optional graph type
            graphene_type = convert_sqlalchemy_column(column)
            columns_dict[column.name] = graphene_type
        return columns_dict

    def __eq__(self, other):
        return isinstance(other, SQLAlchemyList) and (
            self.of_type == other.of_type and
            self.args == other.args and
            self.kwargs == other.kwargs
        )
