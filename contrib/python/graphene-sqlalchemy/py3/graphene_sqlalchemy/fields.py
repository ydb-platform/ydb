import enum
import warnings
from functools import partial

from promise import Promise, is_thenable
from sqlalchemy.orm.query import Query

from graphene import NonNull
from graphene.relay import Connection, ConnectionField
from graphene.relay.connection import connection_adapter, page_info_adapter
from graphql_relay import connection_from_array_slice

from .batching import get_batch_resolver
from .utils import EnumValue, get_query


class UnsortedSQLAlchemyConnectionField(ConnectionField):
    @property
    def type(self):
        from .types import SQLAlchemyObjectType

        type_ = super(ConnectionField, self).type
        nullable_type = get_nullable_type(type_)
        if issubclass(nullable_type, Connection):
            return type_
        assert issubclass(nullable_type, SQLAlchemyObjectType), (
            "SQLALchemyConnectionField only accepts SQLAlchemyObjectType types, not {}"
        ).format(nullable_type.__name__)
        assert (
            nullable_type.connection
        ), "The type {} doesn't have a connection".format(
            nullable_type.__name__
        )
        assert type_ == nullable_type, (
            "Passing a SQLAlchemyObjectType instance is deprecated. "
            "Pass the connection type instead accessible via SQLAlchemyObjectType.connection"
        )
        return nullable_type.connection

    @property
    def model(self):
        return get_nullable_type(self.type)._meta.node._meta.model

    @classmethod
    def get_query(cls, model, info, **args):
        return get_query(model, info.context)

    @classmethod
    def resolve_connection(cls, connection_type, model, info, args, resolved):
        if resolved is None:
            resolved = cls.get_query(model, info, **args)
        if isinstance(resolved, Query):
            _len = resolved.count()
        else:
            _len = len(resolved)

        def adjusted_connection_adapter(edges, pageInfo):
            return connection_adapter(connection_type, edges, pageInfo)

        connection = connection_from_array_slice(
            array_slice=resolved,
            args=args,
            slice_start=0,
            array_length=_len,
            array_slice_length=_len,
            connection_type=adjusted_connection_adapter,
            edge_type=connection_type.Edge,
            page_info_type=page_info_adapter,
        )
        connection.iterable = resolved
        connection.length = _len
        return connection

    @classmethod
    def connection_resolver(cls, resolver, connection_type, model, root, info, **args):
        resolved = resolver(root, info, **args)

        on_resolve = partial(cls.resolve_connection, connection_type, model, info, args)
        if is_thenable(resolved):
            return Promise.resolve(resolved).then(on_resolve)

        return on_resolve(resolved)

    def wrap_resolve(self, parent_resolver):
        return partial(
            self.connection_resolver,
            parent_resolver,
            get_nullable_type(self.type),
            self.model,
        )


# TODO Rename this to SortableSQLAlchemyConnectionField
class SQLAlchemyConnectionField(UnsortedSQLAlchemyConnectionField):
    def __init__(self, type_, *args, **kwargs):
        nullable_type = get_nullable_type(type_)
        if "sort" not in kwargs and issubclass(nullable_type, Connection):
            # Let super class raise if type is not a Connection
            try:
                kwargs.setdefault("sort", nullable_type.Edge.node._type.sort_argument())
            except (AttributeError, TypeError):
                raise TypeError(
                    'Cannot create sort argument for {}. A model is required. Set the "sort" argument'
                    " to None to disabling the creation of the sort query argument".format(
                        nullable_type.__name__
                    )
                )
        elif "sort" in kwargs and kwargs["sort"] is None:
            del kwargs["sort"]
        super(SQLAlchemyConnectionField, self).__init__(type_, *args, **kwargs)

    @classmethod
    def get_query(cls, model, info, sort=None, **args):
        query = get_query(model, info.context)
        if sort is not None:
            if not isinstance(sort, list):
                sort = [sort]
            sort_args = []
            # ensure consistent handling of graphene Enums, enum values and
            # plain strings
            for item in sort:
                if isinstance(item, enum.Enum):
                    sort_args.append(item.value.value)
                elif isinstance(item, EnumValue):
                    sort_args.append(item.value)
                else:
                    sort_args.append(item)
            query = query.order_by(*sort_args)
        return query


class BatchSQLAlchemyConnectionField(UnsortedSQLAlchemyConnectionField):
    """
    This is currently experimental.
    The API and behavior may change in future versions.
    Use at your own risk.
    """

    def wrap_resolve(self, parent_resolver):
        return partial(
            self.connection_resolver,
            self.resolver,
            get_nullable_type(self.type),
            self.model,
        )

    @classmethod
    def from_relationship(cls, relationship, registry, **field_kwargs):
        model = relationship.mapper.entity
        model_type = registry.get_type_for_model(model)
        return cls(model_type.connection, resolver=get_batch_resolver(relationship), **field_kwargs)


def default_connection_field_factory(relationship, registry, **field_kwargs):
    model = relationship.mapper.entity
    model_type = registry.get_type_for_model(model)
    return __connectionFactory(model_type, **field_kwargs)


# TODO Remove in next major version
__connectionFactory = UnsortedSQLAlchemyConnectionField


def createConnectionField(type_, **field_kwargs):
    warnings.warn(
        'createConnectionField is deprecated and will be removed in the next '
        'major version. Use SQLAlchemyObjectType.Meta.connection_field_factory instead.',
        DeprecationWarning,
    )
    return __connectionFactory(type_, **field_kwargs)


def registerConnectionFieldFactory(factoryMethod):
    warnings.warn(
        'registerConnectionFieldFactory is deprecated and will be removed in the next '
        'major version. Use SQLAlchemyObjectType.Meta.connection_field_factory instead.',
        DeprecationWarning,
    )
    global __connectionFactory
    __connectionFactory = factoryMethod


def unregisterConnectionFieldFactory():
    warnings.warn(
        'registerConnectionFieldFactory is deprecated and will be removed in the next '
        'major version. Use SQLAlchemyObjectType.Meta.connection_field_factory instead.',
        DeprecationWarning,
    )
    global __connectionFactory
    __connectionFactory = UnsortedSQLAlchemyConnectionField


def get_nullable_type(_type):
    if isinstance(_type, NonNull):
        return _type.of_type
    return _type
