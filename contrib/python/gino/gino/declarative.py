import collections

import sqlalchemy as sa
from sqlalchemy.exc import InvalidRequestError

from . import json_support
from .exceptions import GinoException


class ColumnAttribute:
    """The type of the column wrapper attributes on GINO models.

    This is the core utility to enable GINO models so that:

    * Accessing a column attribute on a model class returns the column itself
    * Accessing a column attribute on a model instance returns the value for that column

    This utility is customizable by defining ``__attr_factory__`` in the model class.
    """

    def __init__(self, prop_name, column):
        self.prop_name = prop_name
        self.column = column

    def __get__(self, instance, owner):
        if instance is None:
            return self.column
        else:
            return instance.__values__.get(self.prop_name)

    def __set__(self, instance, value):
        instance.__values__[self.prop_name] = value

    def __delete__(self, instance):
        raise AttributeError("Cannot delete value.")


class InvertDict(dict):
    """A custom :class:`dict` that allows getting keys by values.

    Used internally by :class:`~Model`.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._inverted_dict = dict()
        for k, v in self.items():
            if v in self._inverted_dict:
                raise GinoException(
                    "Column name {} already maps to {}".format(
                        v, self._inverted_dict[v]
                    )
                )
            self._inverted_dict[v] = k

    def __setitem__(self, key, value):
        if value in self._inverted_dict and self._inverted_dict[value] != key:
            raise GinoException(
                "Column name {} already maps to {}".format(
                    value, self._inverted_dict[value]
                )
            )
        super().__setitem__(key, value)
        self._inverted_dict[value] = key

    def invert_get(self, value, default=None):
        """Get key by value.

        :param value: A value in this dict.
        :param default: If specified value doesn't exist, return default.
        :return: The corresponding key if the value is found, or default otherwise.
        """
        return self._inverted_dict.get(value, default)


class Dict(collections.OrderedDict):
    def __setitem__(self, key, value):
        if isinstance(value, sa.Column) and not value.name:
            value.name = value.key = key
        if isinstance(value, json_support.JSONProperty) and not value.name:
            value.name = key
        return super().__setitem__(key, value)


class ModelType(type):
    def _check_abstract(self):
        if self.__table__ is None:
            raise TypeError(
                "GINO model {} is abstract, no table is "
                "defined.".format(self.__name__)
            )

    def __iter__(self):
        self._check_abstract()
        # noinspection PyUnresolvedReferences
        return iter(self.__table__.columns)

    def __getattr__(self, item):
        try:
            if item in {"insert", "join", "outerjoin", "gino"}:
                self._check_abstract()
                return getattr(self.__table__, item)
            raise AttributeError
        except AttributeError:
            raise AttributeError(
                "type object '{}' has no attribute '{}'".format(self.__name__, item)
            )

    @classmethod
    def __prepare__(mcs, name, bases, **kwargs):
        return Dict()

    def __new__(mcs, name, bases, namespace, **kwargs):
        rv = type.__new__(mcs, name, bases, namespace)
        rv.__namespace__ = namespace
        if rv.__table__ is None:
            rv.__table__ = getattr(rv, "_init_table")(rv)
        return rv


def declared_attr(m):
    """
    Mark a class-level method as a factory of attribute.

    This is intended to be used as decorators on class-level methods of a
    :class:`~Model` class. When initializing the class as well as its
    subclasses, the decorated factory method will be called for each class, the
    returned result will be set on the class in place of the factory method
    under the same name.

    ``@declared_attr`` is implemented differently than
    :class:`~sqlalchemy.ext.declarative.declared_attr` of SQLAlchemy, but they
    are both more often used on mixins to dynamically declare indices or
    constraints (also works for column and ``__table_args__``, or even normal
    class attributes)::

        class TrackedMixin:
            created = db.Column(db.DateTime(timezone=True))

            @db.declared_attr
            def unique_id(cls):
                return db.Column(db.Integer())

            @db.declared_attr
            def unique_constraint(cls):
                return db.UniqueConstraint('unique_id')

            @db.declared_attr
            def poly(cls):
                if cls.__name__ == 'Thing':
                    return db.Column(db.Unicode())

            @db.declared_attr
            def __table_args__(cls):
                if cls.__name__ == 'Thing':
                    return db.UniqueConstraint('poly'),

    .. note::

        This doesn't work if the model already had a ``__table__``.

    """
    m.__declared_attr__ = True
    return m


class Model:
    """The base class of GINO models.

    This is not supposed to be sub-classed directly, :func:`~declarative_base` should
    be used instead to generate a base model class with a given
    :class:`~sqlalchemy.schema.MetaData`. By defining subclasses of a model, instances
    of :class:`sqlalchemy.schema.Table` will be created and added to the bound
    :class:`~sqlalchemy.schema.MetaData`. The columns of the
    :class:`~sqlalchemy.schema.Table` instance are defined as
    :class:`~sqlalchemy.schema.Column` attributes::

        from sqlalchemy import MetaData, Column, Integer, String
        from gino.declarative import declarative_base

        Model = declarative_base()

        class User(db.Model):
            __tablename__ = "users"
            id = Column(Integer(), primary_key=True)
            name = Column(String())

    The name of the columns are automatically set using the attribute name.

    An instance of a model will maintain a memory storage for values of all the defined
    column attributes. You can access these values by the same attribute name, or update
    with new values, just like normal Python objects::

        u = User()
        assert u.name is None

        u.name = "daisy"
        assert u.name == "daisy"

    .. note::

        Accessing column attributes on a model instance will NOT trigger any database
        operation.

    :class:`~sqlalchemy.schema.Constraint` and :class:`~sqlalchemy.schema.Index` are
    also allowed as model class attributes. Their attribute names are not used.

    A concrete model class can be used as a replacement of the
    :class:`~sqlalchemy.schema.Table` it reflects in SQLAlchemy queries. The model class
    is also iterable, yielding all the :class:`~sqlalchemy.schema.Column` instances
    defined in the model.

    Other built-in class attributes:

    * ``__metadata__``

      This is supposed to be set by :func:`~declarative_base` and used only during
      subclass construction. Still, this can be treated as a read-only attribute to
      find out which :class:`~sqlalchemy.schema.MetaData` this model is bound to.

    * ``__tablename__``

      This is a required attribute to define a concrete model, meaning a
      :class:`sqlalchemy.schema.Table` instance will be created, added to the bound
      :class:`~sqlalchemy.schema.MetaData` and set to the class attribute ``__table__``.
      Not defining ``__tablename__`` will result in an abstract model - no table
      instance will be created, and instances of an abstract model are meaningless.

    * ``__table__``

      This should usually be treated as an auto-generated read-only attribute storing
      the :class:`sqlalchemy.schema.Table` instance.

    * ``__attr_factory__``

      An attribute factory that is used to wrap the actual
      :class:`~sqlalchemy.schema.Column` instance on the model class, so that the access
      to the column attributes on model instances is redirected to the in-memory value
      store. The default factory is :class:`~ColumnAttribute`, can be override.

    * ``__values__``

      The internal in-memory value store as a :class:`dict`, only available on model
      instances. Accessing column attributes is equivalent to accessing ``__values__``.

    """

    __metadata__ = None
    __table__ = None
    __attr_factory__ = ColumnAttribute

    def __init__(self):
        self.__values__ = {}

    @classmethod
    def _init_table(cls, sub_cls):
        table_name = None
        columns = []
        inspected_args = []
        updates = {}
        column_name_map = InvertDict()
        visited = set()
        for each_cls in sub_cls.__mro__:
            for k, v in getattr(each_cls, "__namespace__", each_cls.__dict__).items():
                if k in visited:
                    continue
                visited.add(k)
                declared_callable_attr = callable(v) and getattr(
                    v, "__declared_attr__", False
                )
                if k == "__tablename__":
                    if declared_callable_attr:
                        table_name = v(sub_cls)
                    else:
                        table_name = v
                    continue
                if declared_callable_attr:
                    v = updates[k] = v(sub_cls)
                if isinstance(v, sa.Column):
                    v = v.copy()
                    if not v.name:
                        v.name = v.key = k
                    column_name_map[k] = v.name
                    columns.append(v)
                    updates[k] = sub_cls.__attr_factory__(k, v)
                elif isinstance(v, (sa.Index, sa.Constraint)):
                    inspected_args.append(v)
                elif isinstance(v, json_support.JSONProperty):
                    updates[k] = v
        if table_name is None:
            return
        sub_cls._column_name_map = column_name_map

        # handle __table_args__
        table_args = updates.get(
            "__table_args__", getattr(sub_cls, "__table_args__", None)
        )
        args, table_kw = (), {}
        if isinstance(table_args, dict):
            table_kw = table_args
        elif isinstance(table_args, tuple) and table_args:
            if isinstance(table_args[-1], dict):
                args, table_kw = table_args[0:-1], table_args[-1]
            else:
                args = table_args

        args = (*columns, *inspected_args, *args)
        for item in args:
            try:
                _table = getattr(item, "table", None)
            except InvalidRequestError:
                _table = None
            if _table is not None:
                raise ValueError(
                    "{} is already attached to another table. Please do not "
                    "use the same item twice. A common mistake is defining "
                    "constraints and indices in a super class - we are working"
                    " on making it possible."
                )
        rv = sa.Table(table_name, sub_cls.__metadata__, *args, **table_kw)
        for k, v in updates.items():
            setattr(sub_cls, k, v)

        json_prop_names = set()
        for each_cls in sub_cls.__mro__[::-1]:
            for k, v in each_cls.__dict__.items():
                if isinstance(v, json_support.JSONProperty):
                    if not v.name:
                        v.name = k
                    json_prop_names.add(v.prop_name)
                    json_col = getattr(
                        sub_cls.__dict__.get(v.prop_name), "column", None
                    )
                    if not isinstance(json_col, sa.Column) or not isinstance(
                        json_col.type, sa.JSON
                    ):
                        raise AttributeError(
                            '{} "{}" requires a JSON[B] column "{}" '
                            "which is not found or has a wrong type.".format(
                                type(v).__name__, v.name, v.prop_name,
                            )
                        )
        sub_cls.__json_prop_names__ = json_prop_names
        return rv


def declarative_base(metadata, model_classes=(Model,), name="Model"):
    """Create a base GINO model class for declarative table definition.

    :param metadata: A :class:`~sqlalchemy.schema.MetaData` instance to contain the
                     tables.
    :param model_classes: Base class(es) of the base model class to be created. Default:
                          :class:`~Model`.
    :param name: The class name of the base model class to be created. Default:
                 ``Model``.
    :return: A new base model class.
    """
    return ModelType(name, model_classes, {"__metadata__": metadata})


# noinspection PyProtectedMember
@sa.inspection._inspects(ModelType)
def inspect_model_type(target):
    target._check_abstract()
    return sa.inspection.inspect(target.__table__)


__all__ = [
    "ColumnAttribute",
    "Model",
    "declarative_base",
    "declared_attr",
    "InvertDict",
]
