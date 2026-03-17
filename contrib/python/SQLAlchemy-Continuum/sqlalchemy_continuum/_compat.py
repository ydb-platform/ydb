"""
Compatibility module containing functions ported from SQLAlchemy-Utils.

This module contains the specific SQLAlchemy-Utils functions that SQLAlchemy-Continuum
actually uses, eliminating the need for the full SQLAlchemy-Utils dependency.

Functions ported:
- ImproperlyConfigured (exception)
- get_declarative_base()
- naturally_equivalent()
- get_columns()
- get_primary_keys()
- identity()
- get_column_key()
- has_changes()
- JSONType
- generic_relationship()
"""

import json
from collections import OrderedDict
from collections.abc import Iterable
from inspect import isclass

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql.base import ischema_names
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import attributes, class_mapper, ColumnProperty
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.interfaces import MapperProperty, PropComparator
from sqlalchemy.orm.session import _state_session
from sqlalchemy.util import set_creation_order

# PostgreSQL JSON support with fallback
try:
    from sqlalchemy.dialects.postgresql import JSON

    has_postgres_json = True
except ImportError:
    has_postgres_json = False


# ==============================================================================
# EXCEPTIONS
# ==============================================================================


class ImproperlyConfigured(Exception):
    """
    SQLAlchemy-Continuum is improperly configured; normally due to usage of
    a utility that depends on a missing library.
    """


# ==============================================================================
# SIMPLE UTILITY FUNCTIONS
# ==============================================================================


def get_declarative_base(model):
    """
    Returns the declarative base for given model class.

    :param model: SQLAlchemy declarative model
    """
    for parent in model.__bases__:
        try:
            parent.metadata
            return get_declarative_base(parent)
        except AttributeError:
            pass
    return model


def naturally_equivalent(obj, obj2):
    """
    Returns whether or not two given SQLAlchemy declarative instances are
    naturally equivalent (all their non primary key properties are equivalent).

    ::

        from sqlalchemy_continuum._compat import naturally_equivalent

        user = User(name='someone')
        user2 = User(name='someone')

        user == user2  # False

        naturally_equivalent(user, user2)  # True

    :param obj: SQLAlchemy declarative model object
    :param obj2: SQLAlchemy declarative model object to compare with `obj`
    """
    for column_key, column in sa.inspect(obj.__class__).columns.items():
        if column.primary_key:
            continue

        if not (getattr(obj, column_key) == getattr(obj2, column_key)):
            return False
    return True


# ==============================================================================
# CORE ORM UTILITY FUNCTIONS
# ==============================================================================


def get_columns(mixed):
    """
    Return a collection of all Column objects for given SQLAlchemy object.

    The type of the collection depends on the type of the object to return the
    columns from.

    ::

        get_columns(User)
        get_columns(User())
        get_columns(User.__table__)
        get_columns(User.__mapper__)
        get_columns(sa.orm.aliased(User))
        get_columns(sa.orm.aliased(User.__table__))

    :param mixed:
        SA Table object, SA Mapper, SA declarative class, SA declarative class
        instance or an alias of any of these objects
    """
    if isinstance(mixed, sa.sql.selectable.Selectable):
        try:
            return mixed.selected_columns
        except AttributeError:  # SQLAlchemy <1.4
            return mixed.c
    if isinstance(mixed, sa.orm.util.AliasedClass):
        return sa.inspect(mixed).mapper.columns
    if isinstance(mixed, sa.orm.Mapper):
        return mixed.columns
    if isinstance(mixed, InstrumentedAttribute):
        return mixed.property.columns
    if isinstance(mixed, ColumnProperty):
        return mixed.columns
    if isinstance(mixed, sa.Column):
        return [mixed]
    if not isclass(mixed):
        mixed = mixed.__class__
    return sa.inspect(mixed).columns


def get_primary_keys(mixed):
    """
    Return an OrderedDict of all primary keys for given Table object,
    declarative class or declarative class instance.

    :param mixed:
        SA Table object, SA declarative class or SA declarative class instance

    ::

        get_primary_keys(User)
        get_primary_keys(User())
        get_primary_keys(User.__table__)
        get_primary_keys(User.__mapper__)
        get_primary_keys(sa.orm.aliased(User))
        get_primary_keys(sa.orm.aliased(User.__table__))
    """
    return OrderedDict(
        (
            (key, column)
            for key, column in get_columns(mixed).items()
            if column.primary_key
        )
    )


def identity(obj_or_class):
    """
    Return the identity of given sqlalchemy declarative model class or instance
    as a tuple. This differs from obj._sa_instance_state.identity in a way that
    it always returns the identity even if object is still in transient state (
    new object that is not yet persisted into database). Also for classes it
    returns the identity attributes.

    ::

        from sqlalchemy import inspect
        from sqlalchemy_continuum._compat import identity

        user = User(name='John Matrix')
        session.add(user)
        identity(user)  # None
        inspect(user).identity  # None

        session.flush()  # User now has id but is still in transient state

        identity(user)  # (1,)
        inspect(user).identity  # None

        session.commit()

        identity(user)  # (1,)
        inspect(user).identity  # (1, )

    You can also use identity for classes::

        identity(User)  # (User.id, )

    :param obj: SQLAlchemy declarative model object
    """
    return tuple(
        getattr(obj_or_class, column_key)
        for column_key in get_primary_keys(obj_or_class).keys()
    )


def get_column_key(model, column):
    """
    Return the key for given column in given model.

    :param model: SQLAlchemy declarative model object

    ::

        class User(Base):
            __tablename__ = 'user'
            id = sa.Column(sa.Integer, primary_key=True)
            name = sa.Column('_name', sa.String)

        get_column_key(User, User.__table__.c._name)  # 'name'
    """
    mapper = sa.inspect(model)
    try:
        return mapper.get_property_by_column(column).key
    except sa.orm.exc.UnmappedColumnError:
        for key, c in mapper.columns.items():
            if c.name == column.name and c.table is column.table:
                return key
    raise sa.orm.exc.UnmappedColumnError(
        f'No column {column} is configured on mapper {mapper}...'
    )


def has_changes(obj, attrs=None, exclude=None):
    """
    Simple shortcut function for checking if given attributes of given
    declarative model object have changed during the session. Without
    parameters this checks if given object has any modifications. Additionally
    exclude parameter can be given to check if given object has any changes
    in any attributes other than the ones given in exclude.

    ::

        from sqlalchemy_continuum._compat import has_changes

        user = User()

        has_changes(user, 'name')  # False

        user.name = 'someone'

        has_changes(user, 'name')  # True

        has_changes(user)  # True

    You can check multiple attributes as well.
    ::

        has_changes(user, ['age'])  # True
        has_changes(user, ['name', 'age'])  # True

    This function also supports excluding certain attributes.

    ::

        has_changes(user, exclude=['name'])  # False
        has_changes(user, exclude=['age'])  # True

    :param obj: SQLAlchemy declarative model object
    :param attrs: Names of the attributes
    :param exclude: Names of the attributes to exclude
    """
    if attrs:
        if isinstance(attrs, str):
            return sa.inspect(obj).attrs.get(attrs).history.has_changes()
        else:
            return any(has_changes(obj, attr) for attr in attrs)
    else:
        if exclude is None:
            exclude = []
        return any(
            attr.history.has_changes()
            for key, attr in sa.inspect(obj).attrs.items()
            if key not in exclude
        )


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================


def _get_class_registry(class_):
    """
    Helper function to get the class registry for SQLAlchemy models.
    Handles differences between SQLAlchemy versions.
    """
    try:
        return class_.registry._class_registry
    except AttributeError:  # SQLAlchemy <1.4
        return class_._decl_class_registry


# ==============================================================================
# CUSTOM SQLALCHEMY TYPES
# ==============================================================================

# PostgreSQL JSON fallback for older SQLAlchemy versions
if not has_postgres_json:

    class PostgresJSONType(sa.types.UserDefinedType):
        """
        JSON type for PostgreSQL when native JSON support is not available.
        """

        def get_col_spec(self):
            return 'json'

    ischema_names['json'] = PostgresJSONType


class JSONType(sa.types.TypeDecorator):
    """
    JSONType offers way of saving JSON data structures to database. On
    PostgreSQL the underlying implementation of this data type is 'json' while
    on other databases its simply 'text'.

    ::

        from sqlalchemy_continuum._compat import JSONType

        class Product(Base):
            __tablename__ = 'product'
            id = sa.Column(sa.Integer, autoincrement=True)
            name = sa.Column(sa.Unicode(50))
            details = sa.Column(JSONType)

        product = Product()
        product.details = {
            'color': 'red',
            'type': 'car',
            'max-speed': '400 mph'
        }
        session.commit()
    """

    impl = sa.UnicodeText
    hashable = False
    cache_ok = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            # Use the native JSON type.
            if has_postgres_json:
                return dialect.type_descriptor(JSON())
            else:
                return dialect.type_descriptor(PostgresJSONType())
        else:
            return dialect.type_descriptor(self.impl)

    def process_bind_param(self, value, dialect):
        if dialect.name == 'postgresql' and has_postgres_json:
            return value
        if value is not None:
            value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if dialect.name == 'postgresql':
            return value
        if value is not None:
            value = json.loads(value)
        return value


# ==============================================================================
# GENERIC RELATIONSHIP IMPLEMENTATION
# ==============================================================================


class GenericAttributeImpl(attributes.ScalarAttributeImpl):
    """
    Custom attribute implementation for generic relationships.

    Handles the complex logic of resolving relationships based on discriminator
    and identity values, supporting lazy loading through database queries.
    """

    def __init__(self, *args, **kwargs):
        """
        The constructor of attributes.AttributeImpl changed in SQLAlchemy 2.0.22,
        adding a 'default_function' required positional argument before 'dispatch'.
        This adjustment ensures compatibility across versions by inserting None for
        'default_function' in versions >= 2.0.22.

        Arguments received: (class, key, dispatch)
        Required by AttributeImpl: (class, key, default_function, dispatch)
        Setting None as default_function here.
        """
        # Adjust for SQLAlchemy version change
        sqlalchemy_version = tuple(map(int, sa.__version__.split('.')))
        if sqlalchemy_version >= (2, 0, 22):
            args = (*args[:2], None, *args[2:])

        super().__init__(*args, **kwargs)

    def get(self, state, dict_, passive=attributes.PASSIVE_OFF):
        if self.key in dict_:
            return dict_[self.key]

        # Retrieve the session bound to the state in order to perform
        # a lazy query for the attribute.
        session = _state_session(state)
        if session is None:
            # State is not bound to a session; we cannot proceed.
            return None

        # Find class for discriminator.
        # TODO: Perhaps optimize with some sort of lookup?
        discriminator = self.get_state_discriminator(state)
        target_class = _get_class_registry(state.class_).get(discriminator)

        if target_class is None:
            # Unknown discriminator; return nothing.
            return None

        id = self.get_state_id(state)

        target = session.get(target_class, id)

        # Return found (or not found) target.
        return target

    def get_state_discriminator(self, state):
        discriminator = self.parent_token.discriminator
        if isinstance(discriminator, hybrid_property):
            return getattr(state.obj(), discriminator.__name__)
        else:
            return state.attrs[discriminator.key].value

    def get_state_id(self, state):
        # Lookup row with the discriminator and id.
        return tuple(state.attrs[id.key].value for id in self.parent_token.id)

    def set(
        self,
        state,
        dict_,
        initiator,
        passive=attributes.PASSIVE_OFF,
        check_old=None,
        pop=False,
    ):
        # Set us on the state.
        dict_[self.key] = initiator

        if initiator is None:
            # Nullify relationship args
            for id in self.parent_token.id:
                dict_[id.key] = None
            dict_[self.parent_token.discriminator.key] = None
        else:
            # Get the primary key of the initiator and ensure we
            # can support this assignment.
            class_ = type(initiator)
            mapper = class_mapper(class_)

            pk = mapper.identity_key_from_instance(initiator)[1]

            # Set the identifier and the discriminator.
            discriminator = class_.__name__

            for index, id in enumerate(self.parent_token.id):
                dict_[id.key] = pk[index]
            dict_[self.parent_token.discriminator.key] = discriminator


class GenericRelationshipProperty(MapperProperty):
    """
    A generic form of the relationship property.

    Creates a 1 to many relationship between the parent model
    and any other models using a discriminator (the table name).

    :param discriminator:
        Field to discriminate which model we are referring to.
    :param id:
        Field to point to the model we are referring to.
    """

    def __init__(self, discriminator, id, doc=None):
        super().__init__()
        self._discriminator_col = discriminator
        self._id_cols = id
        self._id = None
        self._discriminator = None
        self.doc = doc

        set_creation_order(self)

    def _column_to_property(self, column):
        if isinstance(column, hybrid_property):
            attr_key = column.__name__
            for key, attr in self.parent.all_orm_descriptors.items():
                if key == attr_key:
                    return attr
        else:
            for attr in self.parent.attrs.values():
                if isinstance(attr, ColumnProperty):
                    if attr.columns[0].name == column.name:
                        return attr

    def init(self):
        def convert_strings(column):
            if isinstance(column, str):
                return self.parent.columns[column]
            return column

        self._discriminator_col = convert_strings(self._discriminator_col)
        self._id_cols = convert_strings(self._id_cols)

        if isinstance(self._id_cols, Iterable):
            self._id_cols = list(map(convert_strings, self._id_cols))
        else:
            self._id_cols = [self._id_cols]

        self.discriminator = self._column_to_property(self._discriminator_col)

        if self.discriminator is None:
            raise ImproperlyConfigured('Could not find discriminator descriptor.')

        self.id = list(map(self._column_to_property, self._id_cols))

    class Comparator(PropComparator):
        def __init__(self, prop, parentmapper):
            self.property = prop
            self._parententity = parentmapper

        def __eq__(self, other):
            discriminator = type(other).__name__
            q = self.property._discriminator_col == discriminator
            other_id = identity(other)
            for index, id in enumerate(self.property._id_cols):
                q &= id == other_id[index]
            return q

        def __ne__(self, other):
            return ~(self == other)

        def is_type(self, other):
            mapper = sa.inspect(other)
            # Iterate through the weak sequence in order to get the actual
            # mappers
            class_names = [other.__name__]
            class_names.extend(
                [submapper.class_.__name__ for submapper in mapper._inheriting_mappers]
            )

            return self.property._discriminator_col.in_(class_names)

    def instrument_class(self, mapper):
        attributes.register_attribute(
            mapper.class_,
            self.key,
            comparator=self.Comparator(self, mapper),
            parententity=mapper,
            doc=self.doc,
            impl_class=GenericAttributeImpl,
            parent_token=self,
        )


def generic_relationship(*args, **kwargs):
    """
    Creates a generic relationship that can refer to any table using
    a discriminator column and a foreign key.

    ::

        from sqlalchemy_continuum._compat import generic_relationship

        class Activity(Base):
            __tablename__ = 'activity'
            id = sa.Column(sa.Integer, primary_key=True)

            # Generic relationship columns
            object_type = sa.Column(sa.String)
            object_id = sa.Column(sa.Integer)

            # The generic relationship
            object = generic_relationship(object_type, object_id)

    :param discriminator: Column or column name for discriminator
    :param id: Column, column name, or list of columns for foreign key
    :param doc: Documentation string
    """
    return GenericRelationshipProperty(*args, **kwargs)
