from __future__ import unicode_literals
import sys
from collections import OrderedDict
from itertools import chain
from logging import getLogger

import pytz

from .fields import Field, StringField
from .utils import parse_tsv, NO_VALUE, get_subclass_names, arg_to_sql, unescape
from .query import QuerySet
from .funcs import F
from .engines import Merge, Distributed

logger = getLogger('clickhouse_orm')



class Constraint:
    '''
    Defines a model constraint.
    '''

    name   = None # this is set by the parent model
    parent = None # this is set by the parent model

    def __init__(self, expr):
        '''
        Initializer. Expects an expression that ClickHouse will verify when inserting data.
        '''
        self.expr = expr

    def create_table_sql(self):
        '''
        Returns the SQL statement for defining this constraint during table creation.
        '''
        return 'CONSTRAINT `%s` CHECK %s' % (self.name, arg_to_sql(self.expr))


class Index:
    '''
    Defines a data-skipping index.
    '''

    name   = None # this is set by the parent model
    parent = None # this is set by the parent model

    def __init__(self, expr, type, granularity):
        '''
        Initializer.

        - `expr` - a column, expression, or tuple of columns and expressions to index.
        - `type` - the index type. Use one of the following methods to specify the type:
          `Index.minmax`, `Index.set`, `Index.ngrambf_v1`, `Index.tokenbf_v1` or `Index.bloom_filter`.
        - `granularity` - index block size (number of multiples of the `index_granularity` defined by the engine).
        '''
        self.expr = expr
        self.type = type
        self.granularity = granularity

    def create_table_sql(self):
        '''
        Returns the SQL statement for defining this index during table creation.
        '''
        return 'INDEX `%s` %s TYPE %s GRANULARITY %d' % (self.name, arg_to_sql(self.expr), self.type, self.granularity)

    @staticmethod
    def minmax():
        '''
        An index that stores extremes of the specified expression (if the expression is tuple, then it stores
        extremes for each element of tuple). The stored info is used for skipping blocks of data like the primary key.
        '''
        return 'minmax'

    @staticmethod
    def set(max_rows):
        '''
        An index that stores unique values of the specified expression (no more than max_rows rows,
        or unlimited if max_rows=0). Uses the values to check if the WHERE expression is not satisfiable
        on a block of data.
        '''
        return 'set(%d)' % max_rows

    @staticmethod
    def ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed):
        '''
        An index that stores a Bloom filter containing all ngrams from a block of data.
        Works only with strings. Can be used for optimization of equals, like and in expressions.

        - `n` — ngram size
        - `size_of_bloom_filter_in_bytes` — Bloom filter size in bytes (you can use large values here,
           for example 256 or 512, because it can be compressed well).
        - `number_of_hash_functions` — The number of hash functions used in the Bloom filter.
        - `random_seed` — The seed for Bloom filter hash functions.
        '''
        return 'ngrambf_v1(%d, %d, %d, %d)' % (n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)

    @staticmethod
    def tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed):
        '''
        An index that stores a Bloom filter containing string tokens. Tokens are sequences
        separated by non-alphanumeric characters.

        - `size_of_bloom_filter_in_bytes` — Bloom filter size in bytes (you can use large values here,
           for example 256 or 512, because it can be compressed well).
        - `number_of_hash_functions` — The number of hash functions used in the Bloom filter.
        - `random_seed` — The seed for Bloom filter hash functions.
        '''
        return 'tokenbf_v1(%d, %d, %d)' % (size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)

    @staticmethod
    def bloom_filter(false_positive=0.025):
        '''
        An index that stores a Bloom filter containing values of the index expression.

        - `false_positive` - the probability (between 0 and 1) of receiving a false positive
          response from the filter
        '''
        return 'bloom_filter(%f)' % false_positive


class ModelBase(type):
    '''
    A metaclass for ORM models. It adds the _fields list to model classes.
    '''

    ad_hoc_model_cache = {}

    def __new__(cls, name, bases, attrs):

        # Collect fields, constraints and indexes from parent classes
        fields = {}
        constraints = {}
        indexes = {}
        for base in bases:
            if isinstance(base, ModelBase):
                fields.update(base._fields)
                constraints.update(base._constraints)
                indexes.update(base._indexes)

        # Add fields, constraints and indexes from this class
        for n, obj in attrs.items():
            if isinstance(obj, Field):
                fields[n] = obj
            elif isinstance(obj, Constraint):
                constraints[n] = obj
            elif isinstance(obj, Index):
                indexes[n] = obj

        # Convert fields to a list of (name, field) tuples, in the order they were listed in the class
        fields = sorted(fields.items(), key=lambda item: item[1].creation_counter)

        # Build a dictionary of default values
        defaults = {}
        has_funcs_as_defaults = False
        for n, f in fields:
            if f.alias or f.materialized:
                defaults[n] = NO_VALUE
            elif isinstance(f.default, F):
                defaults[n] = NO_VALUE
                has_funcs_as_defaults = True
            else:
                defaults[n] = f.to_python(f.default, pytz.UTC)

        # Create the model class
        attrs = dict(
            attrs,
            _fields=OrderedDict(fields),
            _constraints=constraints,
            _indexes=indexes,
            _writable_fields=OrderedDict([f for f in fields if not f[1].readonly]),
            _defaults=defaults,
            _has_funcs_as_defaults=has_funcs_as_defaults
        )
        model = super(ModelBase, cls).__new__(cls, str(name), bases, attrs)

        # Let each field, constraint and index know its parent and its own name
        for n, obj in chain(fields, constraints.items(), indexes.items()):
            setattr(obj, 'parent', model)
            setattr(obj, 'name', n)

        return model

    @classmethod
    def create_ad_hoc_model(cls, fields, model_name='AdHocModel'):
        # fields is a list of tuples (name, db_type)
        # Check if model exists in cache
        fields = list(fields)
        cache_key = model_name + ' ' + str(fields)
        if cache_key in cls.ad_hoc_model_cache:
            return cls.ad_hoc_model_cache[cache_key]
        # Create an ad hoc model class
        attrs = {}
        for name, db_type in fields:
            attrs[name] = cls.create_ad_hoc_field(db_type)
        model_class = cls.__new__(cls, model_name, (Model,), attrs)
        # Add the model class to the cache
        cls.ad_hoc_model_cache[cache_key] = model_class
        return model_class

    @classmethod
    def create_ad_hoc_field(cls, db_type):
        import infi.clickhouse_orm.fields as orm_fields
        # Enums
        if db_type.startswith('Enum'):
            return orm_fields.BaseEnumField.create_ad_hoc_field(db_type)
        # DateTime with timezone
        if db_type.startswith('DateTime('):
            timezone = db_type[9:-1]
            return orm_fields.DateTimeField(
                timezone=timezone[1:-1] if timezone else None
            )
        # DateTime64
        if db_type.startswith('DateTime64('):
            precision, *timezone = [s.strip() for s in db_type[11:-1].split(',')]
            return orm_fields.DateTime64Field(
                precision=int(precision),
                timezone=timezone[0][1:-1] if timezone else None
            )
        # Arrays
        if db_type.startswith('Array'):
            inner_field = cls.create_ad_hoc_field(db_type[6 : -1])
            return orm_fields.ArrayField(inner_field)
        # Tuples (poor man's version - convert to array)
        if db_type.startswith('Tuple'):
            types = [s.strip() for s in db_type[6 : -1].split(',')]
            if len(types[0].split()) != 1:
                raise NotImplementedError('No support for named tuples - %s' % db_type)
            if len(set(types)) != 1:
                raise NotImplementedError('No support for mixed types in tuples - %s' % db_type)
            inner_field = cls.create_ad_hoc_field(types[0])
            return orm_fields.ArrayField(inner_field)
        # FixedString
        if db_type.startswith('FixedString'):
            length = int(db_type[12 : -1])
            return orm_fields.FixedStringField(length)
        # Decimal / Decimal32 / Decimal64 / Decimal128
        if db_type.startswith('Decimal'):
            p = db_type.index('(')
            args = [int(n.strip()) for n in db_type[p + 1 : -1].split(',')]
            field_class = getattr(orm_fields, db_type[:p] + 'Field')
            return field_class(*args)
        # Nullable
        if db_type.startswith('Nullable'):
            inner_field = cls.create_ad_hoc_field(db_type[9 : -1])
            return orm_fields.NullableField(inner_field)
        # LowCardinality
        if db_type.startswith('LowCardinality'):
            inner_field = cls.create_ad_hoc_field(db_type[15 : -1])
            return orm_fields.LowCardinalityField(inner_field)
        # Simple fields
        name = db_type + 'Field'
        if not hasattr(orm_fields, name):
            raise NotImplementedError('No field class for %s' % db_type)
        return getattr(orm_fields, name)()


class Model(metaclass=ModelBase):
    '''
    A base class for ORM models. Each model class represent a ClickHouse table. For example:

        class CPUStats(Model):
            timestamp = DateTimeField()
            cpu_id = UInt16Field()
            cpu_percent = Float32Field()
            engine = Memory()
    '''

    engine = None

    # Insert operations are restricted for read only models
    _readonly = False

    # Create table, drop table, insert operations are restricted for system models
    _system = False

    _database = None

    def __init__(self, **kwargs):
        '''
        Creates a model instance, using keyword arguments as field values.
        Since values are immediately converted to their Pythonic type,
        invalid values will cause a `ValueError` to be raised.
        Unrecognized field names will cause an `AttributeError`.
        '''
        super(Model, self).__init__()
        # Assign default values
        self.__dict__.update(self._defaults)
        # Assign field values from keyword arguments
        for name, value in kwargs.items():
            field = self.get_field(name)
            if field:
                setattr(self, name, value)
            else:
                raise AttributeError('%s does not have a field called %s' % (self.__class__.__name__, name))

    def __setattr__(self, name, value):
        '''
        When setting a field value, converts the value to its Pythonic type and validates it.
        This may raise a `ValueError`.
        '''
        field = self.get_field(name)
        if field and (value != NO_VALUE):
            try:
                value = field.to_python(value, pytz.utc)
                field.validate(value)
            except ValueError:
                tp, v, tb = sys.exc_info()
                new_msg = "{} (field '{}')".format(v, name)
                raise tp.with_traceback(tp(new_msg), tb)
        super(Model, self).__setattr__(name, value)

    def set_database(self, db):
        '''
        Sets the `Database` that this model instance belongs to.
        This is done automatically when the instance is read from the database or written to it.
        '''
        # This can not be imported globally due to circular import
        from .database import Database
        assert isinstance(db, Database), "database must be database.Database instance"
        self._database = db

    def get_database(self):
        '''
        Gets the `Database` that this model instance belongs to.
        Returns `None` unless the instance was read from the database or written to it.
        '''
        return self._database

    def get_field(self, name):
        '''
        Gets a `Field` instance given its name, or `None` if not found.
        '''
        return self._fields.get(name)

    @classmethod
    def table_name(cls):
        '''
        Returns the model's database table name. By default this is the
        class name converted to lowercase. Override this if you want to use
        a different table name.
        '''
        return cls.__name__.lower()

    @classmethod
    def has_funcs_as_defaults(cls):
        '''
        Return True if some of the model's fields use a function expression
        as a default value. This requires special handling when inserting instances.
        '''
        return cls._has_funcs_as_defaults

    @classmethod
    def create_table_sql(cls, db):
        '''
        Returns the SQL statement for creating a table for this model.
        '''
        parts = ['CREATE TABLE IF NOT EXISTS `%s`.`%s` (' % (db.db_name, cls.table_name())]
        # Fields
        items = []
        for name, field in cls.fields().items():
            items.append('    %s %s' % (name, field.get_sql(db=db)))
        # Constraints
        for c in cls._constraints.values():
            items.append('    %s' % c.create_table_sql())
        # Indexes
        for i in cls._indexes.values():
            items.append('    %s' % i.create_table_sql())
        parts.append(',\n'.join(items))
        # Engine
        parts.append(')')
        parts.append('ENGINE = ' + cls.engine.create_table_sql(db))
        return '\n'.join(parts)

    @classmethod
    def drop_table_sql(cls, db):
        '''
        Returns the SQL command for deleting this model's table.
        '''
        return 'DROP TABLE IF EXISTS `%s`.`%s`' % (db.db_name, cls.table_name())

    @classmethod
    def from_tsv(cls, line, field_names, timezone_in_use=pytz.utc, database=None):
        '''
        Create a model instance from a tab-separated line. The line may or may not include a newline.
        The `field_names` list must match the fields defined in the model, but does not have to include all of them.

        - `line`: the TSV-formatted data.
        - `field_names`: names of the model fields in the data.
        - `timezone_in_use`: the timezone to use when parsing dates and datetimes. Some fields use their own timezones.
        - `database`: if given, sets the database that this instance belongs to.
        '''
        values = iter(parse_tsv(line))
        kwargs = {}
        for name in field_names:
            field = getattr(cls, name)
            field_timezone = getattr(field, 'timezone', None) or timezone_in_use
            kwargs[name] = field.to_python(next(values), field_timezone)

        obj = cls(**kwargs)
        if database is not None:
            obj.set_database(database)

        return obj

    def to_tsv(self, include_readonly=True):
        '''
        Returns the instance's column values as a tab-separated line. A newline is not included.

        - `include_readonly`: if false, returns only fields that can be inserted into database.
        '''
        data = self.__dict__
        fields = self.fields(writable=not include_readonly)
        return '\t'.join(field.to_db_string(data[name], quote=False) for name, field in fields.items())

    def to_tskv(self, include_readonly=True):
        '''
        Returns the instance's column keys and values as a tab-separated line. A newline is not included.
        Fields that were not assigned a value are omitted.

        - `include_readonly`: if false, returns only fields that can be inserted into database.
        '''
        data = self.__dict__
        fields = self.fields(writable=not include_readonly)
        parts = []
        for name, field in fields.items():
            if data[name] != NO_VALUE:
                parts.append(name + '=' + field.to_db_string(data[name], quote=False))
        return '\t'.join(parts)

    def to_db_string(self):
        '''
        Returns the instance as a bytestring ready to be inserted into the database.
        '''
        s = self.to_tskv(False) if self._has_funcs_as_defaults else self.to_tsv(False)
        s += '\n'
        return s.encode('utf-8')

    def to_dict(self, include_readonly=True, field_names=None):
        '''
        Returns the instance's column values as a dict.

        - `include_readonly`: if false, returns only fields that can be inserted into database.
        - `field_names`: an iterable of field names to return (optional)
        '''
        fields = self.fields(writable=not include_readonly)

        if field_names is not None:
            fields = [f for f in fields if f in field_names]

        data = self.__dict__
        return {name: data[name] for name in fields}

    @classmethod
    def objects_in(cls, database):
        '''
        Returns a `QuerySet` for selecting instances of this model class.
        '''
        return QuerySet(cls, database)

    @classmethod
    def fields(cls, writable=False):
        '''
        Returns an `OrderedDict` of the model's fields (from name to `Field` instance).
        If `writable` is true, only writable fields are included.
        Callers should not modify the dictionary.
        '''
        # noinspection PyProtectedMember,PyUnresolvedReferences
        return cls._writable_fields if writable else cls._fields

    @classmethod
    def is_read_only(cls):
        '''
        Returns true if the model is marked as read only.
        '''
        return cls._readonly

    @classmethod
    def is_system_model(cls):
        '''
        Returns true if the model represents a system table.
        '''
        return cls._system


class BufferModel(Model):

    @classmethod
    def create_table_sql(cls, db):
        '''
        Returns the SQL statement for creating a table for this model.
        '''
        parts = ['CREATE TABLE IF NOT EXISTS `%s`.`%s` AS `%s`.`%s`' % (db.db_name, cls.table_name(), db.db_name,
                                                                        cls.engine.main_model.table_name())]
        engine_str = cls.engine.create_table_sql(db)
        parts.append(engine_str)
        return ' '.join(parts)


class MergeModel(Model):
    '''
    Model for Merge engine
    Predefines virtual _table column an controls that rows can't be inserted to this table type
    https://clickhouse.tech/docs/en/single/index.html#document-table_engines/merge
    '''
    readonly = True

    # Virtual fields can't be inserted into database
    _table = StringField(readonly=True)

    @classmethod
    def create_table_sql(cls, db):
        '''
        Returns the SQL statement for creating a table for this model.
        '''
        assert isinstance(cls.engine, Merge), "engine must be an instance of engines.Merge"
        parts = ['CREATE TABLE IF NOT EXISTS `%s`.`%s` (' % (db.db_name, cls.table_name())]
        cols = []
        for name, field in cls.fields().items():
            if name != '_table':
                cols.append('    %s %s' % (name, field.get_sql(db=db)))
        parts.append(',\n'.join(cols))
        parts.append(')')
        parts.append('ENGINE = ' + cls.engine.create_table_sql(db))
        return '\n'.join(parts)

# TODO: base class for models that require specific engine


class DistributedModel(Model):
    """
    Model class for use with a `Distributed` engine.
    """

    def set_database(self, db):
        '''
        Sets the `Database` that this model instance belongs to.
        This is done automatically when the instance is read from the database or written to it.
        '''
        assert isinstance(self.engine, Distributed), "engine must be an instance of engines.Distributed"
        res = super(DistributedModel, self).set_database(db)
        return res

    @classmethod
    def fix_engine_table(cls):
        """
        Remember: Distributed table does not store any data, just provides distributed access to it.

        So if we define a model with engine that has no defined table for data storage
        (see FooDistributed below), that table cannot be successfully created.
        This routine can automatically fix engine's storage table by finding the first
        non-distributed model among your model's superclasses.

        >>> class Foo(Model):
        ...     id = UInt8Field(1)
        ...
        >>> class FooDistributed(Foo, DistributedModel):
        ...     engine = Distributed('my_cluster')
        ...
        >>> FooDistributed.engine.table
        None
        >>> FooDistributed.fix_engine()
        >>> FooDistributed.engine.table
        <class '__main__.Foo'>

        However if you prefer more explicit way of doing things,
        you can always mention the Foo model twice without bothering with any fixes:

        >>> class FooDistributedVerbose(Foo, DistributedModel):
        ...     engine = Distributed('my_cluster', Foo)
        >>> FooDistributedVerbose.engine.table
        <class '__main__.Foo'>

        See tests.test_engines:DistributedTestCase for more examples
        """

        # apply only when engine has no table defined
        if cls.engine.table_name:
            return

        # find out all the superclasses of the Model that store any data
        storage_models = [b for b in cls.__bases__ if issubclass(b, Model)
                          and not issubclass(b, DistributedModel)]
        if not storage_models:
            raise TypeError("When defining Distributed engine without the table_name "
                            "ensure that your model has a parent model")

        if len(storage_models) > 1:
            raise TypeError("When defining Distributed engine without the table_name "
                            "ensure that your model has exactly one non-distributed superclass")

        # enable correct SQL for engine
        cls.engine.table = storage_models[0]

    @classmethod
    def create_table_sql(cls, db):
        '''
        Returns the SQL statement for creating a table for this model.
        '''
        assert isinstance(cls.engine, Distributed), "engine must be engines.Distributed instance"

        cls.fix_engine_table()

        parts = [
            'CREATE TABLE IF NOT EXISTS `{0}`.`{1}` AS `{0}`.`{2}`'.format(
                db.db_name, cls.table_name(), cls.engine.table_name),
            'ENGINE = ' + cls.engine.create_table_sql(db)]
        return '\n'.join(parts)


# Expose only relevant classes in import *
__all__ = get_subclass_names(locals(), (Model, Constraint, Index))
