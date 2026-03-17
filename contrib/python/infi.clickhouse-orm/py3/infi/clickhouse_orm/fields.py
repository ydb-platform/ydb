from __future__ import unicode_literals
import datetime
import iso8601
import pytz
from calendar import timegm
from decimal import Decimal, localcontext
from uuid import UUID
from logging import getLogger
from pytz import BaseTzInfo
from .utils import escape, parse_array, comma_join, string_or_func, get_subclass_names
from .funcs import F, FunctionOperatorsMixin
from ipaddress import IPv4Address, IPv6Address

logger = getLogger('clickhouse_orm')


class Field(FunctionOperatorsMixin):
    '''
    Abstract base class for all field types.
    '''
    name             = None # this is set by the parent model
    parent           = None # this is set by the parent model
    creation_counter = 0    # used for keeping the model fields ordered
    class_default    = 0    # should be overridden by concrete subclasses
    db_type          = None # should be overridden by concrete subclasses

    def __init__(self, default=None, alias=None, materialized=None, readonly=None, codec=None):
        assert [default, alias, materialized].count(None) >= 2, \
            "Only one of default, alias and materialized parameters can be given"
        assert alias is None or isinstance(alias, F) or isinstance(alias, str) and alias != "",\
            "Alias parameter must be a string or function object, if given"
        assert materialized is None or isinstance(materialized, F) or isinstance(materialized, str) and materialized != "",\
            "Materialized parameter must be a string or function object, if given"
        assert readonly is None or type(readonly) is bool, "readonly parameter must be bool if given"
        assert codec is None or isinstance(codec, str) and codec != "", \
            "Codec field must be string, if given"

        self.creation_counter = Field.creation_counter
        Field.creation_counter += 1
        self.default = self.class_default if default is None else default
        self.alias = alias
        self.materialized = materialized
        self.readonly = bool(self.alias or self.materialized or readonly)
        self.codec = codec

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<%s>' % self.__class__.__name__

    def to_python(self, value, timezone_in_use):
        '''
        Converts the input value into the expected Python data type, raising ValueError if the
        data can't be converted. Returns the converted value. Subclasses should override this.
        The timezone_in_use parameter should be consulted when parsing datetime fields.
        '''
        return value   # pragma: no cover

    def validate(self, value):
        '''
        Called after to_python to validate that the value is suitable for the field's database type.
        Subclasses should override this.
        '''
        pass

    def _range_check(self, value, min_value, max_value):
        '''
        Utility method to check that the given value is between min_value and max_value.
        '''
        if value < min_value or value > max_value:
            raise ValueError('%s out of range - %s is not between %s and %s' % (self.__class__.__name__, value, min_value, max_value))

    def to_db_string(self, value, quote=True):
        '''
        Returns the field's value prepared for writing to the database.
        When quote is true, strings are surrounded by single quotes.
        '''
        return escape(value, quote)

    def get_sql(self, with_default_expression=True, db=None):
        '''
        Returns an SQL expression describing the field (e.g. for CREATE TABLE).

        - `with_default_expression`: If True, adds default value to sql.
            It doesn't affect fields with alias and materialized values.
        - `db`: Database, used for checking supported features.
        '''
        sql = self.db_type
        args = self.get_db_type_args()
        if args:
            sql += '(%s)' % comma_join(args)
        if with_default_expression:
            sql += self._extra_params(db)
        return sql

    def get_db_type_args(self):
        """Returns field type arguments"""
        return []

    def _extra_params(self, db):
        sql = ''
        if self.alias:
            sql += ' ALIAS %s' % string_or_func(self.alias)
        elif self.materialized:
            sql += ' MATERIALIZED %s' % string_or_func(self.materialized)
        elif isinstance(self.default, F):
            sql += ' DEFAULT %s' % self.default.to_sql()
        elif self.default:
            default = self.to_db_string(self.default)
            sql += ' DEFAULT %s' % default
        if self.codec and db and db.has_codec_support and not self.alias:
            sql += ' CODEC(%s)' % self.codec
        return sql

    def isinstance(self, types):
        """
        Checks if the instance if one of the types provided or if any of the inner_field child is one of the types
        provided, returns True if field or any inner_field is one of ths provided, False otherwise

        - `types`: Iterable of types to check inclusion of instance

        Returns: Boolean
        """
        if isinstance(self, types):
            return True
        inner_field = getattr(self, 'inner_field', None)
        while inner_field:
            if isinstance(inner_field, types):
                return True
            inner_field = getattr(inner_field, 'inner_field', None)
        return False


class StringField(Field):

    class_default = ''
    db_type = 'String'

    def to_python(self, value, timezone_in_use):
        if isinstance(value, str):
            return value
        if isinstance(value, bytes):
            return value.decode('UTF-8')
        raise ValueError('Invalid value for %s: %r' % (self.__class__.__name__, value))


class FixedStringField(StringField):

    def __init__(self, length, default=None, alias=None, materialized=None, readonly=None):
        self._length = length
        self.db_type = 'FixedString(%d)' % length
        super(FixedStringField, self).__init__(default, alias, materialized, readonly)

    def to_python(self, value, timezone_in_use):
        value = super(FixedStringField, self).to_python(value, timezone_in_use)
        return value.rstrip('\0')

    def validate(self, value):
        if isinstance(value, str):
            value = value.encode('UTF-8')
        if len(value) > self._length:
            raise ValueError('Value of %d bytes is too long for FixedStringField(%d)' % (len(value), self._length))


class DateField(Field):

    min_value = datetime.date(1970, 1, 1)
    max_value = datetime.date(2105, 12, 31)
    class_default = min_value
    db_type = 'Date'

    def to_python(self, value, timezone_in_use):
        if isinstance(value, datetime.datetime):
            return value.astimezone(pytz.utc).date() if value.tzinfo else value.date()
        if isinstance(value, datetime.date):
            return value
        if isinstance(value, int):
            return DateField.class_default + datetime.timedelta(days=value)
        if isinstance(value, str):
            if value == '0000-00-00':
                return DateField.min_value
            return datetime.datetime.strptime(value, '%Y-%m-%d').date()
        raise ValueError('Invalid value for %s - %r' % (self.__class__.__name__, value))

    def validate(self, value):
        self._range_check(value, DateField.min_value, DateField.max_value)

    def to_db_string(self, value, quote=True):
        return escape(value.isoformat(), quote)


class DateTimeField(Field):

    class_default = datetime.datetime.fromtimestamp(0, pytz.utc)
    db_type = 'DateTime'

    def __init__(self, default=None, alias=None, materialized=None, readonly=None, codec=None,
                 timezone=None):
        super().__init__(default, alias, materialized, readonly, codec)
        # assert not timezone, 'Temporarily field timezone is not supported'
        if timezone:
            timezone = timezone if isinstance(timezone, BaseTzInfo) else pytz.timezone(timezone)
        self.timezone = timezone

    def get_db_type_args(self):
        args = []
        if self.timezone:
            args.append(escape(self.timezone.zone))
        return args

    def to_python(self, value, timezone_in_use):
        if isinstance(value, datetime.datetime):
            return value if value.tzinfo else value.replace(tzinfo=pytz.utc)
        if isinstance(value, datetime.date):
            return datetime.datetime(value.year, value.month, value.day, tzinfo=pytz.utc)
        if isinstance(value, int):
            return datetime.datetime.utcfromtimestamp(value).replace(tzinfo=pytz.utc)
        if isinstance(value, str):
            if value == '0000-00-00 00:00:00':
                return self.class_default
            if len(value) == 10:
                try:
                    value = int(value)
                    return datetime.datetime.utcfromtimestamp(value).replace(tzinfo=pytz.utc)
                except ValueError:
                    pass
            try:
                # left the date naive in case of no tzinfo set
                dt = iso8601.parse_date(value, default_timezone=None)
            except iso8601.ParseError as e:
                raise ValueError(str(e))

            # convert naive to aware
            if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
                dt = timezone_in_use.localize(dt)
            return dt
        raise ValueError('Invalid value for %s - %r' % (self.__class__.__name__, value))

    def to_db_string(self, value, quote=True):
        return escape('%010d' % timegm(value.utctimetuple()), quote)


class DateTime64Field(DateTimeField):
    db_type = 'DateTime64'

    def __init__(self, default=None, alias=None, materialized=None, readonly=None, codec=None,
                 timezone=None, precision=6):
        super().__init__(default, alias, materialized, readonly, codec, timezone)
        assert precision is None or isinstance(precision, int), 'Precision must be int type'
        self.precision = precision

    def get_db_type_args(self):
        args = [str(self.precision)]
        if self.timezone:
            args.append(escape(self.timezone.zone))
        return args

    def to_db_string(self, value, quote=True):
        """
        Returns the field's value prepared for writing to the database

        Returns string in 0000000000.000000 format, where remainder digits count is equal to precision
        """
        return escape(
            '{timestamp:0{width}.{precision}f}'.format(
                timestamp=value.timestamp(),
                width=11 + self.precision,
                precision=self.precision),
            quote
        )

    def to_python(self, value, timezone_in_use):
        try:
            return super().to_python(value, timezone_in_use)
        except ValueError:
            if isinstance(value, (int, float)):
                return datetime.datetime.utcfromtimestamp(value).replace(tzinfo=pytz.utc)
            if isinstance(value, str):
                left_part = value.split('.')[0]
                if left_part == '0000-00-00 00:00:00':
                    return self.class_default
                if len(left_part) == 10:
                    try:
                        value = float(value)
                        return datetime.datetime.utcfromtimestamp(value).replace(tzinfo=pytz.utc)
                    except ValueError:
                        pass
            raise


class BaseIntField(Field):
    '''
    Abstract base class for all integer-type fields.
    '''
    def to_python(self, value, timezone_in_use):
        try:
            return int(value)
        except:
            raise ValueError('Invalid value for %s - %r' % (self.__class__.__name__, value))

    def to_db_string(self, value, quote=True):
        # There's no need to call escape since numbers do not contain
        # special characters, and never need quoting
        return str(value)

    def validate(self, value):
        self._range_check(value, self.min_value, self.max_value)


class UInt8Field(BaseIntField):

    min_value = 0
    max_value = 2**8 - 1
    db_type = 'UInt8'


class UInt16Field(BaseIntField):

    min_value = 0
    max_value = 2**16 - 1
    db_type = 'UInt16'


class UInt32Field(BaseIntField):

    min_value = 0
    max_value = 2**32 - 1
    db_type = 'UInt32'


class UInt64Field(BaseIntField):

    min_value = 0
    max_value = 2**64 - 1
    db_type = 'UInt64'


class Int8Field(BaseIntField):

    min_value = -2**7
    max_value = 2**7 - 1
    db_type = 'Int8'


class Int16Field(BaseIntField):

    min_value = -2**15
    max_value = 2**15 - 1
    db_type = 'Int16'


class Int32Field(BaseIntField):

    min_value = -2**31
    max_value = 2**31 - 1
    db_type = 'Int32'


class Int64Field(BaseIntField):

    min_value = -2**63
    max_value = 2**63 - 1
    db_type = 'Int64'


class BaseFloatField(Field):
    '''
    Abstract base class for all float-type fields.
    '''

    def to_python(self, value, timezone_in_use):
        try:
            return float(value)
        except:
            raise ValueError('Invalid value for %s - %r' % (self.__class__.__name__, value))

    def to_db_string(self, value, quote=True):
        # There's no need to call escape since numbers do not contain
        # special characters, and never need quoting
        return str(value)


class Float32Field(BaseFloatField):

    db_type = 'Float32'


class Float64Field(BaseFloatField):

    db_type = 'Float64'


class DecimalField(Field):
    '''
    Base class for all decimal fields. Can also be used directly.
    '''

    def __init__(self, precision, scale, default=None, alias=None, materialized=None, readonly=None):
        assert 1 <= precision <= 38, 'Precision must be between 1 and 38'
        assert 0 <= scale <= precision, 'Scale must be between 0 and the given precision'
        self.precision = precision
        self.scale = scale
        self.db_type = 'Decimal(%d,%d)' % (self.precision, self.scale)
        with localcontext() as ctx:
            ctx.prec = 38
            self.exp = Decimal(10) ** -self.scale # for rounding to the required scale
            self.max_value = Decimal(10 ** (self.precision - self.scale)) - self.exp
            self.min_value = -self.max_value
        super(DecimalField, self).__init__(default, alias, materialized, readonly)

    def to_python(self, value, timezone_in_use):
        if not isinstance(value, Decimal):
            try:
                value = Decimal(value)
            except:
                raise ValueError('Invalid value for %s - %r' % (self.__class__.__name__, value))
        if not value.is_finite():
                raise ValueError('Non-finite value for %s - %r' % (self.__class__.__name__, value))
        return self._round(value)

    def to_db_string(self, value, quote=True):
        # There's no need to call escape since numbers do not contain
        # special characters, and never need quoting
        return str(value)

    def _round(self, value):
        return value.quantize(self.exp)

    def validate(self, value):
        self._range_check(value, self.min_value, self.max_value)


class Decimal32Field(DecimalField):

    def __init__(self, scale, default=None, alias=None, materialized=None, readonly=None):
        super(Decimal32Field, self).__init__(9, scale, default, alias, materialized, readonly)
        self.db_type = 'Decimal32(%d)' % scale


class Decimal64Field(DecimalField):

    def __init__(self, scale, default=None, alias=None, materialized=None, readonly=None):
        super(Decimal64Field, self).__init__(18, scale, default, alias, materialized, readonly)
        self.db_type = 'Decimal64(%d)' % scale


class Decimal128Field(DecimalField):

    def __init__(self, scale, default=None, alias=None, materialized=None, readonly=None):
        super(Decimal128Field, self).__init__(38, scale, default, alias, materialized, readonly)
        self.db_type = 'Decimal128(%d)' % scale


class BaseEnumField(Field):
    '''
    Abstract base class for all enum-type fields.
    '''

    def __init__(self, enum_cls, default=None, alias=None, materialized=None, readonly=None, codec=None):
        self.enum_cls = enum_cls
        if default is None:
            default = list(enum_cls)[0]
        super(BaseEnumField, self).__init__(default, alias, materialized, readonly, codec)

    def to_python(self, value, timezone_in_use):
        if isinstance(value, self.enum_cls):
            return value
        try:
            if isinstance(value, str):
                try:
                    return self.enum_cls[value]
                except Exception:
                    return self.enum_cls(value)
            if isinstance(value, bytes):
                decoded = value.decode('UTF-8')
                try:
                    return self.enum_cls[decoded]
                except Exception:
                    return self.enum_cls(decoded)
            if isinstance(value, int):
                return self.enum_cls(value)
        except (KeyError, ValueError):
            pass
        raise ValueError('Invalid value for %s: %r' % (self.enum_cls.__name__, value))

    def to_db_string(self, value, quote=True):
        return escape(value.name, quote)

    def get_db_type_args(self):
        return ['%s = %d' % (escape(item.name), item.value) for item in self.enum_cls]

    @classmethod
    def create_ad_hoc_field(cls, db_type):
        '''
        Give an SQL column description such as "Enum8('apple' = 1, 'banana' = 2, 'orange' = 3)"
        this method returns a matching enum field.
        '''
        import re
        from enum import Enum
        members = {}
        for match in re.finditer(r"'([\w ]+)' = (-?\d+)", db_type):
            members[match.group(1)] = int(match.group(2))
        enum_cls = Enum('AdHocEnum', members)
        field_class = Enum8Field if db_type.startswith('Enum8') else Enum16Field
        return field_class(enum_cls)


class Enum8Field(BaseEnumField):

    db_type = 'Enum8'


class Enum16Field(BaseEnumField):

    db_type = 'Enum16'


class ArrayField(Field):

    class_default = []

    def __init__(self, inner_field, default=None, alias=None, materialized=None, readonly=None, codec=None):
        assert isinstance(inner_field, Field), "The first argument of ArrayField must be a Field instance"
        assert not isinstance(inner_field, ArrayField), "Multidimensional array fields are not supported by the ORM"
        self.inner_field = inner_field
        super(ArrayField, self).__init__(default, alias, materialized, readonly, codec)

    def to_python(self, value, timezone_in_use):
        if isinstance(value, str):
            value = parse_array(value)
        elif isinstance(value, bytes):
            value = parse_array(value.decode('UTF-8'))
        elif not isinstance(value, (list, tuple)):
            raise ValueError('ArrayField expects list or tuple, not %s' % type(value))
        return [self.inner_field.to_python(v, timezone_in_use) for v in value]

    def validate(self, value):
        for v in value:
            self.inner_field.validate(v)

    def to_db_string(self, value, quote=True):
        array = [self.inner_field.to_db_string(v, quote=True) for v in value]
        return '[' + comma_join(array) + ']'

    def get_sql(self, with_default_expression=True, db=None):
        sql = 'Array(%s)' % self.inner_field.get_sql(with_default_expression=False, db=db)
        if with_default_expression and self.codec and db and db.has_codec_support:
            sql+= ' CODEC(%s)' % self.codec
        return sql


class UUIDField(Field):

    class_default = UUID(int=0)
    db_type = 'UUID'

    def to_python(self, value, timezone_in_use):
        if isinstance(value, UUID):
            return value
        elif isinstance(value, bytes):
            return UUID(bytes=value)
        elif isinstance(value, str):
            return UUID(value)
        elif isinstance(value, int):
            return UUID(int=value)
        elif isinstance(value, tuple):
            return UUID(fields=value)
        else:
            raise ValueError('Invalid value for UUIDField: %r' % value)

    def to_db_string(self, value, quote=True):
        return escape(str(value), quote)


class IPv4Field(Field):

    class_default = 0
    db_type = 'IPv4'

    def to_python(self, value, timezone_in_use):
        if isinstance(value, IPv4Address):
            return value
        elif isinstance(value, (bytes, str, int)):
            return IPv4Address(value)
        else:
            raise ValueError('Invalid value for IPv4Address: %r' % value)

    def to_db_string(self, value, quote=True):
        return escape(str(value), quote)


class IPv6Field(Field):

    class_default = 0
    db_type = 'IPv6'

    def to_python(self, value, timezone_in_use):
        if isinstance(value, IPv6Address):
            return value
        elif isinstance(value, (bytes, str, int)):
            return IPv6Address(value)
        else:
            raise ValueError('Invalid value for IPv6Address: %r' % value)

    def to_db_string(self, value, quote=True):
        return escape(str(value), quote)


class NullableField(Field):

    class_default = None

    def __init__(self, inner_field, default=None, alias=None, materialized=None,
                 extra_null_values=None, codec=None):
        assert isinstance(inner_field, Field), "The first argument of NullableField must be a Field instance. Not: {}".format(inner_field)
        self.inner_field = inner_field
        self._null_values = [None]
        if extra_null_values:
            self._null_values.extend(extra_null_values)
        super(NullableField, self).__init__(default, alias, materialized, readonly=None, codec=codec)

    def to_python(self, value, timezone_in_use):
        if value == '\\N' or value in self._null_values:
            return None
        return self.inner_field.to_python(value, timezone_in_use)

    def validate(self, value):
        value in self._null_values or self.inner_field.validate(value)

    def to_db_string(self, value, quote=True):
        if value in self._null_values:
            return '\\N'
        return self.inner_field.to_db_string(value, quote=quote)

    def get_sql(self, with_default_expression=True, db=None):
        sql = 'Nullable(%s)' % self.inner_field.get_sql(with_default_expression=False, db=db)
        if with_default_expression:
            sql += self._extra_params(db)
        return sql


class LowCardinalityField(Field):

    def __init__(self, inner_field, default=None, alias=None, materialized=None, readonly=None, codec=None):
        assert isinstance(inner_field, Field), "The first argument of LowCardinalityField must be a Field instance. Not: {}".format(inner_field)
        assert not isinstance(inner_field, LowCardinalityField), "LowCardinality inner fields are not supported by the ORM"
        assert not isinstance(inner_field, ArrayField), "Array field inside LowCardinality are not supported by the ORM. Use Array(LowCardinality) instead"
        self.inner_field = inner_field
        self.class_default = self.inner_field.class_default
        super(LowCardinalityField, self).__init__(default, alias, materialized, readonly, codec)

    def to_python(self, value, timezone_in_use):
        return self.inner_field.to_python(value, timezone_in_use)

    def validate(self, value):
        self.inner_field.validate(value)

    def to_db_string(self, value, quote=True):
        return self.inner_field.to_db_string(value, quote=quote)

    def get_sql(self, with_default_expression=True, db=None):
        if db and db.has_low_cardinality_support:
            sql = 'LowCardinality(%s)' % self.inner_field.get_sql(with_default_expression=False)
        else:
            sql = self.inner_field.get_sql(with_default_expression=False)
            logger.warning('LowCardinalityField not supported on clickhouse-server version < 19.0 using {} as fallback'.format(self.inner_field.__class__.__name__))
        if with_default_expression:
            sql += self._extra_params(db)
        return sql


# Expose only relevant classes in import *
__all__ = get_subclass_names(locals(), Field)
