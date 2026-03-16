import ipaddress
from collections.abc import Iterable
from datetime import datetime

from django.core import checks, exceptions
from django.db.models import IntegerChoices, fields
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _

from clickhouse_backend.validators import MaxBytesValidator

from .array import *  # noqa: F403
from .base import FieldMixin
from .integer import *  # noqa: F403
from .json import *  # noqa: F403
from .map import *  # noqa: F403
from .tuple import *  # noqa: F403

__all__ = [  # noqa: F405
    "Int8Field",
    "UInt8Field",
    "Int16Field",
    "UInt16Field",
    "Int32Field",
    "UInt32Field",
    "Int64Field",
    "UInt64Field",
    "Int128Field",
    "UInt128Field",
    "Int256Field",
    "UInt256Field",
    "Float32Field",
    "Float64Field",
    "DecimalField",
    "BoolField",
    "StringField",
    "FixedStringField",
    "UUIDField",
    "DateField",
    "Date32Field",
    "DateTimeField",
    "DateTime64Field",
    "Enum8Field",
    "Enum16Field",
    "EnumField",
    "IPv4Field",
    "IPv6Field",
    "GenericIPAddressField",
    "ArrayField",
    "TupleField",
    "MapField",
    "JSONField",
]


class Float32Field(FieldMixin, fields.FloatField):
    description = _("Single precision floating point number")

    def __init__(self, *args, low_cardinality=False, **kwargs):
        self.low_cardinality = low_cardinality
        super().__init__(*args, **kwargs)

    def get_internal_type(self):
        return "Float32Field"


class Float64Field(FieldMixin, fields.FloatField):
    description = _("Double precision floating point number")

    def __init__(self, *args, low_cardinality=False, **kwargs):
        self.low_cardinality = low_cardinality
        super().__init__(*args, **kwargs)

    def get_internal_type(self):
        return "Float64Field"


class DecimalField(FieldMixin, fields.DecimalField):
    pass


class BoolField(FieldMixin, fields.BooleanField):
    def __init__(self, *args, low_cardinality=False, **kwargs):
        self.low_cardinality = low_cardinality
        super().__init__(*args, **kwargs)


class StringField(FieldMixin, fields.TextField):
    description = _("Binary string")

    def __init__(self, *args, low_cardinality=False, **kwargs):
        self.low_cardinality = low_cardinality
        super().__init__(*args, **kwargs)

    def get_internal_type(self):
        return "StringField"

    def to_python(self, value):
        if isinstance(value, (str, bytes)) or value is None:
            return value
        return str(value)


class FixedStringField(FieldMixin, fields.TextField):
    description = _("Binary string (up to %(max_bytes)s bytes)")

    def __init__(self, *args, max_bytes=None, low_cardinality=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_bytes = max_bytes
        self.low_cardinality = low_cardinality
        self.validators.append(MaxBytesValidator(self.max_bytes))

    def check(self, **kwargs):
        return [
            *super().check(**kwargs),
            *self._check_max_bytes(),
        ]

    def _check_max_bytes(self):
        if self.max_bytes is None:
            return [
                checks.Error(
                    "FixedStringField must define a 'max_bytes' attribute.",
                    obj=self,
                )
            ]
        elif (
            not isinstance(self.max_bytes, int)
            or isinstance(self.max_bytes, bool)
            or self.max_bytes <= 0
        ):
            return [
                checks.Error(
                    "'max_bytes' must be a positive integer.",
                    obj=self,
                )
            ]
        else:
            return []

    def get_internal_type(self):
        return "FixedStringField"

    def to_python(self, value):
        if isinstance(value, (str, bytes)) or value is None:
            return value
        return str(value)

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        kwargs["max_bytes"] = self.max_bytes
        return name, path, args, kwargs


class UUIDField(FieldMixin, fields.UUIDField):
    def __init__(self, *args, low_cardinality=False, **kwargs):
        self.low_cardinality = low_cardinality
        super().__init__(*args, **kwargs)


# class DateFieldMixin(FieldMixin):
#     def get_db_prep_save(self, value, connection):
#         """Clickhouse Date/Date32 support integer or float value
#         when insert or update (not in where clause)."""
#         if isinstance(value, (float, int)):
#             return value
#         return super().get_db_prep_save(value, connection)


class DateField(FieldMixin, fields.DateField):
    """Support integer or float may cause strange behavior,
    as integer and float are always treated as UTC timestamps,
    clickhouse store them as date in utc, which may not be what you want.
    """

    def __init__(self, *args, low_cardinality=False, **kwargs):
        self.low_cardinality = low_cardinality
        super().__init__(*args, **kwargs)

    def get_internal_type(self):
        return "ClickhouseDateField"


class Date32Field(FieldMixin, fields.DateField):
    def __init__(self, *args, low_cardinality=False, **kwargs):
        self.low_cardinality = low_cardinality
        super().__init__(*args, **kwargs)

    def get_internal_type(self):
        return "Date32Field"


class DateTimeMixin(FieldMixin):
    def get_prep_value(self, value):
        """Clickhouse DateTime('timezone')/DateTime64('timezone')
        support integer or float value in query.

        Integer and float numbers are always treated as UTC timestamps.
        """
        if isinstance(value, (int, float)):
            return value
        return super().get_prep_value(value)


class DateTimeField(DateTimeMixin, fields.DateTimeField):
    def __init__(self, *args, low_cardinality=False, **kwargs):
        self.low_cardinality = low_cardinality
        super().__init__(*args, **kwargs)

    def get_internal_type(self):
        return "ClickhouseDateTimeField"

    def get_prep_value(self, value):
        """For the sake of distinguishing DateTime from DateTime64,
        DateTime value always have microsecond = 0.
        Time string with milliseconds part will cause
        DB::Exception: Cannot convert string 2022-01-01 00:00:00.0 to type DateTime.
        """
        value = super().get_prep_value(value)
        if isinstance(value, datetime):
            value = value.replace(microsecond=0)
        elif isinstance(value, float):
            return int(value)
        return value


class DateTime64Field(DateTimeMixin, fields.DateTimeField):
    DEFAULT_PRECISION = 6

    def __init__(self, *args, precision=DEFAULT_PRECISION, **kwargs):
        super().__init__(*args, **kwargs)
        self.precision = precision

    def check(self, **kwargs):
        return [
            *super().check(**kwargs),
            *self._check_precision(),
        ]

    def _check_precision(self):
        if (
            not isinstance(self.precision, int)
            or isinstance(self.precision, bool)
            or self.precision < 0
            or self.precision > 9
        ):
            return [
                checks.Error(
                    "'precision' must be an integer, valid range: [ 0 : 9 ].",
                    obj=self,
                )
            ]
        else:
            return []

    def get_internal_type(self):
        return "DateTime64Field"

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        if self.precision != self.DEFAULT_PRECISION:
            kwargs["precision"] = self.precision
        return name, path, args, kwargs


class EnumField(FieldMixin, fields.Field):
    description = _("Integer enum value")
    MIN_INT = -32768
    MAX_INT = 32767

    def __init__(self, *args, return_int=True, **kwargs):
        """Use return_int to control whether to get an int or str value when querying from the database."""
        self.return_int = return_int
        super().__init__(*args, **kwargs)

    def _check_choices(self):
        """Note: although clickhouse support arbitrary bytes in Enum name,
        but clickhouse-driver 0.2.5 will raise UnicodeDecodeError when execute query."""

        invalid_errors = [
            checks.Error(
                "'choices' must be an iterable containing (int, str) tuples.",
                obj=self,
                id="fields.E005",
            )
        ]
        if not isinstance(self.choices, Iterable) or isinstance(self.choices, str):
            return invalid_errors

        if not self.choices:
            return [
                checks.Error(
                    f"{self.__class__.__name__} must define a 'choices' attribute.",
                    obj=self,
                )
            ]

        normal_choices = []
        for choice in self.choices:
            try:
                value, name = choice
            except (TypeError, ValueError):
                return invalid_errors
            if not isinstance(value, int) or not isinstance(name, (str, bytes)):
                return invalid_errors
            if value < self.MIN_INT or value > self.MAX_INT:
                return [
                    checks.Error(
                        "'choices' must be in range: [ %s : %s ]."
                        % (self.MIN_INT, self.MAX_INT),
                        obj=self,
                    )
                ]
            if isinstance(name, bytes):
                try:
                    name = name.decode("utf-8")
                except UnicodeDecodeError:
                    pass
            normal_choices.append((value, name))

        normal_choices.sort(key=lambda o: o[0])
        self.choices = normal_choices
        return []

    def get_internal_type(self):
        return "EnumField"

    def db_type(self, connection):
        self._check_backend(connection)
        type_name = connection.data_types[self.get_internal_type()]
        qv = connection.schema_editor().quote_value
        enum_string = ", ".join(
            "%s=%s" % (qv(name), value) for value, name in self.choices
        )
        base_type = "%s(%s)" % (type_name, enum_string)
        return self._nested_type(base_type)

    @cached_property
    def _name_value_map(self):
        return {name: value for value, name in self.choices}

    def value_to_string(self, obj):
        value = self.value_from_object(obj)
        if isinstance(value, int):
            return value
        if isinstance(value, bytes):
            try:
                value = value.decode("utf-8")
            except UnicodeDecodeError:
                pass
        return self._name_value_map[value]

    def get_prep_value(self, value):
        if isinstance(value, bytes):
            try:
                value = value.decode("utf-8")
            except UnicodeDecodeError:
                pass
        elif isinstance(value, IntegerChoices):
            value = value.value
        return value

    def from_db_value(self, value, expression, connection):
        """Clickhouse return enum name for Enum column."""
        if value is None or not self.return_int:
            return value
        return self._name_value_map[value]


class Enum8Field(EnumField):
    description = _("8bit integer enum value")
    MIN_INT = -128
    MAX_INT = 127

    def get_internal_type(self):
        return "Enum8Field"


class Enum16Field(EnumField):
    description = _("16bit integer enum value")

    def get_internal_type(self):
        return "Enum16Field"


class IPv4Field(FieldMixin, fields.GenericIPAddressField):
    description = _("IPv4 address")

    def __init__(self, *args, low_cardinality=False, **kwargs):
        kwargs["protocol"] = "ipv4"
        kwargs["unpack_ipv4"] = False
        self.low_cardinality = low_cardinality
        super().__init__(*args, **kwargs)
        self.max_length = 15

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        for key in ["unpack_ipv4", "protocol", "max_length"]:
            try:
                del kwargs[key]
            except KeyError:
                pass
        return name, path, args, kwargs

    def get_internal_type(self):
        return "IPv4Field"

    def from_db_value(self, value, expression, connection):
        if value is None:
            return value
        return str(value)

    def get_prep_value(self, value):
        value = super(fields.GenericIPAddressField, self).get_prep_value(value)
        if value is None or isinstance(value, ipaddress.IPv4Address):
            return value
        elif isinstance(value, ipaddress.IPv6Address):
            raise exceptions.ValidationError(
                _("This is not a valid IPv4 address."), code="invalid"
            )

        try:
            return ipaddress.IPv4Address(value)
        except ValueError:
            raise exceptions.ValidationError(
                _("This is not a valid IPv4 address."), code="invalid"
            )


class IPv6Field(FieldMixin, fields.GenericIPAddressField):
    description = _("IPv6 address")

    def __init__(self, *args, low_cardinality=False, **kwargs):
        kwargs["protocol"] = "ipv6"
        kwargs["unpack_ipv4"] = False
        self.low_cardinality = low_cardinality
        super().__init__(*args, **kwargs)
        kwargs["max_length"] = 39

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        for key in ["unpack_ipv4", "protocol", "max_length"]:
            try:
                del kwargs[key]
            except KeyError:
                pass
        return name, path, args, kwargs

    def get_internal_type(self):
        return "IPv6Field"

    def from_db_value(self, value, expression, connection):
        if value is None:
            return value
        return str(value)

    def get_prep_value(self, value):
        value = super(fields.GenericIPAddressField, self).get_prep_value(value)
        if value is None:
            return None

        if isinstance(value, str):
            try:
                value = ipaddress.ip_address(value)
            except ValueError:
                raise exceptions.ValidationError(
                    _("This is not a valid IP address."), code="invalid"
                )

        if isinstance(value, ipaddress.IPv4Address):
            value = ipaddress.IPv6Address("::ffff:%s" % value)
        return value


class GenericIPAddressField(FieldMixin, fields.GenericIPAddressField):
    def __init__(self, *args, low_cardinality=False, **kwargs):
        self.low_cardinality = low_cardinality
        super().__init__(*args, **kwargs)

    def db_type(self, connection):
        if self.protocol.lower() == "ipv4":
            return "IPv4"
        else:
            return "IPv6"

    def from_db_value(self, value, expression, connection):
        if value is None:
            return value
        if self.unpack_ipv4 and value.ipv4_mapped:
            return str(value.ipv4_mapped)
        return str(value)

    def get_prep_value(self, value):
        value = super(fields.GenericIPAddressField, self).get_prep_value(value)
        if value is None:
            return None

        if isinstance(value, str):
            try:
                value = ipaddress.ip_address(value)
            except ValueError:
                raise exceptions.ValidationError(
                    _("This is not a valid IP address."), code="invalid"
                )

        if isinstance(value, ipaddress.IPv4Address) and self.protocol.lower() != "ipv4":
            value = ipaddress.IPv6Address("::ffff:%s" % value)
        elif (
            isinstance(value, ipaddress.IPv6Address) and self.protocol.lower() == "ipv4"
        ):
            if value.ipv4_mapped is None:
                raise exceptions.ValidationError(
                    _("This is not a valid IPv4 address."), code="invalid"
                )
            value = value.ipv4_mapped
        return value
