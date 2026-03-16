import collections.abc
import json

from django.contrib.postgres.utils import prefix_validation_error
from django.core import checks, exceptions
from django.db.models import Field, Func, Value, lookups
from django.db.models.fields.mixins import CheckFieldDefaultMixin
from django.utils.translation import gettext_lazy as _

from .array import ArrayField
from .base import FieldMixin
from .integer import UInt64Field
from .utils import AttributeSetter

__all__ = ["MapField"]


class MapField(FieldMixin, CheckFieldDefaultMixin, Field):
    nullable_allowed = False
    empty_strings_allowed = False
    default_error_messages = {
        "key_invalid": _("Key '%(key)s' in the map did not validate:"),
        "value_invalid": _("Value '%(value)s' in the map did not validate:"),
    }
    _default_hint = ("dict", "{}")
    allowed_key_type = {
        "Int8Field",
        "Int16Field",
        "Int32Field",
        "Int64Field",
        "Int128Field",
        "Int256Field",
        "UInt8Field",
        "UInt16Field",
        "UInt32Field",
        "UInt64Field",
        "UInt128Field",
        "UInt256Field",
        "BooleanField",
        "StringField",
        "FixedStringField",
        "UUIDField",
        "ClickhouseDateField",
        "Date32Field",
        "ClickhouseDateTimeField",
        "DateTime64Field",
        "EnumField",
        "Enum8Field",
        "Enum16Field",
        "IPv4Field",
        "IPv6Field",
        "GenericIPAddressField",
    }

    def __init__(self, key_field, value_field, **kwargs):
        self.key_field = key_field
        self.value_field = value_field
        # For performance, only add a from_db_value() method if the base field
        # implements it.
        if hasattr(self.key_field, "from_db_value") or hasattr(
            self.value_field, "from_db_value"
        ):
            self.from_db_value = self._from_db_value
        super().__init__(**kwargs)

    def get_internal_type(self):
        return "MapField"

    @property
    def model(self):
        try:
            return self.__dict__["model"]
        except KeyError:
            raise AttributeError(
                "'%s' object has no attribute 'model'" % self.__class__.__name__
            )

    @model.setter
    def model(self, model):
        self.__dict__["model"] = model
        self.key_field.model = model
        self.value_field.model = model

    @classmethod
    def _choices_is_value(cls, value):
        return isinstance(value, dict) or super()._choices_is_value(value)

    def check(self, **kwargs):
        errors = super().check(**kwargs)
        if self.key_field.remote_field or self.value_field.remote_field:
            errors.append(
                checks.Error(
                    "Key field or value field for map cannot be a related field.",
                    obj=self,
                )
            )
        else:
            key_type = self.key_field.get_internal_type()
            if key_type not in self.allowed_key_type:
                errors.append(
                    checks.Error(
                        "This key field type is invalid.",
                        obj=self,
                    )
                )
            if self.key_field.null:
                errors.append(
                    checks.Error(
                        "Map key must not be null.",
                        obj=self,
                    )
                )
            if getattr(self.key_field, "low_cardinality", False) and key_type not in {
                "StringField",
                "FixedStringField",
            }:
                errors.append(
                    checks.Error(
                        "Only Map key of String and FixedString can be low cardinality.",
                        obj=self,
                    )
                )
            # Remove the field name checks as they are not needed here.
            base_errors = self.key_field.check()
            if base_errors:
                messages = "\n    ".join(
                    "%s (%s)" % (error.msg, error.id) for error in base_errors
                )
                errors.append(
                    checks.Error(
                        "Key field for map has errors:\n    %s" % messages,
                        obj=self,
                    )
                )
            base_errors = self.value_field.check()
            if base_errors:
                messages = "\n    ".join(
                    "%s (%s)" % (error.msg, error.id) for error in base_errors
                )
                errors.append(
                    checks.Error(
                        "Value field for map has errors:\n    %s" % messages,
                        obj=self,
                    )
                )
        return errors

    def set_attributes_from_name(self, name):
        super().set_attributes_from_name(name)
        self.key_field.set_attributes_from_name(name)
        self.value_field.set_attributes_from_name(name)

    @property
    def description(self):
        return "Map of (%s, %s)" % (
            self.key_field.description,
            self.value_field.description,
        )

    def db_type(self, connection):
        return "Map(%s, %s)" % (
            self.key_field.db_type(connection),
            self.value_field.db_type(connection),
        )

    def cast_db_type(self, connection):
        return "Map(%s, %s)" % (
            self.key_field.cast_db_type(connection),
            self.value_field.cast_db_type(connection),
        )

    def get_db_prep_value(self, value, connection, prepared=False):
        if isinstance(value, collections.abc.Mapping):
            return {
                self.key_field.get_db_prep_value(
                    k, connection, prepared=prepared
                ): self.value_field.get_db_prep_value(v, connection, prepared=prepared)
                for k, v in value.items()
            }
        return value

    def get_db_prep_save(self, value, connection):
        if hasattr(value, "as_sql"):
            return value
        if isinstance(value, collections.abc.Mapping):
            return {
                self.key_field.get_db_prep_save(
                    k, connection
                ): self.value_field.get_db_prep_save(v, connection)
                for k, v in value.items()
            }
        return value

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        if path.startswith("clickhouse_backend.models.map"):
            path = path.replace(
                "clickhouse_backend.models.map", "clickhouse_backend.models"
            )
        kwargs.update(
            {
                "key_field": self.key_field.clone(),
                "value_field": self.value_field.clone(),
            }
        )
        return name, path, args, kwargs

    def to_python(self, value):
        if isinstance(value, str):
            # Assume we're deserializing
            value = json.loads(value)
        if value is None:
            return value
        value = dict(value)
        return {
            self.key_field.to_python(k): self.value_field.to_python(v)
            for k, v in value.items()
        }

    @staticmethod
    def from_db_value_noop(value, expression, connection):
        return value

    def _from_db_value(self, value, expression, connection):
        if value is None:
            return value

        if hasattr(self.key_field, "from_db_value"):
            key_func = self.key_field.from_db_value
        else:
            key_func = self.from_db_value_noop
        if hasattr(self.value_field, "from_db_value"):
            value_func = self.value_field.from_db_value
        else:
            value_func = self.from_db_value_noop
        return {
            key_func(k, expression, connection): value_func(v, expression, connection)
            for k, v in value.items()
        }

    def value_to_string(self, obj):
        values = {}
        vals = self.value_from_object(obj)

        for k, v in vals.items():
            if k is not None:
                obj = AttributeSetter(self.key_field.attname, k)
                k = self.key_field.value_to_string(obj)
            if v is not None:
                obj = AttributeSetter(self.value_field.attname, v)
                v = self.value_field.value_to_string(obj)
            values[k] = v
        return json.dumps(values)

    def get_transform(self, name):
        transform = super().get_transform(name)
        if transform:
            return transform

        return KeyTransformFactory(name, self.key_field, self.value_field)

    def validate(self, value, model_instance):
        super().validate(value, model_instance)
        for key, value in value.items():
            try:
                self.key_field.validate(key, model_instance)
            except exceptions.ValidationError as error:
                raise prefix_validation_error(
                    error,
                    prefix=self.error_messages["key_invalid"],
                    code="key_invalid",
                    params={"key": key},
                )
            try:
                self.value_field.validate(value, model_instance)
            except exceptions.ValidationError as error:
                raise prefix_validation_error(
                    error,
                    prefix=self.error_messages["value_invalid"],
                    code="value_invalid",
                    params={"value": value},
                )

    def run_validators(self, value):
        super().run_validators(value)
        for key, value in value.items():
            try:
                self.key_field.run_validators(key)
            except exceptions.ValidationError as error:
                raise prefix_validation_error(
                    error,
                    prefix=self.error_messages["key_invalid"],
                    code="key_invalid",
                    params={"key": key},
                )
            try:
                self.value_field.run_validators(value)
            except exceptions.ValidationError as error:
                raise prefix_validation_error(
                    error,
                    prefix=self.error_messages["value_invalid"],
                    code="value_invalid",
                    params={"value": value},
                )


class MapRHSMixin:
    def __init__(self, lhs, rhs):
        if isinstance(rhs, collections.abc.Mapping):
            expressions = []
            field = lhs.output_field
            for key, value in rhs.items():
                if not hasattr(key, "resolve_expression"):
                    key = Value(field.key_field.get_prep_value(key))
                if not hasattr(value, "resolve_expression"):
                    value = Value(field.value_field.get_prep_value(value))
                expressions.append(key)
                expressions.append(value)
            rhs = Func(*expressions, function="map")
        super().__init__(lhs, rhs)

    def process_rhs(self, compiler, connection):
        rhs, rhs_params = super().process_rhs(compiler, connection)
        cast_type = self.lhs.output_field.cast_db_type(connection)
        return "%s::%s" % (rhs, cast_type), rhs_params


@MapField.register_lookup
class MapHasKey(lookups.FieldGetDbPrepValueMixin, lookups.Lookup):
    lookup_name = "has_key"
    prepare_rhs = False

    def as_clickhouse(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        params = (*lhs_params, *rhs_params)
        return "mapContains(%s, %s)" % (lhs, rhs), params


@MapField.register_lookup
class MapExact(MapRHSMixin, lookups.Exact):
    pass


@MapField.register_lookup
class MapLenTransform(lookups.Transform):
    lookup_name = "len"
    function = "length"
    output_field = UInt64Field()


@MapField.register_lookup
class KeysTransform(lookups.Transform):
    lookup_name = "keys"
    function = "mapKeys"

    @property
    def output_field(self):
        return ArrayField(self.lhs.output_field.key_field)


@MapField.register_lookup
class ValuesTransform(lookups.Transform):
    lookup_name = "values"
    function = "mapValues"

    @property
    def output_field(self):
        return ArrayField(self.lhs.output_field.value_field)


class KeyTransform(lookups.Transform):
    def __init__(self, key, key_field, value_field, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key
        self.key_field = key_field
        self.value_field = value_field

    def as_sql(self, compiler, connection):
        lhs, params = compiler.compile(self.lhs)
        key = self.key_field.get_db_prep_value(self.key, connection)
        return "%s[%%s]" % lhs, (*params, key)

    @property
    def output_field(self):
        return self.value_field


class KeyTransformFactory:
    def __init__(self, key, key_field, value_field):
        self.key = key
        self.key_field = key_field
        self.value_field = value_field

    def __call__(self, *args, **kwargs):
        return KeyTransform(self.key, self.key_field, self.value_field, *args, **kwargs)
