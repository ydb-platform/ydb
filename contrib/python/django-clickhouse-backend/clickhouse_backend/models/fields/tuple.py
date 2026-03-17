import collections
import collections.abc
import copy
import json
from collections.abc import Iterable

from django.contrib.postgres.utils import prefix_validation_error
from django.core import checks, exceptions
from django.db.models import Field, Func, Value, lookups
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _

from .base import FieldMixin
from .utils import AttributeSetter

__all__ = ["TupleField"]


class TupleField(FieldMixin, Field):
    nullable_allowed = False
    empty_strings_allowed = False
    default_error_messages = {
        "item_invalid": _("Item %(nth)s in the array did not validate:"),
        "value_length_mismatch": _("Value length does not match tuple length."),
    }

    def __init__(self, base_fields, **kwargs):
        self.base_fields = self._check_base_fields(base_fields)
        super().__init__(**kwargs)

    def _check_base_fields(self, base_fields):
        """Check base_fields type when init.

        Because some other init actions depend on correct base_fields type."""

        invalid_error = RuntimeError(
            "'base_fields' must be an iterable containing only(not both) "
            "field instances or (field name, field instance) tuples, "
            "and field name must be valid python identifier."
        )
        if not isinstance(base_fields, Iterable) or isinstance(base_fields, str):
            raise invalid_error

        fields = []
        named_field_flags = []
        for index, field in enumerate(base_fields, start=1):
            if isinstance(field, Field):
                fields.append(field)
                named_field_flags.append(False)
            else:
                try:
                    name, field = field
                except (TypeError, ValueError):
                    raise invalid_error
                if (
                    not isinstance(name, str)
                    or not name.isidentifier()
                    or not isinstance(field, Field)
                ):
                    raise invalid_error
                fields.append((name, field))
                named_field_flags.append(True)

            if field.remote_field:
                raise RuntimeError("Field %ss cannot be a related field." % index)

        if not fields:
            raise RuntimeError("'base_fields' must not be empty.")

        if all(named_field_flags):
            self.is_named_tuple = True
            self._base_fields = tuple(f for _, f in fields)
        elif not any(named_field_flags):
            self.is_named_tuple = False
            self._base_fields = fields
        else:
            raise invalid_error

        # For performance, only add a from_db_value() method if any base field
        # implements it.
        if any(hasattr(field, "from_db_value") for field in self._base_fields):
            self.from_db_value = self._from_db_value
        return fields

    def check(self, **kwargs):
        errors = super().check(**kwargs)
        if errors:
            return errors
        for index, field in enumerate(self._base_fields, 1):
            base_errors = field.check()
            if base_errors:
                messages = "\n    ".join(
                    "%s (%s)" % (error.msg, error.id) for error in base_errors
                )
                return [
                    checks.Error(
                        "Field %ss has errors:\n    %s" % (index, messages), obj=self
                    )
                ]

        return []

    def get_internal_type(self):
        return "TupleField"

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
        for field in self.base_fields:
            if isinstance(field, Field):
                field.model = model
            else:
                field[1].model = model

    @classmethod
    def _choices_is_value(cls, value):
        return isinstance(value, (list, tuple)) or super()._choices_is_value(value)

    def set_attributes_from_name(self, name):
        super().set_attributes_from_name(name)
        for field in self.base_fields:
            if isinstance(field, Field):
                field.set_attributes_from_name(name)
            else:
                field[1].set_attributes_from_name(name)
        if self.is_named_tuple:
            name = self.name
            if name:
                name = name.capitalize()
            else:
                name = "Tuple"
            self.container_class = collections.namedtuple(
                name, (fn for fn, _ in self.base_fields)
            )
        else:
            self.container_class = tuple

    def _convert_type(self, value):
        # When used as Expression output_field
        if not hasattr(self, "container_class"):
            self.set_attributes_from_name("")
        if value is None or isinstance(value, self.container_class):
            return value
        if self.is_named_tuple:
            if isinstance(value, dict):
                return self.container_class(**value)
            return self.container_class(*value)
        # From ClickHouse server 24.8 LTS, tuple("a", "b") returns NamedTuple.
        if isinstance(value, dict):
            return self.container_class(value.values())
        return self.container_class(value)

    @property
    def description(self):
        base_description = ", ".join(field.description for field in self._base_fields)
        return "Tuple of %s" % base_description

    def db_type(self, connection):
        base_type = ", ".join(
            "%s %s" % (field[0], field[1].db_type(connection))
            if self.is_named_tuple
            else field.db_type(connection)
            for field in self.base_fields
        )
        return "Tuple(%s)" % base_type

    def cast_db_type(self, connection):
        base_type = ", ".join(
            "%s %s" % (field[0], field[1].cast_db_type(connection))
            if self.is_named_tuple
            else field.cast_db_type(connection)
            for field in self.base_fields
        )
        return "Tuple(%s)" % base_type

    def call_base_fields(self, func_name, value, *args, **kwargs):
        if value is None:
            return value
        self._validate_length(value)
        values = []
        for i, field in zip(value, self._base_fields):
            values.append(getattr(field, func_name)(i, *args, **kwargs))
        return values

    def get_db_prep_value(self, value, connection, prepared=False):
        if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
            values = self.call_base_fields(
                "get_db_prep_value", value, connection, prepared=prepared
            )
            return tuple(values)
        return value

    def get_db_prep_save(self, value, connection):
        if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
            values = self.call_base_fields("get_db_prep_save", value, connection)
            return tuple(values)
        return value

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        if path.startswith("clickhouse_backend.models.tuple"):
            path = path.replace(
                "clickhouse_backend.models.tuple", "clickhouse_backend.models"
            )
        kwargs.update(
            {
                "base_fields": copy.deepcopy(self.base_fields),
            }
        )
        return name, path, args, kwargs

    def to_python(self, value):
        if isinstance(value, str):
            # Assume we're deserializing
            value = json.loads(value)
        if value is None:
            return value
        value = self.call_base_fields("to_python", value)
        return self._convert_type(value)

    def from_db_value(self, value, expression, connection):
        return self._convert_type(value)

    def _from_db_value(self, value, expression, connection):
        if value is None:
            return value
        self._validate_length(value)
        values = []
        # when set allow_experimental_object_type=1 at session level, value will be a dict.
        if isinstance(value, dict):
            value = value.values()
        for i, field in zip(value, self._base_fields):
            if hasattr(field, "from_db_value"):
                values.append(field.from_db_value(i, expression, connection))
            else:
                values.append(i)
        return self._convert_type(values)

    def value_to_string(self, obj):
        values = []
        vals = self.value_from_object(obj)
        self._validate_length(vals)

        for val, field in zip(vals, self._base_fields):
            if val is None:
                values.append(None)
            else:
                obj = AttributeSetter(field.attname, val)
                values.append(field.value_to_string(obj))
        return json.dumps(values)

    @cached_property
    def base_filed_map(self):
        if self.is_named_tuple:
            result = {}
            for i, (name, field) in enumerate(self.base_fields):
                result[i] = result[name] = field
            return result
        else:
            return dict(enumerate(self.base_fields))

    def get_transform(self, name):
        transform = super().get_transform(name)
        if transform:
            return transform
        try:
            name = int(name)
        except ValueError:
            field = self.base_filed_map.get(name)
        else:
            field = self.base_filed_map.get(name)
            name += 1  # Clickhouse uses 1-indexing
        if not field:
            return None
        return IndexTransformFactory(name, field)

    def _validate_length(self, values):
        if len(values) != len(self.base_fields):
            raise exceptions.ValidationError(
                code="value_length_mismatch",
                message=self.error_messages["value_length_mismatch"],
            )

    def validate(self, value, model_instance):
        super().validate(value, model_instance)
        self._validate_length(value)
        for index, (part, field) in enumerate(zip(value, self._base_fields)):
            try:
                field.validate(part, model_instance)
            except exceptions.ValidationError as error:
                raise prefix_validation_error(
                    error,
                    prefix=self.error_messages["item_invalid"],
                    code="item_invalid",
                    params={"nth": index + 1},
                )

    def run_validators(self, value):
        super().run_validators(value)
        self._validate_length(value)
        for index, (part, field) in enumerate(zip(value, self._base_fields)):
            try:
                field.run_validators(part)
            except exceptions.ValidationError as error:
                raise prefix_validation_error(
                    error,
                    prefix=self.error_messages["item_invalid"],
                    code="item_invalid",
                    params={"nth": index + 1},
                )


class TupleRHSMixin:
    def __init__(self, lhs, rhs):
        self.empty_qs = False
        if isinstance(rhs, (tuple, list)):
            expressions = []
            # If compare Tuple(Int8, String) to Tuple(Int8), just return empty.
            if len(rhs) != len(lhs.output_field._base_fields):
                self.empty_qs = True
            else:
                try:
                    for value, field in zip(rhs, lhs.output_field._base_fields):
                        if not hasattr(value, "resolve_expression"):
                            value = Value(field.get_prep_value(value))
                        expressions.append(value)
                    rhs = Func(*expressions, function="tuple")
                except exceptions.ValidationError:
                    self.empty_qs = True
        super().__init__(lhs, rhs)

    def process_rhs(self, compiler, connection):
        rhs, rhs_params = super().process_rhs(compiler, connection)
        cast_type = self.lhs.output_field.cast_db_type(connection)
        return "%s::%s" % (rhs, cast_type), rhs_params

    def as_clickhouse(self, compiler, connection):
        if self.empty_qs:
            return "false", []
        return self.as_sql(compiler, connection)


@TupleField.register_lookup
class TupleExact(TupleRHSMixin, lookups.Exact):
    pass


class IndexTransform(lookups.Transform):
    def __init__(self, index, base_field, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.index = index
        self.base_field = base_field

    def as_clickhouse(self, compiler, connection):
        lhs, params = compiler.compile(self.lhs)
        params = (*params, self.index)
        return f"tupleElement({lhs}, %s)", params

    @property
    def output_field(self):
        return self.base_field


class IndexTransformFactory:
    def __init__(self, index, base_field):
        self.index = index
        self.base_field = base_field

    def __call__(self, *args, **kwargs):
        return IndexTransform(self.index, self.base_field, *args, **kwargs)
