import json
from collections.abc import Iterable

from django.contrib.postgres.utils import prefix_validation_error
from django.contrib.postgres.validators import ArrayMaxLengthValidator
from django.core import checks, exceptions
from django.db.models import Field, Func, Value, lookups
from django.db.models.fields.mixins import CheckFieldDefaultMixin
from django.utils.translation import gettext_lazy as _

from .base import FieldMixin
from .integer import UInt64Field
from .utils import AttributeSetter

__all__ = ["ArrayField"]


class ArrayField(FieldMixin, CheckFieldDefaultMixin, Field):
    nullable_allowed = False
    empty_strings_allowed = False
    default_error_messages = {
        "item_invalid": _("Item %(nth)s in the array did not validate:"),
    }
    _default_hint = ("list", "[]")

    def __init__(self, base_field, size=None, **kwargs):
        self.base_field = base_field
        self.size = size
        if self.size:
            self.default_validators = [
                *self.default_validators,
                ArrayMaxLengthValidator(self.size),
            ]
        # For performance, only add a from_db_value() method if the base field
        # implements it.
        if hasattr(self.base_field, "from_db_value"):
            self.from_db_value = self._from_db_value
        super().__init__(**kwargs)

    def get_internal_type(self):
        return "ArrayField"

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
        self.base_field.model = model

    @classmethod
    def _choices_is_value(cls, value):
        return isinstance(value, (list, tuple)) or super()._choices_is_value(value)

    def check(self, **kwargs):
        errors = super().check(**kwargs)
        if self.base_field.remote_field:
            errors.append(
                checks.Error(
                    "Base field for array cannot be a related field.",
                    obj=self,
                )
            )
        else:
            # Remove the field name checks as they are not needed here.
            base_errors = self.base_field.check()
            if base_errors:
                messages = "\n    ".join(
                    "%s (%s)" % (error.msg, error.id) for error in base_errors
                )
                errors.append(
                    checks.Error(
                        "Base field for array has errors:\n    %s" % messages,
                        obj=self,
                    )
                )
        return errors

    def set_attributes_from_name(self, name):
        super().set_attributes_from_name(name)
        self.base_field.set_attributes_from_name(name)

    @property
    def description(self):
        return "Array of %s" % self.base_field.description

    def db_type(self, connection):
        return "Array(%s)" % self.base_field.db_type(connection)

    def cast_db_type(self, connection):
        return "Array(%s)" % self.base_field.cast_db_type(connection)

    def get_db_prep_value(self, value, connection, prepared=False):
        if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
            return [
                self.base_field.get_db_prep_value(i, connection, prepared=prepared)
                for i in value
            ]
        return value

    def get_db_prep_save(self, value, connection):
        if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
            return [self.base_field.get_db_prep_save(i, connection) for i in value]
        return value

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        if path.startswith("clickhouse_backend.models.array"):
            path = path.replace(
                "clickhouse_backend.models.array", "clickhouse_backend.models"
            )
        kwargs["base_field"] = self.base_field.clone()
        if self.size:
            kwargs["size"] = self.size
        return name, path, args, kwargs

    def to_python(self, value):
        if isinstance(value, str):
            # Assume we're deserializing
            value = json.loads(value)
        if value is None:
            return value

        return [self.base_field.to_python(val) for val in value]

    def _from_db_value(self, value, expression, connection):
        if value is None:
            return value
        return [
            self.base_field.from_db_value(item, expression, connection)
            for item in value
        ]

    def value_to_string(self, obj):
        values = []
        vals = self.value_from_object(obj)
        base_field = self.base_field

        for val in vals:
            if val is None:
                values.append(None)
            else:
                obj = AttributeSetter(base_field.attname, val)
                values.append(base_field.value_to_string(obj))
        return json.dumps(values)

    def get_transform(self, name):
        transform = super().get_transform(name)
        if transform:
            return transform
        # Django always generate "table"."column".size0, which will cause clickhouse exception.
        # DB::Exception: There's no column 'table.column.size0' in table 'table':
        # While processing table.column.size0. (UNKNOWN_IDENTIFIER)
        # if name.startswith("size"):
        #     try:
        #         dimension = int(name[4:])
        #     except ValueError:
        #         pass
        #     else:
        #         return SizeTransformFactory(dimension)

        if "_" not in name:
            try:
                index = int(name)
            except ValueError:
                pass
            else:
                index += 1  # Clickhouse uses 1-indexing
                return IndexTransformFactory(index, self.base_field)
        try:
            start, end = name.split("_")
            start = int(start) + 1
            end = int(end)  # don't add one here because postgres slices are weird
        except ValueError:
            pass
        else:
            return SliceTransformFactory(start, end)

    def validate(self, value, model_instance):
        super().validate(value, model_instance)
        for index, part in enumerate(value):
            try:
                self.base_field.validate(part, model_instance)
            except exceptions.ValidationError as error:
                raise prefix_validation_error(
                    error,
                    prefix=self.error_messages["item_invalid"],
                    code="item_invalid",
                    params={"nth": index + 1},
                )

    def run_validators(self, value):
        super().run_validators(value)
        for index, part in enumerate(value):
            try:
                self.base_field.run_validators(part)
            except exceptions.ValidationError as error:
                raise prefix_validation_error(
                    error,
                    prefix=self.error_messages["item_invalid"],
                    code="item_invalid",
                    params={"nth": index + 1},
                )


class ArrayRHSMixin:
    def __init__(self, lhs, rhs):
        if isinstance(rhs, (tuple, list)):
            field = lhs.output_field
            expressions = []
            for value in rhs:
                if not hasattr(value, "resolve_expression"):
                    value = Value(field.base_field.get_prep_value(value))
                expressions.append(value)
            rhs = Func(*expressions, function="array")
        super().__init__(lhs, rhs)

    def process_rhs(self, compiler, connection):
        rhs, rhs_params = super().process_rhs(compiler, connection)
        cast_type = self.lhs.output_field.cast_db_type(connection)
        return "%s::%s" % (rhs, cast_type), rhs_params


class ArrayLookup(ArrayRHSMixin, lookups.FieldGetDbPrepValueMixin, lookups.Lookup):
    function = None
    swap_args = False

    def as_clickhouse(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        sql = "%s(%s, %s)" % (
            (self.function, rhs, lhs) if self.swap_args else (self.function, lhs, rhs)
        )
        params = (*lhs_params, *rhs_params)
        return sql, params


@ArrayField.register_lookup
class ArrayContains(ArrayLookup):
    lookup_name = "contains"
    function = "hasAll"


@ArrayField.register_lookup
class ArrayContainedBy(ArrayLookup):
    lookup_name = "contained_by"
    function = "hasAll"
    swap_args = True


@ArrayField.register_lookup
class ArrayExact(ArrayRHSMixin, lookups.Exact):
    pass


@ArrayField.register_lookup
class ArrayOverlap(ArrayLookup):
    lookup_name = "overlap"
    function = "hasAny"


@ArrayField.register_lookup
class ArrayAny(ArrayLookup):
    lookup_name = "any"
    function = "has"

    def process_rhs(self, compiler, connection):
        rhs, rhs_params = super(ArrayRHSMixin, self).process_rhs(compiler, connection)
        cast_type = self.lhs.output_field.base_field.db_type(connection)
        return "%s::%s" % (rhs, cast_type), rhs_params


@ArrayField.register_lookup
class ArrayLenTransform(lookups.Transform):
    lookup_name = "len"
    function = "length"
    output_field = UInt64Field()


class IndexTransform(lookups.Transform):
    def __init__(self, index, base_field, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.index = index
        self.base_field = base_field

    def as_sql(self, compiler, connection):
        lhs, params = compiler.compile(self.lhs)
        return "%s[%%s]" % lhs, (*params, self.index)

    @property
    def output_field(self):
        return self.base_field


class IndexTransformFactory:
    def __init__(self, index, base_field):
        self.index = index
        self.base_field = base_field

    def __call__(self, *args, **kwargs):
        return IndexTransform(self.index, self.base_field, *args, **kwargs)


class SliceTransform(lookups.Transform):
    def __init__(self, start, end, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if start > end:
            start, end = end, start
        self.offset = start
        self.length = end - start + 1

    def as_sql(self, compiler, connection):
        lhs, params = compiler.compile(self.lhs)
        return "arraySlice({}, %s, %s)".format(lhs), (*params, self.offset, self.length)


class SliceTransformFactory:
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def __call__(self, *args, **kwargs):
        return SliceTransform(self.start, self.end, *args, **kwargs)


class SizeTransform(lookups.Transform):
    """https://clickhouse.com/docs/en/sql-reference/data-types/array#array-size"""

    def __init__(self, dimension, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dimension = dimension
        self._output_field = UInt64Field()
        while dimension > 0:
            self._output_field = ArrayField(self._output_field)
            dimension -= 1

    def as_sql(self, compiler, connection):
        lhs, params = compiler.compile(self.lhs)
        return "%s.size{}".format(self.dimension) % lhs, params

    @property
    def output_field(self):
        return self._output_field


class SizeTransformFactory:
    def __init__(self, dimension):
        self.dimension = dimension

    def __call__(self, *args, **kwargs):
        return SizeTransform(self.dimension, *args, **kwargs)
