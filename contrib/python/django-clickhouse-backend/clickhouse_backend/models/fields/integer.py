from django.core.validators import MaxValueValidator, MinValueValidator
from django.db.models import fields
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _

from clickhouse_backend.backend.operations import DatabaseOperations

from .base import FieldMixin

__all__ = [
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
]


class IntegerFieldMixin(FieldMixin):
    """
    Make positive integer have correct limitation corresponding to clickhouse uint type.
    """

    @cached_property
    def validators(self):
        validators_ = [*self.default_validators, *self._validators]
        internal_type = self.get_internal_type()
        min_value, max_value = DatabaseOperations.integer_field_ranges[internal_type]
        if min_value is not None and not any(
            (
                isinstance(validator, MinValueValidator)
                and (
                    validator.limit_value()
                    if callable(validator.limit_value)
                    else validator.limit_value
                )
                >= min_value
            )
            for validator in validators_
        ):
            validators_.append(MinValueValidator(min_value))
        if max_value is not None and not any(
            (
                isinstance(validator, MaxValueValidator)
                and (
                    validator.limit_value()
                    if callable(validator.limit_value)
                    else validator.limit_value
                )
                <= max_value
            )
            for validator in validators_
        ):
            validators_.append(MaxValueValidator(max_value))
        return validators_

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        if path.startswith("clickhouse_backend.models.integer"):
            path = path.replace(
                "clickhouse_backend.models.integer", "clickhouse_backend.models"
            )
        return name, path, args, kwargs


class BaseIntegerField(fields.IntegerField):
    def __init__(self, *args, low_cardinality=False, **kwargs):
        self.low_cardinality = low_cardinality
        super().__init__(*args, **kwargs)


class Int8Field(IntegerFieldMixin, BaseIntegerField):
    description = _("8bit integer")

    def get_internal_type(self):
        return "Int8Field"


class UInt8Field(IntegerFieldMixin, BaseIntegerField):
    description = _("8bit unsigned integer")

    def get_internal_type(self):
        return "UInt8Field"


class Int16Field(IntegerFieldMixin, BaseIntegerField):
    description = _("16bit integer")

    def get_internal_type(self):
        return "Int16Field"


class UInt16Field(IntegerFieldMixin, BaseIntegerField):
    description = _("16bit unsigned integer")

    def get_internal_type(self):
        return "UInt16Field"


class Int32Field(IntegerFieldMixin, BaseIntegerField):
    description = _("32bit integer")

    def get_internal_type(self):
        return "Int32Field"


class UInt32Field(IntegerFieldMixin, BaseIntegerField):
    description = _("32bit unsigned integer")

    def get_internal_type(self):
        return "UInt32Field"


class Int64Field(IntegerFieldMixin, BaseIntegerField):
    description = _("64bit integer")

    def get_internal_type(self):
        return "Int64Field"


class UInt64Field(IntegerFieldMixin, BaseIntegerField):
    description = _("64bit unsigned integer")

    def get_internal_type(self):
        return "UInt64Field"


class Int128Field(IntegerFieldMixin, BaseIntegerField):
    description = _("128bit integer")

    def get_internal_type(self):
        return "Int128Field"


class UInt128Field(IntegerFieldMixin, BaseIntegerField):
    description = _("128bit unsigned integer")

    def get_internal_type(self):
        return "UInt128Field"


class Int256Field(IntegerFieldMixin, BaseIntegerField):
    description = _("256bit integer")

    def get_internal_type(self):
        return "Int256Field"


class UInt256Field(IntegerFieldMixin, BaseIntegerField):
    description = _("256bit unsigned integer")

    def get_internal_type(self):
        return "UInt256Field"
