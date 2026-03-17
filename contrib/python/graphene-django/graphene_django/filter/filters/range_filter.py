from django.core.exceptions import ValidationError
from django.forms import Field

from .typed_filter import TypedFilter


def validate_range(value):
    """
    Validator for range filter input: the list of value must be of length 2.
    Note that validators are only run if the value is not empty.
    """
    if len(value) != 2:
        raise ValidationError(
            "Invalid range specified: it needs to contain 2 values.", code="invalid"
        )


class RangeField(Field):
    default_validators = [validate_range]
    empty_values = [None]


class RangeFilter(TypedFilter):
    field_class = RangeField
