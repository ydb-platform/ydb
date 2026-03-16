from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.constants import (
    EMPTY,
    INTEGER,
    STRING,
)
from flex.utils import pluralize
from flex.validation.common import (
    generate_object_validator,
)
from flex.decorators import (
    pull_keys_from_obj,
    suffix_reserved_words,
    skip_if_any_kwargs_empty,
)


@pull_keys_from_obj('minLength', 'maxLength')
@skip_if_any_kwargs_empty('minLength', 'maxLength')
def validate_max_length_greater_than_or_equal_to_min_length(minLength, maxLength, **kwargs):
    if minLength is EMPTY or maxLength is EMPTY:
        return

    if not maxLength >= minLength:
        raise ValidationError(
            MESSAGES['max_length']['must_be_greater_than_min_length']
        )


@pull_keys_from_obj('type', 'maxLength')
@suffix_reserved_words
@skip_if_any_kwargs_empty('type_', 'maxLength')
def validate_type_for_max_length(type_, maxLength, **kwargs):
    types = pluralize(type_)

    if not set(types).intersection((STRING,)):
        raise ValidationError(
            MESSAGES['type']['invalid_type_for_max_length'],
        )


max_length_schema = {
    'type': INTEGER,
    'minimum': 1,
}
max_length_validator = generate_object_validator(
    schema=max_length_schema,
)
