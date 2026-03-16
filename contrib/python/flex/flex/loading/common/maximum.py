from flex.exceptions import ValidationError
from flex.error_messages import MESSAGES
from flex.constants import (
    INTEGER,
    NUMBER,
    EMPTY,
    BOOLEAN
)
from flex.utils import (
    pluralize,
)
from flex.validation.common import (
    generate_object_validator,
)
from flex.decorators import (
    pull_keys_from_obj,
    suffix_reserved_words,
    skip_if_any_kwargs_empty,
)


@pull_keys_from_obj('minimum', 'maximum')
def validate_maximum_is_gte_minimum(minimum, maximum, **kwargs):
    if minimum is EMPTY or maximum is EMPTY:
        return
    if not maximum >= minimum:
        raise ValidationError(MESSAGES['maximum']['must_be_greater_than_minimum'])


@pull_keys_from_obj('maximum', 'exclusiveMaximum')
def validate_maximum_required_if_exclusive_maximum_set(maximum, exclusiveMaximum, **kwargs):
    if exclusiveMaximum is EMPTY:
        return
    if exclusiveMaximum is True and maximum is EMPTY:
        raise ValidationError(
            MESSAGES['maximum']['exclusive_maximum_required_maximum'],
        )


@pull_keys_from_obj('type', 'maximum')
@suffix_reserved_words
@skip_if_any_kwargs_empty('type_', 'maximum')
def validate_type_for_maximum(type_, maximum, **kwargs):
    types = pluralize(type_)

    if not set(types).intersection((INTEGER, NUMBER)):
        raise ValidationError(
            MESSAGES['type']['invalid_type_for_maximum'],
        )


maximum_schema = {
    'type': NUMBER,
}
maximum_validator = generate_object_validator(
    schema=maximum_schema,
)

exclusive_maximum_schema = {
    'type': BOOLEAN,
}
exclusive_maximum_validator = generate_object_validator(
    schema=exclusive_maximum_schema,
)
