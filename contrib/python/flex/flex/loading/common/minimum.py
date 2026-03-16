from flex.exceptions import ValidationError
from flex.constants import (
    INTEGER,
    NUMBER,
    EMPTY,
    BOOLEAN,
)
from flex.utils import (
    pluralize,
)
from flex.error_messages import MESSAGES
from flex.validation.common import (
    generate_object_validator,
)
from flex.decorators import (
    suffix_reserved_words,
    pull_keys_from_obj,
    skip_if_any_kwargs_empty,
)


@pull_keys_from_obj('minimum', 'exclusiveMinimum')
@skip_if_any_kwargs_empty('exclusiveMinimum')
def validate_minimum_required_if_exclusive_minimum_set(minimum, exclusiveMinimum, **kwargs):
    if exclusiveMinimum is True and minimum is EMPTY:
        raise ValidationError(
            MESSAGES['minimum']['exclusive_minimum_required_minimum'],
        )


@pull_keys_from_obj('type', 'minimum')
@suffix_reserved_words
@skip_if_any_kwargs_empty('type_', 'minimum')
def validate_type_for_minimum(type_, minimum, **kwargs):
    types = pluralize(type_)

    if not set(types).intersection((INTEGER, NUMBER)):
        raise ValidationError(
            MESSAGES['type']['invalid_type_for_minimum'],
        )


minimum_schema = {
    'type': NUMBER,
}
minimum_validator = generate_object_validator(
    schema=minimum_schema,
)

exclusive_minimum_schema = {
    'type': BOOLEAN,
}
exclusive_minimum_validator = generate_object_validator(
    schema=exclusive_minimum_schema,
)
