from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.constants import (
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


@pull_keys_from_obj('type', 'minLength')
@suffix_reserved_words
@skip_if_any_kwargs_empty('type_', 'minLength')
def validate_type_for_min_length(type_, minLength, **kwargs):
    types = pluralize(type_)

    if not set(types).intersection((STRING,)):
        raise ValidationError(
            MESSAGES['type']['invalid_type_for_min_length'],
        )


min_length_schema = {
    'type': INTEGER,
    'minimum': 0,
}
min_length_validator = generate_object_validator(
    schema=min_length_schema,
)
