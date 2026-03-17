from flex.exceptions import (
    ValidationError,
)
from flex.error_messages import (
    MESSAGES,
)
from flex.constants import (
    NUMBER,
    INTEGER,
)
from flex.validation.common import (
    generate_object_validator,
)
from flex.utils import (
    pluralize,
)
from flex.decorators import (
    pull_keys_from_obj,
    suffix_reserved_words,
    skip_if_any_kwargs_empty,
)


multiple_of_schema = {
    'type': NUMBER,
    'minimum': 0,
}

multiple_of_validator = generate_object_validator(
    schema=multiple_of_schema,
)


@pull_keys_from_obj('type', 'multipleOf')
@suffix_reserved_words
@skip_if_any_kwargs_empty('type_', 'multipleOf')
def validate_type_for_multiple_of(type_, multipleOf, **kwargs):
    types = pluralize(type_)

    if not set(types).intersection((INTEGER, NUMBER)):
        raise ValidationError(
            MESSAGES['type']['invalid_type_for_multiple_of'],
        )
