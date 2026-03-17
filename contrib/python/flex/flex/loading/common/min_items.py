from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.constants import (
    ARRAY,
    INTEGER,
)
from flex.utils import pluralize
from flex.validation.common import (
    generate_object_validator,
)
from flex.validation.schema import (
    construct_schema_validators,
)
from flex.decorators import (
    pull_keys_from_obj,
    suffix_reserved_words,
    skip_if_any_kwargs_empty,
)


@pull_keys_from_obj('type', 'minItems')
@suffix_reserved_words
@skip_if_any_kwargs_empty('type_', 'minItems')
def validate_type_for_min_items(type_, minItems, **kwargs):
    types = pluralize(type_)

    if not set(types).intersection((ARRAY,)):
        raise ValidationError(
            MESSAGES['type']['invalid_type_for_min_items'],
        )


min_items_schema = {
    'type': INTEGER,
    'minimum': 0,
}
min_items_validators = construct_schema_validators(min_items_schema, {})

min_items_validator = generate_object_validator(
    field_validators=min_items_validators,
)
