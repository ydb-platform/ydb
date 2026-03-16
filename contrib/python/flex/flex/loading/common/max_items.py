from flex.exceptions import ValidationError
from flex.error_messages import MESSAGES
from flex.constants import (
    INTEGER,
    EMPTY,
    ARRAY,
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


@pull_keys_from_obj('minItems', 'maxItems')
def validate_max_items_greater_than_or_equal_to_min_items(minItems, maxItems, **kwargs):
    if minItems is EMPTY or maxItems is EMPTY:
        return
    if not maxItems >= minItems:
        raise ValidationError(
            MESSAGES['max_items']['must_be_greater_than_min_items']
        )


@pull_keys_from_obj('type', 'maxItems')
@suffix_reserved_words
@skip_if_any_kwargs_empty('type_', 'maxItems')
def validate_type_for_max_items(type_, maxItems, **kwargs):
    types = pluralize(type_)

    if not set(types).intersection((ARRAY,)):
        raise ValidationError(
            MESSAGES['type']['invalid_type_for_max_items'],
        )


max_items_schema = {
    'type': INTEGER,
}
max_items_validators = construct_schema_validators(max_items_schema, {})

max_items_validator = generate_object_validator(
    field_validators=max_items_validators,
)
