from flex.exceptions import ValidationError
from flex.error_messages import MESSAGES
from flex.constants import (
    BOOLEAN,
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


@pull_keys_from_obj('type', 'uniqueItems')
@suffix_reserved_words
@skip_if_any_kwargs_empty('type_', 'uniqueItems')
def validate_type_for_unique_items(type_, uniqueItems, **kwargs):
    types = pluralize(type_)

    if not uniqueItems:
        return

    if not set(types).intersection((ARRAY,)):
        raise ValidationError(
            MESSAGES['type']['invalid_type_for_unique_items'],
        )


unique_items_schema = {
    'type': BOOLEAN,
}
unique_items_validators = construct_schema_validators(unique_items_schema, {})

unique_items_validator = generate_object_validator(
    field_validators=unique_items_validators,
)
