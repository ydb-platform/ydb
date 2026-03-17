from flex.exceptions import ValidationError
from flex.error_messages import MESSAGES
from flex.constants import (
    INTEGER,
    OBJECT,
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


@pull_keys_from_obj('type', 'minProperties')
@suffix_reserved_words
@skip_if_any_kwargs_empty('type_', 'minProperties')
def validate_type_for_min_properties(type_, minProperties, **kwargs):
    types = pluralize(type_)

    if not set(types).intersection((OBJECT,)):
        raise ValidationError(
            MESSAGES['type']['invalid_type_for_min_properties'],
        )


min_properties_schema = {
    'type': INTEGER,
    'minimum': 0,
}
min_properties_validators = construct_schema_validators(min_properties_schema, {})
min_properties_validator = generate_object_validator(
    field_validators=min_properties_validators,
)
