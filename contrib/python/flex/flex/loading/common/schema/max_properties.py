from flex.exceptions import ValidationError
from flex.error_messages import MESSAGES
from flex.constants import (
    INTEGER,
    EMPTY,
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


@pull_keys_from_obj('minProperties', 'maxProperties')
def validate_max_properties_is_greater_than_or_equal_to_min_properties(minProperties,
                                                                       maxProperties,
                                                                       **kwargs):
    if maxProperties is EMPTY or minProperties is EMPTY:
        return
    if not maxProperties >= minProperties:
        raise ValidationError(
            MESSAGES['max_properties']['must_be_greater_than_min_properties'],
        )


@pull_keys_from_obj('type', 'maxProperties')
@suffix_reserved_words
@skip_if_any_kwargs_empty('type_', 'maxProperties')
def validate_type_for_max_properties(type_, maxProperties, **kwargs):
    types = pluralize(type_)

    if not set(types).intersection((OBJECT,)):
        raise ValidationError(
            MESSAGES['type']['invalid_type_for_max_properties'],
        )


max_properties_schema = {
    'type': INTEGER,
    'minimum': 0,
}
max_properties_validators = construct_schema_validators(max_properties_schema, {})
max_properties_validator = generate_object_validator(
    field_validators=max_properties_validators,
)
