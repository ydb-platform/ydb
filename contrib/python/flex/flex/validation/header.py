from flex.datastructures import (
    ValidationDict,
)
from flex.validation.common import (
    generate_type_validator,
    generate_format_validator,
    generate_maximum_validator,
    generate_minimum_validator,
    generate_min_length_validator,
    generate_max_length_validator,
    generate_pattern_validator,
    generate_min_items_validator,
    generate_max_items_validator,
    generate_unique_items_validator,
    generate_enum_validator,
    generate_multiple_of_validator,
)
from flex.validation.schema import (
    generate_items_validator,
)


validator_mapping = {
    # TODO: for array types, the value will come through as a string that *may*
    # be delimited.  Need to split on the delimiter and turn it into an array
    # before running validation.
    'type': generate_type_validator,
    'format': generate_format_validator,
    'items': generate_items_validator,
    'maximum': generate_maximum_validator,
    'minimum': generate_minimum_validator,
    'minLength': generate_min_length_validator,
    'maxLength': generate_max_length_validator,
    'pattern': generate_pattern_validator,
    'minItems': generate_min_items_validator,
    'maxItems': generate_max_items_validator,
    'uniqueItems': generate_unique_items_validator,
    'enum': generate_enum_validator,
    'multipleOf': generate_multiple_of_validator,
    # TODO:
    # collectionFormat
}


def construct_header_validators(header_definition, context):
    validators = ValidationDict()

    for key in header_definition:
        if key in validator_mapping:
            validators.add_validator(
                key,
                validator_mapping[key](context=context, **header_definition),
            )

    return validators
