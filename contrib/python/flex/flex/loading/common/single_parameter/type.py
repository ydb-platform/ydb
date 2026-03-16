from flex.exceptions import (
    ValidationError,
    ErrorList,
)
from flex.constants import (
    NULL,
    BOOLEAN,
    INTEGER,
    NUMBER,
    STRING,
    ARRAY,
    FILE,
)
from flex.utils import (
    pluralize,
)
from flex.validation.common import (
    generate_object_validator,
)
from flex.decorators import (
    suffix_reserved_words,
    skip_if_not_of_type,
)
from flex.validation.schema import (
    construct_schema_validators,
)


single_type_schema = {
    'type': STRING,
    'enum': [
        NULL,
        BOOLEAN,
        INTEGER,
        NUMBER,
        STRING,
        ARRAY,
        FILE,
    ],
}
single_type_validators = construct_schema_validators(single_type_schema, {})

single_type_validator = generate_object_validator(
    field_validators=single_type_validators,
)


@suffix_reserved_words
@skip_if_not_of_type(ARRAY, STRING)
def validate_types(type_, **kwargs):
    types = pluralize(type_)

    with ErrorList() as errors:
        for value in types:
            try:
                single_type_validator(value)
            except ValidationError as err:
                errors.add_error(err.detail)


type_schema = {
    'type': [
        STRING,
        ARRAY,
    ],
}
type_validators = construct_schema_validators(type_schema, {})
type_validators.add_validator('type', validate_types)
type_validator = generate_object_validator(
    field_validators=type_validators,
)
