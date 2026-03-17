from flex.exceptions import ValidationError
from flex.error_messages import MESSAGES
from flex.datastructures import (
    ValidationList,
)
from flex.constants import (
    STRING,
    ARRAY,
    NUMBER,
    INTEGER,
    BOOLEAN,
    FILE,
)
from flex.decorators import (
    skip_if_empty,
    skip_if_not_of_type,
)
from flex.validation.common import (
    generate_object_validator,
)

ALLOWED_TYPES = [STRING, NUMBER, INTEGER, BOOLEAN, ARRAY, FILE]

type_schema = {
    'type': [STRING, ARRAY],
    'items': {
        'type': STRING,
        'enum': ALLOWED_TYPES,
    },
}


@skip_if_not_of_type(STRING)
@skip_if_empty
def validate_type(_type, **kwargs):
    if _type not in ALLOWED_TYPES:
        raise ValidationError(MESSAGES['enum']['invalid'].format(_type, ALLOWED_TYPES))


non_field_validators = ValidationList()
non_field_validators.add_validator(validate_type)


type_validator = generate_object_validator(
    schema=type_schema,
    non_field_validators=non_field_validators,
)
