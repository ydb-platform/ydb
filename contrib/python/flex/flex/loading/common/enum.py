from flex.constants import (
    ARRAY,
)
from flex.validation.common import (
    generate_object_validator,
)
from flex.validation.schema import (
    construct_schema_validators,
)


enum_schema = {
    'type': ARRAY,
    'minItems': 1,
}
enum_validators = construct_schema_validators(enum_schema, {})

enum_validator = generate_object_validator(
    field_validators=enum_validators,
)
