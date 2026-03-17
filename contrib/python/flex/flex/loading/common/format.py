from flex.constants import (
    STRING,
)
from flex.validation.common import (
    generate_object_validator,
)
from flex.validation.schema import (
    construct_schema_validators,
)


format_schema = {
    'type': STRING,
}
format_validators = construct_schema_validators(format_schema, {})

format_validator = generate_object_validator(
    field_validators=format_validators,
)
