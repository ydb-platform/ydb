from flex.constants import (
    BOOLEAN,
)
from flex.validation.common import (
    generate_object_validator,
)
from flex.validation.schema import (
    construct_schema_validators,
)


required_schema = {
    'type': BOOLEAN,
}
required_validators = construct_schema_validators(required_schema, {})

required_validator = generate_object_validator(
    field_validators=required_validators,
)
