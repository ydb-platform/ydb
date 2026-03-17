from flex.constants import (
    BOOLEAN,
)
from flex.validation.common import (
    generate_object_validator,
)
from flex.validation.schema import (
    construct_schema_validators,
)


read_only_schema = {
    'type': BOOLEAN,
}
read_only_validators = construct_schema_validators(read_only_schema, {})

read_only_validator = generate_object_validator(
    field_validators=read_only_validators,
)
