import functools

from flex.datastructures import (
    ValidationList,
)
from flex.constants import OBJECT
from flex.validation.common import (
    generate_object_validator,
    apply_validator_to_object,
)
from .schema import (
    schema_validator,
)


schema_definitions_schema = {
    'type': OBJECT,
}

non_field_validators = ValidationList()
non_field_validators.add_validator(
    functools.partial(apply_validator_to_object, validator=schema_validator),
)

schema_definitions_validator = generate_object_validator(
    schema=schema_definitions_schema,
    non_field_validators=non_field_validators,
)
