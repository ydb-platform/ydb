import functools

from flex.datastructures import ValidationList
from flex.constants import (
    OBJECT,
)
from flex.validation.common import (
    generate_object_validator,
    apply_validator_to_object,
)

from .single import (
    single_header_validator,
)


headers_schema = {
    'type': OBJECT,
}


non_field_validators = ValidationList()
non_field_validators.add_validator(
    functools.partial(apply_validator_to_object, validator=single_header_validator),
)


headers_validator = generate_object_validator(
    schema=headers_schema,
    non_field_validators=non_field_validators,
)
