import functools
from flex.datastructures import (
    ValidationList,
)
from flex.constants import (
    OBJECT,
)
from flex.validation.common import (
    generate_object_validator,
    apply_validator_to_object,
)
from .single import (
    single_parameter_validator,
)


parameters_schema = {
    'type': OBJECT,
}

parameters_non_field_validators = ValidationList()
parameters_non_field_validators.add_validator(
    functools.partial(apply_validator_to_object, validator=single_parameter_validator),
)

parameters_validator = generate_object_validator(
    schema=parameters_schema,
    non_field_validators=parameters_non_field_validators,
)
