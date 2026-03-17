from flex.constants import (
    ARRAY,
    SCHEMES,
)
from flex.exceptions import ValidationError
from flex.datastructures import (
    ValidationList,
)
from flex.error_messages import MESSAGES
from flex.validation.common import (
    generate_object_validator,
)
from flex.decorators import (
    skip_if_empty,
    skip_if_not_of_type,
)


@skip_if_empty
@skip_if_not_of_type(ARRAY)
def validate_schemes(schemes, **kwargs):
    for value in schemes:
        if value not in SCHEMES:
            raise ValidationError(
                MESSAGES['schemes']['invalid'].format(value),
            )


scheme_schema = {
    'type': ARRAY,
}

non_field_validators = ValidationList()
non_field_validators.add_validator(validate_schemes)

schemes_validator = generate_object_validator(
    schema=scheme_schema,
    non_field_validators=non_field_validators,
)
