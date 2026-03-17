import functools

from flex.datastructures import (
    ValidationDict,
    ValidationList,
)
from flex.constants import (
    OBJECT,
)
from flex.validation.common import (
    generate_object_validator,
    apply_validator_to_object,
)
from flex.validation.utils import (
    generate_any_validator,
)
from flex.loading.common.reference import (
    reference_object_validator,
)
from .single import (
    single_response_validator,
)


responses_schema = {
    'type': OBJECT,
}


field_validators = ValidationDict()

field_validators.add_property_validator(
    'default',
    generate_any_validator(
        referenceObject=reference_object_validator,
        responseObject=single_response_validator,
    ),
)


non_field_validators = ValidationList()
non_field_validators.add_validator(
    functools.partial(
        apply_validator_to_object,
        validator=generate_any_validator(
            referenceObject=reference_object_validator,
            responseObject=single_response_validator,
        ),
    )
)


responses_validator = generate_object_validator(
    schema=responses_schema,
    field_validators=field_validators,
    non_field_validators=non_field_validators,
)
