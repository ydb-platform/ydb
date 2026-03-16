from flex.datastructures import (
    ValidationDict,
)
from flex.constants import (
    OBJECT,
)
from flex.validation.common import (
    generate_object_validator,
)
from .operation import (
    operation_validator,
)
from .parameters import (
    parameters_validator,
)


path_item_schema = {
    'type': OBJECT,
}

non_field_validators = ValidationDict()
non_field_validators.add_property_validator('get', operation_validator)
non_field_validators.add_property_validator('put', operation_validator)
non_field_validators.add_property_validator('post', operation_validator)
non_field_validators.add_property_validator('delete', operation_validator)
non_field_validators.add_property_validator('options', operation_validator)
non_field_validators.add_property_validator('head', operation_validator)
non_field_validators.add_property_validator('patch', operation_validator)
non_field_validators.add_property_validator('parameters', parameters_validator)

path_item_validator = generate_object_validator(
    schema=path_item_schema,
    non_field_validators=non_field_validators,
)
