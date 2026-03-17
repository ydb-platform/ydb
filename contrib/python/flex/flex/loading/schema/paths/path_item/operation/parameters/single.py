from flex.datastructures import (
    ValidationDict,
)
from flex.loading.common.single_parameter import (
    single_parameter_schema,
    single_parameter_field_validators as common_single_parameter_field_validators,
    single_parameter_non_field_validators as common_single_parameter_non_field_validators,
)
from flex.validation.common import (
    generate_object_validator,
)
from flex.loading.schema.paths.path_item.operation.responses.single.schema import (
    schema_validator,
    items_validator,
)


single_parameter_field_validators = ValidationDict()
single_parameter_field_validators.update(
    common_single_parameter_field_validators
)

# schema fields
single_parameter_field_validators.add_property_validator('schema', schema_validator)
single_parameter_field_validators.add_property_validator('items', items_validator)

single_parameter_non_field_validators = ValidationDict()
single_parameter_non_field_validators.update(
    common_single_parameter_non_field_validators
)

single_parameter_validator = generate_object_validator(
    schema=single_parameter_schema,
    field_validators=single_parameter_field_validators,
    non_field_validators=single_parameter_non_field_validators,
)
