from flex.datastructures import (
    ValidationDict,
)
from flex.loading.common.single_header import (
    single_header_schema,
    single_header_field_validators as common_single_header_field_validators,
    single_header_non_field_validators as common_single_header_non_field_validators,
)
from flex.validation.common import (
    generate_object_validator,
)
from flex.loading.schema.paths.path_item.operation.responses.single.schema import (
    schema_validator,
    items_validator,
)


single_header_field_validators = ValidationDict()
single_header_field_validators.update(common_single_header_field_validators)

single_header_field_validators.add_property_validator('schema', schema_validator)
single_header_field_validators.add_property_validator('items', items_validator)


single_header_validator = generate_object_validator(
    schema=single_header_schema,
    field_validators=single_header_field_validators,
    non_field_validators=common_single_header_non_field_validators,
)
