from flex.datastructures import (
    ValidationDict,
)
from flex.constants import (
    OBJECT,
)
from flex.loading.common import (
    field_validators as common_field_validators,
    non_field_validators as common_non_field_validators,
    type_validators as common_type_validators,
)

from .in_ import (
    in_validator,
    validate_path_parameters_must_be_required,
    validate_body_parameters_must_include_a_schema,
    validate_type_declared_for_non_body_parameters,
)
from .name import (
    name_validator,
)
from .description import (
    description_validator,
)
from flex.loading.common.required import (
    required_validator,
)
from .type import (
    type_validator,
)
from flex.loading.common.format import (
    format_validator,
)
from .collection_format import (
    collection_format_validator,
    validate_collection_format_based_on_in_value,
)
from flex.loading.common.default import (
    validate_default_is_of_one_of_declared_types,
)
from .items import (
    validate_items_required_if_array_type,
)


single_parameter_schema = {
    'type': OBJECT,
    'required': [
        'name',
        'in',
    ],
}

single_parameter_field_validators = ValidationDict()
single_parameter_field_validators.update(common_field_validators)

single_parameter_field_validators.add_property_validator('in', in_validator)
single_parameter_field_validators.add_property_validator('name', name_validator)
single_parameter_field_validators.add_property_validator('description', description_validator)
single_parameter_field_validators.add_property_validator('required', required_validator)
single_parameter_field_validators.add_property_validator('type', type_validator)
single_parameter_field_validators.add_property_validator('format', format_validator)
single_parameter_field_validators.add_property_validator(
    'collectionFormat', collection_format_validator,
)

single_parameter_non_field_validators = ValidationDict()
single_parameter_non_field_validators.update(common_non_field_validators)
single_parameter_non_field_validators.update(common_type_validators)
single_parameter_non_field_validators.add_validator(
    'default', validate_default_is_of_one_of_declared_types,
)
single_parameter_non_field_validators.add_validator(
    'required', validate_path_parameters_must_be_required,
)
single_parameter_non_field_validators.add_validator(
    'schema', validate_body_parameters_must_include_a_schema,
)
single_parameter_non_field_validators.add_validator(
    'type', validate_type_declared_for_non_body_parameters,
)
single_parameter_non_field_validators.add_validator(
    'collectionFormat', validate_collection_format_based_on_in_value,
)
single_parameter_non_field_validators.add_validator(
    'items', validate_items_required_if_array_type,
)
