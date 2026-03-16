from flex.constants import (
    OBJECT,
    ARRAY,
)
from flex.datastructures import (
    ValidationDict,
)

from flex.loading.common.format import (
    format_validator,
)
from .title import (
    title_validator,
)
from flex.loading.common.default import (
    validate_default_is_of_one_of_declared_types,
)
from flex.loading.common.external_docs import (
    external_docs_validator,
)
from .min_properties import (
    min_properties_validator,
    validate_type_for_min_properties,
)
from .max_properties import (
    max_properties_validator,
    validate_max_properties_is_greater_than_or_equal_to_min_properties,
    validate_type_for_max_properties,
)
from .required import (
    required_validator,
)
from .type import (
    type_validator,
)
from .read_only import (
    read_only_validator,
)
from flex.loading.common import (
    field_validators as common_field_validators,
    non_field_validators as common_non_field_validators,
    type_validators as common_type_validators,
)

schema_schema = {
    'type': OBJECT,
    'properties': {
        'properties': {
            'type': OBJECT,
        },
    }
}

schema_field_validators = ValidationDict()
schema_field_validators.update(common_field_validators)

schema_field_validators.add_property_validator('format', format_validator)
schema_field_validators.add_property_validator('title', title_validator)
schema_field_validators.add_property_validator('minProperties', min_properties_validator)
schema_field_validators.add_property_validator('maxProperties', max_properties_validator)
schema_field_validators.add_property_validator('required', required_validator)
schema_field_validators.add_property_validator('type', type_validator)
schema_field_validators.add_property_validator('readOnly', read_only_validator)
schema_field_validators.add_property_validator('externalDocs', external_docs_validator)

schema_non_field_validators = ValidationDict()
schema_non_field_validators.update(common_non_field_validators)
schema_non_field_validators.update(common_type_validators)


schema_non_field_validators.add_validator(
    'default', validate_default_is_of_one_of_declared_types,
)
schema_non_field_validators.add_validator(
    'maxProperties', validate_max_properties_is_greater_than_or_equal_to_min_properties,
)
schema_non_field_validators.add_validator(
    'type', validate_type_for_min_properties,
)
schema_non_field_validators.add_validator(
    'type', validate_type_for_max_properties,
)


#
# Properties.
#
properties_schema = {
    'type': OBJECT,
}


#
# Items
#
items_schema = {
    'type': [
        ARRAY,  # Array of schemas
        OBJECT,  # Single Schema
    ]
}
