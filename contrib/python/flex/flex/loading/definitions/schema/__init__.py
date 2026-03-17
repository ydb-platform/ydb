import functools

from flex.constants import (
    OBJECT,
    ARRAY,
)
from flex.decorators import (
    skip_if_not_of_type,
    skip_if_empty,
)
from flex.validation.common import (
    generate_object_validator,
    apply_validator_to_object,
    apply_validator_to_array,
)
from flex.datastructures import (
    ValidationDict,
    ValidationList,
)
from .ref import (
    ref_validator,
)
from flex.loading.common.schema import (
    schema_schema as common_schema_schema,
    schema_field_validators as common_schema_field_validators,
    schema_non_field_validators as common_schema_non_field_validators,
    properties_schema as common_properties_schema,
    items_schema as common_items_schema,
)

schema_field_validators = ValidationDict()
schema_field_validators.update(common_schema_field_validators)

schema_field_validators.add_property_validator('$ref', ref_validator)

schema_non_field_validators = ValidationDict()
schema_non_field_validators.update(common_schema_non_field_validators)


schema_validator = generate_object_validator(
    schema=common_schema_schema,
    field_validators=schema_field_validators,
    non_field_validators=schema_non_field_validators,
)


#
# Properties.
#
properties_non_field_validators = ValidationDict()
properties_non_field_validators.add_validator(
    'properties', functools.partial(apply_validator_to_object, validator=schema_validator),
)

properties_validator = generate_object_validator(
    schema=common_properties_schema,
    non_field_validators=properties_non_field_validators,
)

# Now put the properties validator onto the schema validator.
schema_field_validators.add_property_validator('properties', properties_validator)


#
# Items
#
items_non_field_validators = ValidationList()
items_non_field_validators.add_validator(
    skip_if_empty(skip_if_not_of_type(OBJECT)(schema_validator))
)
items_non_field_validators.add_validator(
    skip_if_empty(skip_if_not_of_type(ARRAY)(
        functools.partial(apply_validator_to_array, validator=schema_validator),
    )),
)

items_validator = generate_object_validator(
    schema=common_items_schema,
    non_field_validators=items_non_field_validators,
)

# Now put the items validator onto the schema validator
schema_field_validators.add_property_validator('items', items_validator)
