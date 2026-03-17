from flex.datastructures import (
    ValidationDict,
)

from .multiple_of import (
    multiple_of_validator,
    validate_type_for_multiple_of,
)
from .maximum import (
    maximum_validator,
    exclusive_maximum_validator,
    validate_maximum_is_gte_minimum,
    validate_maximum_required_if_exclusive_maximum_set,
    validate_type_for_maximum,
)
from .minimum import (
    minimum_validator,
    exclusive_minimum_validator,
    validate_minimum_required_if_exclusive_minimum_set,
    validate_type_for_minimum,
)
from .min_length import (
    min_length_validator,
    validate_type_for_min_length,
)
from .max_length import (
    max_length_validator,
    validate_max_length_greater_than_or_equal_to_min_length,
    validate_type_for_max_length,
)
from .pattern import (
    pattern_validator,
)
from .min_items import (
    min_items_validator,
    validate_type_for_min_items,
)
from .max_items import (
    max_items_validator,
    validate_max_items_greater_than_or_equal_to_min_items,
    validate_type_for_max_items,
)
from .unique_items import (
    unique_items_validator,
    validate_type_for_unique_items,
)
from .enum import (
    enum_validator,
)


field_validators = ValidationDict()

field_validators.add_property_validator('multipleOf', multiple_of_validator)
field_validators.add_property_validator('minimum', minimum_validator)
field_validators.add_property_validator('maximum', maximum_validator)
field_validators.add_property_validator('exclusiveMinimum', exclusive_minimum_validator)
field_validators.add_property_validator('exclusiveMaximum', exclusive_maximum_validator)
field_validators.add_property_validator('minLength', min_length_validator)
field_validators.add_property_validator('maxLength', max_length_validator)
field_validators.add_property_validator('pattern', pattern_validator)
field_validators.add_property_validator('minItems', min_items_validator)
field_validators.add_property_validator('maxItems', max_items_validator)
field_validators.add_property_validator('uniqueItems', unique_items_validator)
field_validators.add_property_validator('enum', enum_validator)


non_field_validators = ValidationDict()

non_field_validators.add_validator(
    'maximum', validate_maximum_is_gte_minimum,
)
non_field_validators.add_validator(
    'maximum', validate_maximum_required_if_exclusive_maximum_set,
)
non_field_validators.add_validator(
    'minimum', validate_minimum_required_if_exclusive_minimum_set,
)
non_field_validators.add_validator(
    'maxLength', validate_max_length_greater_than_or_equal_to_min_length,
)
non_field_validators.add_validator(
    'maxItems', validate_max_items_greater_than_or_equal_to_min_items,
)


type_validators = ValidationDict()
type_validators.add_validator(
    'type', validate_type_for_multiple_of,
)
type_validators.add_validator(
    'type', validate_type_for_maximum,
)
type_validators.add_validator(
    'type', validate_type_for_minimum,
)
type_validators.add_validator(
    'type', validate_type_for_max_length,
)
type_validators.add_validator(
    'type', validate_type_for_min_length,
)
type_validators.add_validator(
    'type', validate_type_for_max_items,
)
type_validators.add_validator(
    'type', validate_type_for_min_items,
)
type_validators.add_validator(
    'type', validate_type_for_unique_items,
)
