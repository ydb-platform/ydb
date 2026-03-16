from flex.datastructures import (
    ValidationDict,
)
from flex.error_messages import (
    MESSAGES,
)
from flex.constants import (
    OBJECT,
    ARRAY,
    EMPTY,
)
from flex.exceptions import ValidationError
from flex.utils import (
    pluralize,
)
from flex.decorators import (
    pull_keys_from_obj,
    suffix_reserved_words,
)
from flex.loading.common import (
    field_validators as common_field_validators,
    non_field_validators as common_non_field_validators,
    type_validators as common_type_validators,
)
from flex.loading.common.default import (
    validate_default_is_of_one_of_declared_types,
)
from flex.validation.common import (
    generate_object_validator,
)
from flex.loading.common.format import (
    format_validator,
)
from .description import (
    description_validator,
)
from .type import (
    type_validator,
)
from .collection_format import (
    collection_format_validator,
)


single_header_schema = {
    'type': OBJECT,
    'required': [
        'type',
    ]
}

single_header_field_validators = ValidationDict()
single_header_field_validators.update(common_field_validators)
single_header_field_validators.add_property_validator('description', description_validator)
single_header_field_validators.add_property_validator('type', type_validator)
single_header_field_validators.add_property_validator('format', format_validator)
single_header_field_validators.add_property_validator(
    'collectionFormat', collection_format_validator,
)


@pull_keys_from_obj('type', 'items')
@suffix_reserved_words
def validate_items_required_if_type_arraw(type_, items, **kwargs):
    types = pluralize(type_)
    if ARRAY in types and items is EMPTY:
        raise ValidationError(MESSAGES['required']['required'])


single_header_non_field_validators = ValidationDict()
single_header_non_field_validators.update(common_non_field_validators)
single_header_non_field_validators.update(common_type_validators)
single_header_non_field_validators.add_validator(
    'default', validate_default_is_of_one_of_declared_types,
)
single_header_non_field_validators.add_validator(
    'items', validate_items_required_if_type_arraw,
)

single_header_validator = generate_object_validator(
    schema=single_header_schema,
    field_validators=single_header_field_validators,
    non_field_validators=single_header_non_field_validators,
)
