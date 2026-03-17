from flex.exceptions import ValidationError
from flex.error_messages import MESSAGES
from flex.constants import (
    STRING,
    MULTI,
    QUERY,
    FORM_DATA,
    COLLECTION_FORMATS,
)
from flex.validation.common import (
    generate_object_validator,
)
from flex.decorators import (
    pull_keys_from_obj,
    suffix_reserved_words,
    skip_if_any_kwargs_empty,
)


@pull_keys_from_obj('in', 'collectionFormat')
@skip_if_any_kwargs_empty('in', 'collectionFormat')
@suffix_reserved_words
def validate_collection_format_based_on_in_value(in_, collectionFormat, **kwargs):
    if collectionFormat == MULTI:
        if in_ not in (QUERY, FORM_DATA):
            raise ValidationError(MESSAGES['collection_format']['invalid_based_on_in_value'])


collection_format_schema = {
    'type': STRING,
    'enum': COLLECTION_FORMATS,
}


collection_format_validator = generate_object_validator(
    schema=collection_format_schema,
)
