from six.moves import urllib_parse as urlparse

import jsonpointer

from flex.exceptions import (
    ValidationError,
)
from flex.datastructures import (
    ValidationDict,
)
from flex.error_messages import MESSAGES
from flex.constants import (
    OBJECT,
    STRING,
)
from flex.decorators import (
    skip_if_not_of_type,
)
from flex.validation.common import (
    generate_object_validator,
)

reference_object_schema = {
    'type': OBJECT,
    'additionalProperties': False,
    'required': [
        '$ref',
    ],
    'properties': {
        '$ref': {
            'type': STRING,
        },
    },
}


@skip_if_not_of_type(STRING)
def validate_reference_pointer(reference, context, **kwargs):
    parts = urlparse.urlparse(reference)
    if any((parts.scheme, parts.netloc, parts.path, parts.params, parts.query)):
        raise ValidationError(
            MESSAGES['reference']['unsupported'].format(reference),
        )

    try:
        jsonpointer.resolve_pointer(context, parts.fragment)
    except jsonpointer.JsonPointerException:
        raise ValidationError(
            MESSAGES['reference']['undefined'].format(reference),
        )


non_field_validators = ValidationDict()
non_field_validators.add_property_validator('$ref', validate_reference_pointer)

reference_object_validator = generate_object_validator(
    schema=reference_object_schema,
    non_field_validators=non_field_validators,
)
