from six.moves import urllib_parse as urlparse

import jsonpointer

from flex.exceptions import ValidationError
from flex.error_messages import MESSAGES
from flex.constants import (
    STRING,
)
from flex.datastructures import (
    ValidationList,
)
from flex.validation.common import (
    generate_object_validator,
)
from flex.decorators import (
    skip_if_not_of_type,
    skip_if_empty,
)


@skip_if_empty
@skip_if_not_of_type(STRING)
def validate_reference(reference, context, **kwargs):
    try:
        parts = urlparse.urlparse(reference)
        jsonpointer.resolve_pointer(context, parts.fragment)
    except jsonpointer.JsonPointerException:
        raise ValidationError(MESSAGES['reference']['undefined'].format(reference))


ref_schema = {
    'type': STRING,
}

non_field_validators = ValidationList()
non_field_validators.add_validator(validate_reference)

ref_validator = generate_object_validator(
    schema=ref_schema,
    non_field_validators=non_field_validators,
)
