from six.moves import urllib_parse as urlparse

from flex.datastructures import (
    ValidationList,
)
from flex.constants import (
    STRING,
)
from flex.exceptions import ValidationError
from flex.error_messages import MESSAGES
from flex.validation.common import (
    generate_object_validator,
    generate_type_validator,
)
from flex.decorators import (
    skip_if_empty,
    skip_if_not_of_type,
)


string_type_validator = generate_type_validator(STRING)


@skip_if_empty
@skip_if_not_of_type(STRING)
def base_path_validator(value, **kwargs):
    if not value.startswith('/'):
        raise ValidationError(MESSAGES['path']['must_start_with_slash'])
    parts = urlparse.urlparse(value)
    if value != parts.path:
        raise ValidationError(MESSAGES['path']['invalid'])


base_path_schema = {
    'type': STRING,
}

non_field_validators = ValidationList()
non_field_validators.add_validator(base_path_validator)

base_path_validator = generate_object_validator(
    schema=base_path_schema,
    non_field_validators=non_field_validators,
)
