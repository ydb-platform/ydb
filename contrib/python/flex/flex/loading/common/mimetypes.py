import re

from flex.datastructures import (
    ValidationList,
)
from flex.constants import (
    ARRAY,
)
from flex.exceptions import ValidationError
from flex.error_messages import MESSAGES
from flex.validation.common import (
    generate_object_validator,
)
from flex.decorators import (
    skip_if_empty,
    skip_if_not_of_type,
)


# top-level type name / [ tree. ] subtype name [ +suffix ] [ ; parameters ]
MIMETYPE_PATTERN = (
    '^'
    # https://www.iana.org/assignments/media-types/media-types.xhtml
    '(application|audio|example|font|image|message|model|multipart|text|video)'  # media type
    '/'
    '(vnd(\.[-a-zA-Z0-9]+)*\.)?'  # vendor tree
    '([-a-zA-Z0-9]+)'  # media subtype
    # https://www.iana.org/assignments/media-type-structured-suffix/media-type-structured-suffix.xml
    '(\+(xml|json|ber|cbor|der|fastinfoset|wbxml|zip|tlv|json-seq|sqlite3|jwt|gzip))?'
    '((; ?[-a-zA-Z0-9]+=(([-\.a-zA-Z0-9]+)|(("|\')[-\.a-zA-Z0-9]+("|\'))))+)?'  # parameters
    '$'
)


@skip_if_empty
@skip_if_not_of_type(ARRAY)
def validate_mimetype(values, **kwargs):
    for value in values:
        if not re.match(MIMETYPE_PATTERN, value):
            raise ValidationError(
                MESSAGES['mimetype']['invalid'].format(value),
            )


mimetype_schema = {
    'type': ARRAY,
}

non_field_validators = ValidationList()
non_field_validators.add_validator(validate_mimetype)

mimetype_validator = generate_object_validator(
    schema=mimetype_schema,
    non_field_validators=non_field_validators,
)
