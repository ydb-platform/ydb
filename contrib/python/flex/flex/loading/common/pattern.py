import re

from flex.exceptions import ValidationError
from flex.constants import (
    STRING,
)
from flex.error_messages import MESSAGES
from flex.validation.common import (
    generate_object_validator,
)
from flex.validation.schema import (
    construct_schema_validators,
)
from flex.decorators import (
    skip_if_empty,
    skip_if_not_of_type,
)


@skip_if_empty
@skip_if_not_of_type(STRING)
def regex_validator(value, **kwargs):
    try:
        re.compile(value)
    except re.error:
        raise ValidationError(
            MESSAGES['pattern']['invalid_regex'].format(value)
        )


pattern_schema = {
    'type': STRING,
}
pattern_validators = construct_schema_validators(pattern_schema, {})
pattern_validators.add_validator('regex', regex_validator)

pattern_validator = generate_object_validator(
    field_validators=pattern_validators,
)
