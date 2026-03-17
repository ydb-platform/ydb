from flex.constants import (
    STRING,
)
from flex.validation.common import (
    generate_object_validator,
)
from flex.validation.schema import (
    construct_schema_validators,
)


title_schema = {
    'type': STRING,
}
title_validators = construct_schema_validators(title_schema, {})

title_validator = generate_object_validator(
    field_validators=title_validators,
)
