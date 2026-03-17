from flex.constants import (
    STRING,
)
from flex.validation.common import (
    generate_object_validator,
)


name_schema = {
    'type': STRING,
}


name_validator = generate_object_validator(
    schema=name_schema,
)
