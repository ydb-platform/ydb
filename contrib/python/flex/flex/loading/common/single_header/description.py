from flex.constants import (
    STRING,
)
from flex.validation.common import (
    generate_object_validator,
)

description_schema = {
    'type': STRING,
}


description_validator = generate_object_validator(
    schema=description_schema,
)
