from flex.constants import (
    STRING,
)
from flex.validation.common import (
    generate_object_validator,
)


swagger_version_schema = {
    'enum': ['2.0'],
    'type': STRING,
}

swagger_version_validator = generate_object_validator(
    schema=swagger_version_schema,
)
