from flex.constants import (
    OBJECT,
    URI,
    STRING,
)
from flex.validation.common import (
    generate_object_validator,
)


external_docs_schema = {
    'type': OBJECT,
    'required': [
        'url',
    ],
    'properties': {
        'description': {
            'type': STRING,
        },
        'url': {
            'type': STRING,
            'format': URI,
        },
    },
}
external_docs_validator = generate_object_validator(
    schema=external_docs_schema,
)
