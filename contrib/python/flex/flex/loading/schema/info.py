from flex.constants import (
    STRING, OBJECT, EMAIL, URI
)
from flex.validation.common import (
    generate_object_validator,
)


info_schema = {
    'required': [
        'title',
    ],
    'properties': {
        'title': {
            'type': STRING,
        },
        'description': {'type': STRING},
        'termsOfService': {'type': STRING},
        'contact': {
            'type': OBJECT,
            'properties': {
                'name': {'type': STRING},
                'email': {'type': STRING, 'format': EMAIL},
                'url': {'type': STRING, 'format': URI},
            },
        },
        'license': {
            'type': OBJECT,
            'properties': {
                'name': {'type': STRING},
                'url': {'type': STRING, 'format': URI},
            },
        },
        'version': {'type': STRING},
    }
}

info_validator = generate_object_validator(
    schema=info_schema,
)
