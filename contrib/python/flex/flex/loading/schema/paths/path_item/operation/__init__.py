from flex.datastructures import (
    ValidationDict,
)
from flex.constants import (
    BOOLEAN,
    ARRAY,
    STRING,
    OBJECT,
)
from flex.validation.common import (
    generate_object_validator,
)
from flex.loading.common.mimetypes import (
    mimetype_validator,
)
from flex.loading.common.external_docs import (
    external_docs_validator,
)
from .parameters import (
    parameters_validator,
)
from .responses import (
    responses_validator,
)


operation_schema = {
    'type': OBJECT,
    'properties': {
        'tags': {
            'type': ARRAY,
            'items': {
                'type': STRING,
            }
        },
        'summary': {
            'type': STRING,
        },
        'description': {
            'type': STRING,
        },
        'operationId': {
            'type': STRING,
        },
        'consumes': {
            'type': ARRAY,
            'items': {
                'type': STRING,
            },
        },
        'produces': {
            'type': ARRAY,
            'items': {
                'type': STRING,
            },
        },
        # 'schemes':  # TODO:  Implement this.
        'deprecated': {
            'type': BOOLEAN,
        },
        # 'security':  # TODO:  Implement this.
    },
}


field_validators = ValidationDict()
field_validators.add_property_validator('parameters', parameters_validator)
field_validators.add_property_validator('responses', responses_validator)
field_validators.add_property_validator('externalDocs', external_docs_validator)

non_field_validators = ValidationDict()
non_field_validators.add_property_validator('consumes', mimetype_validator)
non_field_validators.add_property_validator('produces', mimetype_validator)

operation_validator = generate_object_validator(
    schema=operation_schema,
    field_validators=field_validators,
)
