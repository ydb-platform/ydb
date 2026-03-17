from flex.constants import (
    OBJECT,
)
from flex.datastructures import (
    ValidationDict,
)
from flex.validation.common import (
    generate_object_validator,
)
from flex.loading.common.mimetypes import (
    mimetype_validator,
)
from .info import info_validator
from .swagger import swagger_version_validator
from .host import host_validator
from .base_path import base_path_validator
from .schemes import schemes_validator
from .paths import paths_validator


__ALL__ = [
    'info_validator',
    'swagger_schema_validators',
    'host_validator',
    'base_path_validator',
    'schemes_validator',
    'mimetype_validator',
    'paths_validator',
]

swagger_schema = {
    'type': OBJECT,
    'required': [
        'info',
        'paths',
        'swagger',
    ],
}

non_field_validators = ValidationDict()
non_field_validators.add_property_validator('info', info_validator)
non_field_validators.add_property_validator('swagger', swagger_version_validator)
non_field_validators.add_property_validator('host', host_validator)
non_field_validators.add_property_validator('basePath', base_path_validator)
non_field_validators.add_property_validator('schemes', schemes_validator)
non_field_validators.add_property_validator('produces', mimetype_validator)
non_field_validators.add_property_validator('consumes', mimetype_validator)
non_field_validators.add_property_validator('paths', paths_validator)

swagger_schema_validator = generate_object_validator(
    schema=swagger_schema,
    non_field_validators=non_field_validators,
)
