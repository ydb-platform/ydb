"""OpenAPI core shortcuts module"""
# backward compatibility
from openapi_core.spec.shortcuts import create_spec
from openapi_core.validation.request.shortcuts import (
    spec_validate_body as validate_body,
    spec_validate_parameters as validate_parameters,
)
from openapi_core.validation.request.validators import RequestValidator
from openapi_core.validation.response.shortcuts import (
    spec_validate_data as validate_data
)
from openapi_core.validation.response.validators import ResponseValidator

__all__ = [
    'create_spec', 'validate_body', 'validate_parameters', 'validate_data',
    'RequestValidator', 'ResponseValidator',
]
