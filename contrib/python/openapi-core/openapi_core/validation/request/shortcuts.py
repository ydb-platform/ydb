"""OpenAPI core validation request shortcuts module"""
import warnings

from openapi_core.validation.request.validators import RequestValidator


def validate_request(validator, request, failsafe=None):
    if failsafe is None:
        failsafe = ()
    result = validator.validate(request)
    try:
        result.raise_for_errors()
    except failsafe:
        pass
    return result


def validate_parameters(validator, request):
    warnings.warn(
        "validate_parameters shortcut is deprecated, "
        "use validator.validate instead",
        DeprecationWarning,
    )
    result = validator._validate_parameters(request)
    result.raise_for_errors()
    return result


def validate_body(validator, request):
    warnings.warn(
        "validate_body shortcut is deprecated, "
        "use validator.validate instead",
        DeprecationWarning,
    )
    result = validator._validate_body(request)
    result.raise_for_errors()
    return result


def spec_validate_parameters(spec, request, request_factory=None):
    if request_factory is not None:
        request = request_factory(request)

    validator = RequestValidator(spec)
    result = validate_parameters(validator, request)

    return result.parameters


def spec_validate_body(spec, request, request_factory=None):
    if request_factory is not None:
        request = request_factory(request)

    validator = RequestValidator(spec)
    result = validate_body(validator, request)

    return result.body
