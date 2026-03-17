"""OpenAPI core validation exceptions module"""
import attr

from openapi_core.exceptions import OpenAPIError


class ValidationError(OpenAPIError):
    pass


@attr.s(hash=True)
class InvalidSecurity(ValidationError):

    def __str__(self):
        return "Security not valid for any requirement"
