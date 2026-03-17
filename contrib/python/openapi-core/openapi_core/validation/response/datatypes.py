"""OpenAPI core validation response datatypes module"""
import attr

from openapi_core.validation.datatypes import BaseValidationResult


@attr.s
class OpenAPIResponse(object):
    """OpenAPI request dataclass.

    Attributes:
        data
            The response body, as string.
        status_code
            The status code as integer.
        mimetype
            Lowercase content type without charset.
    """

    data = attr.ib()
    status_code = attr.ib()

    mimetype = attr.ib()


@attr.s
class ResponseValidationResult(BaseValidationResult):
    data = attr.ib(default=None)
    headers = attr.ib(factory=dict)
