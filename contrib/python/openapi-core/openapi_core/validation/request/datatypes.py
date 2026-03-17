"""OpenAPI core validation request datatypes module"""
import attr
from werkzeug.datastructures import ImmutableMultiDict, Headers

from openapi_core.validation.datatypes import BaseValidationResult


@attr.s
class RequestParameters(object):
    """OpenAPI request parameters dataclass.

    Attributes:
        query
            Query string parameters as MultiDict. Must support getlist method.
        header
            Request headers as Headers.
        cookie
            Request cookies as dict.
        path
            Path parameters as dict. Gets resolved against spec if empty.
    """
    query = attr.ib(factory=ImmutableMultiDict)
    header = attr.ib(factory=Headers, converter=Headers)
    cookie = attr.ib(factory=dict)
    path = attr.ib(factory=dict)

    def __getitem__(self, location):
        return getattr(self, location)


@attr.s
class OpenAPIRequest(object):
    """OpenAPI request dataclass.

    Attributes:
        full_url_pattern
            The matched url with scheme, host and path pattern.
            For example:
            https://localhost:8000/api/v1/pets
            https://localhost:8000/api/v1/pets/{pet_id}
        method
            The request method, as lowercase string.
        parameters
            A RequestParameters object.
        body
            The request body, as string.
        mimetype
            Like content type, but without parameters (eg, without charset,
            type etc.) and always lowercase.
            For example if the content type is "text/HTML; charset=utf-8"
            the mimetype would be "text/html".
    """

    full_url_pattern = attr.ib()
    method = attr.ib()
    body = attr.ib()
    mimetype = attr.ib()
    parameters = attr.ib(factory=RequestParameters)


@attr.s
class RequestValidationResult(BaseValidationResult):
    body = attr.ib(default=None)
    parameters = attr.ib(factory=RequestParameters)
    security = attr.ib(default=None)
    server = attr.ib(default=None)
    path = attr.ib(default=None)
    operation = attr.ib(default=None)
