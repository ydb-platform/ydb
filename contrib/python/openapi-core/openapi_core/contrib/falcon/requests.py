"""OpenAPI core contrib falcon responses module"""
from json import dumps

from werkzeug.datastructures import ImmutableMultiDict

from openapi_core.contrib.falcon.compat import get_request_media
from openapi_core.validation.request.datatypes import (
    OpenAPIRequest, RequestParameters,
)


class FalconOpenAPIRequestFactory:

    @classmethod
    def create(cls, request, default_when_empty={}):
        """
        Create OpenAPIRequest from falcon Request and route params.
        """
        default = default_when_empty
        method = request.method.lower()

        # gets deduced by path finder against spec
        path = {}

        media = get_request_media(request, default=default)
        # Support falcon-jsonify.
        body = (
            dumps(getattr(request, "json", media))
        )
        mimetype = request.options.default_media_type
        if request.content_type:
            mimetype = request.content_type.partition(";")[0]

        query = ImmutableMultiDict(request.params.items())
        parameters = RequestParameters(
            query=query,
            header=request.headers,
            cookie=request.cookies,
            path=path,
        )
        url_pattern = request.prefix + request.path
        return OpenAPIRequest(
            full_url_pattern=url_pattern,
            method=method,
            parameters=parameters,
            body=body,
            mimetype=mimetype,
        )
