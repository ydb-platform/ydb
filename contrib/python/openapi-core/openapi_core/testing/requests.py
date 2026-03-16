"""OpenAPI core testing requests module"""
from six.moves.urllib.parse import urljoin
from werkzeug.datastructures import ImmutableMultiDict

from openapi_core.validation.request.datatypes import (
    RequestParameters, OpenAPIRequest,
)


class MockRequestFactory(object):

    @classmethod
    def create(
            cls, host_url, method, path, path_pattern=None, args=None,
            view_args=None, headers=None, cookies=None, data=None,
            mimetype='application/json'):
        parameters = RequestParameters(
            path=view_args or {},
            query=ImmutableMultiDict(args or []),
            header=headers or {},
            cookie=cookies or {},
        )
        path_pattern = path_pattern or path
        method = method.lower()
        body = data or ''
        full_url_pattern = urljoin(host_url, path_pattern)
        return OpenAPIRequest(
            full_url_pattern=full_url_pattern,
            method=method,
            parameters=parameters,
            body=body,
            mimetype=mimetype,
        )
