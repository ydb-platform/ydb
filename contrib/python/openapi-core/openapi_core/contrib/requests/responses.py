"""OpenAPI core contrib requests responses module"""
from openapi_core.validation.response.datatypes import OpenAPIResponse


class RequestsOpenAPIResponseFactory(object):

    @classmethod
    def create(cls, response):
        mimetype = response.headers.get('Content-Type')
        return OpenAPIResponse(
            data=response.content,
            status_code=response.status_code,
            mimetype=mimetype,
        )
