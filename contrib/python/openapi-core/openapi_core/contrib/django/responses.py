"""OpenAPI core contrib django responses module"""
from openapi_core.validation.response.datatypes import OpenAPIResponse


class DjangoOpenAPIResponseFactory(object):

    @classmethod
    def create(cls, response):
        mimetype = response["Content-Type"]
        return OpenAPIResponse(
            data=response.content,
            status_code=response.status_code,
            mimetype=mimetype,
        )
