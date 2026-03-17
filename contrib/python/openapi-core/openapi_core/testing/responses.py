"""OpenAPI core testing responses module"""
from openapi_core.validation.response.datatypes import OpenAPIResponse


class MockResponseFactory(object):

    @classmethod
    def create(cls, data, status_code=200, mimetype='application/json'):
        return OpenAPIResponse(
            data=data,
            status_code=status_code,
            mimetype=mimetype,
        )
