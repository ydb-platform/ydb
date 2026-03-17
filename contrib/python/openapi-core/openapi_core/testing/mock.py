"""OpenAPI core testing mock module"""
# backward compatibility
from openapi_core.testing.requests import MockRequestFactory
from openapi_core.testing.responses import MockResponseFactory

__all__ = ['MockRequestFactory', 'MockResponseFactory']
